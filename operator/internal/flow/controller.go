package flow

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Reconciler observes MxlFlow resources and prunes status.locations
// entries naming a node the cluster no longer has.
//
// The agent owns the entry for its own node: it publishes on
// fanotify create and demotes on delete. That contract holds only
// while the agent runs. A node removed from the cluster takes its
// agent with it, so the entry it published survives indefinitely,
// and on a cluster that recycles capacity the list grows without
// bound. Readers are kept off the corpses only by the origin Lease
// going stale, which leaves an entry no consumer can tell apart from
// a node that is merely quiet.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Lease gates the Origin locations this reconciler trusts when
	// deciding whether a flow still has a producer. Nil treats any
	// Origin location as live, which keeps the collector off flows it
	// cannot judge.
	Lease LeaseChecker

	// GracePeriod is how long a flow has to stay collectable before
	// it is deleted. Zero means defaultGracePeriod.
	GracePeriod time.Duration

	mu sync.Mutex
	// firstCollectable records when each flow was first observed with
	// no producer and no mirror. Held in memory rather than on the
	// object so the collector adds no field to the API and no write to
	// a flow it may yet leave alone; an operator restart costs one
	// extra grace period before the flow is collected, which a garbage
	// collector can afford.
	firstCollectable map[string]time.Time
}

// LeaseChecker reports whether the agent on nodeName still holds a
// renewed origin Lease for flowID. Matches the receiver package's
// interface of the same name so both consume one leasecheck.Checker.
type LeaseChecker interface {
	IsFresh(ctx context.Context, flowID, nodeName string) (fresh bool, deadline time.Time, err error)
}

// defaultGracePeriod is how long a flow must look collectable before
// it is deleted. It has to outlast a producer rolling over: between
// the old pod releasing its flow and the new one publishing again the
// flow has no Origin, and deleting it in that window would take the
// definition away from a consumer that is about to need it.
const defaultGracePeriod = 5 * time.Minute

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile drops every status.locations entry whose node is absent
// from the API.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflow", req.NamespacedName)

	var obj mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	departed, err := r.departedNodes(ctx, obj.Status.Locations)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(departed) == 0 {
		l.V(1).Info("observed MxlFlow", "id", obj.Spec.ID, "locations", len(obj.Status.Locations))
		return r.collect(ctx, &obj)
	}

	// An agent rewrites its own entry on its own schedule, so this
	// read-modify-write races it. Retrying on conflict re-reads and
	// re-applies instead of clobbering an entry published between
	// the Get and the Update.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var live mxlv1alpha1.MxlFlow
		if err := r.Get(ctx, req.NamespacedName, &live); err != nil {
			return err
		}
		kept := make([]mxlv1alpha1.MxlFlowLocation, 0, len(live.Status.Locations))
		for _, loc := range live.Status.Locations {
			if _, gone := departed[loc.NodeName]; !gone {
				kept = append(kept, loc)
			}
		}
		if len(kept) == len(live.Status.Locations) {
			return nil
		}
		live.Status.Locations = kept
		return r.Status().Update(ctx, &live)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("prune departed locations: %w", err)
	}

	l.Info("pruned MxlFlow locations for departed nodes",
		"id", obj.Spec.ID, "nodes", departedNames(departed))
	return ctrl.Result{}, nil
}

// collect deletes an MxlFlow that no longer describes anything: no
// node holds a copy the control plane can route a consumer to, and no
// mirror needs its definition.
//
// Nothing else removes these. The agent demotes its own location to
// Stale when the directory goes away but leaves the object, and the
// prune above only drops entries for nodes that left the cluster, so
// on a cluster whose producers come and go the flow list grows
// without bound. Every one of those entries is a name resolveSourceNode
// walks and an OriginFresh condition the operator keeps evaluating.
//
// Deleting is safe because the object is derived state: an agent that
// still has the flow on disk republishes it from flow_def.json on its
// next pass, so a flow collected while its producer was mid-restart
// comes back with the same name and spec.
func (r *Reconciler) collect(ctx context.Context, flow *mxlv1alpha1.MxlFlow) (ctrl.Result, error) {
	live, deadline, err := r.hasLiveCopy(ctx, flow)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !live {
		var mirrors mxlv1alpha1.MxlFlowMirrorList
		if err := r.List(ctx, &mirrors); err != nil {
			return ctrl.Result{}, fmt.Errorf("list MxlFlowMirrors: %w", err)
		}
		for i := range mirrors.Items {
			if mirrors.Items[i].Spec.FlowID == flow.Spec.ID {
				live = true
				break
			}
		}
	}

	if live {
		r.forget(flow.Name)
		// A Lease falling out of its window raises no event, so a flow
		// held alive only by one has to be looked at again by the
		// clock. Mirrors and their deletions arrive on the watch.
		if !deadline.IsZero() {
			if wait := time.Until(deadline); wait > 0 {
				return ctrl.Result{RequeueAfter: wait}, nil
			}
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, nil
	}

	grace := r.GracePeriod
	if grace <= 0 {
		grace = defaultGracePeriod
	}
	if since := time.Since(r.markCollectable(flow.Name)); since < grace {
		return ctrl.Result{RequeueAfter: grace - since}, nil
	}

	// Preconditioned on the version the decision was made against: an
	// agent republishing the flow between the read above and here
	// makes the delete fail rather than drop a flow that just came
	// back. The conflict returns the object to the queue.
	err = r.Delete(ctx, flow, client.Preconditions{
		UID:             &flow.UID,
		ResourceVersion: &flow.ResourceVersion,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(flow.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("delete MxlFlow %s: %w", flow.Name, err)
	}
	r.forget(flow.Name)
	log.FromContext(ctx).Info("collected MxlFlow with no copy and no mirror",
		"id", flow.Spec.ID)
	return ctrl.Result{}, nil
}

// hasLiveCopy reports whether any node still holds a copy of the flow
// a consumer could be routed to: an Origin whose Lease is being
// renewed, or a materialized mirror the target gateway has published.
// A Stale location is neither. The returned deadline is the moment the
// sustaining Lease lapses, and is zero when the answer does not rest
// on one.
func (r *Reconciler) hasLiveCopy(ctx context.Context, flow *mxlv1alpha1.MxlFlow) (bool, time.Time, error) {
	var soonest time.Time
	live := false
	for _, loc := range flow.Status.Locations {
		switch loc.Phase {
		case mxlv1alpha1.MxlFlowLocationReady, mxlv1alpha1.MxlFlowLocationMirroring:
			return true, time.Time{}, nil
		case mxlv1alpha1.MxlFlowLocationOrigin:
			if r.Lease == nil {
				return true, time.Time{}, nil
			}
			fresh, deadline, err := r.Lease.IsFresh(ctx, flow.Spec.ID, loc.NodeName)
			if err != nil {
				return false, time.Time{}, fmt.Errorf("check origin lease for %s on %s: %w",
					flow.Spec.ID, loc.NodeName, err)
			}
			if !fresh {
				continue
			}
			live = true
			if soonest.IsZero() || deadline.After(soonest) {
				soonest = deadline
			}
		}
	}
	return live, soonest, nil
}

// markCollectable records the first time name was seen collectable and
// returns that instant, so the grace period measures how long the flow
// has been unreferenced rather than how old it is.
func (r *Reconciler) markCollectable(name string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.firstCollectable == nil {
		r.firstCollectable = make(map[string]time.Time)
	}
	if at, ok := r.firstCollectable[name]; ok {
		return at
	}
	now := time.Now()
	r.firstCollectable[name] = now
	return now
}

func (r *Reconciler) forget(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.firstCollectable, name)
}

// departedNodes returns the node names in locs that have no Node
// object. The reads are served by the cache the Node watch already
// requires; the operator is a single Deployment, not a per-node
// DaemonSet, so that cache exists once cluster-wide.
func (r *Reconciler) departedNodes(ctx context.Context, locs []mxlv1alpha1.MxlFlowLocation) (map[string]struct{}, error) {
	departed := map[string]struct{}{}
	seen := map[string]struct{}{}
	for _, loc := range locs {
		if loc.NodeName == "" {
			continue
		}
		if _, dup := seen[loc.NodeName]; dup {
			continue
		}
		seen[loc.NodeName] = struct{}{}

		var node corev1.Node
		err := r.Get(ctx, types.NamespacedName{Name: loc.NodeName}, &node)
		switch {
		case err == nil:
		case apierrors.IsNotFound(err):
			departed[loc.NodeName] = struct{}{}
		default:
			return nil, fmt.Errorf("look up node %s: %w", loc.NodeName, err)
		}
	}
	return departed, nil
}

func departedNames(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlow{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.nodeToFlows),
			builder.WithPredicates(nodeDeletedOnly()),
		).
		Watches(
			&mxlv1alpha1.MxlFlowMirror{},
			handler.EnqueueRequestsFromMapFunc(mirrorToFlow),
		).
		Named("mxlflow").
		Complete(r)
}

// nodeToFlows enqueues every MxlFlow carrying a location for the
// departed node. A Node deletion raises no event on the flows that
// reference it, so without this the stranded entry survives until
// something else happens to touch the same flow.
func (r *Reconciler) nodeToFlows(ctx context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	var flows mxlv1alpha1.MxlFlowList
	if err := r.List(ctx, &flows); err != nil {
		log.FromContext(ctx).Error(err, "list MxlFlows for departed node", "node", node.Name)
		return nil
	}
	var out []reconcile.Request
	for i := range flows.Items {
		for _, loc := range flows.Items[i].Status.Locations {
			if loc.NodeName == node.Name {
				out = append(out, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: flows.Items[i].Name},
				})
				break
			}
		}
	}
	return out
}

// mirrorToFlow enqueues the MxlFlow a mirror references. The
// collector treats a mirror as a reason to keep the flow, so the
// mirror going away is the event that can make the flow collectable,
// and it raises nothing on the flow itself.
func mirrorToFlow(_ context.Context, obj client.Object) []reconcile.Request {
	mirror, ok := obj.(*mxlv1alpha1.MxlFlowMirror)
	if !ok || mirror.Spec.FlowID == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Name: mirror.Spec.FlowID},
	}}
}

// nodeDeletedOnly limits the Node watch to deletions. Node objects
// are among the busiest in a cluster -- heartbeats, conditions,
// allocatable churn -- and only a departure can strand a location,
// so reconciling every flow on the rest would be noise.
func nodeDeletedOnly() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
	}
}
