package flow

import (
	"context"
	"fmt"

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
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows/status,verbs=get;update;patch
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
		return ctrl.Result{}, nil
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
