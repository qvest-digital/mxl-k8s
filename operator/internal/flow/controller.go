// Package flow hosts the operator's MxlFlow lifecycle controller.
//
// An MxlFlow is derived state. Agents publish one when a flow
// directory appears on their node and each records its own entry in
// status.locations; nothing in the cluster authors a flow by hand. The
// controller here owns what no single agent can see: which node holds
// the authoritative copy, whether any node still holds one at all, and
// when a flow that describes nothing is deleted.
package flow

import (
	"context"
	"fmt"
	"sort"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
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

// Reconciler keeps one MxlFlow's status honest and collects the flow
// once nothing holds a copy of it.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Recorder publishes the origin-move and collection events. Nil
	// records nothing, which keeps the reconciler usable in tests that
	// do not wire a manager.
	Recorder record.EventRecorder

	// Lease gates the Origin locations this reconciler trusts. Nil
	// treats any Origin location as live, which keeps the collector
	// off flows it cannot judge.
	Lease LeaseChecker

	// GracePeriod is how long a flow has to read Live=False before it
	// is deleted. Zero means DefaultGracePeriod.
	GracePeriod time.Duration
}

// LeaseChecker reports whether the agent on nodeName still holds a
// renewed origin Lease for flowID. Matches the receiver package's
// interface of the same name so both consume one leasecheck.Checker.
type LeaseChecker interface {
	IsFresh(ctx context.Context, flowID, nodeName string) (fresh bool, deadline time.Time, err error)
}

// DefaultGracePeriod is how long a flow must read Live=False before it
// is deleted. It has to outlast a producer rolling over: between the
// old pod releasing its flow and the new one publishing again the flow
// has no Origin, and deleting it in that window would take the
// definition away from a consumer that is about to need it.
const DefaultGracePeriod = 5 * time.Minute

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile brings one MxlFlow's status in line with what the cluster
// actually holds, then collects the flow if that is nothing.
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

	// Liveness is judged against the locations that survive the prune,
	// so a flow held up only by an entry for a node that left the
	// cluster is not counted live for one more pass.
	v, err := r.judge(ctx, &obj, keepPresent(obj.Status.Locations, departed))
	if err != nil {
		return ctrl.Result{}, err
	}

	written, err := r.writeStatus(ctx, &obj, departed, v)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if len(departed) > 0 {
		l.Info("pruned MxlFlow locations for departed nodes",
			"id", obj.Spec.ID, "nodes", sortedNames(departed))
		r.event(written, corev1.EventTypeWarning, ReasonLocationsPruned,
			"Dropped locations for departed nodes: %v", sortedNames(departed))
	}
	l.V(1).Info("judged MxlFlow", "id", obj.Spec.ID, "live", v.Live, "reason", v.Reason)

	return r.collect(ctx, written, v)
}

// Event reasons recorded on an MxlFlow.
const (
	// ReasonOriginMoved marks the authoritative copy landing on a
	// different node.
	ReasonOriginMoved = "OriginMoved"
	// ReasonOriginLost marks the authoritative copy leaving the last
	// node that held it with no other node claiming it. A producer
	// restart begins exactly like a permanent loss, and once the agent
	// logs have rotated the flow's own status is the only place the
	// difference is still visible.
	ReasonOriginLost = "OriginLost"
	// ReasonFlowCollected marks the flow being deleted because nothing
	// holds a copy any more. Without it a flow simply disappears, and
	// a consumer that was waiting on it has no record of why.
	ReasonFlowCollected = "FlowCollected"
	// ReasonLocationsPruned marks locations dropped because their node
	// left the cluster. This is the node-lifecycle half: an entry can
	// vanish without the flow changing in any other visible way.
	ReasonLocationsPruned = "LocationsPruned"
)

// verdict is what one pass concluded about a flow: where its
// authoritative copy is, whether anything holds a copy at all, and
// when that answer needs looking at again.
type verdict struct {
	// Live is false when no node holds a copy a consumer could be
	// routed to.
	Live bool
	// Reason and Message explain Live on the object.
	Reason  string
	Message string
	// Deadline is when Live could change with no event firing, which
	// is only ever a Lease lapsing. Zero when nothing is on a timer.
	Deadline time.Time

	// OriginClaimed is false when no location claims Origin at all,
	// which is not the same as a claim whose Lease has expired. The
	// operator publishes OriginFresh only when it has that opinion:
	// stamping False on every flow whose producer has simply not
	// published yet would drown the genuine lease-expired signal.
	OriginClaimed bool
	// OriginFresh is whether some claimed Origin holds a renewed
	// Lease. Only meaningful when OriginClaimed.
	OriginFresh bool
}

// judge decides whether the flow still describes a copy a consumer can
// be routed to.
//
// Two things count, and a Ready location is neither of them. An Origin
// whose Lease is being renewed is a producer the cluster can still
// reach. An MxlFlowMirror referencing the flow is a copy something
// asked for, and the mirror controller collects a mirror nothing asks
// for, so a mirror that survives its own pass is a live claim rather
// than a leftover.
//
// Counting Ready locations is the gap this closes. A Ready location is
// written by the node holding a mirror's target copy and cleared only
// when that node's agent notices the directory go, which happens a
// domain sweep after the mirror is torn down and never at all while
// the mirror survives. So the flow kept the mirror's copy alive and
// the mirror kept the flow's definition alive, each citing the other,
// and neither was ever collected. Asking the mirror directly breaks
// the cycle: a mirror is judged on its own claim and its own source,
// and the flow follows.
func (r *Reconciler) judge(ctx context.Context, flow *mxlv1alpha1.MxlFlow, locs []mxlv1alpha1.MxlFlowLocation) (verdict, error) {
	var v verdict

	for _, loc := range locs {
		if loc.Phase != mxlv1alpha1.MxlFlowLocationOrigin {
			continue
		}
		v.OriginClaimed = true
		if r.Lease == nil {
			return verdict{
				Live:          true,
				Reason:        mxlv1alpha1.ReasonOriginLive,
				Message:       fmt.Sprintf("origin on %s", loc.NodeName),
				OriginClaimed: true,
				OriginFresh:   true,
			}, nil
		}
		fresh, deadline, err := r.Lease.IsFresh(ctx, flow.Spec.ID, loc.NodeName)
		if err != nil {
			return verdict{}, fmt.Errorf("check origin lease for %s on %s: %w",
				flow.Spec.ID, loc.NodeName, err)
		}
		if !fresh {
			continue
		}
		v.Live = true
		v.OriginFresh = true
		v.Reason = mxlv1alpha1.ReasonOriginLive
		v.Message = fmt.Sprintf("origin on %s holds a renewed lease", loc.NodeName)
		if deadline.After(v.Deadline) {
			v.Deadline = deadline
		}
	}
	if v.Live {
		return v, nil
	}

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors); err != nil {
		return verdict{}, fmt.Errorf("list MxlFlowMirrors: %w", err)
	}
	for i := range mirrors.Items {
		if mirrors.Items[i].Spec.FlowID != flow.Spec.ID {
			continue
		}
		v.Live = true
		v.Reason = mxlv1alpha1.ReasonMirrored
		v.Message = fmt.Sprintf("mirror %s/%s references this flow",
			mirrors.Items[i].Namespace, mirrors.Items[i].Name)
		return v, nil
	}

	v.Reason = mxlv1alpha1.ReasonNoLiveCopy
	v.Message = "no origin holds a renewed lease and no mirror references this flow"
	return v, nil
}

// writeStatus applies the prune, the origin record and the Live
// condition in one update, and returns the object as it now stands.
//
// One write rather than three. An agent rewrites its own location
// entry on its own schedule, so every read-modify-write here races
// several of them; doing it once means one conflict window instead of
// three, and leaves the status internally consistent at every
// resourceVersion a consumer could observe.
func (r *Reconciler) writeStatus(ctx context.Context, flow *mxlv1alpha1.MxlFlow, departed map[string]struct{}, v verdict) (*mxlv1alpha1.MxlFlow, error) {
	key := types.NamespacedName{Name: flow.Name}
	var live mxlv1alpha1.MxlFlow
	var moved, lost bool
	var previous string

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		moved, lost, previous = false, false, ""
		if err := r.Get(ctx, key, &live); err != nil {
			return err
		}
		before := live.Status.DeepCopy()

		live.Status.Locations = keepPresent(live.Status.Locations, departed)

		// Re-derived from the object being written rather than from
		// the read the verdict was taken against: an agent that
		// published an Origin in between must not be recorded absent.
		origin := originOf(live.Status.Locations)
		if origin != live.Status.OriginNode {
			previous = live.Status.OriginNode
			if previous != "" {
				live.Status.PreviousOriginNode = previous
			}
			live.Status.OriginNode = origin
			now := metav1.Now()
			live.Status.OriginChangedAt = &now
			moved = origin != ""
			lost = origin == ""
		}

		meta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
			Type:               mxlv1alpha1.ConditionTypeLive,
			Status:             conditionStatus(v.Live),
			Reason:             v.Reason,
			Message:            v.Message,
			ObservedGeneration: live.Generation,
		})
		// OriginFresh is left untouched on a flow that claims no
		// Origin at all: that is a producer the cluster has not seen
		// yet, and writing False for it would be indistinguishable
		// from a producer it has lost.
		if v.OriginClaimed {
			reason, message := mxlv1alpha1.ReasonRecovered, "origin lease is within its renewal window"
			if !v.OriginFresh {
				reason, message = mxlv1alpha1.ReasonLeaseExpired, "no origin location holds a renewed lease"
			}
			meta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
				Type:               mxlv1alpha1.ConditionTypeOriginFresh,
				Status:             conditionStatus(v.OriginFresh),
				Reason:             reason,
				Message:            message,
				ObservedGeneration: live.Generation,
			})
		}

		if equalIgnoringTimestamps(before, &live.Status) {
			return nil
		}
		return r.Status().Update(ctx, &live)
	})
	if err != nil {
		return nil, fmt.Errorf("update MxlFlow %s status: %w", flow.Name, err)
	}

	switch {
	case moved && previous == "":
		r.event(&live, corev1.EventTypeNormal, ReasonOriginMoved,
			"Origin established on %s", live.Status.OriginNode)
	case moved:
		r.event(&live, corev1.EventTypeNormal, ReasonOriginMoved,
			"Origin moved from %s to %s; mirrors sourcing from %s are repointed by the mirror controller",
			previous, live.Status.OriginNode, previous)
	case lost && previous != "":
		r.event(&live, corev1.EventTypeWarning, ReasonOriginLost,
			"Origin left %s and no node claims it", previous)
	}
	return &live, nil
}

// collect deletes an MxlFlow that has read Live=False for longer than
// the grace period.
//
// Nothing else removes these. The agent demotes its own location when
// the directory goes away but leaves the object, and the prune only
// drops entries for nodes that left the cluster, so on a cluster whose
// producers come and go the flow list grows without bound. Every one
// of those entries is a name the origin resolver walks and a condition
// the operator keeps evaluating.
//
// Deleting is safe because the object is derived state: an agent that
// still has the flow on disk republishes it from flow_def.json on its
// next pass, so a flow collected while its producer was mid-restart
// comes back with the same name and spec.
func (r *Reconciler) collect(ctx context.Context, flow *mxlv1alpha1.MxlFlow, v verdict) (ctrl.Result, error) {
	if v.Live {
		// A Lease falling out of its window raises no event, so a flow
		// held alive only by one has to be looked at again by the
		// clock. Mirrors, Leases and their deletions arrive on a watch.
		return requeueAt(v.Deadline), nil
	}

	grace := r.GracePeriod
	if grace <= 0 {
		grace = DefaultGracePeriod
	}
	since := notLiveSince(flow)
	if wait := grace - time.Since(since); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	// Preconditioned on the version the decision was made against: an
	// agent republishing the flow between the read above and here
	// makes the delete fail rather than drop a flow that just came
	// back. The conflict returns the object to the queue.
	err := r.Delete(ctx, flow, client.Preconditions{
		UID:             &flow.UID,
		ResourceVersion: &flow.ResourceVersion,
	})
	switch {
	case err == nil:
	case apierrors.IsNotFound(err):
		return ctrl.Result{}, nil
	case apierrors.IsConflict(err):
		return ctrl.Result{Requeue: true}, nil
	default:
		return ctrl.Result{}, fmt.Errorf("delete MxlFlow %s: %w", flow.Name, err)
	}

	log.FromContext(ctx).Info("collected MxlFlow with no live copy",
		"id", flow.Spec.ID, "notLiveFor", time.Since(since).Truncate(time.Second))
	// Recorded against the object being deleted, so the event outlives
	// it for the event TTL. A flow that simply vanishes leaves whoever
	// was waiting on it nothing to read.
	r.event(flow, corev1.EventTypeNormal, ReasonFlowCollected, "Collected: %s", v.Message)
	return ctrl.Result{}, nil
}

// notLiveSince is when the flow last stopped being live, taken from
// the Live condition's lastTransitionTime. A flow with no condition
// yet -- one this operator has never written -- counts as having just
// turned, so an upgrade gives every flow a full grace period rather
// than collecting the collectable ones on sight.
func notLiveSince(flow *mxlv1alpha1.MxlFlow) time.Time {
	c := meta.FindStatusCondition(flow.Status.Conditions, mxlv1alpha1.ConditionTypeLive)
	if c == nil || c.LastTransitionTime.IsZero() {
		return time.Now()
	}
	return c.LastTransitionTime.Time
}

// originOf returns the node whose location claims Origin, or the empty
// string when none does.
func originOf(locs []mxlv1alpha1.MxlFlowLocation) string {
	for _, loc := range locs {
		if loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			return loc.NodeName
		}
	}
	return ""
}

// keepPresent drops every location whose node is in departed.
func keepPresent(locs []mxlv1alpha1.MxlFlowLocation, departed map[string]struct{}) []mxlv1alpha1.MxlFlowLocation {
	if len(departed) == 0 {
		return locs
	}
	kept := make([]mxlv1alpha1.MxlFlowLocation, 0, len(locs))
	for _, loc := range locs {
		if _, gone := departed[loc.NodeName]; !gone {
			kept = append(kept, loc)
		}
	}
	return kept
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

func sortedNames(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func conditionStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// requeueAt schedules the next pass for the moment the answer can
// change on its own. A deadline already past means look again now.
func requeueAt(deadline time.Time) ctrl.Result {
	if deadline.IsZero() {
		return ctrl.Result{}
	}
	if wait := time.Until(deadline); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}
	}
	return ctrl.Result{Requeue: true}
}

// equalIgnoringTimestamps reports whether two statuses differ in
// anything worth a write. Condition timestamps are excluded because
// SetStatusCondition leaves lastTransitionTime alone when the status
// itself is unchanged, so comparing the rest is what decides whether
// this pass has anything to say.
func equalIgnoringTimestamps(a, b *mxlv1alpha1.MxlFlowStatus) bool {
	if a.OriginNode != b.OriginNode ||
		a.PreviousOriginNode != b.PreviousOriginNode ||
		len(a.Locations) != len(b.Locations) ||
		len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Locations {
		if a.Locations[i].NodeName != b.Locations[i].NodeName ||
			a.Locations[i].Phase != b.Locations[i].Phase {
			return false
		}
	}
	for i := range a.Conditions {
		if a.Conditions[i].Type != b.Conditions[i].Type ||
			a.Conditions[i].Status != b.Conditions[i].Status ||
			a.Conditions[i].Reason != b.Conditions[i].Reason ||
			a.Conditions[i].Message != b.Conditions[i].Message {
			return false
		}
	}
	return true
}

func (r *Reconciler) event(obj client.Object, kind, reason, format string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, kind, reason, format, args...)
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
		Watches(
			&coordinationv1.Lease{},
			handler.EnqueueRequestsFromMapFunc(leaseToFlow),
			builder.WithPredicates(leaseInMxlSystem()),
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

// mirrorToFlow enqueues the MxlFlow a mirror references. A mirror is
// what keeps a flow with no live origin alive, so the mirror going
// away is the event that can make the flow collectable, and it raises
// nothing on the flow itself.
func mirrorToFlow(_ context.Context, obj client.Object) []reconcile.Request {
	mirror, ok := obj.(*mxlv1alpha1.MxlFlowMirror)
	if !ok || mirror.Spec.FlowID == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Name: mirror.Spec.FlowID},
	}}
}

// leaseToFlow enqueues the MxlFlow whose id the Lease name encodes. An
// agent shutting down gracefully deletes its Leases, and that is the
// only prompt signal that a producer has gone; without this the flow
// waits out the whole renewal window before anything looks at it.
func leaseToFlow(_ context.Context, obj client.Object) []reconcile.Request {
	flowID, _, ok := mxlv1alpha1.ParseLeaseName(obj.GetName())
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: flowID}}}
}

// leaseInMxlSystem keeps the Lease watch confined to the namespace the
// agent publishes Origin Leases in. Other Leases (kube-system leader
// election, kube-node-lease) would otherwise wake a flow on every
// renew tick in the cluster.
func leaseInMxlSystem() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == mxlv1alpha1.LeaseNamespace
	})
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
