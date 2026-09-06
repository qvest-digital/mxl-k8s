// Package leasegc collects the per-flow Origin Leases the agent
// publishes once nothing can renew them again.
//
// The agent that writes a Lease is the only thing that deletes one: it
// releases on a vanished flow directory and on graceful shutdown. That
// contract covers every case in which the agent is still there to
// honour it, and none of the cases in which it is not. A node removed
// from the cluster takes its agent with it and leaves one Lease per
// flow it held, renewed for the last time whenever the node went; a
// flow collected while its agent was down leaves the same. Nothing
// else looks at them, so they accumulate for the life of the cluster
// -- eight of them on a showcase cluster named a node that had been
// gone for a week.
//
// They are inert rather than harmful: a consumer reads RenewTime and
// an expired Lease answers "not fresh" correctly forever. What they
// cost is the ability to read the namespace, which is where an
// operator looks to find out which producers a cluster believes in.
package leasegc

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// Reconciler deletes an expired Origin Lease whose holder or whose
// flow is gone.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile deletes one Lease if it can never be renewed again.
//
// Both halves of the test are required and neither is a heuristic. An
// expired Lease whose holder node still exists is one the agent will
// renew on its next pass or release on its next rescan, and deleting
// it would race a live producer. A fresh Lease whose flow is missing
// is one the agent is actively renewing, which means the flow it
// describes is on disk and about to be republished. Only a Lease that
// is both past its window and orphaned by node or by flow is one
// nothing will come back for.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var lease coordinationv1.Lease
	if err := r.Get(ctx, req.NamespacedName, &lease); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	flowID, nodeName, ok := mxlv1alpha1.ParseLeaseName(lease.Name)
	if !ok {
		// Not one of ours. The namespace is the agent's, but nothing
		// stops another component from putting a Lease in it.
		return ctrl.Result{}, nil
	}

	deadline := expiry(&lease)
	if wait := time.Until(deadline); wait > 0 {
		// Time passing raises no event, so the only way an unrenewed
		// Lease is ever looked at again is a wake-up scheduled here.
		return ctrl.Result{RequeueAfter: wait + time.Second}, nil
	}

	orphan, why, err := r.orphaned(ctx, flowID, nodeName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !orphan {
		// Expired but still explainable: the agent owns it and will
		// renew or release it. Nothing here to do until that changes,
		// and both changes arrive on a watch.
		return ctrl.Result{}, nil
	}

	// Preconditioned on the version the decision was made against, so
	// an agent that renewed between the read and here keeps its Lease.
	err = r.Delete(ctx, &lease, client.Preconditions{
		UID:             &lease.UID,
		ResourceVersion: &lease.ResourceVersion,
	})
	switch {
	case err == nil:
	case apierrors.IsNotFound(err):
		return ctrl.Result{}, nil
	case apierrors.IsConflict(err):
		return ctrl.Result{Requeue: true}, nil
	default:
		return ctrl.Result{}, fmt.Errorf("delete Lease %s: %w", lease.Name, err)
	}

	log.FromContext(ctx).Info("collected orphaned origin Lease",
		"lease", lease.Name, "flowID", flowID, "node", nodeName, "reason", why,
		"expiredFor", time.Since(deadline).Truncate(time.Second))
	return ctrl.Result{}, nil
}

// orphaned reports whether the Lease can no longer be renewed by
// anything, and why.
func (r *Reconciler) orphaned(ctx context.Context, flowID, nodeName string) (bool, string, error) {
	var node corev1.Node
	err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node)
	switch {
	case apierrors.IsNotFound(err):
		return true, "holder node has left the cluster", nil
	case err != nil:
		return false, "", fmt.Errorf("look up node %s: %w", nodeName, err)
	}

	var flow mxlv1alpha1.MxlFlow
	err = r.Get(ctx, types.NamespacedName{Name: flowID}, &flow)
	switch {
	case apierrors.IsNotFound(err):
		return true, "the flow it names has been collected", nil
	case err != nil:
		return false, "", fmt.Errorf("look up MxlFlow %s: %w", flowID, err)
	}
	return false, "", nil
}

// expiry is RenewTime plus the declared duration. A Lease that was
// never renewed has no window to be inside, so it reads as expired at
// the zero time and is judged on the orphan test alone.
func expiry(lease *coordinationv1.Lease) time.Time {
	if lease.Spec.RenewTime == nil {
		return time.Time{}
	}
	duration := mxlv1alpha1.DefaultLeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil && *lease.Spec.LeaseDurationSeconds > 0 {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	return lease.Spec.RenewTime.Time.Add(duration)
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
//
// Renewals are deliberately not watched. A renewal can only make a
// Lease less collectable, so the one thing that has to be noticed is
// time passing, and the requeue in Reconcile is what notices it.
// Watching them instead would wake this controller once per flow per
// renew interval for the whole cluster to conclude nothing.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&coordinationv1.Lease{}, builder.WithPredicates(
			inMxlSystem(), createOnly(),
		)).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.nodeToLeases),
			builder.WithPredicates(deletedOnly()),
		).
		Watches(
			&mxlv1alpha1.MxlFlow{},
			handler.EnqueueRequestsFromMapFunc(r.flowToLeases),
			builder.WithPredicates(deletedOnly()),
		).
		Named("mxlflowlease").
		Complete(r)
}

// nodeToLeases enqueues every Origin Lease the departed node held.
func (r *Reconciler) nodeToLeases(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.leasesMatching(ctx, func(flowID, nodeName string) bool {
		return nodeName == obj.GetName()
	})
}

// flowToLeases enqueues every Origin Lease naming the collected flow,
// on any node.
func (r *Reconciler) flowToLeases(ctx context.Context, obj client.Object) []reconcile.Request {
	flow, ok := obj.(*mxlv1alpha1.MxlFlow)
	if !ok {
		return nil
	}
	return r.leasesMatching(ctx, func(flowID, nodeName string) bool {
		return flowID == flow.Spec.ID
	})
}

func (r *Reconciler) leasesMatching(ctx context.Context, keep func(flowID, nodeName string) bool) []reconcile.Request {
	var leases coordinationv1.LeaseList
	if err := r.List(ctx, &leases, client.InNamespace(mxlv1alpha1.LeaseNamespace)); err != nil {
		log.FromContext(ctx).Error(err, "list origin Leases for a watch event")
		return nil
	}
	var out []reconcile.Request
	for i := range leases.Items {
		flowID, nodeName, ok := mxlv1alpha1.ParseLeaseName(leases.Items[i].Name)
		if !ok || !keep(flowID, nodeName) {
			continue
		}
		out = append(out, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&leases.Items[i]),
		})
	}
	return out
}

// inMxlSystem confines the Lease watch to the namespace the agent
// publishes Origin Leases in. kube-node-lease alone holds one Lease
// per node renewed every few seconds.
func inMxlSystem() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == mxlv1alpha1.LeaseNamespace
	})
}

// createOnly passes the event an informer raises for every Lease at
// sync and for each new one afterwards, and drops the renewals.
func createOnly() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// deletedOnly passes only deletions, which is the only transition of a
// Node or an MxlFlow that can orphan a Lease.
func deletedOnly() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
