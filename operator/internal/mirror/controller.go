// Package mirror hosts the operator's intent-mirror garbage
// collector.
//
// Mirrors created on demand by the per-node agent in response to a
// fanotify probe carry LabelCreatedByIntent and a Spec.Requestor
// PodRef. When the requestor pod disappears (or is replaced with the
// same name but a fresh UID), nothing in the receiver-driven path
// reaps the mirror -- the receiver reconciler only owns the mirrors
// it stamped with LabelCreatedByReceiver. This reconciler closes that
// gap so an agent-created mirror does not outlive the pod that asked
// for it.
//
// Mirrors stamped with LabelCreatedByReceiver and no Requestor are
// left untouched here; their lifecycle belongs to the receiver
// reconciler.
package mirror

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// MxlFlowMirrorIntentFinalizer is added to MxlFlowMirror objects
// stamped with LabelCreatedByIntent. It blocks the API server from
// removing the mirror until this reconciler has had a chance to
// observe the deletion; the gateway's own finalizer (added by the
// libmxl-fabrics side) is the one that holds the object back while
// the data plane tears down.
const MxlFlowMirrorIntentFinalizer = "mxl.qvest-digital.com/intent-gc"

// requestorIndex names the field index registered on
// MxlFlowMirror.spec.requestor (namespace+name) so a Pod event can be
// mapped to the mirrors that reference that pod in O(matches) without
// scanning every mirror in the cluster.
const requestorIndex = "spec.requestor.nsname"

// Reconciler garbage-collects MxlFlowMirror objects created by the
// agent on a pod's behalf whose requestor pod is gone or has been
// replaced.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile drives one MxlFlowMirror through the intent-GC decision.
// Mirrors without LabelCreatedByIntent are ignored entirely so the
// two ownership domains (receiver-driven and intent-driven) never
// reap each other's objects.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)

	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &mirror); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	_, intent := mirror.Labels[mxlv1alpha1.LabelCreatedByIntent]
	if !intent {
		// Receiver-driven or externally-managed mirror; the intent GC
		// does not own it. Strip a stale finalizer if one ever got
		// added by an earlier label state so the API server is not
		// blocked from completing an unrelated deletion.
		if controllerutil.ContainsFinalizer(&mirror, MxlFlowMirrorIntentFinalizer) {
			controllerutil.RemoveFinalizer(&mirror, MxlFlowMirrorIntentFinalizer)
			if err := r.Update(ctx, &mirror); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("remove intent finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !mirror.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&mirror, MxlFlowMirrorIntentFinalizer) {
			controllerutil.RemoveFinalizer(&mirror, MxlFlowMirrorIntentFinalizer)
			if err := r.Update(ctx, &mirror); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("remove intent finalizer on deletion: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&mirror, MxlFlowMirrorIntentFinalizer) {
		controllerutil.AddFinalizer(&mirror, MxlFlowMirrorIntentFinalizer)
		if err := r.Update(ctx, &mirror); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("add intent finalizer: %w", err)
		}
	}

	req2 := mirror.Spec.Requestor
	if req2 == nil {
		// Labelled intent but missing Requestor: the agent crashed
		// between stamping the label and writing the field. Leave
		// the mirror alone; the agent will repair on next probe.
		return ctrl.Result{}, nil
	}

	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: req2.Namespace, Name: req2.Name}, &pod)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("get requestor pod: %w", err)
	}
	gone := apierrors.IsNotFound(err)
	replaced := !gone && req2.UID != "" && string(pod.UID) != req2.UID

	if !gone && !replaced {
		return ctrl.Result{}, nil
	}

	l.Info("garbage-collecting intent mirror",
		"reason", gcReason(gone, replaced),
		"requestor", req2.Namespace+"/"+req2.Name,
		"requestorUID", req2.UID)
	if err := r.Delete(ctx, &mirror); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete mirror: %w", err)
	}
	return ctrl.Result{}, nil
}

func gcReason(gone, replaced bool) string {
	switch {
	case gone:
		return "requestor pod gone"
	case replaced:
		return "requestor pod UID mismatch"
	default:
		return ""
	}
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager. The field index on spec.requestor lets a Pod event
// enqueue exactly the mirrors that named the deleted pod, rather
// than scanning every mirror in the namespace on every pod
// transition.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&mxlv1alpha1.MxlFlowMirror{},
		requestorIndex,
		func(o client.Object) []string {
			m, ok := o.(*mxlv1alpha1.MxlFlowMirror)
			if !ok || m.Spec.Requestor == nil {
				return nil
			}
			return []string{m.Spec.Requestor.Namespace + "/" + m.Spec.Requestor.Name}
		},
	); err != nil {
		return fmt.Errorf("index MxlFlowMirror by %s: %w", requestorIndex, err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.podToMirrors),
			builder.WithPredicates(podLifecyclePredicate()),
		).
		Named("mxlflowmirror-intent-gc").
		Complete(r)
}

// podToMirrors enqueues every MxlFlowMirror whose Spec.Requestor
// names the pod that fired the event. The field index keeps the
// lookup bounded by the number of mirrors actually referencing that
// pod.
func (r *Reconciler) podToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors, client.MatchingFields{
		requestorIndex: pod.Namespace + "/" + pod.Name,
	}); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(mirrors.Items))
	for i := range mirrors.Items {
		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: mirrors.Items[i].Namespace,
				Name:      mirrors.Items[i].Name,
			},
		})
	}
	return out
}

// podLifecyclePredicate keeps the pod watch from firing on noisy
// status ticks. The GC only cares about pod disappearance and UID
// replacement, both of which surface as Delete or Create events
// (a recreated pod with the same name gets a fresh UID). Update
// events fire only when the pod's UID itself changes, which the
// API server treats as a delete+create pair, but keeping the
// predicate strict guards against future API behavior.
func podLifecyclePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(_ event.CreateEvent) bool { return true },
		DeleteFunc:  func(_ event.DeleteEvent) bool { return true },
		GenericFunc: func(_ event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, oldOK := e.ObjectOld.(*corev1.Pod)
			newPod, newOK := e.ObjectNew.(*corev1.Pod)
			if !oldOK || !newOK {
				return false
			}
			return oldPod.UID != newPod.UID
		},
	}
}
