package nodecaps

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
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

// ReasonNodeDeparted is the event reason recorded when a node's
// capabilities object is removed because the Node itself is gone. It
// is the node-lifecycle counterpart to the flow controller's location
// prune, and the two together explain a fabric shrinking.
const ReasonNodeDeparted = "NodeDeparted"

// Reconciler deletes MxlNodeCapabilities whose node has left the
// cluster. The gateway owns their status and stamps an owner
// reference on the ones it creates, which collects them with the
// node; this catches the ones created before that reference existed,
// for which no gateway will ever run again.
type Reconciler struct {
	// Recorder publishes the node-departure event. Nil records nothing.
	Recorder record.EventRecorder

	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlnodecapabilities,verbs=delete;get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlnodecapabilities/status,verbs=get
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile deletes the resource when its node is absent.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlnodecapabilities", req.NamespacedName)

	var obj mxlv1alpha1.MxlNodeCapabilities
	if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	nodeName := obj.Spec.NodeName
	if nodeName == "" {
		nodeName = obj.Name
	}
	var node corev1.Node
	err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node)
	if err == nil {
		l.V(1).Info("observed MxlNodeCapabilities",
			"nodeName", nodeName, "providers", len(obj.Status.Providers))
		return ctrl.Result{}, nil
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("get Node %q: %w", nodeName, err)
	}

	// Delete against the observed UID so a node that rejoins between
	// the read and the write keeps the resource its gateway rebuilt.
	if err := r.Delete(ctx, &obj, client.Preconditions{UID: &obj.UID}); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(fmt.Errorf("delete MxlNodeCapabilities: %w", err))
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(&obj, corev1.EventTypeWarning, ReasonNodeDeparted,
			"Deleting: node %s is no longer registered", nodeName)
	}
	l.Info("deleted MxlNodeCapabilities for a departed node", "nodeName", nodeName)
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlNodeCapabilities{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.nodeToCapabilities),
			builder.WithPredicates(nodeDeletedOnly()),
		).
		Named("mxlnodecapabilities").
		Complete(r)
}

// nodeToCapabilities enqueues the resources naming the departed node.
// A Node deletion raises no event on them, so without this a resource
// carrying no owner reference survives until something else touches
// it.
func (r *Reconciler) nodeToCapabilities(ctx context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	var caps mxlv1alpha1.MxlNodeCapabilitiesList
	if err := r.List(ctx, &caps); err != nil {
		log.FromContext(ctx).Error(err, "list MxlNodeCapabilities for a departed node",
			"nodeName", node.Name)
		return nil
	}
	var out []reconcile.Request
	for i := range caps.Items {
		item := &caps.Items[i]
		name := item.Spec.NodeName
		if name == "" {
			name = item.Name
		}
		if name == node.Name {
			out = append(out, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: item.Name},
			})
		}
	}
	return out
}

func nodeDeletedOnly() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
	}
}
