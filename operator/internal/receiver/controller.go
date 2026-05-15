package receiver

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Reconciler observes MxlReceiver resources. In a later phase this
// reconciler will translate each MxlReceiver into one or more
// MxlFlowMirror resources; currently it only records events.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile is the entry point for MxlReceiver change events.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlreceiver", req.NamespacedName)
	var obj mxlv1alpha1.MxlReceiver
	if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	l.V(1).Info("observed MxlReceiver",
		"flowID", obj.Spec.FlowID,
		"provider", obj.Spec.Provider,
		"phase", obj.Status.Phase)
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlReceiver{}).
		Named("mxlreceiver").
		Complete(r)
}
