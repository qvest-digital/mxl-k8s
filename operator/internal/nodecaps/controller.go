package nodecaps

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Reconciler observes MxlNodeCapabilities resources. The gateway
// owns their status; the operator uses them to validate provider
// selection in MxlFlowMirror placement (future phase).
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlnodecapabilities,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlnodecapabilities/status,verbs=get

// Reconcile is the entry point for MxlNodeCapabilities change events.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlnodecapabilities", req.NamespacedName)
	var obj mxlv1alpha1.MxlNodeCapabilities
	if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	l.V(1).Info("observed MxlNodeCapabilities",
		"nodeName", obj.Spec.NodeName,
		"providers", len(obj.Status.Providers))
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlNodeCapabilities{}).
		Named("mxlnodecapabilities").
		Complete(r)
}
