package mirror

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Reconciler observes MxlFlowMirror resources. In a later phase this
// reconciler will dispatch placement decisions to the gateway via
// the local agent's UDS; currently it only records events.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/finalizers,verbs=update

// Reconcile is the entry point for MxlFlowMirror change events.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)
	var obj mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	l.V(1).Info("observed MxlFlowMirror",
		"flowID", obj.Spec.FlowID,
		"sourceNode", obj.Spec.SourceNode,
		"targetNode", obj.Spec.TargetNode,
		"provider", obj.Spec.Provider,
		"phase", obj.Status.Phase)
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Named("mxlflowmirror").
		Complete(r)
}
