package domain

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Reconciler summarises an MxlDomain's per-node entries into its
// Materialised condition, and removes objects left in the per-node
// shape the resource had before it described a domain.
//
// The per-node entries are the agents'; this never writes them.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxldomains,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxldomains/status,verbs=get;update;patch

// Reconcile is the entry point for MxlDomain change events.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxldomain", req.Name)

	var d mxlv1alpha1.MxlDomain
	if err := r.Get(ctx, req.NamespacedName, &d); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// An id is required by the schema, so an object without one was
	// stored before the resource described a domain: the agent's
	// per-node record, named after its node. Nothing reads that shape
	// any more, and leaving it would list a node as if it were a domain.
	if d.Spec.ID == "" {
		l.Info("deleting MxlDomain left in the per-node shape")
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, &d))
	}

	cond := materialised(&d)
	cond.ObservedGeneration = d.Generation
	if !meta.SetStatusCondition(&d.Status.Conditions, cond) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.Status().Update(ctx, &d)
}

func materialised(d *mxlv1alpha1.MxlDomain) metav1.Condition {
	c := metav1.Condition{Type: mxlv1alpha1.ConditionTypeMaterialised}
	if len(d.Status.Nodes) == 0 {
		c.Status, c.Reason = metav1.ConditionFalse, "NoNodes"
		c.Message = "no agent has reported this domain"
		return c
	}
	var failing []string
	for _, n := range d.Status.Nodes {
		if !n.Ready {
			failing = append(failing, n.NodeName)
		}
	}
	if len(failing) > 0 {
		sort.Strings(failing)
		c.Status, c.Reason = metav1.ConditionFalse, "NodesNotReady"
		c.Message = fmt.Sprintf("not materialised on %s", strings.Join(failing, ", "))
		return c
	}
	c.Status, c.Reason = metav1.ConditionTrue, "AllNodesReady"
	c.Message = fmt.Sprintf("materialised on %d node(s)", len(d.Status.Nodes))
	return c
}

// SetupWithManager registers the reconciler with the manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlDomain{}).
		Complete(r)
}
