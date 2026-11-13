package instance

import (
	"context"
	"path/filepath"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Opener opens every MxlDomain below domains/ that is materialised on
// this node, whether or not a mirror or a placeholder needs it.
//
// What Set.Watch hands every opened domain to, the domain sweep and the
// clearing of stale placeholder markers, runs only in a domain the set
// has opened. Opened on first use alone, a domain this node only
// produces into was never swept: a producer killed there left its flow
// for good, and so did a copy or a placeholder a previous gateway left
// in a domain no mirror on the node used again.
type Opener struct {
	Client   client.Client
	NodeName string
	Set      *Set
}

// Reconcile opens one MxlDomain once the agent reports it ready here.
// A domain not yet ready is looked at again when its status changes.
func (o *Opener) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var d mxlv1alpha1.MxlDomain
	if err := o.Client.Get(ctx, req.NamespacedName, &d); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	// The primary domain is open from the start, and a domain in any
	// other directory is not mounted here; Set.For refuses both.
	if filepath.Dir(d.Spec.Path()) != mxlv1alpha1.DomainsDir {
		return reconcile.Result{}, nil
	}
	if n := d.Status.Node(o.NodeName); n == nil || !n.Ready {
		return reconcile.Result{}, nil
	}
	_, err := o.Set.For(ctx, d.Name)
	return reconcile.Result{}, err
}

// SetupWithManager wires the opener to MxlDomain events.
func (o *Opener) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("mxldomain-opener").
		For(&mxlv1alpha1.MxlDomain{}).
		Complete(o)
}
