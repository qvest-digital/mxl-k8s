package receiver

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// LeaseChecker reports whether the agent on nodeName still holds a
// renewed origin Lease for flowID. The receiver consults it before
// trusting a status.locations[?].phase == Origin entry so a
// partitioned or crashed agent's stale Origin does not become the
// answer resolveSourceNode hands back. The returned deadline is the
// moment after which IsFresh would flip to false; the reconciler
// uses it to schedule a RequeueAfter so an unrenewed Lease is
// noticed even when k8s emits no event for time passing.
type LeaseChecker interface {
	IsFresh(ctx context.Context, flowID, nodeName string) (fresh bool, deadline time.Time, err error)
}

// resolveSourceNode reads the flow and hands it to the shared
// resolver.
//
// The walk itself lives in api/v1alpha1 because the flow collector,
// the mirror lifecycle controller and the agent's intent dispatcher
// all have to reach the same answer as this one. When each kept its
// own copy they did not: one treated a missing Lease as fresh and
// another skipped the all-stale distinction, so a flow could be
// routable from one component's view and not from another's at the
// same instant.
func (r *Reconciler) resolveSourceNode(ctx context.Context, flowID string) (mxlv1alpha1.OriginResolution, error) {
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			return mxlv1alpha1.OriginResolution{}, nil
		}
		return mxlv1alpha1.OriginResolution{}, err
	}
	return mxlv1alpha1.ResolveOrigin(&flow, r.leaseFreshness(ctx))
}

// leaseFreshness adapts the reconciler's LeaseChecker to the callback
// ResolveOrigin takes. A nil checker yields a nil callback, which is
// what makes ResolveOrigin trust every Origin location -- the
// pre-Lease behaviour the unit tests are built around.
func (r *Reconciler) leaseFreshness(ctx context.Context) mxlv1alpha1.LeaseFreshness {
	if r.Lease == nil {
		return nil
	}
	return func(flowID, nodeName string) (bool, time.Time, error) {
		return r.Lease.IsFresh(ctx, flowID, nodeName)
	}
}
