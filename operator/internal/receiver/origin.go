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

// originResolution distinguishes the three outcomes the controller
// has to react to differently: a fresh Origin was found (Node set,
// Found true, Deadline = the moment the Lease falls stale), no
// Origin location exists yet (Found false, all other fields zero),
// or one or more Origins exist but every one has a stale or missing
// Lease (Found false, AllStale true). The controller writes the
// OriginFresh condition based on AllStale and schedules a
// RequeueAfter Deadline when one is set.
type originResolution struct {
	Node     string
	Found    bool
	AllStale bool
	Deadline time.Time
}

// resolveSourceNode walks flow.Status.Locations and returns the
// first Origin whose Lease is still fresh, falling back to the
// raw Origin pick when no LeaseChecker is wired. AllStale flags the
// case where every Origin candidate was rejected by the checker so
// the caller can surface ConditionTypeOriginFresh=False.
func (r *Reconciler) resolveSourceNode(ctx context.Context, flowID string) (originResolution, error) {
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			return originResolution{}, nil
		}
		return originResolution{}, err
	}

	sawOrigin := false
	for _, loc := range flow.Status.Locations {
		if loc.Phase != mxlv1alpha1.MxlFlowLocationOrigin {
			continue
		}
		sawOrigin = true
		if r.Lease == nil {
			return originResolution{Node: loc.NodeName, Found: true}, nil
		}
		fresh, deadline, err := r.Lease.IsFresh(ctx, flowID, loc.NodeName)
		if err != nil {
			return originResolution{}, err
		}
		if fresh {
			return originResolution{Node: loc.NodeName, Found: true, Deadline: deadline}, nil
		}
	}
	return originResolution{AllStale: sawOrigin}, nil
}
