package mirror

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// orphanRecheckInterval is how often a reconciler re-examines a
// deleting MxlFlowMirror that belongs to another node. A Node
// deletion produces no event on the MxlFlowMirror, so a mirror whose
// deletion started while its node was still registered would
// otherwise never be looked at again once the owning gateway died
// mid-teardown. The interval only costs a cached Get per deleting
// foreign mirror; in the common case the owning gateway completes
// the deletion long before the first re-check fires.
const orphanRecheckInterval = time.Minute

// nodeGone reports whether the named Node object is absent from the
// API.
//
// The read deliberately bypasses the manager cache. The gateway runs
// as a DaemonSet and has no other reason to hold a cluster-wide Node
// informer; a cached Get would start one on every node in the
// cluster to serve a lookup that only fires while a mirror is being
// deleted.
//
// An empty nodeName reports false: a mirror with no node recorded
// has no owner to be orphaned from, and reaping on that basis would
// strip the finalizer from an object a gateway may still be setting
// up.
func nodeGone(ctx context.Context, reader client.Reader, nodeName string) (bool, error) {
	if nodeName == "" {
		return false, nil
	}
	var node corev1.Node
	err := reader.Get(ctx, types.NamespacedName{Name: nodeName}, &node)
	switch {
	case err == nil:
		return false, nil
	case apierrors.IsNotFound(err):
		return true, nil
	default:
		return false, err
	}
}
