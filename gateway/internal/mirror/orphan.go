package mirror

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// orphanRecheckInterval re-examines a deleting foreign mirror on a
// timer, because a Node deletion raises no event on the mirror.
const orphanRecheckInterval = time.Minute

// nodeGone reports whether the named Node is absent from the API.
// The read bypasses the manager cache so a DaemonSet does not carry
// a cluster-wide Node informer for a lookup that only fires while a
// mirror is deleting. An empty nodeName reports false: a mirror with
// no node recorded has no owner to be orphaned from.
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
