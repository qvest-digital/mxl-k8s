package receiver

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// resolveSourceNode finds the node hosting the flow's authoritative
// copy (status.locations[?].phase == Origin).
func (r *Reconciler) resolveSourceNode(ctx context.Context, flowID string) (string, bool, error) {
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	for _, loc := range flow.Status.Locations {
		if loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			return loc.NodeName, true, nil
		}
	}
	return "", false, nil
}
