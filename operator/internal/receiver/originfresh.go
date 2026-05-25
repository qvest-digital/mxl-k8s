package receiver

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// applyOriginFreshCondition writes ConditionTypeOriginFresh on the
// MxlFlow's status using server-side apply with
// FieldOwner=mxl-operator. The receiver only writes the condition
// when it has an opinion: a fresh Origin (True) or one or more
// Origins that the LeaseChecker rejected (False). The "no Origin at
// all" case is left untouched because the publisher path through
// receiver covers many flows whose Origin has simply not been
// published yet, and stamping False on every such flow would mask
// the genuine Lease-expired signal.
func (r *Reconciler) applyOriginFreshCondition(ctx context.Context, flowID string, res originResolution) error {
	if r.Lease == nil {
		return nil
	}
	var (
		status  metav1.ConditionStatus
		reason  string
		message string
	)
	switch {
	case res.Found:
		status = metav1.ConditionTrue
		reason = mxlv1alpha1.ReasonRecovered
		message = "origin lease is within its renewal window"
	case res.AllStale:
		status = metav1.ConditionFalse
		reason = mxlv1alpha1.ReasonLeaseExpired
		message = "no origin location holds a renewed lease"
	default:
		return nil
	}

	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlow"))
	patch.SetName(flowID)
	conditionsField := []any{map[string]any{
		"type":               mxlv1alpha1.ConditionTypeOriginFresh,
		"status":             string(status),
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
	}}
	if err := unstructured.SetNestedField(patch.Object, map[string]any{
		"conditions": conditionsField,
	}, "status"); err != nil {
		return fmt.Errorf("build SSA payload: %w", err)
	}
	return r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(MxlOperatorFieldManager),
		client.ForceOwnership,
	)
}
