package mirror

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A mirror's progress timestamps live in two heartbeat Leases, one per
// side, rather than in the mirror's status. Every status write stores
// the whole mirror again in etcd and wakes every controller watching
// mirrors; a Lease is a few hundred bytes that nothing reconciles on.
// The mirror's status still changes on every phase or reason
// transition, and carries the timestamps as of that transition.

// heartbeat renews one side's heartbeat Lease on behalf of a flusher,
// whose goroutine is the only one to touch it.
type heartbeat struct {
	side   mxlv1alpha1.MirrorSide
	owner  string
	mirror types.NamespacedName
	holder string

	// written is the renewTime of the last successful write.
	written *time.Time
	retry   publishBackoff
}

// renew writes at as the heartbeat once it has moved a statusQuantum
// past the last one written, and backs off after a failed write.
func (h *heartbeat) renew(ctx context.Context, c client.Client, at time.Time, interval time.Duration) {
	if h.written != nil && sameQuantum(at, *h.written) {
		return
	}
	now := time.Now()
	if !h.retry.due(now) {
		return
	}
	if err := applyHeartbeat(ctx, c, h.mirror, h.side, h.owner, h.holder, at); err != nil {
		h.retry.failed(now, interval)
		ctrl.Log.WithName("heartbeat").Error(err, "renew",
			"mirror", h.mirror, "side", h.side, "retryIn", h.retry.wait)
		return
	}
	h.retry.succeeded()
	h.written = &at
}

// applyHeartbeat writes one side's heartbeat Lease, creating it when
// it does not exist.
func applyHeartbeat(ctx context.Context, c client.Client, mirror types.NamespacedName, side mxlv1alpha1.MirrorSide, owner, holder string, at time.Time) error {
	lease := &unstructured.Unstructured{}
	lease.SetGroupVersionKind(coordinationv1.SchemeGroupVersion.WithKind("Lease"))
	lease.SetNamespace(mxlv1alpha1.LeaseNamespace)
	lease.SetName(mxlv1alpha1.MirrorLeaseName(mirror.Namespace, mirror.Name, side))
	lease.SetLabels(map[string]string{mxlv1alpha1.LabelMirrorSide: string(side)})
	lease.SetAnnotations(map[string]string{mxlv1alpha1.AnnotationMirror: mirror.String()})
	if err := unstructured.SetNestedField(lease.Object, map[string]any{
		"holderIdentity": holder,
		"renewTime":      at.UTC().Format(metav1.RFC3339Micro),
	}, "spec"); err != nil {
		return fmt.Errorf("build heartbeat payload: %w", err)
	}
	return c.Patch(ctx, lease, client.Apply, client.FieldOwner(owner), client.ForceOwnership)
}

// deleteHeartbeat removes one side's heartbeat Lease. The Lease lives
// in another namespace than most mirrors, so no owner reference
// collects it: the gateway that renews it deletes it when it releases
// the mirror, and the operator collects one whose mirror is gone.
//
// Best effort, because it runs ahead of a finalizer's removal: a Lease
// left behind costs a few hundred bytes until it is collected and reads
// as an old heartbeat, while a finalizer held on it would leave a
// mirror no delete can clear.
func deleteHeartbeat(ctx context.Context, c client.Client, mirror types.NamespacedName, side mxlv1alpha1.MirrorSide) {
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace: mxlv1alpha1.LeaseNamespace,
		Name:      mxlv1alpha1.MirrorLeaseName(mirror.Namespace, mirror.Name, side),
	}}
	if err := c.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
		ctrl.Log.WithName("heartbeat").Error(err, "delete; left for the operator to collect",
			"mirror", mirror, "side", side)
	}
}

// heartbeatAt is one side's latest progress time: the later of its
// heartbeat Lease and the timestamp the mirror's status carries for
// it. The status is consulted as well because a gateway that predates
// the Lease writes only the status, and because a transition writes
// the status first.
func heartbeatAt(ctx context.Context, c client.Reader, mirror *mxlv1alpha1.MxlFlowMirror, side mxlv1alpha1.MirrorSide) *time.Time {
	var latest *time.Time
	field := mirror.Status.LastGrainAt
	if side == mxlv1alpha1.MirrorSideSource {
		field = mirror.Status.LastSentAt
	}
	if field != nil {
		t := field.Time
		latest = &t
	}
	var lease coordinationv1.Lease
	key := types.NamespacedName{
		Namespace: mxlv1alpha1.LeaseNamespace,
		Name:      mxlv1alpha1.MirrorLeaseName(mirror.Namespace, mirror.Name, side),
	}
	if err := c.Get(ctx, key, &lease); err == nil && lease.Spec.RenewTime != nil {
		if t := lease.Spec.RenewTime.Time; latest == nil || t.After(*latest) {
			latest = &t
		}
	}
	return latest
}
