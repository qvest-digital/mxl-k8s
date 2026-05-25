// Package originlease publishes a coordination.k8s.io Lease per
// Origin flow the agent holds on disk. The operator's receiver
// reconciler and the agent's intent dispatcher both treat
// resolveSourceNode's Origin pick as authoritative; without a
// liveness signal a crashed or partitioned agent leaves a stale
// Origin location behind that downstream lookups will keep choosing.
// The Lease's RenewTime + LeaseDurationSeconds window is that
// liveness signal: consumers skip an Origin whose Lease is gone or
// has expired.
package originlease

import (
	"context"
	"time"

	apiv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeaseNamespace re-exports the shared api/v1alpha1 constant so
// existing call sites here keep compiling unchanged.
const LeaseNamespace = apiv1alpha1.LeaseNamespace

const (
	// DefaultLeaseDuration is the renewal window stamped on every
	// Lease the manager writes. A consumer treats RenewTime +
	// LeaseDurationSeconds as the freshness deadline.
	DefaultLeaseDuration = 30 * time.Second

	// DefaultRenewInterval is the cadence RunRenewLoop uses when the
	// caller does not pass one. Set to a third of the default
	// duration so two missed renewals still leave the Lease fresh.
	DefaultRenewInterval = 10 * time.Second
)

// Manager owns the per-flow Lease lifecycle for one node. Renew,
// Release, and IsFresh are safe to call concurrently; each request
// goes straight to the API server.
type Manager struct {
	Client               client.Client
	NodeName             string
	LeaseDurationSeconds int32
}

// New constructs a Manager with the default duration when
// LeaseDurationSeconds is unset.
func New(c client.Client, nodeName string) *Manager {
	return &Manager{
		Client:               c,
		NodeName:             nodeName,
		LeaseDurationSeconds: int32(DefaultLeaseDuration / time.Second),
	}
}

// LeaseName re-exports the shared api/v1alpha1 helper. The agent and
// the operator must agree on the name; the shared definition lives
// in api so both sides cannot drift.
func LeaseName(flowID, nodeName string) string {
	return apiv1alpha1.LeaseName(flowID, nodeName)
}

// Renew upserts the Lease for flowID held by this node. Create on
// first call, Update with conflict retry afterwards. RenewTime is
// stamped to now; HolderIdentity to this node so the operator's
// freshness check can verify the Lease belongs to the same node the
// Origin location names.
func (m *Manager) Renew(ctx context.Context, flowID string) error {
	if m == nil || m.Client == nil {
		return nil
	}
	duration := m.LeaseDurationSeconds
	if duration <= 0 {
		duration = int32(DefaultLeaseDuration / time.Second)
	}
	name := LeaseName(flowID, m.NodeName)
	holder := m.NodeName

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var existing coordinationv1.Lease
		err := m.Client.Get(ctx, types.NamespacedName{Namespace: LeaseNamespace, Name: name}, &existing)
		if apierrors.IsNotFound(err) {
			now := metav1.NewMicroTime(time.Now())
			lease := &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: LeaseNamespace,
					Name:      name,
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       &holder,
					LeaseDurationSeconds: &duration,
					RenewTime:            &now,
				},
			}
			if err := m.Client.Create(ctx, lease); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		now := metav1.NewMicroTime(time.Now())
		existing.Spec.HolderIdentity = &holder
		existing.Spec.LeaseDurationSeconds = &duration
		existing.Spec.RenewTime = &now
		return m.Client.Update(ctx, &existing)
	})
}

// Release deletes the Lease for flowID. A missing Lease is treated
// as success so a vanish event that races a never-renewed flow does
// not log noise.
func (m *Manager) Release(ctx context.Context, flowID string) error {
	if m == nil || m.Client == nil {
		return nil
	}
	name := LeaseName(flowID, m.NodeName)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: LeaseNamespace, Name: name},
	}
	if err := m.Client.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// IsFresh reports whether the Lease for (flowID, nodeName) exists
// and was renewed within its declared duration. A missing Lease is
// not fresh: an Origin location without a Lease is either a node
// that never published one or one that already Released it.
func (m *Manager) IsFresh(ctx context.Context, flowID, nodeName string) (bool, error) {
	if m == nil || m.Client == nil {
		return false, nil
	}
	name := LeaseName(flowID, nodeName)
	var lease coordinationv1.Lease
	if err := m.Client.Get(ctx, types.NamespacedName{Namespace: LeaseNamespace, Name: name}, &lease); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if lease.Spec.RenewTime == nil {
		return false, nil
	}
	duration := DefaultLeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil && *lease.Spec.LeaseDurationSeconds > 0 {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	deadline := lease.Spec.RenewTime.Time.Add(duration)
	return time.Now().Before(deadline), nil
}
