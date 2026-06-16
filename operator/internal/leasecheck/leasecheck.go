// Package leasecheck reads the per-flow Origin Leases the agent
// publishes and answers IsFresh for the operator's receiver
// reconciler. Read-only by design: the agent owns Create/Update/
// Delete on its node's Leases; the operator's RBAC carries only
// get/list/watch on coordination.k8s.io/leases.
package leasecheck

import (
	"context"
	"time"

	apiv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeaseNamespace re-exports the shared api/v1alpha1 constant so the
// operator can run with a Role scoped to a single namespace instead
// of cluster-scoped RBAC.
const LeaseNamespace = apiv1alpha1.LeaseNamespace

// DefaultLeaseDuration is the freshness window applied when a Lease
// omits LeaseDurationSeconds. The agent writes 30s explicitly;
// the fallback only matters for Leases authored outside the agent.
const DefaultLeaseDuration = 30 * time.Second

// Checker implements receiver.LeaseChecker on top of a kube client.
type Checker struct {
	Client client.Client
}

// LeaseName re-exports the shared api/v1alpha1 helper. The agent and
// the operator must agree on the name; the shared definition lives
// in api so drift between the two sides is impossible by
// construction.
func LeaseName(flowID, nodeName string) string {
	return apiv1alpha1.LeaseName(flowID, nodeName)
}

// IsFresh reports whether the Lease for (flowID, nodeName) was
// renewed inside its declared duration. Missing Lease counts as not
// fresh: an Origin location without a Lease is either an agent that
// has not yet published or one that already Released. The returned
// deadline is RenewTime+LeaseDuration so the receiver reconciler
// can schedule a Reconcile right after the deadline elapses; k8s
// emits no event when an unrenewed Lease passes its window, so
// without this wake-up the operator never re-checks freshness.
// Deadline is zero when the Lease is missing or has no RenewTime.
func (c *Checker) IsFresh(ctx context.Context, flowID, nodeName string) (bool, time.Time, error) {
	if c == nil || c.Client == nil {
		return false, time.Time{}, nil
	}
	var lease coordinationv1.Lease
	err := c.Client.Get(ctx, types.NamespacedName{
		Namespace: LeaseNamespace,
		Name:      LeaseName(flowID, nodeName),
	}, &lease)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, err
	}
	if lease.Spec.RenewTime == nil {
		return false, time.Time{}, nil
	}
	duration := DefaultLeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil && *lease.Spec.LeaseDurationSeconds > 0 {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	deadline := lease.Spec.RenewTime.Time.Add(duration)
	return time.Now().Before(deadline), deadline, nil
}
