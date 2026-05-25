// Package leasecheck reads the per-flow Origin Leases the agent
// publishes and answers IsFresh for the operator's receiver
// reconciler. Read-only by design: the agent owns Create/Update/
// Delete on its node's Leases; the operator's RBAC carries only
// get/list/watch on coordination.k8s.io/leases.
package leasecheck

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeaseNamespace mirrors the agent-side constant. Pinned so the
// operator can run with a Role scoped to a single namespace instead
// of cluster-scoped RBAC.
const LeaseNamespace = "mxl-system"

// DefaultLeaseDuration is the freshness window applied when a Lease
// omits LeaseDurationSeconds. The agent writes 30s explicitly;
// the fallback only matters for Leases authored outside the agent.
const DefaultLeaseDuration = 30 * time.Second

// Checker implements receiver.LeaseChecker on top of a kube client.
type Checker struct {
	Client client.Client
}

// LeaseName mirrors the agent-side naming convention. Drift between
// the two sides would silently turn every IsFresh call into a
// not-found-treated-as-stale and demote every Origin.
func LeaseName(flowID, nodeName string) string {
	return fmt.Sprintf("mxl-flow-%s-%s", flowID, nodeName)
}

// IsFresh reports whether the Lease for (flowID, nodeName) was
// renewed inside its declared duration. Missing Lease counts as not
// fresh: an Origin location without a Lease is either an agent that
// has not yet published or one that already Released.
func (c *Checker) IsFresh(ctx context.Context, flowID, nodeName string) (bool, error) {
	if c == nil || c.Client == nil {
		return false, nil
	}
	var lease coordinationv1.Lease
	err := c.Client.Get(ctx, types.NamespacedName{
		Namespace: LeaseNamespace,
		Name:      LeaseName(flowID, nodeName),
	}, &lease)
	if err != nil {
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
