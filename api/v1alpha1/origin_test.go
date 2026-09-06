package v1alpha1

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The flow collector, the mirror lifecycle controller, the receiver
// reconciler and the agent's intent dispatcher all have to reach the
// same answer about where a flow lives. They each used to walk the
// locations themselves and the copies drifted, so a flow could be
// routable from one component's view and not from another's at the
// same instant.

const testFlowID = "11111111-2222-3333-4444-555555555555"

func flowWith(locs ...MxlFlowLocation) *MxlFlow {
	return &MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: testFlowID},
		Spec:       MxlFlowSpec{ID: testFlowID},
		Status:     MxlFlowStatus{Locations: locs},
	}
}

func at(node string, phase MxlFlowLocationPhase) MxlFlowLocation {
	return MxlFlowLocation{NodeName: node, Phase: phase}
}

// freshOn reports the named nodes fresh and everything else stale.
func freshOn(deadline time.Time, nodes ...string) LeaseFreshness {
	live := map[string]struct{}{}
	for _, n := range nodes {
		live[n] = struct{}{}
	}
	return func(_, nodeName string) (bool, time.Time, error) {
		if _, ok := live[nodeName]; ok {
			return true, deadline, nil
		}
		return false, time.Time{}, nil
	}
}

func TestResolveOrigin_PicksTheOriginWhoseLeaseIsRenewed(t *testing.T) {
	deadline := time.Now().Add(30 * time.Second)
	// A stale Origin first, so a resolver that returned the first one
	// it saw would fail here.
	flow := flowWith(
		at("stale", MxlFlowLocationOrigin),
		at("live", MxlFlowLocationOrigin),
		at("mirror", MxlFlowLocationReady),
	)

	res, err := ResolveOrigin(flow, freshOn(deadline, "live"))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.Equal(t, "live", res.Node)
	assert.False(t, res.AllStale)
	assert.Equal(t, deadline, res.Deadline,
		"the deadline is what a controller schedules its next look from; "+
			"time passing raises no event")
}

// The two empty answers mean different things and callers act on them
// differently: a producer the cluster has lost is worth a False
// condition, one it has not seen yet is not.
func TestResolveOrigin_SeparatesLostFromNeverSeen(t *testing.T) {
	lost, err := ResolveOrigin(flowWith(at("n1", MxlFlowLocationOrigin)), freshOn(time.Time{}))
	require.NoError(t, err)
	assert.False(t, lost.Found)
	assert.True(t, lost.AllStale)

	never, err := ResolveOrigin(flowWith(at("n1", MxlFlowLocationReady)), freshOn(time.Time{}))
	require.NoError(t, err)
	assert.False(t, never.Found)
	assert.False(t, never.AllStale)
}

// A Ready or Mirroring copy is not somewhere a consumer can be sourced
// from: it is a mirror's target, filled by a gateway that is itself
// reading the origin.
func TestResolveOrigin_IgnoresNonOriginPhases(t *testing.T) {
	flow := flowWith(
		at("a", MxlFlowLocationReady),
		at("b", MxlFlowLocationMirroring),
		at("c", MxlFlowLocationStale),
	)
	res, err := ResolveOrigin(flow, freshOn(time.Now().Add(time.Minute), "a", "b", "c"))
	require.NoError(t, err)
	assert.False(t, res.Found)
	assert.False(t, res.AllStale)
}

// A component wired without a Lease client resolves the same origin
// the pre-Lease code did, rather than resolving none at all.
func TestResolveOrigin_NilCheckerTrustsEveryOrigin(t *testing.T) {
	res, err := ResolveOrigin(flowWith(at("n1", MxlFlowLocationOrigin)), nil)
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.Equal(t, "n1", res.Node)
	assert.True(t, res.Deadline.IsZero(), "there is no window to schedule against")
}

// A Lease read that failed says nothing about the producer. Reporting
// it as absent would collect flows and mirrors on an API-server
// hiccup.
func TestResolveOrigin_CheckerErrorIsSurfaced(t *testing.T) {
	boom := errors.New("apiserver unreachable")
	_, err := ResolveOrigin(flowWith(at("n1", MxlFlowLocationOrigin)),
		func(string, string) (bool, time.Time, error) { return false, time.Time{}, boom })
	require.ErrorIs(t, err, boom)
}

func TestResolveOrigin_NilFlowAndNoLocations(t *testing.T) {
	res, err := ResolveOrigin(nil, nil)
	require.NoError(t, err)
	assert.False(t, res.Found)

	res, err = ResolveOrigin(flowWith(), nil)
	require.NoError(t, err)
	assert.False(t, res.Found)
	assert.False(t, res.AllStale)
}

// OriginNode is the raw claim, without the Lease check, because
// status.originNode has to equal the locations list rather than the
// subset of it that is live.
func TestOriginNode_MirrorsTheLocationsList(t *testing.T) {
	assert.Equal(t, "n1", OriginNode(flowWith(
		at("n0", MxlFlowLocationReady),
		at("n1", MxlFlowLocationOrigin),
	)))
	assert.Empty(t, OriginNode(flowWith(at("n0", MxlFlowLocationStale))))
	assert.Empty(t, OriginNode(nil))
}
