package mirror

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// openedEntry builds a targetEntry whose fabric side opened openedAgo
// in the past and which has never seen a grain commit.
func openedEntry(openedAgo time.Duration) *targetEntry {
	e := &targetEntry{}
	t := time.Now().Add(-openedAgo)
	e.fabricOpenedAt.Store(&t)
	return e
}

// Reconcile sets Phase=Ready as soon as the fabric side opens, before
// any grain arrives. A target that never receives one therefore kept
// that optimistic Ready for its whole life, because the flusher
// published nothing while lastCommitAt was nil. That is the exact
// shape a mirror pointed at a departed source node takes: Ready, no
// TargetProgress condition, no lastGrainAt -- indistinguishable from
// a healthy mirror to anything reading status.
func TestObservedTargetState_NeverAnyGrain_DemotesAfterWindow(t *testing.T) {
	got := observedTargetState(openedEntry(time.Minute), 10*time.Second, targetProgressState{})

	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorDegraded, got.phase,
		"a target that has never received a grain must not keep reporting "+
			"the Ready that Reconcile set optimistically at open time")
	assert.Equal(t, metav1.ConditionFalse, got.status)
	assert.Equal(t, mxlv1alpha1.ReasonNoGrains, got.reason)
}

// Inside the window the silence is still ordinary handshake latency,
// so publishing Degraded would flap every freshly opened target.
func TestObservedTargetState_NeverAnyGrain_SilentInsideWindow(t *testing.T) {
	got := observedTargetState(openedEntry(time.Second), 10*time.Second, targetProgressState{})

	assert.Equal(t, targetProgressState{}, got,
		"a target still inside the freshness window has not failed yet; "+
			"publishing here would demote every target during its handshake")
}

// A nil fabricOpenedAt means the flusher is running against an entry
// whose fabric side is not up yet. There is no baseline to measure
// against, so nothing is published.
func TestObservedTargetState_NoOpenTimestamp_StaysSilent(t *testing.T) {
	got := observedTargetState(&targetEntry{}, 10*time.Second, targetProgressState{})

	assert.Equal(t, targetProgressState{}, got)
}

// The pre-existing path is untouched: an entry that has committed a
// grain inside the window still reports Ready.
func TestObservedTargetState_RecentCommit_StillReady(t *testing.T) {
	e := openedEntry(time.Minute)
	now := time.Now()
	e.lastCommitAt.Store(&now)

	got := observedTargetState(e, 10*time.Second, targetProgressState{})

	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorReady, got.phase)
	assert.Equal(t, metav1.ConditionTrue, got.status)
}

// And an entry that flowed and then stalled still demotes through the
// original lastCommitAt path rather than the new open-time one.
func TestObservedTargetState_StalledAfterCommit_DemotesOnCommitAge(t *testing.T) {
	e := openedEntry(time.Hour)
	stalled := time.Now().Add(-time.Minute)
	e.lastCommitAt.Store(&stalled)

	got := observedTargetState(e, 10*time.Second, targetProgressState{})

	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorDegraded, got.phase)
	assert.Equal(t, mxlv1alpha1.ReasonNoGrains, got.reason)
	assert.NotNil(t, got.lastCommitAt,
		"the stall path reports when the last grain landed; the "+
			"never-flowed path has no such timestamp to offer")
}
