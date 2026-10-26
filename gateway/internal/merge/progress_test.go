package merge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A producer whose pod runs on but commits nothing keeps every object of
// its flow as it was; its head standing still is the only sign. A grain
// ring's head moves a grain at a time, a channel buffer's by the samples
// of a batch, and a late discrete commit steps it back: each is a move.
// Each change is reported once, for the gateway to write it once.
func TestProgressFindsAMemberWhoseHeadStandsStill(t *testing.T) {
	t0 := time.Unix(1000, 0)
	p := newProgress([]uint64{100, 48000}, t0, StallAfter)
	audio := func(i int) uint64 { return 48000 + uint64(i)*480 }
	at := func(i int) time.Time { return t0.Add(time.Duration(i) * 40 * time.Millisecond) }

	// Both move once, then the grain member stands still.
	require.True(t, p.observe([]uint64{101, audio(1)}, at(1)), "a member that moved was not decided")
	n := int(StallAfter/(40*time.Millisecond)) + 1
	for i := 2; i < n; i++ {
		require.False(t, p.observe([]uint64{101, audio(i)}, at(i)), "a member counted as stalled before its window")
	}
	stalled, undecided := p.stalled()
	require.Empty(t, stalled)
	require.Empty(t, undecided)

	require.True(t, p.observe([]uint64{101, audio(n)}, at(n)))
	stalled, _ = p.stalled()
	require.Equal(t, []int{0}, stalled, "the grain member stood still, the audio one moved")
	require.False(t, p.observe([]uint64{101, audio(n + 1)}, at(n+1)), "an unchanged stall was reported again")

	require.True(t, p.observe([]uint64{99, audio(n + 2)}, at(n+2)))
	stalled, _ = p.stalled()
	require.Empty(t, stalled, "a head stepped back was not counted as a move")

	later := at(n + 2).Add(StallAfter)
	require.True(t, p.observe([]uint64{99, audio(n + 2)}, later))
	stalled, _ = p.stalled()
	require.Equal(t, []int{0, 1}, stalled, "an audio member whose samples stopped was not found")
}

// An engine is started at every set start, node rejoin and gateway
// restart, over members that may be placeholders a mirror fills only
// seconds later. A member whose head has not moved since is not judged
// within StartGrace, and is stalled once StartGrace passes without a
// move: a producer that never commits is still found.
func TestProgressGivesAMemberThatHasNotMovedYetStartGrace(t *testing.T) {
	t0 := time.Unix(1000, 0)
	p := newProgress([]uint64{0, 0}, t0, StallAfter)

	require.True(t, p.observe([]uint64{0, 5}, t0.Add(6*time.Second)), "a member that moved was not decided")
	stalled, undecided := p.stalled()
	require.Empty(t, stalled, "a placeholder not filled yet was found stalled")
	require.Equal(t, []int{0}, undecided)

	require.True(t, p.observe([]uint64{0, 5}, t0.Add(StartGrace)))
	stalled, undecided = p.stalled()
	require.Equal(t, []int{0, 1}, stalled, "a member that never moved was not found stalled after the grace")
	require.Empty(t, undecided)
}

// A grain period longer than StallAfter would leave a slow flow stalled
// between two of its own commits.
func TestStallWindowCoversSlowFlows(t *testing.T) {
	require.Equal(t, StallAfter, stallWindow(20*time.Millisecond))
	require.Equal(t, StallAfter, stallWindow(0), "audio, which has no grain period")
	require.Equal(t, 3*time.Second, stallWindow(time.Second))
}

// A data flow's members are not judged: its head moves only when an
// ancillary packet arrives, seconds apart on a live source. Its engine
// has no progress, and its loop must run on.
func TestProgressOfMembersNotJudgedObservesNothing(t *testing.T) {
	var p *progress
	require.False(t, p.observe([]uint64{1, 2}, time.Now()))
	stalled, undecided := p.stalled()
	require.Empty(t, stalled)
	require.Empty(t, undecided)
}
