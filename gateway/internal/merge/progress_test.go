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

	// A late commit steps the head back; that is its first move.
	require.False(t, p.observe([]uint64{99, audio(n + 2)}, at(n+2)), "a stalled member moved once counted as moving")
	k := n + 2 + int(RecoverAfter/(40*time.Millisecond))
	for i := n + 3; i < k; i++ {
		require.False(t, p.observe([]uint64{uint64(97 + i - n), audio(i)}, at(i)), "moving again before RecoverAfter")
	}
	require.True(t, p.observe([]uint64{uint64(97 + k - n), audio(k)}, at(k)),
		"a head stepped back was not counted as the first move of a recovery")
	stalled, _ = p.stalled()
	require.Empty(t, stalled)

	later := at(k).Add(StallAfter)
	require.True(t, p.observe([]uint64{uint64(97 + k - n), audio(k)}, later))
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

// tick is how often the engine's loop looks at the heads at least.
const tick = 20 * time.Millisecond

// delivery drives a member through p from t0 for d, committing a grain
// at every tick for which on is true, and returns when its reported
// state changed: the first entry is its first decision.
func delivery(t *testing.T, p *progress, t0 time.Time, d time.Duration, on func(time.Duration) bool) []time.Duration {
	t.Helper()
	var changes []time.Duration
	head := uint64(0)
	for at := time.Duration(0); at <= d; at += tick {
		if on(at) {
			head++
		}
		if p.observe([]uint64{head}, t0.Add(at)) {
			changes = append(changes, at)
		}
	}
	return changes
}

// stalledNow is whether p reports its only member stalled.
func stalledNow(p *progress) bool {
	s, _ := p.stalled()
	return len(s) > 0
}

// A member is reported stalled within its window of its last commit,
// and moving within RecoverAfter of committing steadily again: both
// well inside the ten seconds MembersReady has to follow either in.
// A pause of half the window starts the recovery over.
func TestProgressHoldsAStalledMemberUntilItMovesSteadily(t *testing.T) {
	t0 := time.Unix(1000, 0)
	p := newProgress([]uint64{0}, t0, StallAfter)
	stop, resume, pause, last := 10*time.Second, 20*time.Second, 23*time.Second, 40*time.Second
	changes := delivery(t, p, t0, 50*time.Second, func(at time.Duration) bool {
		switch {
		case at >= last:
			return false
		case at < stop:
			return true
		case at < resume:
			return false
		default:
			return at < pause || at >= pause+StallAfter/2
		}
	})
	require.Equal(t, []time.Duration{
		0,                                   // decided on its first commit
		stop - tick + StallAfter,            // stalled a window after its last commit
		pause + StallAfter/2 + RecoverAfter, // moving RecoverAfter after the pause ended
		last - tick + StallAfter,            // stalled again
	}, changes)
	require.Less(t, StallAfter, 10*time.Second)
	require.Less(t, RecoverAfter, 10*time.Second)
}

// A producer behind a dead input commits a grain now and then. Each
// commit read as a recovery flipped the member at every one of them,
// 121 times in ten minutes on one set, each flip a status write of every
// gateway merging the set and one of the operator's. Such a member is
// stalled once and stays so; one that delivers on and off steadily
// enough is never stalled at all.
func TestProgressDoesNotFlipWithAMemberThatDeliversOnAndOff(t *testing.T) {
	period := func(on, off time.Duration) func(time.Duration) bool {
		return func(at time.Duration) bool { return at%(on+off) < on }
	}
	for name, tc := range map[string]struct {
		on      func(time.Duration) bool
		stalled bool
	}{
		"a grain every 2.1 s": {func(at time.Duration) bool { return at%(2100*time.Millisecond) == 0 }, true},
		"1 s on, 3 s off":     {period(time.Second, 3*time.Second), true},
		"1 s on, 1 s off":     {period(time.Second, time.Second), false},
		"stalled first, then 1 s on, 1 s off": {func(at time.Duration) bool {
			return at < tick || at >= 5*time.Second && at%(2*time.Second) < time.Second
		}, true},
	} {
		t.Run(name, func(t *testing.T) {
			p := newProgress([]uint64{0}, time.Unix(1000, 0), StallAfter)
			changes := delivery(t, p, time.Unix(1000, 0), 2*time.Minute, tc.on)
			require.NotEmpty(t, changes, "the member was never decided")
			flips := len(changes) - 1
			require.LessOrEqual(t, flips, 1, "the member flipped at %v", changes)
			require.Equal(t, tc.stalled, stalledNow(p))
		})
	}
}

// However a member delivers, it is moving no sooner than RecoverAfter
// after its first commit since it stalled, and stalled again no sooner
// than half a window after that: a moving member committed less than
// half a window ago. One member changes state at most twice in every
// RecoverAfter plus half a window.
func TestProgressBoundsHowOftenAMemberChanges(t *testing.T) {
	for _, cycle := range []struct{ on, off time.Duration }{
		{RecoverAfter + 500*time.Millisecond, StallAfter + 500*time.Millisecond},
		{RecoverAfter + tick, StallAfter + tick},
		{8 * time.Second, 3 * time.Second},
	} {
		p := newProgress([]uint64{0}, time.Unix(1000, 0), StallAfter)
		changes := delivery(t, p, time.Unix(1000, 0), 2*time.Minute, func(at time.Duration) bool {
			return at%(cycle.on+cycle.off) < cycle.on
		})
		require.GreaterOrEqual(t, len(changes), 3, "on %v, off %v: the member did not stall and recover", cycle.on, cycle.off)
		// Moving first, the member is stalled after every odd change.
		for i := 1; i < len(changes); i++ {
			least := RecoverAfter
			if i%2 == 1 {
				least = StallAfter / 2
			}
			require.GreaterOrEqual(t, changes[i]-changes[i-1], least,
				"on %v, off %v: change %d too soon: %v", cycle.on, cycle.off, i, changes)
		}
		limit := int(2*time.Minute/(RecoverAfter+StallAfter/2))*2 + 1
		require.LessOrEqual(t, len(changes), limit, "on %v, off %v: %v", cycle.on, cycle.off, changes)
	}
}

// A member that recovers delivers with gaps: a mirror catching up, a
// producer starved for a moment. A gap shorter than half the window is
// no pause, so such a member is moving once RecoverAfter passed, and
// stays so, its gaps being shorter than the window too. Here grains of
// 40 ms come for 1.3 s and then not for 0.7 s, between a quarter and
// half of the window.
func TestProgressCountsGapsShorterThanHalfTheWindowAsSteady(t *testing.T) {
	require.Greater(t, 700*time.Millisecond, StallAfter/4)
	require.Less(t, 700*time.Millisecond+tick, StallAfter/2)
	resume := StallAfter + time.Second
	p := newProgress([]uint64{0}, time.Unix(1000, 0), StallAfter)
	changes := delivery(t, p, time.Unix(1000, 0), time.Minute, func(at time.Duration) bool {
		if at < resume {
			return at == 0
		}
		phase := (at - resume) % (2 * time.Second)
		return phase < 1300*time.Millisecond && phase%(40*time.Millisecond) == 0
	})
	require.Equal(t, []time.Duration{0, StallAfter, resume + RecoverAfter}, changes)
	require.False(t, stalledNow(p))
}
