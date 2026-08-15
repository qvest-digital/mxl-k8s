package mirror

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/mxl"
)

// rate50 is 50 grains per second, a 20 ms grain interval.
var rate50 = mxl.Rational{Num: 50, Den: 1}

func TestNewPacer_WindowIsFractionOfGrainInterval(t *testing.T) {
	p := newPacer(rate50, 0.5, 8, "flow")
	require.True(t, p.enabled())
	assert.Equal(t, 10*time.Millisecond, p.window,
		"half of a 20ms grain interval")
}

func TestNewPacer_DisabledInputs(t *testing.T) {
	// A negative fraction is how pacing is turned off; the rest are
	// inputs libmxl can legitimately hand back for a flow whose rate
	// is not usable, and must degrade to whole-grain transfers rather
	// than to a divide-by-zero or a zero-length sleep loop.
	for name, p := range map[string]pacer{
		"negative fraction": newPacer(rate50, -1, 8, "f"),
		"zero fraction":     newPacer(rate50, 0, 8, "f"),
		"one chunk":         newPacer(rate50, 0.5, 1, "f"),
		"zero rate num":     newPacer(mxl.Rational{Num: 0, Den: 1}, 0.5, 8, "f"),
		"zero rate den":     newPacer(mxl.Rational{Num: 50, Den: 0}, 0.5, 8, "f"),
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, p.enabled())
			plan := p.plan(1080)
			require.Len(t, plan, 1, "a disabled pacer hands the grain over whole")
			assert.Equal(t, sliceChunk{start: 0, end: 1080, due: 0}, plan[0])
		})
	}
}

func TestPacerPlan_CoversEverySliceExactlyOnce(t *testing.T) {
	// Gaps would leave lines unwritten at the target and overlaps would
	// resend them; either way the valid-slice count the target gates its
	// commit on would never reach total.
	for _, total := range []uint16{2, 7, 8, 9, 1080, 1125} {
		p := newPacer(rate50, 0.5, 8, "flow")
		plan := p.plan(total)

		require.NotEmpty(t, plan)
		assert.Equal(t, uint16(0), plan[0].start, "must start at slice 0")
		assert.Equal(t, total, plan[len(plan)-1].end,
			"last chunk must end on the total so the target sees a complete grain")
		for i := 1; i < len(plan); i++ {
			assert.Equalf(t, plan[i-1].end, plan[i].start,
				"total=%d: chunk %d must resume where %d ended", total, i, i-1)
		}
		for _, c := range plan {
			assert.Lessf(t, c.start, c.end, "total=%d: empty range %+v", total, c)
		}
	}
}

func TestPacerPlan_SpreadsChunksAcrossTheWindow(t *testing.T) {
	p := newPacer(rate50, 0.5, 8, "flow")
	plan := p.plan(1080)
	require.Len(t, plan, 8)

	// Due times rise by a constant chunk period, and the last chunk is
	// released within the window rather than after it: a grain whose
	// transmission ran past its own interval would collide with the
	// next one.
	period := p.window / 8
	for i, c := range plan {
		assert.Equal(t, p.offset+time.Duration(i)*period, c.due)
	}
	assert.Less(t, plan[len(plan)-1].due, p.window+p.offset)
}

func TestPacerPlan_ChunksCannotExceedSlices(t *testing.T) {
	// Asking for 8 chunks of a 3-slice grain must not produce empty
	// ranges; libmxl-fabrics would reject start == end.
	p := newPacer(rate50, 0.5, 8, "flow")
	plan := p.plan(3)
	assert.Len(t, plan, 3)
}

func TestPacerPlan_SingleSliceGrainTakesTheWholeGrainFastPath(t *testing.T) {
	// libmxl-fabrics writes a whole grain in one RMA operation but a
	// partial range in one per plane, so a grain that cannot be split
	// must still be handed over as [0, total).
	p := newPacer(rate50, 0.5, 8, "flow")
	plan := p.plan(1)
	require.Len(t, plan, 1)
	assert.Equal(t, sliceChunk{start: 0, end: 1, due: 0}, plan[0])
}

func TestNewPacer_PhaseOffsetDiffersPerFlowAndStaysInsideAChunkPeriod(t *testing.T) {
	// Flows sharing an edit rate advance their heads on the same MXL
	// clock instants. Without distinct offsets every mirror on a node
	// would release its chunks simultaneously, which is the alignment
	// the pacing is meant to break up.
	p := newPacer(rate50, 0.5, 8, "flow-a")
	period := p.window / 8

	seen := map[time.Duration]int{}
	for _, key := range []string{"flow-a", "flow-b", "flow-c", "flow-d", "flow-e", "flow-f"} {
		o := newPacer(rate50, 0.5, 8, key).offset
		assert.GreaterOrEqual(t, o, time.Duration(0))
		assert.Less(t, o, period,
			"an offset beyond one chunk period would delay the grain, not stagger it")
		seen[o]++
	}
	assert.Greater(t, len(seen), 1, "distinct flows must not share one offset")

	// Same key, same offset: the schedule has to be stable across
	// mirror rebuilds, not random per process.
	assert.Equal(t, p.offset, newPacer(rate50, 0.5, 8, "flow-a").offset)
}

func TestNewPacer_SameFlowToDifferentTargetsGetsDifferentOffsets(t *testing.T) {
	a := newPacer(rate50, 0.5, 8, "flow-a\x00target-1").offset
	b := newPacer(rate50, 0.5, 8, "flow-a\x00target-2").offset
	assert.NotEqual(t, a, b)
}

// fakeClock records what a schedule would have waited for without
// spending it.
type fakeClock struct {
	t     time.Time
	slept []time.Duration
	err   error
}

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) sleepUntil(_ context.Context, t time.Time) error {
	if c.err != nil {
		return c.err
	}
	c.slept = append(c.slept, t.Sub(c.t))
	return nil
}

func TestTransferPaced_TransfersEveryChunkAndWaitsItsDueTime(t *testing.T) {
	p := newPacer(rate50, 0.5, 4, "flow")
	plan := p.plan(100)
	clk := &fakeClock{t: time.Unix(0, 0)}

	type call struct {
		idx        uint64
		start, end uint16
	}
	var calls []call
	var between int

	err := transferPaced(context.Background(), clk, plan, 42,
		func(idx uint64, s, e uint16) error {
			calls = append(calls, call{idx, s, e})
			return nil
		},
		func() { between++ })
	require.NoError(t, err)

	require.Len(t, calls, 4)
	for i, c := range calls {
		assert.Equal(t, uint64(42), c.idx)
		assert.Equal(t, plan[i].start, c.start)
		assert.Equal(t, plan[i].end, c.end)
	}

	// Deadlines are absolute against one start instant, so a slow chunk
	// cannot push the ones after it.
	require.Len(t, clk.slept, 4)
	for i, d := range clk.slept {
		assert.Equal(t, plan[i].due, d)
	}

	assert.Equal(t, 3, between,
		"progress runs between chunks, not after the last one")
}

func TestTransferPaced_StopsOnTransferError(t *testing.T) {
	p := newPacer(rate50, 0.5, 4, "flow")
	plan := p.plan(100)
	clk := &fakeClock{t: time.Unix(0, 0)}
	boom := errors.New("boom")

	var calls int
	err := transferPaced(context.Background(), clk, plan, 1,
		func(uint64, uint16, uint16) error {
			calls++
			if calls == 2 {
				return boom
			}
			return nil
		}, nil)

	require.ErrorIs(t, err, boom)
	assert.Equal(t, 2, calls, "a failed chunk must not be followed by more")
}

func TestTransferPaced_StopsWhenContextIsCanceled(t *testing.T) {
	// A mirror torn down mid-grain must not keep enqueueing into an
	// initiator the reconciler is about to close. The plan is spelled
	// out rather than taken from a pacer so it is clear which chunk the
	// cancel is expected to land on.
	for name, tc := range map[string]struct {
		plan  []sliceChunk
		calls int
	}{
		"cancel before the first chunk when a phase offset delays it": {
			plan: []sliceChunk{
				{start: 0, end: 50, due: time.Millisecond},
				{start: 50, end: 100, due: 3 * time.Millisecond},
			},
			calls: 0,
		},
		"cancel on the wait after an immediately-due first chunk": {
			plan: []sliceChunk{
				{start: 0, end: 50, due: 0},
				{start: 50, end: 100, due: 2 * time.Millisecond},
			},
			calls: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0), err: context.Canceled}
			var calls int
			err := transferPaced(context.Background(), clk, tc.plan, 1,
				func(uint64, uint16, uint16) error {
					calls++
					return nil
				}, nil)

			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, tc.calls, calls)
		})
	}
}

func TestTransferPaced_RealClockDoesNotWaitOnAZeroDueChunk(t *testing.T) {
	// The unpaced plan has a single chunk due at 0. Driving it through
	// the real clock must not block, or a disabled pacer would still
	// cost a scheduler round trip per grain.
	start := time.Now()
	err := transferPaced(context.Background(), realClock{},
		[]sliceChunk{{start: 0, end: 10, due: 0}}, 1,
		func(uint64, uint16, uint16) error { return nil }, nil)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 5*time.Millisecond)
}
