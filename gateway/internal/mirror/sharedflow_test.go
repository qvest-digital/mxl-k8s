package mirror

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

// sharedFlowVideoDef is a minimal v210 flow libmxl accepts.
const sharedFlowVideoDef = `{
  "description": "mirror shared-flow test",
  "id": "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d34",
  "format": "urn:x-nmos:format:video",
  "label": "mirror shared-flow test",
  "tags": { "urn:x-nmos:tag:grouphint/v1.0": ["mirror shared-flow test:Video"] },
  "parents": [],
  "media_type": "video/v210",
  "grain_rate": { "numerator": 50, "denominator": 1 },
  "frame_width": 1920,
  "frame_height": 1080,
  "interlace_mode": "progressive",
  "colorspace": "BT709",
  "components": [
    { "name": "Y",  "width": 1920, "height": 1080, "bit_depth": 10 },
    { "name": "Cb", "width": 960,  "height": 1080, "bit_depth": 10 },
    { "name": "Cr", "width": 960,  "height": 1080, "bit_depth": 10 }
  ]
}`

// sharedFlowAudioDef is a minimal float32 audio flow libmxl accepts.
const sharedFlowAudioDef = `{
  "description": "mirror shared-flow test audio",
  "format": "urn:x-nmos:format:audio",
  "label": "mirror shared-flow test audio",
  "id": "8c3e0b5f-6a1d-4f2e-b7c9-1d4a2e6f8b03",
  "tags": { "urn:x-nmos:tag:grouphint/v1.0": ["mirror shared-flow test:Audio"] },
  "media_type": "audio/float32",
  "sample_rate": { "numerator": 48000 },
  "channel_count": 2,
  "bit_depth": 32,
  "parents": []
}`

// twoWriters opens the same flow twice in one instance, the way two
// mirrors of one flow into one node do.
func twoWriters(t *testing.T, def string) (*mxl.Writer, *mxl.Writer) {
	t.Helper()
	inst, err := mxl.NewInstance(t.TempDir(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	open := func() *mxl.Writer {
		w, _, err := inst.NewWriter(def)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	return open(), open()
}

// The fix rests on libmxl handing every writer of a flow in an instance
// the same underlying writer: a grain one has committed is already
// committed for the other. This pins that, and that the rejection is
// told apart from a real failure.
func TestCommitArrivedGrain_SecondWriterOfSameFlowSeesAlreadyCommitted(t *testing.T) {
	first, second := twoWriters(t, sharedFlowVideoDef)

	const idx = 1000
	_, err := commitArrivedGrain(first, idx)
	require.NoError(t, err)

	_, err = commitArrivedGrain(second, idx)
	require.ErrorIs(t, err, errGrainAlreadyCommitted)

	_, err = commitArrivedGrain(second, idx+1)
	require.NoError(t, err, "the next grain commits through the other writer")
	_, err = commitArrivedGrain(first, idx+1)
	require.ErrorIs(t, err, errGrainAlreadyCommitted)
}

// Two progress loops committing one flow through two writers at once
// interleave libmxl's open and commit on the one writer they share, and
// a commit then fails for a grain nobody has committed. Through
// sharedFlow the only rejection left is the grain the sibling won.
func TestSharedFlow_ConcurrentWritersOnlyEverSeeAlreadyCommitted(t *testing.T) {
	first, second := twoWriters(t, sharedFlowVideoDef)
	flow := &sharedFlow{}

	const grains = 20000
	var wg sync.WaitGroup
	var unexpected atomic.Value
	for _, w := range []*mxl.Writer{first, second} {
		wg.Add(1)
		go func(w *mxl.Writer) {
			defer wg.Done()
			for idx := uint64(1); idx <= grains; idx++ {
				if _, err := flow.commitGrain(w, idx, nil); err != nil && !errors.Is(err, errGrainAlreadyCommitted) {
					unexpected.CompareAndSwap(nil, err)
				}
			}
		}(w)
	}
	wg.Wait()
	if err, _ := unexpected.Load().(error); err != nil {
		t.Fatalf("commit failed for a reason other than a sibling's commit: %v", err)
	}
}

// Every entry writing one flow through one libmxl instance has to reach
// the same sharedFlow, or their loops keep driving the shared writer
// concurrently. Another instance is another writer.
func TestSharedFlowFor_OnePerFlowAndInstance(t *testing.T) {
	r := &TargetReconciler{}
	h1, h2 := &instance.Handles{}, &instance.Handles{}
	a, b := [16]byte{1}, [16]byte{2}

	assert.Same(t, r.sharedFlowFor(h1, a), r.sharedFlowFor(h1, a))
	assert.NotSame(t, r.sharedFlowFor(h1, a), r.sharedFlowFor(h1, b))
	assert.NotSame(t, r.sharedFlowFor(h1, a), r.sharedFlowFor(h2, a))
}

// A grain a sibling committed moments ago arrived here as well, so it
// counts as progress for this mirror, at the sibling's commit time. A
// mirror that only ever loses the commit would otherwise read as
// receiving nothing and be rebuilt.
func TestSharedFlow_GrainASiblingJustCommittedCountsAsArrival(t *testing.T) {
	first, second := twoWriters(t, sharedFlowVideoDef)
	flow := &sharedFlow{}
	tracker := &recordingCommitTracker{}

	_, err := flow.commitGrain(first, 10, nil)
	require.NoError(t, err)
	committedAt := flow.lastCommitAt

	_, err = flow.commitGrain(second, 10, tracker)
	require.ErrorIs(t, err, errGrainAlreadyCommitted)
	assert.Equal(t, []uint64{10}, tracker.snapshot())
	assert.Equal(t, committedAt, flow.lastCommitAt, "a duplicate does not advance the flow")
}

// An arrival at or behind a head nobody has moved for a while is not a
// sibling's grain but a source that went backwards. Counting it would
// keep the mirror Ready with a frozen head and reset its rebuild budget,
// so it must not reach the tracker.
func TestSharedFlow_StaleAlreadyCommittedArrivalIsNotProgress(t *testing.T) {
	first, second := twoWriters(t, sharedFlowVideoDef)
	flow := &sharedFlow{}
	tracker := &recordingCommitTracker{}

	_, err := flow.commitGrain(first, 10, nil)
	require.NoError(t, err)
	flow.lastCommitAt = time.Now().Add(-2 * siblingCommitFreshness)

	_, err = flow.commitGrain(second, 10, tracker)
	require.ErrorIs(t, err, errGrainAlreadyCommitted)
	assert.Empty(t, tracker.snapshot())
}

// A sample run behind the committed head is a retransmission or a
// sibling's run: dropped, and counted when the sibling's commit is
// fresh. A run libmxl refuses for another reason -- here one longer
// than the ring allows -- is a real failure and must neither be
// labelled already committed nor counted as progress.
func TestSharedFlow_SamplesClassifyOnlyRunsBehindTheHeadAsCommitted(t *testing.T) {
	first, second := twoWriters(t, sharedFlowAudioDef)
	flow := &sharedFlow{}
	tracker := &recordingCommitTracker{}

	_, err := flow.commitSamples(first, 960, 480, nil)
	require.NoError(t, err)

	_, err = flow.commitSamples(second, 960, 480, tracker)
	require.ErrorIs(t, err, errSamplesAlreadyCommitted, "the sibling's run")
	_, err = flow.commitSamples(second, 1200, 480, tracker)
	require.ErrorIs(t, err, errSamplesAlreadyCommitted, "a run overlapping the committed one")
	assert.Equal(t, []uint64{960, 1200}, tracker.snapshot())

	max, err := second.GetMaxWriteLengthSamples()
	require.NoError(t, err)
	tooLong := int(max) * 4
	_, err = flow.commitSamples(second, 960+uint64(tooLong), tooLong, tracker)
	require.Error(t, err)
	assert.NotErrorIs(t, err, errSamplesAlreadyCommitted)
	assert.Equal(t, []uint64{960, 1200}, tracker.snapshot(), "a rejected run is not progress")

	_, err = flow.commitSamples(second, 1440, 480, nil)
	require.NoError(t, err, "the next run commits through the other writer")
}

// A grain the commit function reports as already committed must neither
// be logged as a commit failure nor stall or kill the loop. Counting it
// is the commit function's decision, so the loop hands only its own
// commits to the tracker.
func TestRunTargetProgressLoop_AlreadyCommittedGrainIsDroppedNotFatal(t *testing.T) {
	var seq atomic.Int32
	read := func() (uint64, error) {
		switch n := seq.Add(1); {
		case n <= 3:
			return uint64(100 + n), nil
		default:
			return 0, fabrics.ErrNotReady
		}
	}
	var mu sync.Mutex
	var committed []uint64
	commit := func(idx uint64) error {
		if idx == 102 {
			return fmt.Errorf("OpenGrain(%d): %w: %w", idx, errGrainAlreadyCommitted, mxl.ErrInvalidArg)
		}
		mu.Lock()
		committed = append(committed, idx)
		mu.Unlock()
		return nil
	}
	tracker := &recordingCommitTracker{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, nil, commit,
		func() { t.Error("an already-committed grain must not be fatal") }, tracker)

	require.Eventually(t, func() bool { return len(tracker.snapshot()) == 2 },
		2*time.Second, time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, []uint64{101, 103}, tracker.snapshot())
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint64{101, 103}, committed)
}

// The tracked head outlives the libmxl writer. After the source rewinds
// and the entry is rebuilt on a fresh writer, libmxl accepts the lower
// index again, and the stale head must not refuse it: that would leave
// the mirror rebuilding forever with nothing committed.
func TestSharedFlow_SamplesCommitAfterRewindOnAFreshWriter(t *testing.T) {
	inst, err := mxl.NewInstance(t.TempDir(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	flow := &sharedFlow{}

	old, _, err := inst.NewWriter(sharedFlowAudioDef)
	require.NoError(t, err)
	_, err = flow.commitSamples(old, 1_000_000, 480, nil)
	require.NoError(t, err)
	require.NoError(t, old.Close())

	fresh, _, err := inst.NewWriter(sharedFlowAudioDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	_, err = flow.commitSamples(fresh, 1440, 480, nil)
	require.NoError(t, err)
	_, err = flow.commitSamples(fresh, 1440, 480, nil)
	require.ErrorIs(t, err, errSamplesAlreadyCommitted, "the head follows the rewind")
}
