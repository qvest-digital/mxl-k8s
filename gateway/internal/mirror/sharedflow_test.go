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
	_, err := commitArrivedGrain(first, idx, nil)
	require.NoError(t, err)

	_, err = commitArrivedGrain(second, idx, nil)
	require.ErrorIs(t, err, errGrainAlreadyCommitted)

	_, err = commitArrivedGrain(second, idx+1, nil)
	require.NoError(t, err, "the next grain commits through the other writer")
	_, err = commitArrivedGrain(first, idx+1, nil)
	require.ErrorIs(t, err, errGrainAlreadyCommitted)
}

const sharedFlowDataDef = `{
  "description": "mirror shared-flow test",
  "id": "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d35",
  "format": "urn:x-nmos:format:data",
  "label": "mirror shared-flow test",
  "parents": [],
  "tags": {"urn:x-nmos:tag:grouphint/v1.0": ["mirror-test:Data"]},
  "media_type": "video/smpte291",
  "grain_rate": {"numerator": 50, "denominator": 1}
}`

// A data grain's valid slices are its length, and the initiator's write
// sets them in the slot's header before the target commits. A commit
// with the grain's total slices turned every ANC packet into a full
// grain of trailing garbage on the target.
func TestCommitArrivedGrain_DataGrainKeepsItsLength(t *testing.T) {
	w, _ := twoWriters(t, sharedFlowDataDef)
	const idx = 1000
	// What the initiator's write leaves in the slot: the header of idx
	// with 64 valid slices. A partial commit writes the same header.
	ga, err := w.OpenGrain(idx)
	require.NoError(t, err)
	require.NoError(t, ga.Commit(64, 0))

	_, err = commitArrivedGrain(w, idx, nil)
	require.NoError(t, err)

	g, err := w.GrainInfo(idx)
	require.NoError(t, err)
	require.Equal(t, uint16(64), g.ValidSlices)
}

// Gateways of two versions mirror a data flow. A source from before data
// grains went by their length reads with GetGrainNonBlocking, which
// hands out no data grain short of all its slices, so it never sends a
// target a short one; this source reads what the grain holds. A target
// from before commits only a complete grain, so a short grain from this
// source is never committed there at the length of a full one.
func TestDataGrainAcrossGatewayVersions(t *testing.T) {
	inst, err := mxl.NewInstance(t.TempDir(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	w, _, err := inst.NewWriter(sharedFlowDataDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	r, err := inst.NewReader("5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d35")
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	const idx = 1000
	ga, err := w.OpenGrain(idx)
	require.NoError(t, err)
	require.NoError(t, ga.Commit(64, 0))

	_, err = r.GetGrainNonBlocking(idx)
	require.Error(t, err, "the older source's read never returns a short data grain")

	g, err := r.GetGrainSliceNonBlocking(idx, 1)
	require.NoError(t, err)
	require.Equal(t, uint16(64), g.ValidSlices)
	require.Less(t, g.ValidSlices, g.TotalSlices)
	require.False(t, g.Complete(), "an older target leaves a short grain uncommitted")
}

// A data writer commits a grain with nothing in it, as an ANC writer
// does for a frame with no packets. The source must read and send it,
// header only, or its transfer loop retries it until it resyncs past it
// and drops the grains behind it; the target must commit it empty, not
// as a full grain of whatever the slot held.
func TestEmptyDataGrainIsMirroredEmpty(t *testing.T) {
	inst, err := mxl.NewInstance(t.TempDir(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	w, _, err := inst.NewWriter(sharedFlowDataDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	r, err := inst.NewReader("5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d35")
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	const idx = 1000
	ga, err := w.OpenGrain(idx)
	require.NoError(t, err)
	require.NoError(t, ga.Commit(0, 0))

	g, err := readForTransfer(r, idx, true, mxl.GrainValidSlicesAll)
	require.NoError(t, err, "the source reads an empty data grain")
	require.Zero(t, g.ValidSlices)

	// What the initiator's header-only write leaves in the target's
	// slot is the same header, as in the test above.
	_, err = commitArrivedGrain(w, idx, nil)
	require.NoError(t, err)
	got, err := w.GrainInfo(idx)
	require.NoError(t, err)
	require.Zero(t, got.ValidSlices, "committed as the full grain")
}

// A video grain sent in slice batches is committed with each batch's
// valid slices, as the target in tools/mxl-fabrics-demo/demo.cpp commits
// the grain info it opens, so readers see the first lines before the
// rest lands; the batch carrying the last slice completes the grain.
func TestCommitArrivedGrain_SliceBatchesCommitAsTheyLand(t *testing.T) {
	dir := t.TempDir()
	newWriter := func() *mxl.Writer {
		inst, err := mxl.NewInstance(dir, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = inst.Close() })
		w, _, err := inst.NewWriter(sharedFlowVideoDef)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	// The target's writer, and one of another instance on the same flow
	// that writes the slot header as the initiator's batches do.
	target, initiator := newWriter(), newWriter()
	const idx = 1000
	land := func(valid uint16) {
		t.Helper()
		ga, err := initiator.OpenGrain(idx)
		require.NoError(t, err)
		require.NoError(t, ga.Commit(valid, 0))
	}
	ga, err := initiator.OpenGrain(idx)
	require.NoError(t, err)
	total := ga.TotalSlices
	require.NoError(t, ga.Cancel())

	land(270)
	c, err := commitArrivedGrain(target, idx, nil)
	n, reason := c.bytes, c.reason
	require.NoError(t, err)
	assert.Empty(t, reason)
	assert.Zero(t, n, "the payload is counted with the last batch")
	g, err := target.GrainInfo(idx)
	require.NoError(t, err)
	assert.Equal(t, uint16(270), g.ValidSlices, "the first batch is readable as it landed")

	land(total)
	c, err = commitArrivedGrain(target, idx, nil)
	n = c.bytes
	require.NoError(t, err, "a grain the target has not completed is reopened for its next batch")
	assert.Positive(t, n)
	g, err = target.GrainInfo(idx)
	require.NoError(t, err)
	assert.True(t, g.Complete(), "the last batch completes the grain")

	_, err = commitArrivedGrain(target, idx, nil)
	require.ErrorIs(t, err, errGrainAlreadyCommitted, "a completed grain is not reopened")
}

// Two mirrors of a flow into this node write one slot, and
// libmxl-fabrics sets its valid slices from each arrival's range. A
// sibling's earlier batch landing after a later one lowers them; the
// commit of that arrival keeps what the flow already committed.
func TestSharedFlow_ASiblingsEarlierBatchDoesNotLowerAnOpenGrain(t *testing.T) {
	dir := t.TempDir()
	newWriter := func() *mxl.Writer {
		inst, err := mxl.NewInstance(dir, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = inst.Close() })
		w, _, err := inst.NewWriter(sharedFlowVideoDef)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	target, initiator := newWriter(), newWriter()
	flow := &sharedFlow{}
	const idx = 1000
	land := func(valid uint16) {
		t.Helper()
		ga, err := initiator.OpenGrain(idx)
		require.NoError(t, err)
		require.NoError(t, ga.Commit(valid, 0))
	}

	land(540)
	_, err := flow.commitGrain(target, idx, nil)
	require.NoError(t, err)
	land(270) // the sibling's first batch, late
	_, err = flow.commitGrain(target, idx, nil)
	require.NoError(t, err)

	g, err := target.GrainInfo(idx)
	require.NoError(t, err)
	assert.Equal(t, uint16(540), g.ValidSlices, "a sibling's earlier batch lowered the committed slices")
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
	go runTargetProgressLoop(ctx, done, read, commit,
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

// A source that resyncs to the head or finds its reader aged out sends
// none of the indices it passes over, so the target never receives them.
// Committing the next arrival opens past them, and libmxl's writer flags
// each skipped index invalid (openGrain in PosixDiscreteFlowWriter.cpp),
// which is what readers on the target see.
func TestCommitArrivedGrain_AnIndexNotSentIsFlaggedInvalid(t *testing.T) {
	dir := t.TempDir()
	newWriter := func() *mxl.Writer {
		inst, err := mxl.NewInstance(dir, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = inst.Close() })
		w, _, err := inst.NewWriter(sharedFlowVideoDef)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	// The target's writer, and one writer per arrival standing in for
	// the initiator's header writes, so none of them skips an index.
	target := newWriter()
	land := func(idx uint64) {
		t.Helper()
		ga, err := newWriter().OpenGrain(idx)
		require.NoError(t, err)
		require.NoError(t, ga.Commit(ga.TotalSlices, 0))
	}
	land(1000)
	_, err := commitArrivedGrain(target, 1000, nil)
	require.NoError(t, err)
	land(1002)
	_, err = commitArrivedGrain(target, 1002, nil)
	require.NoError(t, err)

	g, err := target.GrainInfo(1001)
	require.NoError(t, err)
	assert.Equal(t, uint64(1001), g.Index)
	assert.True(t, g.Invalid(), "the index the source did not send is not flagged invalid")
	assert.Zero(t, g.ValidSlices)
}
