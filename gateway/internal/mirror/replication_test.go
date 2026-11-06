package mirror

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingSource is a SourceRecorder that sums what it is told.
type recordingSource struct {
	mu        sync.Mutex
	reads     map[string]uint64
	content   map[ContentIssue]uint64
	skipped   uint64
	transfers map[TransferResult]uint64
}

func (r *recordingSource) Read(err error, _, count uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reads == nil {
		r.reads = map[string]uint64{}
	}
	key := "ok"
	if err != nil {
		key = err.Error()
	}
	r.reads[key] += count
}

func (r *recordingSource) ReadContent(issue ContentIssue, _, count uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.content == nil {
		r.content = map[ContentIssue]uint64{}
	}
	r.content[issue] += count
}

func (r *recordingSource) Skipped(count uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.skipped += count
}

func (r *recordingSource) Transfer(result TransferResult, _, count uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transfers == nil {
		r.transfers = map[TransferResult]uint64{}
	}
	r.transfers[result] += count
}

// sums returns copies of what was recorded.
func (r *recordingSource) sums() (reads map[string]uint64, content map[ContentIssue]uint64, skipped uint64, transfers map[TransferResult]uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reads, content, transfers = map[string]uint64{}, map[ContentIssue]uint64{}, map[TransferResult]uint64{}
	for k, v := range r.reads {
		reads[k] = v
	}
	for k, v := range r.content {
		content[k] = v
	}
	for k, v := range r.transfers {
		transfers[k] = v
	}
	return reads, content, r.skipped, transfers
}

func (rt *recordingTracker) source() SourceRecorder { return &rt.rec }

func (rt *recordingTracker) slot(idx uint64) slotState {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if s, ok := rt.slots[idx]; ok {
		return s
	}
	return slotComplete
}

func (rt *recordingTracker) accounted() uint64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.acct
}

func (rt *recordingTracker) recordAccounted(idx uint64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.acct = idx
}

// A resync passes over grains for two reasons that look alike in its
// count: the loop fell behind (the grain was complete, a fabric-side
// loss) or the writer jumped over them (libmxl flags them invalid, the
// source's loss). Each passed-over slot is told apart by what it holds.
func TestResyncAccountsEachPassedOverGrainBySlot(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11, 20}}
	link := &fakeLink{t: t}
	tr := &recordingTracker{slots: map[uint64]slotState{
		13: slotInvalid, 14: slotTooLate, 15: slotMissing,
	}}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Hour, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 2 }, time.Second, time.Millisecond)
	stop()

	reads, content, skipped, transfers := tr.rec.sums()
	assert.Equal(t, map[TransferResult]uint64{TransferDelivered: 2, TransferDropped: 5}, transfers,
		"11 and 20 delivered; 12 and 16-19 were complete when passed over")
	assert.Equal(t, map[ContentIssue]uint64{ContentInvalid: 1, ContentMissing: 1}, content)
	assert.Equal(t, uint64(1), skipped, "14 had been reused")
	assert.Equal(t, uint64(2), reads["ok"])
	assert.Equal(t, uint64(1), reads[mxl.ErrOutOfRangeLate.Error()], "one too_late per pass-over")
}

// A grain counts once, with the transfer that completes it: delivered
// within its interval, late after it. One flagged invalid at the source
// went as its header and is the source's, not a transfer.
func TestTransferResultIsPerGrain(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		want     TransferResult
	}{
		{"within the interval", time.Hour, TransferDelivered},
		{"after it", time.Nanosecond, TransferLate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &headScript{heads: []uint64{10, 13}}
			link := &fakeLink{t: t}
			transfer := func(idx uint64, start uint16) (grainStep, error) {
				if idx > 13 {
					time.Sleep(time.Millisecond)
					return grainStep{}, mxl.ErrTimeout
				}
				if _, err := link.transferGrain(idx); err != nil {
					return grainStep{next: start}, err
				}
				if idx == 12 {
					return grainStep{invalid: true}, nil
				}
				// Every grain goes in two batches.
				if start == 0 {
					return grainStep{next: 540, bytes: 1}, nil
				}
				return grainStep{bytes: 1}, nil
			}
			tr := &recordingTracker{}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go runTransferLoop(ctx, done, "f", h.probe, transfer, link.makeProgress, link.makeProgress,
				time.Millisecond, tc.interval, tr)
			require.Eventually(t, func() bool { return tr.accounted() == 13 }, time.Second, time.Millisecond)
			cancel()
			<-done

			reads, content, _, transfers := tr.rec.sums()
			assert.Equal(t, map[TransferResult]uint64{tc.want: 2}, transfers, "11 and 13, once each")
			assert.Equal(t, map[ContentIssue]uint64{ContentInvalid: 1}, content, "12")
			assert.Equal(t, uint64(2), reads["ok"], "the read completing 11 and 13")
		})
	}
}

// A transfer outstanding when the target is lost failed. The loop built
// for the next target resumes at the head, and the grains between the
// last one accounted for and it failed too.
func TestLostTargetAndTheGapToTheRebuiltLoopFail(t *testing.T) {
	tr := &recordingTracker{}
	var calls atomic.Int32
	// Connects, then the first transfer's progress fails for good.
	failing := func() error {
		if calls.Add(1) == 1 {
			return nil
		}
		return errors.New("connection reset")
	}
	h := &headScript{heads: []uint64{10, 11}}
	link := &fakeLink{t: t}
	done, _ := runGrainLoop(h.probe, link.transferGrain, failing, time.Hour, tr)
	<-done
	_, _, _, transfers := tr.rec.sums()
	require.Equal(t, map[TransferResult]uint64{TransferFailed: 1}, transfers)
	require.Equal(t, uint64(11), tr.accounted())

	h = &headScript{heads: []uint64{20}}
	link = &fakeLink{t: t}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Hour, tr)
	require.Eventually(t, func() bool { return tr.accounted() == 20 }, time.Second, time.Millisecond)
	stop()
	_, _, _, transfers = tr.rec.sums()
	assert.Equal(t, map[TransferResult]uint64{TransferFailed: 1 + 9}, transfers, "11, then 12-20")
}

// A sample batch counts its samples; samples left behind by the
// readable window are skipped.
func TestSampleLoopAccountsInSamples(t *testing.T) {
	h := &headScript{heads: []uint64{0, 480, 10000}}
	link := &fakeSampleLink{t: t}
	tr := &recordingTracker{}
	_, stop := runSampleLoop(h.probe, link.transferSamples, link.makeProgress, 480, 2000, time.Hour, tr)
	require.Eventually(t, func() bool { return tr.accounted() >= 10000-2000+480 }, time.Second, time.Millisecond)
	stop()

	reads, _, skipped, transfers := tr.rec.sums()
	// 480 went; at head 10000 the oldest readable sample is 10000-1520.
	assert.Equal(t, uint64(10000-(2000-480)-480), skipped)
	assert.Equal(t, transfers[TransferDelivered], reads["ok"], "each delivered batch was read once")
	assert.Zero(t, transfers[TransferDelivered]%480)
	assert.GreaterOrEqual(t, transfers[TransferDelivered], uint64(3*480))
}

func TestSlotOf(t *testing.T) {
	g := func(idx uint64, flags uint32, valid uint16) mxl.Grain {
		return mxl.Grain{Index: idx, Flags: flags, TotalSlices: 1080, ValidSlices: valid}
	}
	assert.Equal(t, slotComplete, slotOf(g(7, 0, 1080), nil, 7))
	assert.Equal(t, slotInvalid, slotOf(g(7, mxl.GrainFlagInvalid, 1080), nil, 7))
	assert.Equal(t, slotIncomplete, slotOf(g(7, 0, 300), nil, 7))
	assert.Equal(t, slotMissing, slotOf(g(3, 0, 1080), nil, 7), "the slot still holds an older grain")
	assert.Equal(t, slotTooLate, slotOf(mxl.Grain{}, mxl.ErrOutOfRangeLate, 7))
}

// A pass-over longer than maxInspected counts its oldest grains as aged
// out without reading them.
func TestAccountPassedOverBoundsTheSlotsItReads(t *testing.T) {
	rec := &recordingSource{}
	var read int
	accountPassedOver(rec, func(uint64) slotState { read++; return slotComplete }, 0, maxInspected+100, TransferDropped)
	_, _, skipped, transfers := rec.sums()
	assert.Equal(t, maxInspected, read)
	assert.Equal(t, uint64(100), skipped)
	assert.Equal(t, uint64(maxInspected), transfers[TransferDropped])
}

// recordingTarget is a TargetRecorder that sums what it is told.
type recordingTarget struct {
	writes  map[string]uint64
	content map[ContentIssue]uint64
	gaps    uint64
}

func (r *recordingTarget) Write(err error, _, count uint64) {
	if r.writes == nil {
		r.writes = map[string]uint64{}
	}
	key := "ok"
	if err != nil {
		key = "error"
	}
	r.writes[key] += count
}

func (r *recordingTarget) WriteContent(issue ContentIssue, _, count uint64) {
	if r.content == nil {
		r.content = map[ContentIssue]uint64{}
	}
	r.content[issue] += count
}

func (r *recordingTarget) Gap(_, count uint64) { r.gaps += count }

func TestRecordGrainCommit(t *testing.T) {
	rec := &recordingTarget{}
	recordGrainCommit(rec, 10, grainCommit{done: true}, nil)
	recordGrainCommit(rec, 11, grainCommit{}, nil) // a batch short of complete
	recordGrainCommit(rec, 12, grainCommit{done: true, reason: invalidFromSource}, nil)
	recordGrainCommit(rec, 13, grainCommit{done: true, reason: invalidIndexMismatch}, nil)
	recordGrainCommit(rec, 14, grainCommit{}, errGrainAlreadyCommitted)
	recordGrainCommit(rec, 18, grainCommit{done: true, gap: 3}, nil)
	recordGrainCommit(rec, 19, grainCommit{}, fabrics.ErrNotReady)

	assert.Equal(t, map[string]uint64{"ok": 2, "error": 1}, rec.writes)
	assert.Equal(t, map[ContentIssue]uint64{ContentInvalid: 1, ContentMissing: 1}, rec.content)
	assert.Equal(t, uint64(3), rec.gaps)
}

// A gap is what lies between the last commit and the next one, bounded
// by the ring; going back below the last commit opens none.
func TestSharedFlowGapBelow(t *testing.T) {
	f := &sharedFlow{}
	assert.Zero(t, f.gapBelow(10, 10, 16), "nothing before the first commit")
	assert.Zero(t, f.gapBelow(11, 11, 16))
	assert.Equal(t, uint64(3), f.gapBelow(15, 15, 16))
	assert.Zero(t, f.gapBelow(12, 12, 16), "a late arrival below the last commit")
	assert.Equal(t, uint64(16), f.gapBelow(100, 100, 16), "at most the ring")
	assert.Equal(t, uint64(480), f.gapBelow(100+480+1, 100+960, 0), "samples")
}
