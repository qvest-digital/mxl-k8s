package mirror

import (
	"context"
	"fmt"
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
	mu      sync.Mutex
	reads   map[string]uint64
	content map[ContentIssue]uint64
	skipped uint64
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

// sums returns copies of what was recorded.
func (r *recordingSource) sums() (reads map[string]uint64, content map[ContentIssue]uint64, skipped uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reads, content = map[string]uint64{}, map[ContentIssue]uint64{}
	for k, v := range r.reads {
		reads[k] = v
	}
	for k, v := range r.content {
		content[k] = v
	}
	return reads, content, r.skipped
}

func (rt *recordingTracker) source() SourceRecorder { return &rt.rec }

// A resync jumps to the head without a read that came back too late,
// so the library records nothing for the grains it passes over.
func TestResyncRecordsNothingForTheGrainsItPassesOver(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11, 20}}
	link := &fakeLink{t: t}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Hour, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 2 }, time.Second, time.Millisecond)
	stop()

	reads, content, skipped := tr.rec.sums()
	assert.Empty(t, content)
	assert.Zero(t, skipped)
	assert.Equal(t, uint64(2), reads["ok"], "11 and 20")
	assert.NotContains(t, reads, mxl.ErrOutOfRangeLate.Error())
}

// A read that comes back too late is recorded with its own error, and
// the grains the loop then jumps over to the head are skipped.
func TestAgedOutReadIsTooLateAndSkips(t *testing.T) {
	h := &headScript{heads: []uint64{10, 14}}
	link := &fakeLink{t: t}
	tooLate := fmt.Errorf("GetGrainSlice(11): %w", mxl.ErrOutOfRangeLate)
	var once atomic.Bool
	transfer := func(idx uint64, start uint16) (grainStep, error) {
		if idx > 14 {
			time.Sleep(time.Millisecond)
			return grainStep{}, mxl.ErrTimeout
		}
		if once.CompareAndSwap(false, true) {
			return grainStep{}, tooLate
		}
		_, err := link.transferGrain(idx)
		return grainStep{bytes: 1}, err
	}
	tr := &recordingTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", h.probe, transfer, link.makeProgress, link.makeProgress,
		time.Millisecond, time.Hour, tr)
	require.Eventually(t, func() bool { _, _, sk := tr.rec.sums(); return sk > 0 }, time.Second, time.Millisecond)
	cancel()
	<-done

	reads, _, skipped := tr.rec.sums()
	assert.Equal(t, uint64(4), skipped, "11-14")
	assert.Equal(t, uint64(1), reads[tooLate.Error()])
}

// A grain counts as read once, with the read that completes it; one
// flagged invalid at the source went as its header and is the writer's.
func TestReadIsCountedPerGrain(t *testing.T) {
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
		time.Millisecond, time.Hour, tr)
	require.Eventually(t, func() bool { r, _, _ := tr.rec.sums(); return r["ok"] == 2 }, time.Second, time.Millisecond)
	cancel()
	<-done

	reads, content, _ := tr.rec.sums()
	assert.Equal(t, map[ContentIssue]uint64{ContentInvalid: 1}, content, "12")
	assert.Equal(t, uint64(2), reads["ok"], "11 and 13, once each")
}

// A sample batch counts its samples. Samples the loop leaves behind
// the readable window were never read, so the library records nothing
// for them.
func TestSampleLoopAccountsInSamples(t *testing.T) {
	h := &headScript{heads: []uint64{0, 480, 10000}}
	link := &fakeSampleLink{t: t}
	tr := &recordingTracker{}
	_, stop := runSampleLoop(h.probe, link.transferSamples, link.makeProgress, 480, 2000, time.Hour, tr)
	require.Eventually(t, func() bool { r, _, _ := tr.rec.sums(); return r["ok"] >= 3*480 }, time.Second, time.Millisecond)
	stop()

	reads, _, skipped := tr.rec.sums()
	assert.Zero(t, skipped)
	assert.Zero(t, reads["ok"]%480)
	assert.Len(t, reads, 1, "only OK reads")
}

// recordingTarget is a TargetRecorder that sums what it is told.
type recordingTarget struct {
	writes  map[string]uint64
	content map[ContentIssue]uint64
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

func TestRecordGrainCommit(t *testing.T) {
	rec := &recordingTarget{}
	recordGrainCommit(rec, 10, grainCommit{done: true}, nil)
	recordGrainCommit(rec, 11, grainCommit{}, nil) // a batch short of complete
	recordGrainCommit(rec, 12, grainCommit{done: true, reason: invalidFromSource}, nil)
	recordGrainCommit(rec, 13, grainCommit{done: true, reason: invalidIndexMismatch}, nil)
	recordGrainCommit(rec, 14, grainCommit{}, errGrainAlreadyCommitted)
	recordGrainCommit(rec, 19, grainCommit{}, fabrics.ErrNotReady)

	assert.Equal(t, map[string]uint64{"ok": 2, "error": 1}, rec.writes, "10, and 14 which a sibling committed")
	assert.Equal(t, map[ContentIssue]uint64{ContentInvalid: 1, ContentMissing: 1}, rec.content)
}

func TestRecordSampleCommit(t *testing.T) {
	rec := &recordingTarget{}
	recordSampleCommit(rec, 960, 480, nil)
	recordSampleCommit(rec, 1440, 480, errSamplesAlreadyCommitted)
	recordSampleCommit(rec, 1920, 480, fabrics.ErrNotReady)
	assert.Equal(t, map[string]uint64{"ok": 960, "error": 1}, rec.writes)
}
