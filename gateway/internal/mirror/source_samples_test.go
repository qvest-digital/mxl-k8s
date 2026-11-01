package mirror

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// A not-ready TransferSamples means a target of the initiator skipped
// the batch; the others have it. The loop counts the batch as offered
// and moves on, so backpressure on one target neither holds the
// samples back from the rest nor reads as a reader that fell behind.

func TestRunSampleTransferLoop_BackpressureDoesNotReportReaderAgedOut(t *testing.T) {
	// The producer writes one chunk per tick and the send queue refuses
	// two attempts out of every three. Retrying in place clears a chunk
	// per tick and keeps pace, so the lag never grows past one chunk and
	// no aged-out is recorded. Abandoning each refused chunk until the
	// next tick instead would let the lag grow past the catch-up bound,
	// and the loop would skip samples and publish ReaderAgedOut for a
	// queue that was merely busy. That is the live symptom: a mirror
	// pinned at ReaderAgedOut whose reader was never at fault.
	var probes atomic.Uint64
	probe := func() (uint64, error) { return 1000 + probes.Add(480), nil }

	var calls atomic.Int32
	transfer := func(uint64, int) error {
		if calls.Add(1)%3 != 0 {
			return fabrics.ErrNotReady
		}
		return nil
	}
	tr := &recordingSampleTracker{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runSampleTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, func() error { return nil },
		480, testReadableWindow, time.Millisecond, tr)

	// Well past the ~15 ticks it takes to cross the window unretried.
	require.Eventually(t, func() bool { return tr.transfers() >= 60 }, 3*time.Second, time.Millisecond,
		"the loop made no progress at all")
	cancel()
	<-done

	assert.Zerof(t, tr.agedOut(),
		"a send queue that fills and drains is congestion, not a reader that fell "+
			"out of its window; got %d aged-out reports", tr.agedOut())
}

func TestRunSampleTransferLoop_NonIdleErrorBreaksImmediately(t *testing.T) {
	// A real failure ends the tick's chunk loop at once; the next tick
	// re-reads the head and tries again.
	var probes atomic.Uint64
	probe := func() (uint64, error) {
		if probes.Add(1) == 1 {
			return 1000, nil
		}
		return 1480, nil
	}
	var attempts atomic.Int32
	transfer := func(uint64, int) error {
		attempts.Add(1)
		return errors.New("endpoint is gone")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runSampleTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, func() error { return nil },
		480, testReadableWindow, 20*time.Millisecond, nil)

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	n := attempts.Load()
	assert.Positive(t, n)
	assert.Lessf(t, n, int32(20),
		"a fatal transfer error must break the chunk loop at once, got %d attempts", n)
}

func TestRunSampleTransferLoop_StillSkipsWhenGenuinelyLapped(t *testing.T) {
	// The producer laps the reader by more than the readable window
	// while transfers succeed. That is a real aged-out and must still
	// be reported: the fix narrows what counts as aged-out, it does not
	// remove the condition.
	var calls atomic.Int32
	probe := func() (uint64, error) {
		if calls.Add(1) == 1 {
			return 1000, nil
		}
		return 100000, nil
	}
	transfer := func(uint64, int) error { return nil }
	tr := &recordingSampleTracker{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	// One second of 48 kHz audio as the readable window; the producer
	// jumps two seconds past it.
	go runSampleTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, func() error { return nil },
		480, 48000, time.Millisecond, tr)

	require.Eventually(t, func() bool { return tr.agedOut() > 0 }, time.Second, time.Millisecond,
		"a producer that lapped the readable window is still ReaderAgedOut")
	cancel()
	<-done
}

// fanoutSampleInitiator stands in for an initiator with two targets,
// answering TransferSamples the way libmxl-fabrics does: the batch is
// offered to every target, and a target that cannot take it skips it
// and reports not-ready after the others have it. The healthy target
// takes every batch; the other one refuses every batch.
type fanoutSampleInitiator struct {
	mu      sync.Mutex
	healthy []sampleXfer
}

func (f *fanoutSampleInitiator) transferSamples(head uint64, count int) error {
	f.mu.Lock()
	f.healthy = append(f.healthy, sampleXfer{head, count})
	f.mu.Unlock()
	return fabrics.ErrNotReady
}

func (f *fanoutSampleInitiator) received() []sampleXfer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sampleXfer(nil), f.healthy...)
}

func TestRunSampleTransferLoop_OffersEachBatchToEveryTargetOnce(t *testing.T) {
	// One target of an audio flow refuses every batch. A not-ready
	// answer says that target skipped the batch, not that none got it:
	// retrying the call hands the same batch again to the target that
	// already took it. Its target then commits the same samples twice,
	// as audio.
	const xferBatch = 480
	var probes atomic.Uint64
	probe := func() (uint64, error) { return 1000 + probes.Add(xferBatch), nil }
	in := &fanoutSampleInitiator{}
	tr := &recordingSampleTracker{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runSampleTransferLoop(ctx, done, "f", probe, in.transferSamples, func() error { return nil }, func() error { return nil },
		xferBatch, testReadableWindow, time.Millisecond, tr)

	require.Eventually(t, func() bool { return len(in.received()) >= 40 }, 3*time.Second, time.Millisecond,
		"the healthy target received nothing")
	cancel()
	<-done

	got := in.received()
	for i := 1; i < len(got); i++ {
		require.Equalf(t, got[i-1].head+uint64(got[i].count), got[i].head,
			"batch %d: the healthy target received %v after %v; every batch must reach it once, in order",
			i, got[i], got[i-1])
	}
	assert.Equal(t, len(got), tr.transfers(),
		"a batch one target skipped still counts as offered")
	assert.Zero(t, tr.agedOut(), "a skipping target is not a reader that fell behind")
}

// recordingSampleTracker counts what the sample loop reports.
type recordingSampleTracker struct {
	mu    sync.Mutex
	xfers int
	aged  int
}

func (r *recordingSampleTracker) recordOffered(uint64, time.Time) {
	r.mu.Lock()
	r.xfers++
	r.mu.Unlock()
}

func (r *recordingSampleTracker) recordSkipped(uint64) {}

func (r *recordingSampleTracker) recordInvalid(time.Time) {}

func (r *recordingSampleTracker) recordPassedOver(unsentReason, uint64) {}

func (r *recordingSampleTracker) recordAgedOut(time.Time) {
	r.mu.Lock()
	r.aged++
	r.mu.Unlock()
}

func (r *recordingSampleTracker) recordHead(uint64, time.Time) {}

func (r *recordingSampleTracker) transfers() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.xfers
}

func (r *recordingSampleTracker) agedOut() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.aged
}

func TestLogSampleTransferFailure_ClassifiesRatherThanErroring(t *testing.T) {
	// The live gateway emitted an error line per refused chunk, which
	// on a 48 kHz flow is hundreds a second and buried everything else.
	// Classification is what keeps a full queue at V(1).
	for name, tc := range map[string]struct {
		err  error
		kind fabricFailure
	}{
		"full send queue": {fabrics.ErrNotReady, fabricIdle},
		"interrupted":     {mxl.ErrInterrupted, fabricTransient},
		"endpoint lost":   {errors.New("gone"), fabricEndpointLost},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.kind, classifyFabricError(tc.err))
		})
	}
}
