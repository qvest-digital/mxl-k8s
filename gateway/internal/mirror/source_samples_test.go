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

// The sample loop's failure mode on a busy fabric was a livelock rather
// than a stall: TransferSamples returned not-ready because the send
// queue was full, the loop abandoned the chunk until the next tick, the
// producer kept writing, and once the gap passed the readable window
// the loop skipped samples and published ReaderAgedOut. Observed on a
// live deployment as a mirror pinned at ReaderAgedOut + NoGrains while
// its reader was healthy, which the reader-reopen watchdog could not
// fix because the reader was never the broken part.

func TestRunSampleTransferLoop_RetriesAFullSendQueueWithinTheTick(t *testing.T) {
	// Discriminates on tick count, not wall clock: the loop probes the
	// head exactly once per tick, plus once at attach. Retrying in place
	// means the chunk lands during the first tick, so the probe count
	// stands at 2 when it does. Abandoning the chunk until the next tick
	// would need one tick per refusal and leave the count at 4.
	var probes atomic.Int32
	probe := func() (uint64, error) {
		if probes.Add(1) == 1 {
			return 1000, nil
		}
		return 1480, nil
	}

	var mu sync.Mutex
	var attempts int
	probesAtFirstSend := int32(-1)
	transfer := func(_ uint64, _ int) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts <= 2 {
			return fabrics.ErrNotReady
		}
		if probesAtFirstSend < 0 {
			probesAtFirstSend = probes.Load()
		}
		return nil
	}
	var progressCalls atomic.Int32
	progress := func() error { progressCalls.Add(1); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runSampleTransferLoop(ctx, done, "f", probe, transfer, progress, func() error { return nil },
		480, testReadableWindow, time.Millisecond, nil)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return probesAtFirstSend >= 0
	}, 2*time.Second, time.Millisecond, "the chunk never landed at all")

	cancel()
	<-done

	mu.Lock()
	got := probesAtFirstSend
	mu.Unlock()
	assert.Equalf(t, int32(2), got,
		"a chunk refused for a full queue must be retried inside the same tick; "+
			"probe count %d means it waited for a later tick", got)
	assert.Positive(t, progressCalls.Load(),
		"the retry must drive MakeProgress, which is what drains the queue")
}

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

func TestRunSampleTransferLoop_BoundsRetriesSoAWedgedFabricYieldsTheTick(t *testing.T) {
	// A permanently full queue must not spin inside one tick. The loop
	// has to give up on the chunk and come back on the next tick, where
	// the head is re-read and the stall becomes visible.
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
		return fabrics.ErrNotReady
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runSampleTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, func() error { return nil },
		480, testReadableWindow, 20*time.Millisecond, nil)

	// Two ticks' worth of time. With the bound, attempts stay near
	// 2 * (1 + maxSampleTransferRetries); without it the loop would
	// spin on the first tick and the count would run away.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	n := attempts.Load()
	assert.Positive(t, n)
	assert.Lessf(t, n, int32(200),
		"a permanently full queue must yield the tick, got %d attempts", n)
}

func TestRunSampleTransferLoop_NonIdleErrorBreaksImmediately(t *testing.T) {
	// Only a full queue earns a retry. A real failure must not be
	// retried eight times per tick against a fabric that cannot serve it.
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

// recordingSampleTracker counts what the sample loop reports.
type recordingSampleTracker struct {
	mu    sync.Mutex
	xfers int
	aged  int
}

func (r *recordingSampleTracker) recordTransfer(uint64, time.Time) {
	r.mu.Lock()
	r.xfers++
	r.mu.Unlock()
}

func (r *recordingSampleTracker) recordSkipped(uint64) {}

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
