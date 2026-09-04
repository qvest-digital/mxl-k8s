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
)

// sampleXfer records one transferSamples(headIndex, count) call so the
// source-loop tests can assert the exact runs that were sent.
type sampleXfer struct {
	head  uint64
	count int
}

func TestRunSampleTransferLoop_TransfersDeltaSinceLastTick(t *testing.T) {
	// Initial probe head=1000 means lastSent starts at 1000; tailing
	// the live flow rather than replaying history. The first tick sees
	// head=1096, so the loop transfers the 96-sample run ending at 1096
	// exactly once. Steady ticks at the same head transfer nothing.
	var probeCalls atomic.Int32
	probe := func() (uint64, error) {
		if probeCalls.Add(1) == 1 {
			return 1000, nil
		}
		return 1096, nil
	}

	var mu sync.Mutex
	var xfers []sampleXfer
	transfer := func(head uint64, count int) error {
		mu.Lock()
		xfers = append(xfers, sampleXfer{head, count})
		mu.Unlock()
		return nil
	}
	xfersSnap := func() []sampleXfer {
		mu.Lock()
		defer mu.Unlock()
		out := make([]sampleXfer, len(xfers))
		copy(out, xfers)
		return out
	}

	var progressCalls atomic.Int32
	makeProgress := func() error { progressCalls.Add(1); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, makeProgress, 480, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return len(xfersSnap()) == 1 },
		time.Second, time.Millisecond, "expected exactly one sample-run transfer")

	cancel()
	<-done

	assert.Equal(t, []sampleXfer{{head: 1096, count: 96}}, xfersSnap(),
		"the loop must transfer the (lastSent, head] delta as one run ending at head")
	transfers, agedOuts := tracker.snapshot()
	assert.Equal(t, []uint64{1096}, transfers,
		"recordTransfer must observe the ending index of each sent run")
	assert.Zero(t, agedOuts, "a within-window catch-up is not an aged-out skip")
	assert.GreaterOrEqual(t, progressCalls.Load(), int32(1),
		"makeProgress must drive the fabric event queues every tick")
}

func TestRunSampleTransferLoop_ChunksLargeDeltaByXferBatch(t *testing.T) {
	// A within-window delta larger than xferBatch must be transferred
	// as several runs of at most xferBatch each, not one oversized
	// TransferSamples (the fabric rejects an over-large count). The
	// delta is exactly the catch-up bound, so this is a normal
	// catch-up with no aged-out skip.
	const xferBatch = 480
	var probeCalls atomic.Int32
	probe := func() (uint64, error) {
		if probeCalls.Add(1) == 1 {
			return 0, nil
		}
		return 960, nil
	}

	var mu sync.Mutex
	var xfers []sampleXfer
	transfer := func(head uint64, count int) error {
		mu.Lock()
		xfers = append(xfers, sampleXfer{head, count})
		mu.Unlock()
		return nil
	}
	xfersSnap := func() []sampleXfer {
		mu.Lock()
		defer mu.Unlock()
		out := make([]sampleXfer, len(xfers))
		copy(out, xfers)
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tracker := &recordingTracker{}
	// The delta is larger than xferBatch but inside the catch-up
	// bound (2*xferBatch), so it must split into full-batch runs
	// with no aged-out skip.
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, xferBatch, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return len(xfersSnap()) == 2 },
		time.Second, time.Millisecond, "expected the 960-sample delta to split into 480+480")

	cancel()
	<-done

	assert.Equal(t, []sampleXfer{{480, 480}, {960, 480}}, xfersSnap(),
		"the loop must chunk the delta into runs of at most xferBatch, each ending at the chunk's last index")
	_, agedOuts := tracker.snapshot()
	assert.Zero(t, agedOuts, "a within-bound delta is a normal catch-up, not an aged-out skip")
}

func TestRunSampleTransferLoop_FellBehindSkipsToBoundedCatchUpAndSignals(t *testing.T) {
	// The producer has lapped the reader far beyond one tick's
	// catch-up. The loop must skip to head-2*xferBatch (the newest
	// batch plus one batch of slack, not the readable-window edge),
	// signal the tracker once, and transfer only what the bound
	// allows. Skipping to the window edge instead would attempt the
	// whole readable window in one tick: a burst that floods the
	// send queue and wedges every mirror sharing the fabric.
	const xferBatch = 480
	var probeCalls atomic.Int32
	probe := func() (uint64, error) {
		if probeCalls.Add(1) == 1 {
			return 0, nil
		}
		return 2000, nil
	}

	var mu sync.Mutex
	var xfers []sampleXfer
	transfer := func(head uint64, count int) error {
		mu.Lock()
		xfers = append(xfers, sampleXfer{head, count})
		mu.Unlock()
		return nil
	}
	xfersSnap := func() []sampleXfer {
		mu.Lock()
		defer mu.Unlock()
		out := make([]sampleXfer, len(xfers))
		copy(out, xfers)
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, xferBatch, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return len(xfersSnap()) == 2 },
		time.Second, time.Millisecond, "expected the bounded catch-up to transfer as two full batches")

	cancel()
	<-done

	assert.Equal(t, []sampleXfer{{head: 1520, count: xferBatch}, {head: 2000, count: xferBatch}}, xfersSnap(),
		"after falling behind, the loop must transfer only the bounded catch-up ending at head")
	transfers, agedOuts := tracker.snapshot()
	assert.Equal(t, []uint64{1520, 2000}, transfers)
	assert.Equal(t, 1, agedOuts,
		"falling more than the catch-up bound behind must record exactly one aged-out skip so the "+
			"reconciler can publish SourceProgress=ReaderAgedOut")
}

func TestRunSampleTransferLoop_NeverBurstsBeyondCatchUpPerTick(t *testing.T) {
	// A mirror that has fallen far behind must not attempt the
	// whole backlog in one tick: at most two xferBatch chunks are
	// transferable per tick, and only while the lag stays inside the
	// bound. This is what keeps a 48 kHz mirror from posting a burst
	// of hundreds of writes -- one per readable-window batch --
	// against the fabric. The loop transfers the bounded catch-up
	// and leaves the rest for the next tick; it does not spin.
	const xferBatch = 480
	var probes atomic.Uint64
	probe := func() (uint64, error) {
		if probes.Add(1) == 1 {
			return 0, nil
		}
		return 2000, nil
	}

	var mu sync.Mutex
	var xfers []sampleXfer
	transfer := func(head uint64, count int) error {
		mu.Lock()
		xfers = append(xfers, sampleXfer{head, count})
		mu.Unlock()
		return nil
	}
	xfersSnap := func() []sampleXfer {
		mu.Lock()
		defer mu.Unlock()
		out := make([]sampleXfer, len(xfers))
		copy(out, xfers)
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, xferBatch, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return len(xfersSnap()) >= 1 },
		time.Second, time.Millisecond, "the bounded catch-up must transfer")

	// Give the loop more ticks than it would need to burst the whole
	// backlog had the bound not held, then verify it never did: two
	// chunks maximum per tick, each ending inside the bound.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	snap := xfersSnap()
	for i, x := range snap {
		assert.LessOrEqualf(t, x.count, int(xferBatch), "chunk %d: a single write must never exceed xferBatch", i)
		if i > 0 {
			assert.LessOrEqualf(t, snap[i].head-snap[i-1].head, uint64(2*xferBatch),
				"chunk %d: consecutive transfers within a tick must not exceed the catch-up bound", i)
		}
	}
	// The first tick is the only one that skips to the bound; with
	// the head frozen afterwards nothing more transfers.
	assert.LessOrEqualf(t, len(snap), 2,
		"a frozen head must not produce further transfers, got %d", len(snap))
}

func TestRunSampleTransferLoop_ExitsAfterSustainedRefusal(t *testing.T) {
	// A send queue that refuses every chunk while the writer keeps
	// producing is a connection that will not drain on its own. The
	// loop must stop after maxSampleRefusedTicks consecutive
	// refusing ticks rather than skip-retrying forever: a live loop
	// keeps probing the head, so the flusher's stale-head watchdog
	// never fires and nothing else on the source side breaks the
	// state. Exiting stops the head probes, which the flusher reads
	// as ReaderNotAdvancing and rebuilds the source end to end.
	const xferBatch = 480
	var probes atomic.Uint64
	probe := func() (uint64, error) {
		// The head advances one batch per tick so every tick finds
		// new samples to refuse.
		return probes.Add(xferBatch), nil
	}
	transfer := func(uint64, int) error {
		return fabrics.ErrNotReady
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, xferBatch, time.Millisecond, tracker)

	select {
	case <-done:
		// The loop exited on its own: the sustained-refusal bound fired.
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not exit under sustained refusal; the wedge would persist forever")
	}

	// Every tick refused, so nothing landed and the loop kept
	// skipping: the exit must not have recorded a transfer.
	transfers, _ := tracker.snapshot()
	assert.Empty(t, transfers, "a refused queue must never be recorded as delivered")
	// The head probes ran for at least the bound's worth of ticks
	// before the exit, proving the loop tolerated the full window
	// before giving up rather than exiting on the first refusal.
	assert.GreaterOrEqual(t, probes.Load(), uint64(maxSampleRefusedTicks*xferBatch),
		"the loop must tolerate the whole refusal window before exiting")
}

func TestRunSampleTransferLoop_RefusalCountResetsOnDelivery(t *testing.T) {
	// A busy queue that intermittently refuses but delivers before
	// the refusal window elapses must run forever: the exit bound
	// counts only *consecutive* refusing ticks, and clearing it on
	// delivery keeps a healthy backpressure episode from being
	// mistaken for a dead connection.
	const xferBatch = 480
	var probes atomic.Uint64
	var calls atomic.Int32
	probe := func() (uint64, error) {
		return probes.Add(xferBatch), nil
	}
	transfer := func(uint64, int) error {
		// Refuse heavily, but land every few attempts.
		if calls.Add(1)%10 != 0 {
			return fabrics.ErrNotReady
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, xferBatch, time.Millisecond, tracker)

	// Far longer than the refusal window: if the counter were not
	// reset by deliveries the loop would have exited inside it.
	time.Sleep(2 * time.Second)
	select {
	case <-done:
		t.Fatal("loop exited despite regular deliveries; the refusal counter is not reset on delivery")
	default:
	}
	cancel()
	<-done
	transfers, _ := tracker.snapshot()
	assert.NotEmpty(t, transfers, "an intermittently delivering queue must have landed chunks")
}

func TestRunSampleTransferLoop_TransferErrorBreaksTickAndRetries(t *testing.T) {
	// A transferSamples error (e.g. the fabric briefly not ready) must
	// not exit the loop or advance lastSent past the failed run: the
	// next tick re-reads head and re-attempts the same delta. The first
	// call fails, the second succeeds against the unchanged head.
	var probeCalls atomic.Int32
	probe := func() (uint64, error) {
		if probeCalls.Add(1) == 1 {
			return 0, nil
		}
		return 240, nil
	}

	var calls atomic.Int32
	var mu sync.Mutex
	var xfers []sampleXfer
	transfer := func(head uint64, count int) error {
		if calls.Add(1) == 1 {
			return errors.New("transient fabric error")
		}
		mu.Lock()
		xfers = append(xfers, sampleXfer{head, count})
		mu.Unlock()
		return nil
	}
	xfersLen := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(xfers)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, 480, time.Millisecond, &recordingTracker{})

	require.Eventually(t, func() bool { return xfersLen() >= 1 },
		time.Second, time.Millisecond, "a transfer error must not wedge the loop; the next tick must retry")

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, xfers)
	assert.Equal(t, sampleXfer{head: 240, count: 240}, xfers[0],
		"the retry must re-send the same run; a failed transfer must not advance lastSent")
}

func TestRunSampleTransferLoop_CtxCancelExitsDuringIdle(t *testing.T) {
	// A steady flow with no new samples must still honour ctx cancel
	// promptly (the loop blocks on the ticker, not on a read).
	probe := func() (uint64, error) { return 500, nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runSampleTransferLoop(ctx, done, "flow-audio", probe,
		func(uint64, int) error { t.Error("no transfer expected on an idle flow"); return nil },
		func() error { return nil }, 480, time.Millisecond, &recordingTracker{})

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not honour ctx cancel on an idle flow")
	}
}

// sampleCommit records one commit(headIndex, count) call so the
// target-loop tests can assert the runs handed to the writer.
type sampleCommit struct {
	head  uint64
	count int
}

func TestRunTargetSampleProgressLoop_CommitsArrivedSampleRuns(t *testing.T) {
	// Two sample runs arrive in order, then ErrNotReady, then cancel.
	// The loop must commit both runs with their exact (head, count) and
	// the idle sleep must not swallow the cancel.
	var seq atomic.Int32
	read := func() (uint64, int, error) {
		switch seq.Add(1) {
		case 1:
			return 100, 480, nil
		case 2:
			return 580, 480, nil
		default:
			return 0, 0, fabrics.ErrNotReady
		}
	}

	var mu sync.Mutex
	var commits []sampleCommit
	commit := func(head uint64, count int) error {
		mu.Lock()
		commits = append(commits, sampleCommit{head, count})
		mu.Unlock()
		return nil
	}
	commitsSnap := func() []sampleCommit {
		mu.Lock()
		defer mu.Unlock()
		out := make([]sampleCommit, len(commits))
		copy(out, commits)
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	fatal := atomic.Int32{}
	go runTargetSampleProgressLoop(ctx, done, read, commit, func() { fatal.Add(1) }, nil)

	require.Eventually(t, func() bool { return len(commitsSnap()) == 2 },
		time.Second, time.Millisecond, "expected both arrived sample runs to commit")

	cancel()
	<-done

	assert.Equal(t, []sampleCommit{{100, 480}, {580, 480}}, commitsSnap())
	assert.Zero(t, fatal.Load(),
		"a clean ctx cancel must not look like a fatal target error")
}

func TestRunTargetSampleProgressLoop_FatalExitsAndCallsOnFatalOnce(t *testing.T) {
	// ReadSamples returns a non-ErrNotReady error once. The loop must
	// exit, close done, and invoke onFatal exactly once; a second call
	// would race two fabric rebuilds against the same writer.
	read := func() (uint64, int, error) {
		return 0, 0, errors.New("EFI_RXMSG dropped, target dead")
	}
	commit := func(uint64, int) error {
		t.Fatal("commit must not run on a fatal read error")
		return nil
	}
	fatalCalls := atomic.Int32{}

	done := make(chan struct{})
	go runTargetSampleProgressLoop(context.Background(), done, read, commit, func() { fatalCalls.Add(1) }, nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on fatal error")
	}
	assert.Equal(t, int32(1), fatalCalls.Load(), "onFatal must fire exactly once")
}

func TestRunTargetSampleProgressLoop_CommitErrorIsLoggedButLoopContinues(t *testing.T) {
	// A commit-side error (e.g. OpenSamples busy under load) must not
	// exit the loop nor look fatal; the next read may surface the next
	// run.
	var seq atomic.Int32
	read := func() (uint64, int, error) {
		switch seq.Add(1) {
		case 1:
			return 1, 64, nil
		case 2:
			return 65, 64, nil
		default:
			return 0, 0, fabrics.ErrNotReady
		}
	}
	commitCalls := atomic.Int32{}
	commit := func(uint64, int) error {
		commitCalls.Add(1)
		return errors.New("transient OpenSamples")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetSampleProgressLoop(ctx, done, read, commit,
		func() { t.Error("commit failure must not be reported as fatal") }, nil)

	require.Eventually(t, func() bool { return commitCalls.Load() >= 2 },
		time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestRunTargetSampleProgressLoop_CtxCancelExitsDuringIdle(t *testing.T) {
	read := func() (uint64, int, error) { return 0, 0, fabrics.ErrNotReady }
	commit := func(uint64, int) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetSampleProgressLoop(ctx, done, read, commit,
		func() { t.Error("ctx cancel must not look fatal") }, nil)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not honour ctx cancel during idle sleep")
	}
}

func TestRunTargetSampleProgressLoop_NilOnFatalDoesNotPanic(t *testing.T) {
	// A nil onFatal would be a caller bug, but a panic in the goroutine
	// would crash the gateway; the loop must guard against it.
	read := func() (uint64, int, error) { return 0, 0, errors.New("fatal") }
	commit := func(uint64, int) error { return nil }

	done := make(chan struct{})
	go runTargetSampleProgressLoop(context.Background(), done, read, commit, nil, nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on fatal read with nil onFatal")
	}
}

func TestRunTargetSampleProgressLoop_TracksOnlySuccessfulCommits(t *testing.T) {
	// The flusher reads commits + lastCommitAt to decide Ready vs
	// Degraded; the loop must hand only successful commits to the
	// tracker. A run whose commit errored must not be recorded, or the
	// flusher would report fresh progress while the consumer's flow is
	// missing samples.
	var seq atomic.Int32
	read := func() (uint64, int, error) {
		switch seq.Add(1) {
		case 1:
			return 100, 48, nil
		case 2:
			return 148, 48, nil
		case 3:
			return 196, 48, nil
		default:
			return 0, 0, fabrics.ErrNotReady
		}
	}
	commit := func(head uint64, _ int) error {
		if head == 148 {
			return errors.New("transient OpenSamples")
		}
		return nil
	}

	tracker := &recordingCommitTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetSampleProgressLoop(ctx, done, read, commit, func() {}, tracker)

	require.Eventually(t, func() bool { return len(tracker.snapshot()) >= 2 },
		time.Second, time.Millisecond)

	cancel()
	<-done

	assert.Equal(t, []uint64{100, 196}, tracker.snapshot(),
		"the tracker must observe only commits that succeeded; recording the failed 148 "+
			"would let the flusher report progress the consumer never received")
}
