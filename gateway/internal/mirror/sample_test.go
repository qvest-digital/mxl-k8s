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

func TestRunSampleTransferLoop_FellBehindSkipsToWindowAndSignals(t *testing.T) {
	// The producer has lapped the reader by more than maxBatch. The
	// loop must skip to head-maxBatch (the oldest still-readable
	// sample), signal the tracker once, and transfer only the final
	// maxBatch run. The dropped samples are unrecoverable: re-reading
	// them would fail because they have left the ring's readable
	// window.
	const maxBatch = 480
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
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, maxBatch, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return len(xfersSnap()) == 1 },
		time.Second, time.Millisecond, "expected the clamped final run to transfer")

	cancel()
	<-done

	assert.Equal(t, []sampleXfer{{head: 2000, count: maxBatch}}, xfersSnap(),
		"after falling behind, the loop must transfer only the final maxBatch run ending at head")
	transfers, agedOuts := tracker.snapshot()
	assert.Equal(t, []uint64{2000}, transfers)
	assert.Equal(t, 1, agedOuts,
		"falling more than maxBatch behind must record exactly one aged-out skip so the "+
			"reconciler can publish SourceProgress=ReaderAgedOut")
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
