package mirror

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// sampleXfer records one transferSamples(headIndex, count) call so the
// source-loop tests can assert the exact runs that were sent.
type sampleXfer struct {
	head  uint64
	count int
}

// fakeSampleLink is fakeLink for sample batches.
type fakeSampleLink struct {
	t        *testing.T
	notReady int
	hold     func(end uint64)
	postErr  func(n int) error

	mu          sync.Mutex
	outstanding bool
	current     uint64
	pending     int
	posted      []sampleXfer
	progress    int
}

func (f *fakeSampleLink) transferSamples(end uint64, count int) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postErr != nil {
		if err := f.postErr(len(f.posted)); err != nil {
			f.posted = append(f.posted, sampleXfer{end, count})
			return 0, err
		}
	}
	if f.outstanding {
		f.t.Errorf("batch ending %d posted while batch ending %d was outstanding", end, f.current)
	}
	f.outstanding, f.current, f.pending = true, end, f.notReady
	f.posted = append(f.posted, sampleXfer{end, count})
	return uint64(count) * 8, nil
}

func (f *fakeSampleLink) makeProgress() error {
	f.mu.Lock()
	hold, end := f.hold, f.current
	f.mu.Unlock()
	if hold != nil {
		hold(end)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progress++
	if f.pending > 0 {
		f.pending--
		return fabrics.ErrNotReady
	}
	f.outstanding = false
	return nil
}

func (f *fakeSampleLink) snapshot() ([]sampleXfer, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sampleXfer(nil), f.posted...), f.progress
}

// runSampleLoop starts runSampleTransferLoop with a 1 ms poll and
// returns a stop function that cancels it and waits for it.
func runSampleLoop(probe RuntimeProbe, transfer SampleTransferFunc, progress ProgressFunc, xferBatch, window uint64, interval time.Duration, tr progressTracker) (done chan struct{}, stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan struct{})
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, progress, progress, xferBatch, window, time.Millisecond, interval, tr)
	return done, func() {
		cancel()
		<-done
	}
}

func TestRunSampleTransferLoop_TransfersDeltaSinceLastPoll(t *testing.T) {
	// The first probe is where the mirror tunes in. The next head
	// 96 samples on is one run ending at it, sent once.
	h := &headScript{heads: []uint64{1000, 1096}}
	link := &fakeSampleLink{t: t}
	tr := &recordingTracker{}
	_, stop := runSampleLoop(h.probe, link.transferSamples, link.makeProgress, 480, testReadableWindow, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 1 }, time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	stop()

	posted, _ := link.snapshot()
	assert.Equal(t, []sampleXfer{{head: 1096, count: 96}}, posted)
	got, agedOuts := tr.snapshot()
	assert.Equal(t, []uint64{1096}, got)
	assert.Zero(t, agedOuts)
	assert.Equal(t, uint64(96*8), tr.bytes)
}

// A delta larger than one batch goes as one batch per transfer, each
// driven to completion before the next is posted.
func TestRunSampleTransferLoop_SendsOneBatchAtATime(t *testing.T) {
	h := &headScript{heads: []uint64{0, 1440}}
	link := &fakeSampleLink{t: t, notReady: 2}
	tr := &recordingTracker{}
	_, stop := runSampleLoop(h.probe, link.transferSamples, link.makeProgress, 480, testReadableWindow, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 3 }, time.Second, time.Millisecond)
	stop()

	posted, progress := link.snapshot()
	assert.Equal(t, []sampleXfer{{480, 480}, {960, 480}, {1440, 480}}, posted)
	assert.Equal(t, 1+3*3, progress, "one call to connect, then two not-ready answers and one completion per batch")
	assert.Zero(t, tr.skippedSnapshot())
}

// A batch that misses its deadline is still driven to completion, and
// the loop then resumes one batch short of the live head, counting what
// it passed over.
func TestRunSampleTransferLoop_LateBatchResumesAtTheHead(t *testing.T) {
	var head atomic.Uint64
	head.Store(0)
	probe := func() (uint64, error) { return head.Load(), nil }
	link := &fakeSampleLink{t: t}
	link.hold = func(end uint64) {
		if end == 480 && head.Load() == 480 {
			time.Sleep(20 * time.Millisecond)
			head.Store(4800)
		}
	}
	tr := &recordingTracker{}
	_, stop := runSampleLoop(probe, link.transferSamples, link.makeProgress, 480, testReadableWindow, 5*time.Millisecond, tr)
	require.Eventually(t, func() bool { return len(tr.headSnapshot()) > 1 }, time.Second, time.Millisecond)
	head.Store(480)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) >= 2 }, time.Second, time.Millisecond)
	stop()

	posted, _ := link.snapshot()
	assert.Equal(t, []sampleXfer{{480, 480}, {4800, 480}}, posted[:2])
	assert.Equal(t, uint64(4800-480-480), tr.skippedSnapshot())
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, 1, tr.late)
	assert.Zero(t, tr.agedOuts, "a late batch is not the reader ageing out")
}

func TestRunSampleTransferLoop_RecordsSkippedSamplesWhenLapped(t *testing.T) {
	// The counter behind mxl_gateway_mirror_skipped_samples_total: the
	// only signal separating "the target received this" from "the
	// target's head moved over it".
	const xferBatch = 480
	const window = uint64(48000)
	h := &headScript{heads: []uint64{0, 100000}}
	link := &fakeSampleLink{t: t}
	tr := &recordingTracker{}
	_, stop := runSampleLoop(h.probe, link.transferSamples, link.makeProgress, xferBatch, window, time.Second, tr)
	require.Eventually(t, func() bool { return tr.skippedSnapshot() > 0 }, time.Second, time.Millisecond)
	stop()

	// The window less one batch of slack stays readable, so everything
	// before head-(window-xferBatch) is abandoned.
	assert.Equal(t, 100000-(window-xferBatch), tr.skippedSnapshot())
	_, agedOuts := tr.snapshot()
	assert.Positive(t, agedOuts)
}

func TestRunSampleTransferLoop_DroppedTargetStopsTheLoop(t *testing.T) {
	h := &headScript{heads: []uint64{0, 480}}
	link := &fakeSampleLink{t: t}
	tr := &recordingTracker{}
	var calls atomic.Int32
	progress := func() error {
		if calls.Add(1) == 1 {
			return nil // connected
		}
		return mxl.ErrInterrupted
	}
	done, stop := runSampleLoop(h.probe, link.transferSamples, progress, 480, testReadableWindow, time.Second, tr)
	defer stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the loop kept running against a dropped target")
	}
	assert.Equal(t, []bool{true}, tr.droppedSnapshot())
}

// A batch the target is not ready for is offered again; one the
// initiator rejects for good gives the target up.
func TestRunSampleTransferLoop_PostFailures(t *testing.T) {
	h := &headScript{heads: []uint64{0, 480}}
	link := &fakeSampleLink{t: t, postErr: func(n int) error {
		if n == 0 {
			return fabrics.ErrNotReady
		}
		return nil
	}}
	tr := &recordingTracker{}
	_, stop := runSampleLoop(h.probe, link.transferSamples, link.makeProgress, 480, testReadableWindow, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 1 }, time.Second, time.Millisecond)
	stop()
	posted, _ := link.snapshot()
	assert.Equal(t, []sampleXfer{{480, 480}, {480, 480}}, posted, "the same batch is offered again")
	assert.Empty(t, tr.droppedSnapshot())

	link = &fakeSampleLink{t: t, postErr: func(int) error { return fmt.Errorf("count too large: %w", mxl.ErrInvalidArg) }}
	tr = &recordingTracker{}
	h = &headScript{heads: []uint64{0, 480}}
	done, stop := runSampleLoop(h.probe, link.transferSamples, link.makeProgress, 480, testReadableWindow, time.Second, tr)
	defer stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the loop kept running after a rejected post")
	}
	assert.Equal(t, []bool{false}, tr.droppedSnapshot())
}

func TestRunSampleTransferLoop_CtxCancelExitsDuringIdle(t *testing.T) {
	probe := func() (uint64, error) { return 500, nil }
	transfer := func(uint64, int) (uint64, error) {
		t.Error("no transfer expected on an idle flow")
		return 0, nil
	}
	_, stop := runSampleLoop(probe, transfer, func() error { return nil }, 480, testReadableWindow, time.Second, &recordingTracker{})
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
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

func TestRunTargetSampleProgressLoop_DuplicateArrivalIsDroppedNotRetried(t *testing.T) {
	// A source that retransmits or overlaps a range produces an
	// arrival whose index does not strictly advance the writer's
	// last committed index; libmxl rejects the OpenSamples with
	// invalid argument, and the arrival's bytes are already in the
	// ring. Parking on that arrival -- retrying a commit that can
	// never succeed -- stalls the ingress and starves the
	// initiator's send queue to the rate at which the loop gives up
	// on one entry. The loop must drop the arrival and keep reading:
	// the next arrival is the one that moves the flow.
	var seq atomic.Int32
	read := func() (uint64, int, error) {
		switch seq.Add(1) {
		case 1:
			return 480, 480, nil // committed normally
		case 2:
			return 480, 480, nil // retransmission of the same range
		case 3:
			return 960, 480, nil // the next range: must still be read
		default:
			return 0, 0, fabrics.ErrNotReady
		}
	}
	committed := make(chan uint64, 3)
	commit := func(head uint64, _ int) error {
		if head == 480 && len(committed) > 0 {
			// The second arrival for the same range is the
			// already-committed one: wrap the real error shape.
			return fmt.Errorf("OpenSamples(%d,480): %w: %w", head, errSamplesAlreadyCommitted, mxl.ErrInvalidArg)
		}
		committed <- head
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetSampleProgressLoop(ctx, done, read, commit,
		func() { t.Error("a duplicate arrival must not be reported as fatal") }, nil)

	// The first and third arrivals commit; the duplicate between
	// them is consumed without a commit and without exiting.
	require.Eventually(t, func() bool { return len(committed) == 2 },
		2*time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("loop exited on a duplicate arrival")
	default:
	}
	cancel()
	<-done
	assert.Equal(t, uint64(480), <-committed)
	assert.Equal(t, uint64(960), <-committed)
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
