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
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, makeProgress, func() error { return nil }, 480, testReadableWindow, time.Millisecond, tracker)

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
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, func() error { return nil }, xferBatch, testReadableWindow, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return len(xfersSnap()) == 2 },
		time.Second, time.Millisecond, "expected the 960-sample delta to split into 480+480")

	cancel()
	<-done

	assert.Equal(t, []sampleXfer{{480, 480}, {960, 480}}, xfersSnap(),
		"the loop must chunk the delta into runs of at most xferBatch, each ending at the chunk's last index")
	_, agedOuts := tracker.snapshot()
	assert.Zero(t, agedOuts, "a within-bound delta is a normal catch-up, not an aged-out skip")
}

func TestRunSampleTransferLoop_CatchesUpInsideTheWindowRatherThanSkipping(t *testing.T) {
	// A backlog that is still on the ring is owed to the target and
	// must be sent, not passed over.
	//
	// Skipping is not the cheap option it is on the grain path. The
	// next transfer commits at its own index, so the target's head
	// moves across the skipped range and republishes whatever its ring
	// already held there. Every skipped sample is a hole filled with
	// stale audio, which is why only the readable window may end a
	// catch-up.
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
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, func() error { return nil }, xferBatch, testReadableWindow, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return len(xfersSnap()) == 5 },
		time.Second, time.Millisecond, "the whole 2000-sample backlog must be transferred")

	cancel()
	<-done

	// Four batches in the first tick, the 80-sample remainder in the
	// next: contiguous from zero, nothing passed over.
	assert.Equal(t, []sampleXfer{
		{head: 480, count: 480},
		{head: 960, count: 480},
		{head: 1440, count: 480},
		{head: 1920, count: 480},
		{head: 2000, count: 80},
	}, xfersSnap())
	transfers, agedOuts := tracker.snapshot()
	assert.Equal(t, []uint64{480, 960, 1440, 1920, 2000}, transfers)
	assert.Zero(t, agedOuts, "a backlog inside the readable window is not aged out")
	assert.Zero(t, tracker.skippedSnapshot(), "and nothing may be recorded as skipped")
}

// The pacing that keeps a multi-chunk tick from putting its writes on
// the wire back to back. libmxl-fabrics gives an initiator an
// eight-entry completion queue with no way to raise it, and the sample
// ingress protocol keeps one posted receive, so a backlog drained
// without reaping between chunks outruns both.
//
// The reap has to be the non-blocking call. The blocking one parks for
// its whole timeout when nothing has landed, and that timeout is the
// tick: pacing with it costs a tick per chunk, so the loop delivers one
// batch per two ticks -- half real time -- and falls permanently behind
// a live producer. That regression is what the timing assertion here
// catches; the blocking stub sleeps like the real one.
func TestRunSampleTransferLoop_ReapsBetweenChunksWithoutBlocking(t *testing.T) {
	const xferBatch = 480
	const tick = 5 * time.Millisecond
	var probeCalls atomic.Int32
	probe := func() (uint64, error) {
		if probeCalls.Add(1) == 1 {
			return 0, nil
		}
		return 1920, nil
	}

	var mu sync.Mutex
	var seq []string
	note := func(what string) {
		mu.Lock()
		seq = append(seq, what)
		mu.Unlock()
	}
	transfer := func(uint64, int) error { note("xfer"); return nil }
	// Blocking, exactly like progressFuncFor returns for verbs.
	blocking := func() error { note("block"); time.Sleep(tick); return nil }
	reap := func() error { note("reap"); return nil }
	seqSnap := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seq...)
	}
	countXfer := func() int {
		n := 0
		for _, s := range seqSnap() {
			if s == "xfer" {
				n++
			}
		}
		return n
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	started := time.Now()
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, blocking, reap,
		xferBatch, testReadableWindow, tick, &recordingTracker{})

	require.Eventually(t, func() bool { return countXfer() >= 4 }, 2*time.Second, time.Millisecond,
		"the four-batch backlog must be transferred")
	elapsed := time.Since(started)
	cancel()
	<-done

	// Four chunks paced with the blocking call would cost at least four
	// ticks of sleeping on top of the tick they start on.
	assert.Less(t, elapsed, 4*tick,
		"the backlog must clear inside a tick or two; pacing with the blocking call would take at least four")

	// And no blocking call may appear between the first and the fourth
	// chunk -- only reaps.
	snap := seqSnap()
	var first, fourth, seen int
	for i, s := range snap {
		if s == "xfer" {
			seen++
			if seen == 1 {
				first = i
			}
			if seen == 4 {
				fourth = i
				break
			}
		}
	}
	require.Equal(t, 4, seen)
	var reaps, blocks int
	for _, s := range snap[first:fourth] {
		switch s {
		case "reap":
			reaps++
		case "block":
			blocks++
		}
	}
	assert.GreaterOrEqual(t, reaps, 3, "every chunk but the last must be followed by a reap")
	assert.Zero(t, blocks, "the blocking MakeProgress must not be what paces the chunks")
}

func TestRunSampleTransferLoop_RecordsSkippedSamplesWhenLapped(t *testing.T) {
	// The counter behind mxl_gateway_mirror_skipped_samples_total. It
	// is the only signal separating "the target received this" from
	// "the target's head moved over it", and those two are identical in
	// every byte and freshness measure the gateway keeps.
	const xferBatch = 480
	const window = uint64(48000)
	var probeCalls atomic.Int32
	probe := func() (uint64, error) {
		if probeCalls.Add(1) == 1 {
			return 0, nil
		}
		return 100000, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe,
		func(uint64, int) error { return nil }, func() error { return nil }, func() error { return nil },
		xferBatch, window, time.Millisecond, tracker)

	require.Eventually(t, func() bool { return tracker.skippedSnapshot() > 0 },
		time.Second, time.Millisecond)
	cancel()
	<-done

	// The window less one batch of slack stays readable, so everything
	// before head-(window-xferBatch) is abandoned.
	assert.Equal(t, 100000-(window-xferBatch), tracker.skippedSnapshot())
	_, agedOuts := tracker.snapshot()
	assert.Positive(t, agedOuts)
}

func TestRunSampleTransferLoop_NeverBurstsBeyondCatchUpPerTick(t *testing.T) {
	// A mirror that has fallen far behind must not attempt the whole
	// backlog in one tick: at most sampleCatchUpBatches chunks are
	// transferable per tick. This is what keeps a 48 kHz mirror from
	// posting a burst of hundreds of writes -- one per readable-window
	// batch -- against the fabric. The loop transfers the bounded
	// catch-up and leaves the rest for the next tick; it does not spin.
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
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, func() error { return nil }, xferBatch, testReadableWindow, time.Millisecond, tracker)

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
			assert.LessOrEqualf(t, snap[i].head-snap[i-1].head, uint64(xferBatch),
				"chunk %d: consecutive transfers must advance by at most one batch", i)
		}
	}
	// 2000 samples of backlog is five chunks; with the head frozen
	// afterwards nothing more transfers.
	assert.LessOrEqualf(t, len(snap), 5,
		"a frozen head must not produce further transfers, got %d", len(snap))
}

func TestRunSampleTransferLoop_ExitsAfterSustainedStarvation(t *testing.T) {
	// A wedged fabric connection refuses most chunks while the writer
	// keeps producing: a live loop keeps probing the head, so the
	// flusher's stale-head watchdog never fires, and the occasional
	// chunk that does land reads as delivery to every freshness-based
	// signal. The loop must stop after maxSampleStarvedTicks
	// consecutive ticks under a quarter of the produced samples
	// rather than skip-retrying forever. Exiting stops the head
	// probes, which the flusher reads as ReaderNotAdvancing and
	// rebuilds the source end to end.
	const xferBatch = 480
	var probes atomic.Uint64
	probe := func() (uint64, error) {
		// The head advances one batch per tick so every tick finds
		// new samples to starve.
		return probes.Add(xferBatch), nil
	}
	transfer := func(uint64, int) error {
		return fabrics.ErrNotReady
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, func() error { return nil }, xferBatch, testReadableWindow, time.Millisecond, tracker)

	select {
	case <-done:
		// The loop exited on its own: the starvation bound fired.
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not exit under sustained starvation; the wedge would persist forever")
	}

	// Every tick refused, so nothing landed and the loop kept
	// skipping: the exit must not have recorded a transfer.
	transfers, _ := tracker.snapshot()
	assert.Empty(t, transfers, "a refused queue must never be recorded as delivered")
	// The head probes ran for at least the bound's worth of ticks
	// before the exit, proving the loop tolerated the full window
	// before giving up rather than exiting on the first refusal.
	assert.GreaterOrEqual(t, probes.Load(), uint64(maxSampleStarvedTicks*xferBatch),
		"the loop must tolerate the whole starvation window before exiting")
}

func TestRunSampleTransferLoop_StarvationResetsOnDelivery(t *testing.T) {
	// A busy queue that intermittently refuses but delivers most of
	// what the writer produces must run forever: the exit bound
	// counts only consecutive starved ticks, and a tick that lands
	// at least a quarter of the produced samples is not starved.
	const xferBatch = 480
	var probes atomic.Uint64
	var calls atomic.Int32
	probe := func() (uint64, error) {
		return probes.Add(xferBatch), nil
	}
	transfer := func(uint64, int) error {
		// Refuse heavily, but land every few attempts: each landed
		// chunk empties the tick's delta, so the delivered fraction
		// is far above the starvation threshold.
		if calls.Add(1)%10 != 0 {
			return fabrics.ErrNotReady
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, func() error { return nil }, xferBatch, testReadableWindow, time.Millisecond, tracker)

	// Far longer than the starvation window: if the counter were not
	// reset by deliveries the loop would have exited inside it.
	time.Sleep(2 * time.Second)
	select {
	case <-done:
		t.Fatal("loop exited despite regular deliveries; the starvation counter is not reset on delivery")
	default:
	}
	cancel()
	<-done
	transfers, _ := tracker.snapshot()
	assert.NotEmpty(t, transfers, "an intermittently delivering queue must have landed chunks")
}

func TestRunSampleTransferLoop_TrickleDeliveryStillStarves(t *testing.T) {
	// The wedge that defeats every freshness-based signal: the send
	// queue drains one slot at a time, so a thin trickle of chunks
	// lands while the bulk of each tick's production is aged out and
	// skipped. A loop that exited only on total refusal -- or reset
	// its bound on any delivery -- pins the wedge forever, because
	// the trickle reads as delivery. The starvation ratio must count
	// the samples the writer produced, before the skip folds the
	// delta: one landing in eight is a starvation even though the
	// folded delta is half delivered.
	const xferBatch = 480
	var probes atomic.Uint64
	var calls atomic.Int32
	probe := func() (uint64, error) {
		// The head advances eight batches per tick, like a loop that
		// spent the tick's budget retrying against a full queue.
		return probes.Add(8 * xferBatch), nil
	}
	transfer := func(uint64, int) error {
		// Land one call in ten -- the first chunk of a tick plus
		// never a retry -- so every tick delivers one batch of the
		// eight produced and every retry is refused, the shape of a
		// send queue that frees a single slot per tick.
		if calls.Add(1)%10 == 1 {
			return nil
		}
		return fabrics.ErrNotReady
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	tracker := &recordingTracker{}
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, func() error { return nil }, xferBatch, testReadableWindow, time.Millisecond, tracker)

	select {
	case <-done:
		// The loop exited on its own: the trickle did not read as
		// healthy against the true production.
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not exit under trickle delivery; the wedge would persist forever")
	}

	transfers, _ := tracker.snapshot()
	assert.NotEmpty(t, transfers, "the trickle landed chunks before the exit")
	assert.LessOrEqual(t, len(transfers), maxSampleStarvedTicks,
		"the loop must stop delivering the trickle once the bound fires")
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
	go runSampleTransferLoop(ctx, done, "flow-audio", probe, transfer, func() error { return nil }, func() error { return nil }, 480, testReadableWindow, time.Millisecond, &recordingTracker{})

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
		func() error { return nil }, func() error { return nil }, 480, testReadableWindow, time.Millisecond, &recordingTracker{})

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
