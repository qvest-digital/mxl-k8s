package mirror

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestMain(m *testing.M) {
	// Catch goroutine leaks from the loops under test. Every Test*
	// in this package starts at most one progress goroutine; if it
	// outlives the test, the loop forgot to honour ctx.
	goleak.VerifyTestMain(m)
}

// headScript answers Runtime probes from a list, holding the last value.
type headScript struct {
	mu    sync.Mutex
	heads []uint64
	n     int
}

func (h *headScript) probe() (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := h.heads[min(h.n, len(h.heads)-1)]
	h.n++
	return v, nil
}

// fakeLink stands in for one mirror's initiator: TransferGrain posts,
// MakeProgress answers not-ready notReady times per transfer and then
// completes it. It fails the test on a post while one is outstanding.
type fakeLink struct {
	t        *testing.T
	notReady int
	// hold, when set, is called by MakeProgress before it answers, so a
	// test can keep a transfer outstanding while the head moves.
	hold func(idx uint64)

	mu          sync.Mutex
	outstanding bool
	current     uint64
	pending     int
	posted      []uint64
	progress    int
}

func (f *fakeLink) transferGrain(idx uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.outstanding {
		f.t.Errorf("grain %d posted while grain %d was outstanding", idx, f.current)
	}
	f.outstanding, f.current, f.pending = true, idx, f.notReady
	f.posted = append(f.posted, idx)
	return 100, nil
}

func (f *fakeLink) makeProgress() error {
	f.mu.Lock()
	hold, idx := f.hold, f.current
	f.mu.Unlock()
	if hold != nil {
		hold(idx)
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

func (f *fakeLink) snapshot() (posted []uint64, progress int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.posted), f.progress
}

// recordingTracker captures what a transfer loop reports.
type recordingTracker struct {
	mu        sync.Mutex
	completed []uint64
	bytes     uint64
	late      int
	dropped   []bool
	agedOuts  int
	skipped   uint64
	passed    map[unsentReason]uint64
	offered   int
	invalid   int
	// lost counts recordTargetLost, postErrors recordPostFailed.
	lost       int
	postErrors []string
	heads      []uint64
}

// testReadableWindow stands in for the reader's readable window in the
// tests that are not about ageing out. It is larger than any head they
// reach, so nothing in them is ever old enough to abandon.
const testReadableWindow = 1 << 20

func (rt *recordingTracker) recordCompleted(idx, bytes uint64, late bool, _ time.Time) {
	rt.mu.Lock()
	rt.completed = append(rt.completed, idx)
	rt.bytes += bytes
	if late {
		rt.late++
	}
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordDropped(outstanding bool) {
	rt.mu.Lock()
	rt.dropped = append(rt.dropped, outstanding)
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordAgedOut(time.Time) {
	rt.mu.Lock()
	rt.agedOuts++
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordSkipped(samples uint64) {
	rt.mu.Lock()
	rt.skipped += samples
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordOffered(time.Time) {
	rt.mu.Lock()
	rt.offered++
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordInvalid(time.Time) {
	rt.mu.Lock()
	rt.invalid++
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordTargetLost() {
	rt.mu.Lock()
	rt.lost++
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordPostFailed(err error) {
	rt.mu.Lock()
	rt.postErrors = append(rt.postErrors, err.Error())
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordPassedOver(reason unsentReason, grains uint64) {
	rt.mu.Lock()
	if rt.passed == nil {
		rt.passed = map[unsentReason]uint64{}
	}
	rt.passed[reason] += grains
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordHead(head uint64, _ time.Time) {
	rt.mu.Lock()
	rt.heads = append(rt.heads, head)
	rt.mu.Unlock()
}

func (rt *recordingTracker) skippedSnapshot() uint64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.skipped
}

func (rt *recordingTracker) headSnapshot() []uint64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return slices.Clone(rt.heads)
}

// snapshot returns the completed indices and the aged-out count.
func (rt *recordingTracker) snapshot() ([]uint64, int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return slices.Clone(rt.completed), rt.agedOuts
}

func (rt *recordingTracker) droppedSnapshot() []bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return slices.Clone(rt.dropped)
}

// runGrainLoop starts runTransferLoop with a 1 ms poll and returns a
// stop function that cancels it and waits for it.
func runGrainLoop(probe RuntimeProbe, transfer wholeGrain, progress ProgressFunc, interval time.Duration, tr progressTracker) (done chan struct{}, stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan struct{})
	p, t := readable(probe, transfer)
	go runTransferLoop(ctx, done, "flow-a", p, t, progress, progress, time.Millisecond, interval, tr)
	return done, func() {
		cancel()
		<-done
	}
}

// wholeGrain posts grain idx in one transfer and returns its payload.
type wholeGrain func(idx uint64) (uint64, error)

// readable makes transfer a TransferFunc that reads like a blocking
// reader: a grain past the head last probed is waited for a millisecond
// and reported as timed out.
func readable(probe RuntimeProbe, transfer wholeGrain) (RuntimeProbe, TransferFunc) {
	var head atomic.Uint64
	p := func() (uint64, error) {
		h, err := probe()
		if err == nil {
			head.Store(h)
		}
		return h, err
	}
	return p, func(idx uint64, _ uint16) (uint16, uint64, error) {
		if idx > head.Load() {
			time.Sleep(time.Millisecond)
			return 0, 0, mxl.ErrTimeout
		}
		bytes, err := transfer(idx)
		return 0, bytes, err
	}
}

// The loop waits in the blocking read for a grain not written yet, as
// runDiscrete in tools/mxl-fabrics-demo/demo.cpp blocks in
// mxlFlowReaderGetGrainSlice, and posts it as soon as the read returns
// it, without waiting a poll period.
func TestRunTransferLoop_WaitsInTheBlockingRead(t *testing.T) {
	src := &blockingSource{ready: time.Now().Add(20 * time.Millisecond)}
	dst := &offeredCounter{}
	tr := &recordingTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", func() (uint64, error) { return 10, nil },
		newGrainTransfer(src, dst, false, 1, 0), func() error { return nil }, func() error { return nil },
		time.Hour, time.Second, tr)
	defer func() { cancel(); <-done }()

	require.Eventually(t, func() bool { return dst.n.Load() >= 1 }, time.Second, time.Millisecond,
		"grain 11 was not posted once the read returned it")
	assert.Equal(t, sourceReadTimeout, src.timeout.Load().(time.Duration))
}

// blockingSource has grain 11 from ready on; a read of it before then
// blocks for its timeout and reports a timeout.
type blockingSource struct {
	ready   time.Time
	timeout atomic.Value
}

func (s *blockingSource) GetGrain(idx uint64, timeout time.Duration) (mxl.Grain, error) {
	s.timeout.Store(timeout)
	if idx != 11 || time.Now().Before(s.ready) {
		time.Sleep(timeout)
		return mxl.Grain{}, mxl.ErrTimeout
	}
	return mxl.Grain{Index: idx, TotalSlices: 1, ValidSlices: 1}, nil
}

func (s *blockingSource) GetGrainSlice(idx uint64, _ uint16, timeout time.Duration) (mxl.Grain, error) {
	return s.GetGrain(idx, timeout)
}

func TestRunTransferLoop_TailsFromHead(t *testing.T) {
	// The first probe is where the mirror tunes in; the producer's
	// earlier grains are not replayed.
	h := &headScript{heads: []uint64{10, 12, 15}}
	link := &fakeLink{t: t}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) >= 5 }, time.Second, time.Millisecond)
	stop()

	got, _ := tr.snapshot()
	assert.Equal(t, []uint64{11, 12, 13, 14, 15}, got)
}

// A transfer is complete only once MakeProgress stops returning
// not-ready (docs/FabricsDeveloperGuide.md, "Transferring discrete flows"). The
// loop drives each transfer that far and posts nothing while it is
// outstanding, as runDiscrete in tools/mxl-fabrics-demo/demo.cpp does.
func TestRunTransferLoop_ProgressesEachTransferUntilItCompletes(t *testing.T) {
	h := &headScript{heads: []uint64{10, 13}}
	link := &fakeLink{t: t, notReady: 3}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 3 }, time.Second, time.Millisecond)
	stop()

	posted, progress := link.snapshot()
	assert.Equal(t, []uint64{11, 12, 13}, posted)
	assert.Equal(t, 1+3*4, progress, "one call to connect, then three not-ready answers and one completion per transfer")
	got, _ := tr.snapshot()
	assert.Equal(t, []uint64{11, 12, 13}, got)
	assert.Zero(t, tr.late)
}

// A transfer that completes after its interval is counted late, with
// nothing posted meanwhile, and the next transfer is the next grain, not
// the head, as runDiscrete in tools/mxl-fabrics-demo/demo.cpp continues
// at ++grainIndex. Nothing is skipped.
func TestRunTransferLoop_LateTransferIsFollowedByTheNextGrain(t *testing.T) {
	var head atomic.Uint64
	head.Store(10)
	probe := func() (uint64, error) { return head.Load(), nil }
	link := &fakeLink{t: t}
	link.hold = func(idx uint64) {
		if idx == 11 && head.Load() == 11 {
			// The producer moves on by three grains, inside
			// maxMirrorLag, while grain 11 is still outstanding past its
			// deadline.
			time.Sleep(20 * time.Millisecond)
			head.Store(14)
		}
	}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(probe, link.transferGrain, link.makeProgress, 5*time.Millisecond, tr)
	require.Eventually(t, func() bool { return len(tr.headSnapshot()) > 1 }, time.Second, time.Millisecond)
	head.Store(11)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) >= 4 }, time.Second, time.Millisecond)
	stop()

	posted, _ := link.snapshot()
	assert.Equal(t, []uint64{11, 12, 13, 14}, posted[:4], "the grains written while 11 was outstanding go next")
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, 1, tr.late)
	assert.Zero(t, tr.passed[unsentResync], "a late transfer skips nothing")
	assert.Zero(t, tr.agedOuts, "a late transfer is not the reader ageing out")
}

// A connection-oriented initiator that dropped its only target answers
// interrupted, "No more targets" in RCInitiator::makeProgress
// (lib/fabrics/ofi/src/internal/RCInitiator.cpp). The loop gives the
// target up and stops, so the reconciler adds it again.
func TestRunTransferLoop_DroppedTargetStopsTheLoop(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11}}
	link := &fakeLink{t: t}
	var calls atomic.Int32
	progress := func() error {
		if calls.Add(1) == 1 {
			return nil // connected
		}
		return mxl.ErrInterrupted
	}
	tr := &recordingTracker{}
	done, stop := runGrainLoop(h.probe, link.transferGrain, progress, time.Second, tr)
	defer stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the loop kept running against a dropped target")
	}
	assert.Equal(t, []bool{true}, tr.droppedSnapshot(), "the outstanding transfer is counted lost")
	got, _ := tr.snapshot()
	assert.Empty(t, got)
}

// libmxl-fabrics skips a transfer to a target that is not connected
// yet and reports nothing outstanding (RCInitiatorEndpoint::transferGrain,
// RDMInitiatorTarget::transferGrain). The loop posts nothing and counts
// nothing until progress reports the target connected, as the connect
// loop after mxlFabricsInitiatorAddTarget in
// tools/mxl-fabrics-demo/demo.cpp does.
func TestRunTransferLoop_WaitsForTheTargetToConnect(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11, 12}}
	var connected atomic.Bool
	var calls atomic.Int32
	link := &fakeLink{t: t}
	progress := func() error {
		if !connected.Load() {
			if calls.Add(1) < 5 {
				return fabrics.ErrNotReady
			}
			connected.Store(true)
			return nil
		}
		return link.makeProgress()
	}
	transfer := func(idx uint64) (uint64, error) {
		if !connected.Load() {
			t.Errorf("grain %d posted before the target connected", idx)
		}
		return link.transferGrain(idx)
	}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, transfer, progress, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 2 }, time.Second, time.Millisecond)
	stop()

	assert.EqualValues(t, 5, calls.Load())
	got, _ := tr.snapshot()
	assert.Equal(t, []uint64{11, 12}, got, "the loop tunes in at the head it saw once connected")
}

// A target that does not connect within abandonAfter is given up with
// nothing posted, so the reconciler adds it again.
func TestRunTransferLoop_GivesUpATargetThatDoesNotConnect(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11}}
	link := &fakeLink{t: t}
	tr := &recordingTracker{}
	done, stop := runGrainLoop(h.probe, link.transferGrain, func() error { return fabrics.ErrNotReady }, time.Second, tr)
	defer stop()

	select {
	case <-done:
	case <-time.After(abandonAfter + time.Second):
		t.Fatal("the loop kept waiting for a target that does not connect")
	}
	assert.Equal(t, []bool{false}, tr.droppedSnapshot())
	posted, _ := link.snapshot()
	assert.Empty(t, posted)
	assert.Zero(t, h.n, "the head was read before the target connected")
}

// Progress is driven for a posted transfer only, never on a tick: with
// nothing new from the producer the loop makes no progress call, and
// nothing is left outstanding to be reaped later
// (docs/FabricsDeveloperGuide.md, "Transferring discrete flows").
func TestRunTransferLoop_MakesNoProgressCallWhileIdle(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11}}
	link := &fakeLink{t: t, notReady: 2}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Second, tr)
	defer stop()
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 1 }, time.Second, time.Millisecond)
	_, after := link.snapshot()

	// The loop polls the head every millisecond and finds nothing new.
	require.Eventually(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.n > 20 }, time.Second, time.Millisecond)
	_, idle := link.snapshot()
	assert.Equal(t, 1+3, after, "one call to connect, then two not-ready answers and one completion")
	assert.Equal(t, after, idle, "progress was called with no transfer outstanding")
}

// A grain goes in batches, each driven to completion before the next,
// and the loop moves to the next grain only after the last batch, as
// runDiscrete in tools/mxl-fabrics-demo/demo.cpp works on one grain
// until its slices are all transferred.
func TestRunTransferLoop_SendsAGrainInSliceBatches(t *testing.T) {
	h := &headScript{heads: []uint64{10, 12}}
	link := &fakeLink{t: t, notReady: 1}
	var mu sync.Mutex
	var starts []uint16
	transfer := func(idx uint64, start uint16) (uint16, uint64, error) {
		mu.Lock()
		starts = append(starts, start)
		mu.Unlock()
		if idx > 12 {
			time.Sleep(time.Millisecond)
			return 0, 0, mxl.ErrTimeout
		}
		if _, err := link.transferGrain(idx); err != nil {
			return start, 0, err
		}
		if start < 540 {
			return start + 270, 270, nil
		}
		return 0, 540, nil
	}
	tr := &recordingTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", h.probe, transfer, link.makeProgress, link.makeProgress,
		time.Millisecond, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) >= 6 }, time.Second, time.Millisecond)
	cancel()
	<-done

	posted, _ := link.snapshot()
	assert.Equal(t, []uint64{11, 11, 11, 12, 12, 12}, posted[:6])
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint16{0, 270, 540, 0, 270, 540}, starts[:6])
}

// A post the target is not ready for posts nothing. The loop drives
// progress, which moves a connecting target on, and offers the same
// grain again.
func TestRunTransferLoop_RetriesAPostTheTargetWasNotReadyFor(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11}}
	var posts []uint64
	var mu sync.Mutex
	transfer := func(idx uint64) (uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		posts = append(posts, idx)
		if len(posts) == 1 {
			return 0, fmt.Errorf("%w: %w", errPost, fabrics.ErrNotReady)
		}
		return 1, nil
	}
	var progress atomic.Int32
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, transfer, func() error { progress.Add(1); return nil }, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 1 }, time.Second, time.Millisecond)
	stop()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint64{11, 11}, posts)
	assert.Empty(t, tr.droppedSnapshot())
}

func TestRunTransferLoop_FailedPostGivesTheTargetUp(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11}}
	transfer := func(uint64) (uint64, error) {
		return 0, fmt.Errorf("%w: %w", errPost, errors.New("endpoint gone"))
	}
	tr := &recordingTracker{}
	done, stop := runGrainLoop(h.probe, transfer, func() error { return nil }, time.Second, tr)
	defer stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the loop kept running after a failed post")
	}
	assert.Equal(t, []bool{false}, tr.droppedSnapshot(), "nothing was outstanding")
}

func TestRunTransferLoop_ResyncsToHeadWhenLapped(t *testing.T) {
	// Attach race: the first probe reports head 0, then the real head
	// appears far ahead. Replaying from index 1 would wedge on grains
	// the producer no longer holds.
	h := &headScript{heads: []uint64{0, 1000}}
	link := &fakeLink{t: t}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) > 0 }, time.Second, time.Millisecond)
	stop()

	posted, _ := link.snapshot()
	assert.Equal(t, uint64(1000), posted[0])
	_, agedOuts := tr.snapshot()
	assert.Zero(t, agedOuts, "the attach case never held the grains it skips")
}

// The loop resyncs to the head once it trails it by more than
// maxMirrorLag grains, the gateway's own bound on mirror latency.
func TestRunTransferLoop_ResyncRecordsAgedOutOnFallBehind(t *testing.T) {
	h := &headScript{heads: []uint64{0, 2, 2, 100}}
	link := &fakeLink{t: t}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Second, tr)
	require.Eventually(t, func() bool { _, n := tr.snapshot(); return n >= 1 }, time.Second, time.Millisecond)
	stop()

	posted, _ := link.snapshot()
	assert.Equal(t, []uint64{1, 2, 100}, posted[:3])
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, uint64(97), tr.passed[unsentResync])
}

// A grain the reader reports too late for makes the loop jump to the
// head, as runDiscrete in tools/mxl-fabrics-demo/demo.cpp jumps to
// mxlGetCurrentIndex on MXL_ERR_OUT_OF_RANGE_TOO_LATE.
func TestRunTransferLoop_AdvancesLastSentOnAgedOutError(t *testing.T) {
	// A grain ages out while the lag stays within maxMirrorLag, so it
	// surfaces only as the reader's error. The loop never retries the
	// unrecoverable indices.
	h := &headScript{heads: []uint64{0, 3, 3, 6}}
	link := &fakeLink{t: t}
	var mu sync.Mutex
	var attempts []uint64
	transfer := func(idx uint64) (uint64, error) {
		mu.Lock()
		attempts = append(attempts, idx)
		mu.Unlock()
		if idx == 1 {
			return 0, errors.New("MXL_ERR_OUT_OF_RANGE: requested index 1 is out of range (too late)")
		}
		return link.transferGrain(idx)
	}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, transfer, link.makeProgress, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return slices.Contains(got, 4) }, time.Second, time.Millisecond)
	stop()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, uint64(1), attempts[0])
	assert.NotContains(t, attempts, uint64(2))
	assert.NotContains(t, attempts, uint64(3))
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.GreaterOrEqual(t, tr.agedOuts, 1)
	assert.Equal(t, uint64(3), tr.passed[unsentAgedOut])
}

// A grain the reader cannot hand out yet is read again after a poll;
// the loop does not give the target up over it.
func TestRunTransferLoop_UnreadableGrainIsReadAgain(t *testing.T) {
	h := &headScript{heads: []uint64{0, 3}}
	var calls atomic.Int32
	transfer := func(idx uint64) (uint64, error) {
		calls.Add(1)
		return 0, errors.New("out of range (too early)")
	}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, transfer, func() error { return nil }, time.Second, tr)
	require.Eventually(t, func() bool { return calls.Load() > 3 }, time.Second, time.Millisecond)
	stop()

	assert.Empty(t, tr.droppedSnapshot())
}

func TestRunTransferLoop_InitialProbeErrorReturnsEarly(t *testing.T) {
	probe := func() (uint64, error) { return 0, errors.New("dead reader") }
	transfer := func(uint64) (uint64, error) {
		t.Error("transferGrain must not be called when the initial probe errors")
		return 0, nil
	}
	done, stop := runGrainLoop(probe, transfer, func() error { return nil }, time.Second, &recordingTracker{})
	defer stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on initial probe error")
	}
}

func TestRunTransferLoop_CtxCancelExitsImmediately(t *testing.T) {
	h := &headScript{heads: []uint64{0}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	probe, transfer := readable(h.probe, func(uint64) (uint64, error) { return 0, nil })
	go runTransferLoop(ctx, done, "f", probe, transfer,
		func() error { return nil }, func() error { return nil }, time.Hour, time.Second, &recordingTracker{})
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not honour ctx cancel")
	}
}

// The entry is what the flusher and the collector read: completions,
// bytes, the last completed index and when it completed.
func TestRunTransferLoop_RecordsCompletionsOnTheEntry(t *testing.T) {
	h := &headScript{heads: []uint64{0, 3}}
	link := &fakeLink{t: t}
	e := &sourceEntry{}
	_, stop := runGrainLoop(h.probe, link.transferGrain, link.makeProgress, time.Second, e)
	require.Eventually(t, func() bool { return e.progress.Load() == 3 }, time.Second, time.Millisecond)
	stop()

	assert.Equal(t, uint64(300), e.bytes.Load())
	assert.Equal(t, uint64(3), e.lastCompleted.Load())
	assert.NotNil(t, e.lastSentAt.Load())
	assert.Zero(t, e.late.Load())
}

// fakeOpener is the inline initiatorOpener used by the Reconcile
// tests. openFn and attachFn are whatever the scenario wants the calls
// to return; loopFn, when set, runs as each mirror's transfer loop. The
// counters and the added / removed logs let a test assert how much
// libmxl work a reconcile did.
//
// opens counts readers: several mirrors of one flow must drive attaches
// without driving opens.
type fakeOpener struct {
	openFn   func(flowID string, provider fabrics.Provider) (*sharedSource, error)
	attachFn func(s *sharedSource, targetInfoStr string) error
	loopFn   func(ctx context.Context, targetInfoStr string, e *sourceEntry)

	opens    atomic.Int32
	attaches atomic.Int32
	detaches atomic.Int32

	mu      sync.Mutex
	names   map[*sourceLink]string
	added   []string
	removed []string
}

func (f *fakeOpener) open(key sourceKey) (*sharedSource, error) {
	f.opens.Add(1)
	if f.openFn != nil {
		return f.openFn(key.flowID, key.provider)
	}
	return &sharedSource{}, nil
}

func (f *fakeOpener) attach(s *sharedSource, targetInfoStr string, e *sourceEntry) (*sourceLink, error) {
	f.attaches.Add(1)
	if f.attachFn != nil {
		if err := f.attachFn(s, targetInfoStr); err != nil {
			return nil, err
		}
	}
	link := &sourceLink{}
	if f.loopFn != nil {
		ctx, cancel := context.WithCancel(context.Background())
		link.cancel, link.done = cancel, make(chan struct{})
		go func() {
			defer close(link.done)
			f.loopFn(ctx, targetInfoStr, e)
		}()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.names == nil {
		f.names = make(map[*sourceLink]string)
	}
	f.names[link] = targetInfoStr
	f.added = append(f.added, targetInfoStr)
	return link, nil
}

func (f *fakeOpener) detach(link *sourceLink) {
	f.detaches.Add(1)
	link.stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, f.names[link])
}

// addedTargets and removedTargets report the target infos AddTarget
// and RemoveTarget were called with, in call order.
func (f *fakeOpener) addedTargets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.added)
}

func (f *fakeOpener) removedTargets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.removed)
}

// testFlowKey is the shared source the entry helpers below hang off,
// for tests that need an entry attached to something without opening
// libmxl handles.
var testFlowKey = sourceKey{flowID: "flow-1", provider: fabrics.ProviderTCP}

// testShared builds a sharedSource carrying no libmxl handles.
func testShared(flowID string, provider fabrics.Provider) *sharedSource {
	return &sharedSource{key: sourceKey{flowID: flowID, provider: provider}}
}

func newSourceTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func mirrorWithFinalizer(name, ns, node, flowID, targetInfo string) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  ns,
			Finalizers: []string{SourceFinalizerName},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: node,
			TargetNode: "node-b",
			// Mirrors carry a concrete provider once the agent or
			// operator has resolved it; an unresolved spec stops at
			// providerForSetup before the paths these tests exercise.
			Provider: mxlv1alpha1.ProviderTCP,
		},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{
			TargetInfo: targetInfo,
		},
	}
}

func TestReconcile_AddTargetFailureSurfacesConditionAndCapsBackoffAt30s(t *testing.T) {
	// A persistently failing AddTarget (e.g. target gateway not yet
	// reachable on the fabric address) must: publish SourceProgress=
	// AddTargetFailed via SSA on every attempt, count attempts in
	// status.attemptCount, and cap the requeue delay at 30s so a
	// permanently-down target consumes one reconcile per 30s rather
	// than spinning on the initiator rebuild path.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "stale-info")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	addErr := errors.New("connect: target offline")
	clock, advance := fixedClock(time.Now())
	r := &SourceReconciler{
		nowFn:    clock,
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		opener: &fakeOpener{
			attachFn: func(*sharedSource, string) error {
				return errors.Join(errAddTargetFailed, addErr)
			},
		},
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	req := reconcile.Request{NamespacedName: key}

	// First failure: requeue at 100ms, attempts=1, condition published.
	res, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err,
		"AddTarget failure must be returned as a benign requeue, not a "+
			"controller error, so controller-runtime does not log it as a "+
			"reconciler crash on every tick")
	assert.Equal(t, 100*time.Millisecond, res.RequeueAfter,
		"first failure backoff must be 100ms - the seed of the geometric series")

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.Len(t, got.Status.Conditions, 1)
	cond := got.Status.Conditions[0]
	assert.Equal(t, mxlv1alpha1.ConditionTypeSourceProgress, cond.Type)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonAddTargetFailed, cond.Reason)
	assert.Equal(t, int32(1), got.Status.AttemptCount)
	assert.Contains(t, got.Status.LastError, addErr.Error())

	// Drive enough additional failures that the geometric series hits
	// the 30s cap (2^9 * 100ms = 51.2s > 30s, so attempts=9 caps).
	var lastResult ctrl.Result
	for i := 0; i < 9; i++ {
		advance(backoffFor(uint32(i + 1)))
		lastResult, err = r.Reconcile(context.Background(), req)
		require.NoError(t, err)
	}
	assert.Equal(t, 30*time.Second, lastResult.RequeueAfter,
		"unbounded backoff would let a flapping AddTarget mark the gateway "+
			"unresponsive to a real recovery; the 30s cap matches the "+
			"controller-runtime default rate-limiter ceiling")

	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.GreaterOrEqual(t, got.Status.AttemptCount, int32(10),
		"attemptCount must keep advancing past the cap so an operator can "+
			"distinguish 'unreachable for an hour' from 'just failed once'")
}

func TestReconcile_FlowOriginRotationReopensReader(t *testing.T) {
	// Pod restart on the source node: the agent re-registers the flow
	// and the MxlFlow's Origin location entry gets a fresh AppearedAt
	// timestamp. The source reconciler must tear down the existing
	// FlowReader and reopen against the freshly bound writer, otherwise
	// the reader holds an invalid handle and the transfer loop stalls
	// silently.
	scheme := newSourceTestScheme(t)
	flowID := "flow-1"
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", flowID, "info-1")
	originalOriginAt := metav1.NewTime(time.Now().Add(-time.Hour))
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{
				NodeName:     "node-a",
				Phase:        mxlv1alpha1.MxlFlowLocationOrigin,
				LastObserved: &originalOriginAt,
				AppearedAt:   &originalOriginAt,
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, flow).
		Build()

	opener := &fakeOpener{
		openFn: func(string, fabrics.Provider) (*sharedSource, error) {
			// A real opener would spawn the transfer goroutine and
			// hand it the source as tracker. The test never wires
			// that up, so the source stays inert and closeShared
			// below is a no-op.
			return &sharedSource{}, nil
		},
	}

	r := &SourceReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		opener:        opener,
		FlushInterval: time.Hour,
		sources:       map[types.NamespacedName]*sourceEntry{},
		attempts:      attemptTable[sourceAddInputs]{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	req := reconcile.Request{NamespacedName: key}

	// First reconcile: opens the initiator, records the origin timestamp.
	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int32(1), opener.opens.Load())
	t.Cleanup(func() { r.closeEntry(key) })

	// Same timestamp on the MxlFlow: no rotation, no reopen.
	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(1), opener.opens.Load(),
		"a Reconcile with no origin rotation must hit the fast path; "+
			"opening the initiator twice would tear down the live transfer "+
			"goroutine every controller-runtime tick")

	// Bump the MxlFlow's Origin AppearedAt into the future and ensure
	// the next reconcile reopens.
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: flowID}, &f))
	newer := metav1.NewTime(originalOriginAt.Time.Add(time.Minute))
	f.Status.Locations[0].AppearedAt = &newer
	require.NoError(t, c.Status().Update(context.Background(), &f))

	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(2), opener.opens.Load(),
		"a fresher Origin AppearedAt must reopen the FlowReader so the "+
			"gateway tails the rebound writer instead of the stale handle")
}

func TestReconcile_SourceNodeMutatedAwayClosesEntry(t *testing.T) {
	// The operator mutates spec.sourceNode away from this node (e.g.
	// the source agent moves to a different host). The MxlFlowMirror
	// watch is unfiltered, so the spec change wakes Reconcile here.
	// Without the closeEntry call ahead of the early return, the
	// in-memory sourceEntry stays installed with an RCInitiator open
	// against a producer-less .mxl-flow until the pod is bounced.
	scheme := newSourceTestScheme(t)
	const selfNode = "node-a"
	const otherNode = "node-b"
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "m1",
			Namespace:  "ns1",
			Finalizers: []string{SourceFinalizerName},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     "flow-1",
			SourceNode: otherNode,
			TargetNode: "node-c",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: selfNode,
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}

	// Pre-seed: a previous reconcile (when spec.sourceNode == selfNode)
	// installed an entry on this node. The teardown is nil-safe
	// for every field, so a zero-value entry is enough to detect the
	// removal without spinning up real fabrics handles.
	r.sources[key] = &sourceEntry{}

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)

	r.mu.Lock()
	_, present := r.sources[key]
	r.mu.Unlock()
	require.False(t, present,
		"entry must be removed when spec.sourceNode is mutated away; "+
			"otherwise the RCInitiator keeps ticking against a producer-less "+
			"flow until the pod restarts")
}

func TestSource_SeedAttemptsFromStatus(t *testing.T) {
	// A gateway pod restart drops every in-memory counter. Without a
	// seed from status, a mirror whose target has been unreachable
	// long enough to climb the backoff schedule resets to attempts=0
	// and the next reconcile retries the AddTarget immediately. The
	// seed restores the counter so the geometric backoff resumes
	// from the persisted attemptCount; the timestamp is deliberately
	// not restored so one free retry on restart still fires.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "stale-info")
	mirror.Status.AttemptCount = 5
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		opener: &fakeOpener{
			attachFn: func(*sharedSource, string) error {
				return errors.Join(errAddTargetFailed, errors.New("offline"))
			},
		},
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)

	r.mu.Lock()
	got := r.attempts.count(key)
	r.mu.Unlock()
	assert.Equal(t, uint32(6), got,
		"seed must restore the persisted attemptCount (5) before the "+
			"failed AddTarget on this reconcile bumps it to 6; without "+
			"the seed the counter would have started from 0 and a "+
			"long-unreachable target would re-enter fast retries on "+
			"every gateway pod restart")
}

func TestBackoffFor_Schedule(t *testing.T) {
	// 100ms * 2^(attempts-1) capped at 30s. The bookend cases catch
	// off-by-one regressions in the geometric series.
	cases := []struct {
		attempts uint32
		want     time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{8, 12800 * time.Millisecond},
		{9, 25600 * time.Millisecond},
		{10, 30 * time.Second},
		{100, 30 * time.Second},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.want, backoffFor(tc.attempts),
			"backoffFor(%d)", tc.attempts)
	}
}

// mirrorFlowIDIndexer is the same indexer the production
// SetupWithManager registers; declared once here so every Lease
// watch test wires the fake client with the identical key/value
// shape and a typo in either place fails at compile time rather
// than as a silent empty list at runtime.
var mirrorFlowIDIndexer = indexMirrorByFlow

func TestSource_LeaseWatch_NodeFiltered(t *testing.T) {
	// The Lease name encodes (flowID, nodeName). A Lease for another
	// node must not wake this gateway's source reconciler - an N-pod,
	// M-flow cluster would otherwise produce N*M wakeups on every
	// renew tick. A Lease for this node with no matching mirror at
	// the gateway must return nil too: the agent renews Leases for
	// every flow it hosts, and only the subset whose mirrors live
	// here is interesting.
	scheme := newSourceTestScheme(t)
	// ParseLeaseName splits on the last dash, so node names used in
	// LeaseName round-trip cleanly only when they contain no dash.
	const (
		flowID = "11111111-2222-3333-4444-555555555555"
		myNode = "nodea"
	)

	managed := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "m-managed"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: myNode,
			TargetNode: "nodeb",
		},
	}
	// Mirror at this gateway but with a different flowID - the
	// field-index lookup must exclude it.
	otherFlow := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "m-other-flow"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     "other-flow",
			SourceNode: myNode,
			TargetNode: "nodeb",
		},
	}
	// Mirror for the right flow but with SourceNode on another
	// gateway - the SourceNode in-memory filter must exclude it.
	foreignSource := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "m-foreign"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: "nodex",
			TargetNode: "nodeb",
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&mxlv1alpha1.MxlFlowMirror{}, mirrorFlowIDIndex, mirrorFlowIDIndexer).
		WithObjects(managed, otherFlow, foreignSource).
		Build()
	r := &SourceReconciler{Client: c, NodeName: myNode}

	t.Run("local lease for managed flow enqueues only the local mirror", func(t *testing.T) {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: mxlv1alpha1.LeaseNamespace,
				Name:      mxlv1alpha1.LeaseName(flowID, myNode),
			},
		}
		got := r.leaseToMirrors(context.Background(), lease)
		require.Len(t, got, 1,
			"the field index returns both mirrors for flowID, then the "+
				"in-memory SourceNode filter drops the one whose source "+
				"is on another gateway; one match remains")
		assert.Equal(t, client.ObjectKey{Namespace: "ns1", Name: "m-managed"},
			got[0].NamespacedName)
	})

	t.Run("lease for another node returns nil", func(t *testing.T) {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: mxlv1alpha1.LeaseNamespace,
				Name:      mxlv1alpha1.LeaseName(flowID, "nodex"),
			},
		}
		assert.Empty(t, r.leaseToMirrors(context.Background(), lease),
			"without the per-pod nodeName filter every gateway pod would "+
				"wake on every renew tick for every flow")
	})

	t.Run("lease for unknown flow returns nil", func(t *testing.T) {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: mxlv1alpha1.LeaseNamespace,
				Name:      mxlv1alpha1.LeaseName("nomirrorhere", myNode),
			},
		}
		assert.Empty(t, r.leaseToMirrors(context.Background(), lease),
			"a Lease the agent publishes for a flow no local mirror "+
				"references must not generate a reconcile request")
	})
}

func TestSource_LeaseInMxlSystemPredicate(t *testing.T) {
	pred := leaseInMxlSystem()

	cases := []struct {
		ns   string
		want bool
	}{
		{ns: mxlv1alpha1.LeaseNamespace, want: true},
		{ns: "kube-system", want: false},
		{ns: "kube-node-lease", want: false},
		{ns: "default", want: false},
		{ns: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.ns, func(t *testing.T) {
			obj := &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Namespace: tc.ns, Name: "x"},
			}
			assert.Equal(t, tc.want, pred.Create(event.CreateEvent{Object: obj}))
			assert.Equal(t, tc.want, pred.Delete(event.DeleteEvent{Object: obj}))
			assert.Equal(t, tc.want, pred.Update(event.UpdateEvent{ObjectOld: obj, ObjectNew: obj}))
			assert.Equal(t, tc.want, pred.Generic(event.GenericEvent{Object: obj}))
		})
	}
}

func TestSource_LeaseToMirrors_MalformedNameDropped(t *testing.T) {
	scheme := newSourceTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&mxlv1alpha1.MxlFlowMirror{}, mirrorFlowIDIndex, mirrorFlowIDIndexer).
		WithObjects(&mxlv1alpha1.MxlFlowMirror{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "m1"},
			Spec: mxlv1alpha1.MxlFlowMirrorSpec{
				FlowID:     "flow-id",
				SourceNode: "node-a",
				TargetNode: "node-b",
			},
		}).
		Build()
	r := &SourceReconciler{Client: c, NodeName: "node-a"}

	cases := []string{
		"unrelated-lease",         // no mxl-flow- prefix
		"mxl-flow-",               // empty remainder
		"mxl-flow-only-prefix",    // no trailing -node
		"mxl-flow-flow-id-",       // empty node segment
		"kube-controller-manager", // leader election lease
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			lease := &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: mxlv1alpha1.LeaseNamespace,
					Name:      name,
				},
			}
			assert.Empty(t, r.leaseToMirrors(context.Background(), lease),
				"a Lease whose name does not match the LeaseName format "+
					"must not enqueue anything; otherwise an unrelated "+
					"coordination Lease that leaked into mxl-system would "+
					"wake every source-side mirror on this gateway")
		})
	}
}

func TestSource_LeaseToMirrors_NonLeaseObjectReturnsNil(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newSourceTestScheme(t)).Build()
	r := &SourceReconciler{Client: c, NodeName: "node-a"}
	assert.Nil(t, r.leaseToMirrors(context.Background(), &corev1.Pod{}),
		"the mapper guards against a misconfigured Watches by ignoring "+
			"non-Lease events; without this every Pod event would feed "+
			"ParseLeaseName a pod name")
}

func TestPublishSourceProgress_IncludesLastSentAt(t *testing.T) {
	// The target-side stuck-handshake watchdog discriminates a wedged
	// post-handshake target from a legitimately idle flow by comparing
	// mirror.Status.LastSentAt against the local lastCommitAt. The
	// source-side SSA payload must carry that timestamp on every
	// publish that has one; omitting it after the first publish would
	// release SSA field-ownership and let the apiserver strip the
	// field on the next tick.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	sent := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, r.publishSourceProgress(context.Background(), key, sourceProgressState{
		status:     metav1.ConditionTrue,
		reason:     mxlv1alpha1.ReasonHandshakeComplete,
		message:    "initiator running",
		lastSentAt: &sent,
	}))

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.NotNil(t, got.Status.LastSentAt,
		"publishSourceProgress with a non-nil lastSentAt must stamp "+
			"status.lastSentAt; without it the target-side watchdog cannot "+
			"tell a wedged post-handshake target from an idle flow")
	assert.True(t, got.Status.LastSentAt.Time.Equal(sent),
		"the stamped timestamp must round-trip: target-side recovery "+
			"compares it against lastCommitAt to decide whether to rebuild")
}

func TestSourceStateEqual_DereferencesLastSentAtPointers(t *testing.T) {
	// observedState allocates a fresh *time.Time on every flusher tick
	// (copying the atomic-loaded value through a local variable to
	// detach it from the pointer that recordCompleted stored). The
	// dedupe must therefore compare timestamps by value: a struct-level
	// == would always disagree once lastSentAt is non-nil and the
	// flusher would publish on every tick. Codifies the contract:
	// equality holds when the wall-clock value is the same, not when
	// the pointer is the same.
	now := time.Now()
	a := sourceProgressState{
		status:  metav1.ConditionTrue,
		reason:  mxlv1alpha1.ReasonHandshakeComplete,
		message: "initiator running",
	}
	b := a

	// Two distinct *time.Time values with the same wall-clock equal.
	t1 := now
	t2 := now
	a.lastSentAt = &t1
	b.lastSentAt = &t2
	assert.True(t, sourceStateEqual(a, b),
		"two states whose lastSentAt pointers reference distinct copies of "+
			"the same wall-clock must compare equal; without this the source "+
			"flusher would publish status on every tick after the first "+
			"transfer and burn through the apiserver write budget")

	// A timestamp that moved less than one quantum is still equal: at
	// grain rate the value differs on every tick, and publishing each
	// one is the write stream this dedupe exists to prevent.
	nudged := now.Add(time.Millisecond)
	b.lastSentAt = &nudged
	assert.True(t, sourceStateEqual(a, b),
		"a sub-quantum move must not defeat the dedupe; a millisecond of "+
			"freshness is not something any observer of this status can act "+
			"on, and republishing it costs an etcd write and a cluster-wide "+
			"watch fan-out")

	// A move of a full quantum or more is published: the target-side
	// stuck-handshake watchdog discriminates on this field, so the
	// freshness has to keep reaching it well inside its own window.
	later := now.Add(statusQuantum)
	b.lastSentAt = &later
	assert.False(t, sourceStateEqual(a, b),
		"a lastSentAt that advanced by a full quantum must defeat the "+
			"dedupe so the next SSA payload carries it; the target-side "+
			"watchdog reads this field to tell a wedged source from an "+
			"idle one")
	assert.Less(t, statusQuantum, defaultStuckHandshakeAfter,
		"the publish quantum has to stay well inside the stuck-handshake "+
			"window, or the watchdog would trip on a timestamp that is "+
			"merely waiting to be published")

	// nil/non-nil asymmetry is not equal.
	b.lastSentAt = nil
	assert.False(t, sourceStateEqual(a, b))
	a.lastSentAt = nil
	assert.True(t, sourceStateEqual(a, b),
		"two states both missing lastSentAt must compare equal regardless "+
			"of other field defaults; otherwise the pre-transfer ticks would "+
			"churn writes for an unchanged state")
}

func TestSource_FlusherSuppressesIdenticalTicksAfterFirstTransfer(t *testing.T) {
	// After one successful transfer the entry's lastSentAt stays
	// frozen until the next transferGrain call. The flusher must
	// detect that the observed state has not changed and skip the
	// publish; otherwise every tick after the first grain would write
	// status with a duplicate condition and accumulate apiserver load
	// proportional to the flusher tick frequency.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &SourceReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		FlushInterval: 2 * time.Millisecond,
		sources:       map[types.NamespacedName]*sourceEntry{},
		attempts:      attemptTable[sourceAddInputs]{},
	}

	entry := &sourceEntry{key: key, shared: testShared("flow-1", fabrics.ProviderTCP), infoStr: "info-1"}
	entry.progress.Store(1)
	transferAt := time.Now()
	entry.lastSentAt.Store(&transferAt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.runFlusher(ctx, done, entry, 2*time.Millisecond)

	// Wait for the first publish so the assertion has a known
	// baseline ResourceVersion to compare against.
	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		if err := c.Get(context.Background(), key, &got); err != nil {
			return false
		}
		return got.Status.LastSentAt != nil
	}, time.Second, time.Millisecond,
		"flusher must publish the first state so the dedupe has a baseline")

	var baseline mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &baseline))
	baselineRV := baseline.ResourceVersion

	// Let several more ticks fire without changing any tracker on the
	// entry. The dedupe must suppress every one of them.
	time.Sleep(40 * time.Millisecond)

	cancel()
	<-done

	var after mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &after))
	assert.Equal(t, baselineRV, after.ResourceVersion,
		"ResourceVersion must not advance when nothing has changed on the "+
			"entry; a comparison that treated fresh *time.Time allocations as "+
			"distinct would write status on every flusher tick and let the "+
			"source gateway flood the apiserver with redundant patches")
}

func TestPublishSourceProgress_OmitsLastSentAtWhenNeverTransferred(t *testing.T) {
	// Before any grain has been transferred, lastSentAt must stay
	// absent: a zero-time stamp would let the target-side watchdog
	// compute a delta against an arbitrary point in 0001-01-01 and
	// trip the wedge condition immediately.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	require.NoError(t, r.publishSourceProgress(context.Background(), key, sourceProgressState{
		status:  metav1.ConditionTrue,
		reason:  mxlv1alpha1.ReasonHandshakeComplete,
		message: "initiator running",
	}))

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.Nil(t, got.Status.LastSentAt,
		"a sourceProgressState with nil lastSentAt must omit the key from "+
			"the SSA payload; stamping a zero time would trigger an immediate "+
			"target-side rebuild on every freshly created mirror")
}

// A target the initiator dropped is marked lost, so detach does not
// remove it from an initiator that already evicted it (removeTarget in
// RCInitiator.cpp answers "not found" for it).
func TestRunTransferLoop_MarksADroppedTargetLost(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11}}
	link := &fakeLink{t: t}
	var calls atomic.Int32
	progress := func() error {
		if calls.Add(1) == 1 {
			return nil // connected
		}
		return mxl.ErrInterrupted
	}
	tr := &recordingTracker{}
	done, stop := runGrainLoop(h.probe, link.transferGrain, progress, time.Second, tr)
	defer stop()
	<-done

	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, 1, tr.lost)
	assert.Empty(t, tr.postErrors)
}

// A post the initiator rejects for good leaves the target connected: it
// is kept as the reason, not marked lost.
func TestRunTransferLoop_KeepsAPostRejectedForGood(t *testing.T) {
	h := &headScript{heads: []uint64{10, 11}}
	transfer := func(uint64) (uint64, error) {
		return 0, fmt.Errorf("%w: %w", errPost, mxl.ErrInvalidArg)
	}
	tr := &recordingTracker{}
	done, stop := runGrainLoop(h.probe, transfer, func() error { return nil }, time.Second, tr)
	defer stop()
	<-done

	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Zero(t, tr.lost)
	require.Len(t, tr.postErrors, 1)
}

// A link's loop marks the link itself when its target is lost.
func TestLinkTracker_MarksTheLink(t *testing.T) {
	link := &sourceLink{}
	linkTracker{sourceEntry: &sourceEntry{}, link: link}.recordTargetLost()
	assert.True(t, link.targetLost.Load())
}

// The loop moves past an invalid video grain without posting it and
// counts it; the next grain goes as usual.
func TestRunTransferLoop_SkipsAnInvalidVideoGrain(t *testing.T) {
	h := &headScript{heads: []uint64{10, 13}}
	link := &fakeLink{t: t}
	transfer := func(idx uint64) (uint64, error) {
		if idx == 12 {
			return 0, errInvalidGrain
		}
		return link.transferGrain(idx)
	}
	tr := &recordingTracker{}
	_, stop := runGrainLoop(h.probe, transfer, link.makeProgress, time.Second, tr)
	require.Eventually(t, func() bool { got, _ := tr.snapshot(); return len(got) == 2 }, time.Second, time.Millisecond)
	stop()

	posted, _ := link.snapshot()
	assert.Equal(t, []uint64{11, 13}, posted)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, 1, tr.invalid)
	assert.Equal(t, 2, tr.offered, "an invalid grain is not counted as offered")
}
