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
	"go.uber.org/goleak"

	"github.com/qvest-digital/go-mxl/fabrics"
)

func TestMain(m *testing.M) {
	// Catch goroutine leaks from the loops under test. Every Test*
	// in this package starts at most one progress goroutine; if it
	// outlives the test, the loop forgot to honour ctx.
	goleak.VerifyTestMain(m)
}

// transferFixture wraps the canned probes/transfers a test wants to
// feed into runTransferLoop, plus the counters and call logs that
// the assertions read.
type transferFixture struct {
	mu sync.Mutex

	headSeq   []uint64
	headIdx   int
	headErr   error
	headCalls int

	transferLog  []uint64
	transferErr  error
	transferSkip map[uint64]bool

	progressErr   error
	progressCalls int
}

func (f *transferFixture) probeRuntime() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headCalls++
	if f.headErr != nil {
		return 0, f.headErr
	}
	if f.headIdx >= len(f.headSeq) {
		return f.headSeq[len(f.headSeq)-1], nil
	}
	h := f.headSeq[f.headIdx]
	f.headIdx++
	return h, nil
}

func (f *transferFixture) transferGrain(idx uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transferLog = append(f.transferLog, idx)
	if f.transferErr != nil {
		return false, f.transferErr
	}
	if f.transferSkip[idx] {
		return true, nil
	}
	return false, nil
}

func (f *transferFixture) makeProgress() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progressCalls++
	return f.progressErr
}

func (f *transferFixture) transferred() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]uint64, len(f.transferLog))
	copy(out, f.transferLog)
	return out
}

func TestRunTransferLoop_TailsFromHead(t *testing.T) {
	// First Runtime probe reports head=10. Subsequent probes report
	// 12 and 15. The loop must transfer (11, 12, 13, 14, 15) and
	// never anything <= 10 - the initial head is "where we tune in",
	// the producer's historical grains are not replayed.
	fx := &transferFixture{
		headSeq: []uint64{10, 12, 15, 15},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go runTransferLoop(ctx, done, "flow-a", fx.probeRuntime, fx.transferGrain, fx.makeProgress, time.Millisecond)

	// Let the loop run a few ticks then cancel.
	require.Eventually(t, func() bool {
		fx.mu.Lock()
		defer fx.mu.Unlock()
		return len(fx.transferLog) >= 5
	}, time.Second, time.Millisecond, "expected >=5 transfers")

	cancel()
	<-done

	got := fx.transferred()
	assert.GreaterOrEqual(t, len(got), 5)
	for i, idx := range got[:5] {
		expected := uint64(11 + i)
		assert.Equalf(t, expected, idx,
			"transfers must arrive in head-tail order; got idx[%d]=%d, want %d",
			i, idx, expected)
	}
}

func TestRunTransferLoop_TransferErrorBreaksInnerLoopButLoopSurvives(t *testing.T) {
	// On a tick that fails the transfer, the loop must break the
	// per-grain inner pass but keep going on the next tick. If it
	// exited entirely, a single transient TransferGrain error would
	// stall the flow forever.

	// Custom probe that reports 0 once, then 3 forever.
	var calls atomic.Int32
	probe := func() (uint64, error) {
		if calls.Add(1) == 1 {
			return 0, nil
		}
		return 3, nil
	}

	var transferCalls atomic.Int32
	transfer := func(idx uint64) (bool, error) {
		transferCalls.Add(1)
		if idx == 2 {
			return false, errors.New("transient")
		}
		return false, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, time.Millisecond)

	// Give the loop time to attempt many ticks (so the error path is
	// hit at least once, and the next-tick recovery is tried too).
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Even though idx=2 always errors, the loop must keep being
	// invoked - so transferCalls grows past a single tick's worth.
	assert.Greater(t, int(transferCalls.Load()), 3,
		"a transient transfer error must not stop the loop; future ticks "+
			"must retry from lastSent+1 (idx=2 again here)")
}

func TestRunTransferLoop_InitialProbeErrorReturnsEarly(t *testing.T) {
	// If the very first Runtime probe fails, the loop returns and
	// closes done without ever spinning. Catches a regression where
	// the loop would proceed with a zero head index and silently
	// flood the initiator with stale grains.
	probe := func() (uint64, error) { return 0, errors.New("dead reader") }
	transfer := func(uint64) (bool, error) {
		t.Fatal("transferGrain must not be called when initial probe errors")
		return false, nil
	}
	progress := func() error {
		t.Fatal("makeProgress must not be called when initial probe errors")
		return nil
	}

	done := make(chan struct{})
	go runTransferLoop(context.Background(), done, "f", probe, transfer, progress, time.Millisecond)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on initial probe error")
	}
}

func TestRunTransferLoop_ProgressErrNotReadyIsSwallowed(t *testing.T) {
	// fabrics.ErrNotReady is the normal "queue is empty" signal;
	// the loop must keep ticking. Any other error from MakeProgress
	// is logged but does not stop the loop either - the next tick
	// recovers.
	probe := func() (uint64, error) { return 0, nil }
	transfer := func(uint64) (bool, error) {
		return false, nil
	}
	calls := atomic.Int32{}
	progress := func() error {
		calls.Add(1)
		return fabrics.ErrNotReady
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", probe, transfer, progress, time.Millisecond)

	require.Eventually(t, func() bool { return calls.Load() >= 3 },
		time.Second, time.Millisecond,
		"expected MakeProgress to be called multiple times despite ErrNotReady")

	cancel()
	<-done
}

func TestRunTransferLoop_CtxCancelExitsImmediately(t *testing.T) {
	probe := func() (uint64, error) { return 0, nil }
	transfer := func(uint64) (bool, error) { return false, nil }
	progress := func() error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", probe, transfer, progress, time.Hour)

	// Cancel before the first tick fires.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not honour ctx cancel before first tick")
	}
}
