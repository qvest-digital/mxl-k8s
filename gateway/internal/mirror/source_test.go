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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
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

	go runTransferLoop(ctx, done, "flow-a", fx.probeRuntime, fx.transferGrain, fx.makeProgress, time.Millisecond, nil)

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
	go runTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, time.Millisecond, nil)

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
	go runTransferLoop(context.Background(), done, "f", probe, transfer, progress, time.Millisecond, nil)

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
	go runTransferLoop(ctx, done, "f", probe, transfer, progress, time.Millisecond, nil)

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
	go runTransferLoop(ctx, done, "f", probe, transfer, progress, time.Hour, nil)

	// Cancel before the first tick fires.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not honour ctx cancel before first tick")
	}
}

// recordingTracker captures every transfer/agedOut call the loop
// makes so assertions can read them after cancel.
type recordingTracker struct {
	mu        sync.Mutex
	transfers []uint64
	agedOuts  int
}

func (rt *recordingTracker) recordTransfer(idx uint64, _ time.Time) {
	rt.mu.Lock()
	rt.transfers = append(rt.transfers, idx)
	rt.mu.Unlock()
}

func (rt *recordingTracker) recordAgedOut(_ time.Time) {
	rt.mu.Lock()
	rt.agedOuts++
	rt.mu.Unlock()
}

func (rt *recordingTracker) snapshot() ([]uint64, int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]uint64, len(rt.transfers))
	copy(out, rt.transfers)
	return out, rt.agedOuts
}

func TestRunTransferLoop_AdvancesLastSentOnAgedOutError(t *testing.T) {
	// The writer has lapped the reader. Initial probe head=0 means
	// the loop's lastSent starts at 0. On the first tick the probe
	// reports head=5, so the loop tries idx=1 first; transferGrain
	// returns the libmxl "out of range (too late)" string. The loop
	// must advance lastSent to head (5) and signal the tracker. On
	// the next tick the probe reports head=20: transfers must resume
	// from 6 onward, never re-attempting the unrecoverable 2..5.
	var probeCalls atomic.Int32
	probe := func() (uint64, error) {
		switch probeCalls.Add(1) {
		case 1:
			return 0, nil
		case 2:
			return 5, nil
		default:
			return 20, nil
		}
	}

	var mu sync.Mutex
	var attempts []uint64
	transfer := func(idx uint64) (bool, error) {
		mu.Lock()
		attempts = append(attempts, idx)
		mu.Unlock()
		if idx == 1 {
			return false, errors.New("MXL_ERR_OUT_OF_RANGE: requested index 1 is out of range (too late)")
		}
		return false, nil
	}

	tracker := &recordingTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, time.Millisecond, tracker)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, v := range attempts {
			if v == 6 {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond,
		"loop must resume from head+1 after an aged-out skip; "+
			"a retry from lastSent+1 would loop on idx=1 forever")

	cancel()
	<-done

	mu.Lock()
	gotAttempts := append([]uint64(nil), attempts...)
	mu.Unlock()
	transfers, agedOuts := tracker.snapshot()

	assert.Equal(t, uint64(1), gotAttempts[0],
		"first transfer attempt must be lastSent+1 = 1; "+
			"head0=0 was the initial probe, so the loop starts from 1")
	assert.GreaterOrEqual(t, agedOuts, 1,
		"the aged-out skip must reach the tracker so the reconciler "+
			"can surface SourceProgress=ReaderAgedOut")
	for _, idx := range transfers {
		assert.NotContains(t, []uint64{1, 2, 3, 4, 5}, idx,
			"a recorded transfer for an aged-out index would mean the "+
				"loop counted a fictional success against the tracker")
	}
}

func TestRunTransferLoop_TracksProgressAndLastSentAt(t *testing.T) {
	// Every successful transferGrain must be reflected on the tracker:
	// the per-mirror flusher reads progress + lastSentAt to decide
	// whether to publish a SourceProgress=Recovered condition.
	var calls atomic.Int32
	probe := func() (uint64, error) {
		if calls.Add(1) == 1 {
			return 0, nil
		}
		return 3, nil
	}
	transfer := func(uint64) (bool, error) { return false, nil }

	tracker := &recordingTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, time.Millisecond, tracker)

	require.Eventually(t, func() bool {
		got, _ := tracker.snapshot()
		return len(got) >= 3
	}, time.Second, time.Millisecond,
		"tracker must observe one recordTransfer per successful grain")

	cancel()
	<-done

	transfers, _ := tracker.snapshot()
	assert.GreaterOrEqual(t, len(transfers), 3)
	for i := 0; i < 3; i++ {
		assert.Equal(t, uint64(i+1), transfers[i],
			"recordTransfer must observe the same monotonic index sequence "+
				"the loop hands to transferGrain")
	}
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
	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		openInitiatorFn: func(string, string, fabrics.Provider) (*sourceEntry, error) {
			return nil, errors.Join(errAddTargetFailed, addErr)
		},
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: map[types.NamespacedName]uint32{},
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
	// and the MxlFlow's Origin location entry gets a fresh LastObserved
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
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, flow).
		Build()

	var openCalls atomic.Int32
	openFn := func(string, string, fabrics.Provider) (*sourceEntry, error) {
		openCalls.Add(1)
		// A real openInitiator would spawn the transfer goroutine and
		// hand it the entry as tracker. The test never wires that up,
		// so the entry stays inert and closeSourceHandles below is a
		// no-op.
		return &sourceEntry{infoStr: "info-1"}, nil
	}

	r := &SourceReconciler{
		Client:          c,
		Scheme:          scheme,
		NodeName:        "node-a",
		openInitiatorFn: openFn,
		FlushInterval:   time.Hour,
		sources:         map[types.NamespacedName]*sourceEntry{},
		attempts:        map[types.NamespacedName]uint32{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	req := reconcile.Request{NamespacedName: key}

	// First reconcile: opens the initiator, records the origin timestamp.
	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int32(1), openCalls.Load())
	t.Cleanup(func() { r.closeEntry(key) })

	// Same timestamp on the MxlFlow: no rotation, no reopen.
	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(1), openCalls.Load(),
		"a Reconcile with no origin rotation must hit the fast path; "+
			"opening the initiator twice would tear down the live transfer "+
			"goroutine every controller-runtime tick")

	// Bump the MxlFlow's Origin LastObserved into the future and ensure
	// the next reconcile reopens.
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: flowID}, &f))
	newer := metav1.NewTime(originalOriginAt.Time.Add(time.Minute))
	f.Status.Locations[0].LastObserved = &newer
	require.NoError(t, c.Status().Update(context.Background(), &f))

	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(2), openCalls.Load(),
		"a fresher Origin LastObserved must reopen the FlowReader so the "+
			"gateway tails the rebound writer instead of the stale handle")
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
