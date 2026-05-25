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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// TestMain in source_test.go runs goleak.VerifyTestMain for the
// whole package; the recovery paths exercised below all rely on
// progress goroutines exiting cleanly when ctx is cancelled or when
// the loop hits a fatal error.

func TestRunTargetProgressLoop_CommitsArrivedGrains(t *testing.T) {
	// Sequence: idx=10 arrives, idx=11 arrives, then ErrNotReady,
	// then cancel. The loop must commit 10 and 11 in order; the
	// idle sleep must not eat the cancel signal.
	var seq atomic.Int32
	read := func() (uint64, error) {
		switch seq.Add(1) {
		case 1:
			return 10, nil
		case 2:
			return 11, nil
		default:
			return 0, fabrics.ErrNotReady
		}
	}

	var (
		mu        sync.Mutex
		committed []uint64
	)
	commit := func(idx uint64) error {
		mu.Lock()
		committed = append(committed, idx)
		mu.Unlock()
		return nil
	}
	committedSnap := func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		out := make([]uint64, len(committed))
		copy(out, committed)
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	fatal := atomic.Int32{}
	onFatal := func() { fatal.Add(1) }

	go runTargetProgressLoop(ctx, done, read, commit, onFatal, nil)

	require.Eventually(t, func() bool { return len(committedSnap()) == 2 },
		time.Second, time.Millisecond, "expected 2 commits")

	cancel()
	<-done

	assert.Equal(t, []uint64{10, 11}, committedSnap())
	assert.Zero(t, fatal.Load(),
		"a clean ctx cancel must not look like a fatal target error; "+
			"that would trigger spurious fabric rebuilds on every restart")
}

func TestRunTargetProgressLoop_FatalErrorExitsAndCallsOnFatalOnce(t *testing.T) {
	// ReadGrain returns a non-ErrNotReady error once. The loop must
	// exit, close done, and invoke onFatal exactly once. A second
	// call would double-rebuild the fabric side.
	read := func() (uint64, error) {
		return 0, errors.New("EFI_RXMSG dropped, target dead")
	}
	commit := func(uint64) error {
		t.Fatal("commit must not be called on a fatal read error")
		return nil
	}

	fatalCalls := atomic.Int32{}
	onFatal := func() { fatalCalls.Add(1) }

	done := make(chan struct{})
	go runTargetProgressLoop(context.Background(), done, read, commit, onFatal, nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on fatal error")
	}
	assert.Equal(t, int32(1), fatalCalls.Load(),
		"onFatal must fire exactly once; a duplicate call would race two "+
			"concurrent recoverFromFatalError invocations against the same writer")
}

func TestRunTargetProgressLoop_CommitErrorIsLoggedButLoopContinues(t *testing.T) {
	// A commit-side error (e.g. OpenGrain returning busy under load)
	// must not exit the loop. The next ReadGrain is allowed to
	// surface the next index.
	var seq atomic.Int32
	read := func() (uint64, error) {
		switch seq.Add(1) {
		case 1:
			return 1, nil
		case 2:
			return 2, nil
		default:
			return 0, fabrics.ErrNotReady
		}
	}
	commitCalls := atomic.Int32{}
	commit := func(uint64) error {
		commitCalls.Add(1)
		return errors.New("transient")
	}
	onFatal := func() { t.Fatal("commit failure must not be reported as fatal") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, commit, onFatal, nil)

	require.Eventually(t, func() bool { return commitCalls.Load() >= 2 },
		time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestRunTargetProgressLoop_CtxCancelExitsImmediately(t *testing.T) {
	read := func() (uint64, error) { return 0, fabrics.ErrNotReady }
	commit := func(uint64) error { return nil }
	onFatal := func() { t.Fatal("ctx cancel must not look fatal") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, commit, onFatal, nil)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not honour ctx cancel during idle sleep")
	}
}

func TestRunTargetProgressLoop_NilOnFatalDoesNotPanic(t *testing.T) {
	// A nil onFatal would be a programming mistake from the caller,
	// but the loop must defend against it (a panic in the goroutine
	// would crash the gateway). Catches a regression where someone
	// forgot the if onFatal != nil guard.
	read := func() (uint64, error) { return 0, errors.New("fatal") }
	commit := func(uint64) error { return nil }

	done := make(chan struct{})
	go runTargetProgressLoop(context.Background(), done, read, commit, nil, nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on fatal read with nil onFatal")
	}
}

// recordingCommitTracker captures every recordCommit invocation the
// loop makes so per-grain assertions can inspect the sequence after
// cancel.
type recordingCommitTracker struct {
	mu      sync.Mutex
	indices []uint64
}

func (rt *recordingCommitTracker) recordCommit(idx uint64, _ time.Time) {
	rt.mu.Lock()
	rt.indices = append(rt.indices, idx)
	rt.mu.Unlock()
}

func (rt *recordingCommitTracker) snapshot() []uint64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]uint64, len(rt.indices))
	copy(out, rt.indices)
	return out
}

func TestRunTargetProgressLoop_TracksCommits(t *testing.T) {
	// The flusher reads commits + lastCommitAt to decide whether to
	// publish TargetProgress=Ready or =Degraded; the loop must hand
	// each successful commit to the tracker. A commit that returned
	// an error does NOT count: recording it would mask a partial
	// failure as healthy progress.
	var seq atomic.Int32
	read := func() (uint64, error) {
		switch seq.Add(1) {
		case 1:
			return 100, nil
		case 2:
			return 101, nil
		case 3:
			return 102, nil
		default:
			return 0, fabrics.ErrNotReady
		}
	}
	commit := func(idx uint64) error {
		if idx == 101 {
			return errors.New("transient OpenGrain")
		}
		return nil
	}

	tracker := &recordingCommitTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, commit, func() {}, tracker)

	require.Eventually(t, func() bool { return len(tracker.snapshot()) >= 2 },
		time.Second, time.Millisecond)

	cancel()
	<-done

	assert.Equal(t, []uint64{100, 102}, tracker.snapshot(),
		"tracker must observe only commits that succeeded; recording the "+
			"failed 101 would let the flusher report fresh progress while "+
			"the consumer's flow file is missing a grain")
}

func mirrorWithTargetFinalizer(name, ns, node, flowID string, status mxlv1alpha1.MxlFlowMirrorStatus) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  ns,
			Finalizers: []string{TargetFinalizerName},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: "node-x",
			TargetNode: node,
		},
		Status: status,
	}
}

func TestTarget_FlusherPublishesDegradedAfterIdle(t *testing.T) {
	// An entry whose lastCommitAt sits outside the degraded window
	// must be published as Phase=Degraded with TargetProgress=False/
	// NoGrains. The flusher owns the demotion: there is no fatal
	// error from libmxl-fabrics to trigger the recovery path in this
	// case, the writer's just sitting idle while the source is gone.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{
		Phase:      mxlv1alpha1.MxlFlowMirrorReady,
		TargetInfo: "info-1",
	})
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		FlushInterval: 5 * time.Millisecond,
		DegradedAfter: 10 * time.Millisecond,
		targets:       map[types.NamespacedName]*targetEntry{},
	}
	entry := &targetEntry{}
	stale := time.Now().Add(-time.Minute)
	entry.lastCommitAt.Store(&stale)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry)

	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		if err := c.Get(context.Background(), key, &got); err != nil {
			return false
		}
		return got.Status.Phase == mxlv1alpha1.MxlFlowMirrorDegraded
	}, time.Second, 5*time.Millisecond,
		"flusher must demote the mirror to Degraded once lastCommitAt "+
			"falls outside the freshness window; staying Ready would let "+
			"operators believe a silently-stalled mirror is still flowing")

	cancel()
	<-done

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.Len(t, got.Status.Conditions, 1)
	cond := got.Status.Conditions[0]
	assert.Equal(t, mxlv1alpha1.ConditionTypeTargetProgress, cond.Type)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonNoGrains, cond.Reason,
		"the Degraded transition must carry reason=NoGrains so the "+
			"operator can distinguish a stall from an AddTarget failure")
	require.NotNil(t, got.Status.LastGrainAt,
		"the flusher must publish LastGrainAt so a consumer can decide "+
			"how stale the last commit was")
}

func TestTarget_FlusherPublishesReadyOnGrainResume(t *testing.T) {
	// A Degraded mirror that starts receiving grains again must flip
	// to Ready with reason=Recovered, not =HandshakeComplete. The
	// distinction lets an operator see "this previously stalled and
	// came back" versus "this just came up for the first time".
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{
		Phase:      mxlv1alpha1.MxlFlowMirrorReady,
		TargetInfo: "info-1",
	})
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		FlushInterval: 5 * time.Millisecond,
		DegradedAfter: 50 * time.Millisecond,
		targets:       map[types.NamespacedName]*targetEntry{},
	}
	entry := &targetEntry{}
	// Park lastCommitAt outside the freshness window so the first
	// flusher tick publishes Degraded.
	stale := time.Now().Add(-time.Minute)
	entry.lastCommitAt.Store(&stale)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry)

	// Wait for Degraded to land.
	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		if err := c.Get(context.Background(), key, &got); err != nil {
			return false
		}
		return got.Status.Phase == mxlv1alpha1.MxlFlowMirrorDegraded
	}, time.Second, 5*time.Millisecond)

	// Simulate a grain commit landing in the future so the freshness
	// check stays true through the rest of the test even under load.
	fresh := time.Now().Add(time.Hour)
	entry.lastCommitAt.Store(&fresh)
	entry.commits.Add(1)

	// Wait for Ready/Recovered to land.
	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		if err := c.Get(context.Background(), key, &got); err != nil {
			return false
		}
		if got.Status.Phase != mxlv1alpha1.MxlFlowMirrorReady {
			return false
		}
		if len(got.Status.Conditions) == 0 {
			return false
		}
		return got.Status.Conditions[0].Reason == mxlv1alpha1.ReasonRecovered
	}, time.Second, 5*time.Millisecond,
		"a previously-Degraded mirror that starts seeing commits again "+
			"must publish TargetProgress=True with reason=Recovered; "+
			"a HandshakeComplete reason would hide the recovery history")

	cancel()
	<-done
}

func TestReconcile_FastPathBypassedWhenLastGrainStale(t *testing.T) {
	// The Reconcile fast-path may only short-circuit when the cached
	// status is both Ready AND fresh: a Ready status whose
	// LastGrainAt is older than DegradedAfter means the libmxl-fabrics
	// side has died silently (no fatal ReadGrain to trigger
	// recoverFromFatalError) and the writer is no longer being filled.
	// Falling through forces re-establish through openTarget.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	flowID := "flow-1"

	// MxlFlow is intentionally absent. When the fast-path falls
	// through, the Reconcile loop hits the IsNotFound branch and
	// flips Phase to Materializing - that is the observable proof
	// the short-circuit did not engage.
	build := func(lastGrainAt *metav1.Time, window time.Duration) (*TargetReconciler, types.NamespacedName) {
		mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", flowID, mxlv1alpha1.MxlFlowMirrorStatus{
			Phase:       mxlv1alpha1.MxlFlowMirrorReady,
			TargetInfo:  "info-1",
			LastGrainAt: lastGrainAt,
		})
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
			WithObjects(mirror).
			Build()
		r := &TargetReconciler{
			Client:        c,
			Scheme:        scheme,
			NodeName:      "node-a",
			DegradedAfter: window,
			targets: map[types.NamespacedName]*targetEntry{
				key: {},
			},
		}
		return r, key
	}

	t.Run("fresh LastGrainAt short-circuits", func(t *testing.T) {
		fresh := metav1.NewTime(time.Now())
		r, k := build(&fresh, time.Hour)
		req := reconcile.Request{NamespacedName: k}
		_, err := r.Reconcile(context.Background(), req)
		require.NoError(t, err)

		var got mxlv1alpha1.MxlFlowMirror
		require.NoError(t, r.Get(context.Background(), k, &got))
		assert.Equal(t, mxlv1alpha1.MxlFlowMirrorReady, got.Status.Phase,
			"fresh grain activity must keep the fast-path engaged; "+
				"falling through here would tear down the live writer on every tick")
	})

	t.Run("stale LastGrainAt falls through", func(t *testing.T) {
		stale := metav1.NewTime(time.Now().Add(-time.Hour))
		r, k := build(&stale, 50*time.Millisecond)
		req := reconcile.Request{NamespacedName: k}
		_, err := r.Reconcile(context.Background(), req)
		require.NoError(t, err)

		var got mxlv1alpha1.MxlFlowMirror
		require.NoError(t, r.Get(context.Background(), k, &got))
		assert.Equal(t, mxlv1alpha1.MxlFlowMirrorMaterializing, got.Status.Phase,
			"a stale LastGrainAt must defeat the fast-path; otherwise a "+
				"silently-dead fabric side never gets re-established")
	})

	t.Run("nil LastGrainAt falls through", func(t *testing.T) {
		r, k := build(nil, 50*time.Millisecond)
		req := reconcile.Request{NamespacedName: k}
		_, err := r.Reconcile(context.Background(), req)
		require.NoError(t, err)

		var got mxlv1alpha1.MxlFlowMirror
		require.NoError(t, r.Get(context.Background(), k, &got))
		assert.Equal(t, mxlv1alpha1.MxlFlowMirrorMaterializing, got.Status.Phase,
			"a Ready status with no LastGrainAt yet must also fall through; "+
				"otherwise a crashed gateway whose status survived but whose "+
				"writer is gone never re-establishes")
	})
}
