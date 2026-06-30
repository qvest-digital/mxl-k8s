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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
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
	entry := &targetEntry{infoStr: "info-1"}
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
	entry := &targetEntry{infoStr: "info-1"}
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

func TestTarget_FlusherPreservesTargetInfo(t *testing.T) {
	// The unified SSA writer routes every status mutation through one
	// FieldOwner. SSA releases ownership of any field a subsequent
	// patch from the same owner omits, so a flusher tick that did not
	// re-stamp status.targetInfo would let the apiserver strip the
	// field after the second tick. Consumers polling status.targetInfo
	// would then see it vanish from under a still-Ready mirror, which
	// the 30-mirror-ready kind-integration case caught in CI.
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
		DegradedAfter: time.Hour,
		targets:       map[types.NamespacedName]*targetEntry{},
	}
	entry := &targetEntry{infoStr: "info-1"}
	fresh := time.Now()
	entry.lastCommitAt.Store(&fresh)
	entry.commits.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry)

	// Let the flusher run several ticks so any field-ownership churn
	// would have surfaced; one tick alone would not.
	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		if err := c.Get(context.Background(), key, &got); err != nil {
			return false
		}
		return got.Status.Phase == mxlv1alpha1.MxlFlowMirrorReady &&
			len(got.Status.Conditions) > 0
	}, time.Second, 5*time.Millisecond)

	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.Equal(t, "info-1", got.Status.TargetInfo,
		"flusher must re-stamp status.targetInfo on every SSA payload; "+
			"omitting it releases FieldOwner ownership and the apiserver "+
			"strips the field, leaving a Ready mirror that consumers "+
			"cannot dial")
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

func TestTarget_StatusStampsObservedGeneration(t *testing.T) {
	// Every target-side status write must carry the mirror's current
	// Generation. Operators gate user-visible "the controller has seen
	// this spec" feedback on observedGeneration; a stale value freezes
	// the UI on the previous spec's state.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{})
	mirror.Generation = 7
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		DegradedAfter: 50 * time.Millisecond,
		targets:       map[types.NamespacedName]*targetEntry{},
	}
	// The MxlFlow is absent so the reconcile lands on the flow-not-
	// found branch, which exercises the applyTargetStatus path with
	// the minimal payload (phase + observedGeneration only).
	req := reconcile.Request{NamespacedName: key}
	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.Equal(t, int64(7), got.Status.ObservedGeneration,
		"every applyTargetStatus call must stamp the mirror's current "+
			"Generation; a missing or stale observedGeneration would let "+
			"observers think a fresh spec is still being processed")
}

func TestTarget_FlowNotFoundOmitsTargetInfoAndLastGrain(t *testing.T) {
	// The flow-not-found pre-open branch publishes Phase=Materializing
	// only. Stamping a TargetInfo or LastGrainAt at this point would
	// fabricate fields the gateway has not yet earned: there is no
	// fabrics.Target, and no grain has ever landed.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{})
	mirror.Generation = 3
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		DegradedAfter: 50 * time.Millisecond,
		targets:       map[types.NamespacedName]*targetEntry{},
	}
	req := reconcile.Request{NamespacedName: key}
	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorMaterializing, got.Status.Phase)
	assert.Equal(t, int64(3), got.Status.ObservedGeneration)
	assert.Empty(t, got.Status.TargetInfo,
		"pre-open Materializing must not claim a TargetInfo; the SSA "+
			"payload omits the key so the field stays unset until the "+
			"handshake produces one")
	assert.Nil(t, got.Status.LastGrainAt,
		"pre-open Materializing must not claim a LastGrainAt; the SSA "+
			"payload omits the key so the field stays unset until the "+
			"flusher records a real commit")
}

func TestTarget_EmptyFlowDefinitionSurfacesConditionAndRequeues(t *testing.T) {
	// An MxlFlow that exists but whose producer has not published a
	// spec.definition yet must not wedge the mirror at an empty phase:
	// the target side surfaces Phase=Materializing + a TargetProgress=
	// False/FlowDefinitionEmpty condition and requeues, mirroring the
	// flow-not-found branch. Before this, an empty definition returned a
	// bare reconciler error and left the mirror at an empty phase, so the
	// consumer and the cluster diagnostics (which only see the CR, not
	// the gateway log) had no signal for why it never reached Ready.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{})
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-1"},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: "flow-1"},
		// Definition intentionally omitted -> Raw is empty.
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, flow).
		Build()

	r := &TargetReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{},
	}
	req := reconcile.Request{NamespacedName: key}

	res, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err,
		"an unpublished flow definition is transient, not a reconciler "+
			"error; it must come back as a benign requeue")
	assert.Equal(t, 2*time.Second, res.RequeueAfter)

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorMaterializing, got.Status.Phase,
		"an existing flow with an empty definition must surface as "+
			"Materializing, not the bare-error empty phase that left the "+
			"mirror invisible to consumers and diagnostics")
	require.Len(t, got.Status.Conditions, 1)
	cond := got.Status.Conditions[0]
	assert.Equal(t, mxlv1alpha1.ConditionTypeTargetProgress, cond.Type)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonFlowDefinitionEmpty, cond.Reason)
}

func TestTarget_RecoverFromFatalErrorClearsRecoveringOnRebuildFailure(t *testing.T) {
	// Codifies the comment above target.go's defer entry.recovering.Store(false):
	// when openFabricSide returns an error during recovery the flusher
	// must not stay parked. A stuck recovering=true silences the
	// per-mirror status flusher indefinitely.
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

	entry := &targetEntry{}
	// done is non-nil and already closed so recoverFromFatalError's
	// wait-for-progress-loop does not block.
	closedDone := make(chan struct{})
	close(closedDone)
	entry.done = closedDone

	rebuildErr := errors.New("fabrics rebuild refused")
	r := &TargetReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{key: entry},
		openFabricSideFn: func(*mxl.Writer, fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
			return nil, nil, "", rebuildErr
		},
	}

	r.recoverFromFatalError(key)

	assert.False(t, entry.recovering.Load(),
		"recovering must clear after a failed rebuild; a stuck true value "+
			"would silence the flusher forever and the gateway would never "+
			"publish another phase transition")

	r.mu.Lock()
	_, present := r.targets[key]
	r.mu.Unlock()
	assert.False(t, present,
		"a failed rebuild must drop the entry so the next Reconcile rebuilds "+
			"from scratch instead of operating on torn-down fabric handles")
}

func TestTarget_RecoverFromFatalErrorCancelsRunningProgressLoop(t *testing.T) {
	// Codifies the watchdog-spawn contract: recoverFromFatalError must
	// cancel the previous progress loop before waiting on its done
	// channel. The onFatal caller has already exited the loop, so its
	// cancel is a no-op; the watchdog caller has not, because no fatal
	// ReadGrain ever fires on a silent libmxl-fabrics wedge. Without
	// the explicit cancel the <-entry.done wait blocks forever, pins
	// entry.recovering=true, and the flusher skips every subsequent
	// tick. The kind-integration case 60 reproduced exactly that:
	// after the first watchdog spawn the entry stayed wedged for the
	// full STUCK_SECS window because no further recovery could fire.
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

	// done stays open until entry.cancel runs: the test asserts that
	// recoverFromFatalError closes it via its own cancel rather than
	// blocking on a loop that no fatal signal will ever stop.
	openDone := make(chan struct{})
	entry := &targetEntry{}
	entry.done = openDone
	entry.cancel = func() { close(openDone) }

	rebuildErr := errors.New("fabrics rebuild refused")
	r := &TargetReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{key: entry},
		openFabricSideFn: func(*mxl.Writer, fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
			return nil, nil, "", rebuildErr
		},
	}

	finished := make(chan struct{})
	go func() {
		r.recoverFromFatalError(key)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("recoverFromFatalError did not return; the watchdog spawn path " +
			"must cancel the progress loop before waiting on its done channel, " +
			"otherwise a silent libmxl-fabrics wedge pins recovering=true forever")
	}

	assert.False(t, entry.recovering.Load(),
		"recovering must clear once recoverFromFatalError returns; otherwise "+
			"the flusher's next tick skips the watchdog block and no further "+
			"recovery can fire")
}

func TestRecordCommit_FirstCommitResetsRecoveryAttempts(t *testing.T) {
	// recordCommit must zero recoveryAttempts on the first commit
	// after each fabric open so a previously-rebuilt-successfully
	// entry that later wedges again gets a fresh budget. The cap
	// counts *consecutive* failed rebuilds, not lifetime ones.
	entry := &targetEntry{}
	entry.commits.Store(5)
	entry.commitsAtFabricOpen.Store(5)
	entry.recoveryAttempts.Store(2)

	entry.recordCommit(0, time.Now())

	assert.Equal(t, uint32(0), entry.recoveryAttempts.Load(),
		"the first commit after fabricOpenedAt was set must clear the "+
			"recoveryAttempts counter; without the reset, the cap would "+
			"fire across what are actually unrelated stuck windows")

	// A subsequent commit must not re-reset (the condition fires only
	// on the first-commit-after-open boundary).
	entry.recoveryAttempts.Store(1)
	entry.recordCommit(0, time.Now())
	assert.Equal(t, uint32(1), entry.recoveryAttempts.Load(),
		"only the very first commit after each fabric open resets the "+
			"counter; later commits must leave it alone so the next "+
			"wedge keeps any in-flight attempt count intact")
}

// targetWatchdogFixture sets up a TargetReconciler + entry pair
// configured to exercise the stuck-handshake watchdog in isolation.
// FlushInterval is small (1ms) so a test can drive several ticks
// inside an Eventually budget; StuckHandshakeAfter is shorter still
// (1ms) so the wedge condition is permanently met from the moment
// fabricOpenedAt is stamped. DegradedAfter is wide so the would-be
// Degraded write would still apply if the watchdog branch did not
// pre-empt it.
type targetWatchdogFixture struct {
	r       *TargetReconciler
	entry   *targetEntry
	key     types.NamespacedName
	cancel  context.CancelFunc
	done    chan struct{}
	mirror  *mxlv1alpha1.MxlFlowMirror
	c       client.WithWatch
	spawnCh chan struct{} // unbuffered; receiver blocks each spawn
}

func newTargetWatchdogFixture(t *testing.T, spawnBufLen int) *targetWatchdogFixture {
	t.Helper()
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

	entry := &targetEntry{infoStr: "info-1"}
	// fabricOpenedAt sits far in the past so time.Since() always
	// exceeds StuckHandshakeAfter; commits/commitsAtFabricOpen are
	// both zero so the no-progress branch matches.
	past := time.Now().Add(-time.Hour)
	entry.fabricOpenedAt.Store(&past)
	entry.commitsAtFabricOpen.Store(0)
	// cancel + done so the cap path's <-entry.done does not block:
	// a closed channel reads instantly. Tests that want to assert
	// ordering replace these with channels they control.
	closedDone := make(chan struct{})
	close(closedDone)
	entry.cancel = func() {}
	entry.done = closedDone

	spawnCh := make(chan struct{}, spawnBufLen)
	r := &TargetReconciler{
		Client:              c,
		Scheme:              scheme,
		NodeName:            "node-a",
		FlushInterval:       1 * time.Millisecond,
		DegradedAfter:       time.Hour, // wide so Degraded does not pre-empt the watchdog
		StuckHandshakeAfter: 1 * time.Millisecond,
		targets:             map[types.NamespacedName]*targetEntry{key: entry},
		recoverFn: func(types.NamespacedName) {
			spawnCh <- struct{}{}
			// Re-arm the watchdog for the next tick: clear recovering
			// so CompareAndSwap can succeed again, and stamp
			// fabricOpenedAt back into the past so the wedge condition
			// stays true. A real recoverFromFatalError calls
			// startProgressLoop, which would reset fabricOpenedAt to
			// time.Now() and bury the wedge window; the test's
			// re-wedge scenario instead simulates the case where each
			// rebuild succeeds but the new fabric side wedges again
			// before any commit lands.
			past := time.Now().Add(-time.Hour)
			entry.fabricOpenedAt.Store(&past)
			entry.recovering.Store(false)
		},
	}

	return &targetWatchdogFixture{
		r:       r,
		entry:   entry,
		key:     key,
		mirror:  mirror,
		c:       c,
		spawnCh: spawnCh,
	}
}

func (f *targetWatchdogFixture) startFlusher(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	f.cancel = cancel
	f.done = done
	go f.r.runFlusher(ctx, done, f.key, f.entry)
}

func (f *targetWatchdogFixture) stopFlusher(t *testing.T) {
	t.Helper()
	if f.cancel == nil {
		return
	}
	f.cancel()
	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Fatal("flusher did not exit after cancel")
	}
	f.cancel = nil
}

func TestRunFlusher_StuckHandshake_TriggersRecovery(t *testing.T) {
	// Fabric opened in the distant past, zero commits: the flusher
	// must escalate into the recovery seam exactly once and increment
	// recoveryAttempts. Without the escalation a silently-wedged
	// libmxl-fabrics target stays Ready forever (no fatal ReadGrain
	// to trigger onFatal) and consumer FlowReaders see no grains.
	f := newTargetWatchdogFixture(t, 4)
	f.startFlusher(t)
	defer f.stopFlusher(t)

	select {
	case <-f.spawnCh:
	case <-time.After(time.Second):
		t.Fatal("watchdog did not spawn recovery within 1s of a stuck handshake")
	}

	require.Eventually(t, func() bool {
		return f.entry.recoveryAttempts.Load() >= 1
	}, time.Second, 5*time.Millisecond,
		"the watchdog spawn must increment recoveryAttempts so the cap "+
			"branch can count consecutive failures")
}

func TestRunFlusher_CommitDisarmsWatchdog(t *testing.T) {
	// One commit recorded after the fabric opened: the flusher must
	// NOT spawn recovery. The discriminator is "no progress since
	// open", not "fabric opened long ago".
	f := newTargetWatchdogFixture(t, 4)
	// Simulate a commit landing: bump commits past commitsAtFabricOpen.
	f.entry.commits.Store(1)
	commitAt := time.Now()
	f.entry.lastCommitAt.Store(&commitAt)

	f.startFlusher(t)
	defer f.stopFlusher(t)

	select {
	case <-f.spawnCh:
		t.Fatal("watchdog spawned recovery despite a commit landing after fabric open; " +
			"the discriminator must be 'no progress since open', not 'time since open'")
	case <-time.After(50 * time.Millisecond):
	}

	assert.Equal(t, uint32(0), f.entry.recoveryAttempts.Load(),
		"a commit after fabric open must suppress the watchdog spawn; "+
			"incrementing recoveryAttempts here would let an otherwise "+
			"healthy mirror trip the cap on transient stalls")
}

func TestRunFlusher_CapsRecoveryAttempts(t *testing.T) {
	// After maxStuckRebuilds spawns the next tick must publish
	// Phase=Failed + Reason=StuckHandshakeCapReached, remove the
	// entry from r.targets, and exit the flusher. The plan asserts
	// exact spawn count + terminal status + entry removal + flusher
	// exit in one test.
	f := newTargetWatchdogFixture(t, int(maxStuckRebuilds)+2)
	f.startFlusher(t)

	for i := uint32(0); i < maxStuckRebuilds; i++ {
		select {
		case <-f.spawnCh:
		case <-time.After(time.Second):
			t.Fatalf("watchdog did not spawn recovery attempt %d within 1s", i+1)
		}
	}

	// No further spawn should land; the cap fires on the next tick
	// and tears the entry down. Drain the spawn channel briefly to
	// confirm no fourth spawn slipped through.
	select {
	case <-f.spawnCh:
		t.Fatalf("watchdog spawned more than maxStuckRebuilds=%d recoveries; "+
			"the cap branch must take over before another spawn", maxStuckRebuilds)
	case <-time.After(50 * time.Millisecond):
	}

	// Flusher must exit by itself once the cap branch returns.
	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Fatal("flusher did not exit after the cap branch fired; " +
			"a stuck flusher leaks a goroutine and keeps producing status writes")
	}
	f.cancel = nil // already exited

	f.r.mu.Lock()
	_, present := f.r.targets[f.key]
	f.r.mu.Unlock()
	assert.False(t, present,
		"the cap branch must delete the entry from r.targets so the next "+
			"Reconcile rebuilds from scratch through openTarget")

	assert.Equal(t, uint32(maxStuckRebuilds), f.entry.recoveryAttempts.Load(),
		"recoveryAttempts must equal maxStuckRebuilds at cap time; a higher "+
			"value means another spawn raced the cap branch")

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.c.Get(context.Background(), f.key, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorFailed, got.Status.Phase,
		"the cap branch must publish Phase=Failed so operators see an "+
			"explicit terminal state instead of a silently-dropped entry")
}

func TestRunFlusher_CapPath_TerminalStatus(t *testing.T) {
	// Codifies the operator-visible signal: cap must publish a
	// TargetProgress condition with reason StuckHandshakeCapReached.
	// A bare Phase=Failed without the reason would leave operators
	// unable to distinguish this failure mode from other terminal
	// states.
	f := newTargetWatchdogFixture(t, int(maxStuckRebuilds)+2)
	f.startFlusher(t)

	for i := uint32(0); i < maxStuckRebuilds; i++ {
		select {
		case <-f.spawnCh:
		case <-time.After(time.Second):
			t.Fatalf("recovery spawn %d did not fire", i+1)
		}
	}

	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		if err := f.c.Get(context.Background(), f.key, &got); err != nil {
			return false
		}
		if got.Status.Phase != mxlv1alpha1.MxlFlowMirrorFailed {
			return false
		}
		for _, cond := range got.Status.Conditions {
			if cond.Type == mxlv1alpha1.ConditionTypeTargetProgress &&
				cond.Reason == ReasonStuckHandshakeCapReached {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond,
		"the cap branch must publish TargetProgress with reason "+
			"StuckHandshakeCapReached; a missing reason hides the failure "+
			"mode from operators")

	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Fatal("flusher did not exit after cap branch")
	}
	f.cancel = nil
}

func TestRunFlusher_CapResetsAfterCommit(t *testing.T) {
	// A commit between cap-imminent spawns must reset the counter so
	// the next stuck window starts from zero. The cap counts
	// *consecutive* failed rebuilds; without the reset, a long-lived
	// mirror that survives a couple of transient wedges would
	// eventually trip the cap even though every wedge healed.
	f := newTargetWatchdogFixture(t, int(maxStuckRebuilds)*3)
	// Override the recoverFn to drive the test deterministically:
	// each spawn rearms the wedge without recording a commit. The
	// test injects the commit in between bursts.
	f.r.recoverFn = func(types.NamespacedName) {
		f.spawnCh <- struct{}{}
		past := time.Now().Add(-time.Hour)
		f.entry.fabricOpenedAt.Store(&past)
		f.entry.recovering.Store(false)
	}

	f.startFlusher(t)
	defer func() {
		if f.cancel != nil {
			f.cancel()
			<-f.done
		}
	}()

	// First burst: maxStuckRebuilds-1 spawns, then record a commit
	// before the cap can fire.
	for i := uint32(0); i < maxStuckRebuilds-1; i++ {
		select {
		case <-f.spawnCh:
		case <-time.After(time.Second):
			t.Fatalf("first burst: spawn %d did not fire", i+1)
		}
	}

	// Pause the flusher so the commit + counter reset happen as one
	// atomic transition: stop the flusher, mutate the entry, restart.
	// Without the pause an in-flight tick could spawn a fourth time
	// against the un-reset counter before the test gets to record
	// the commit, racing the cap branch.
	f.stopFlusher(t)

	// Record a commit: recoveryAttempts must clear.
	prev := f.entry.commits.Load()
	f.entry.commitsAtFabricOpen.Store(prev)
	f.entry.recordCommit(0, time.Now())
	require.Equal(t, uint32(0), f.entry.recoveryAttempts.Load(),
		"the post-burst recordCommit must clear recoveryAttempts; without "+
			"the reset, the cap would persist across what are actually "+
			"unrelated stuck windows")

	// Set up a fresh wedge: commitsAtFabricOpen tracks current commits.
	f.entry.commitsAtFabricOpen.Store(f.entry.commits.Load())
	past := time.Now().Add(-time.Hour)
	f.entry.fabricOpenedAt.Store(&past)
	f.startFlusher(t)

	// Second burst must again reach maxStuckRebuilds spawns before
	// the cap fires, proving the counter restarted at zero.
	for i := uint32(0); i < maxStuckRebuilds; i++ {
		select {
		case <-f.spawnCh:
		case <-time.After(time.Second):
			t.Fatalf("second burst: spawn %d did not fire; the post-commit "+
				"counter reset must give a full fresh budget", i+1)
		}
	}

	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Fatal("flusher did not exit after the second burst hit the cap")
	}
	f.cancel = nil
}

func TestRunFlusher_CapPath_NoDeadlock(t *testing.T) {
	// The cap branch waits on entry.done before deleting the entry
	// and closing the writer (mirroring recoverFromFatalError's
	// ordering). If a concurrent closeEntry+closeTargetHandles were
	// to also wait on flusherDone, the two would deadlock: the
	// flusher waits on entry.done while closeTargetHandles waits on
	// flusherDone, and the flusher cannot return until it has waited
	// on entry.done. The cap branch must complete within a bounded
	// timeout regardless. Test by holding entry.done open just long
	// enough for the cap branch to be parked on it, then releasing.
	f := newTargetWatchdogFixture(t, int(maxStuckRebuilds)+2)
	// Replace the closed entry.done with an open one so the cap
	// branch's <-entry.done parks instead of returning instantly.
	openDone := make(chan struct{})
	f.entry.done = openDone
	cancelCalled := make(chan struct{})
	f.entry.cancel = func() { close(cancelCalled) }

	f.startFlusher(t)

	for i := uint32(0); i < maxStuckRebuilds; i++ {
		select {
		case <-f.spawnCh:
		case <-time.After(time.Second):
			t.Fatalf("spawn %d did not fire", i+1)
		}
	}

	// Wait until the cap branch has cancelled the progress loop.
	select {
	case <-cancelCalled:
	case <-time.After(time.Second):
		t.Fatal("cap branch did not cancel the progress loop; the cap path " +
			"must signal the loop to exit before waiting on done")
	}

	// At this point the cap branch is blocked on <-entry.done. The
	// flusher is still running. Release the progress loop so the
	// cap branch can finish its teardown.
	close(openDone)

	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Fatal("cap branch deadlocked after progress loop released; the " +
			"teardown ordering must not wait on the flusher's own done")
	}
	f.cancel = nil
}

func TestRunFlusher_RecoveryGate_NoDoubleSpawnUnderRace(t *testing.T) {
	// CompareAndSwap on entry.recovering is the spawn gate: two
	// flusher ticks observing "not recovering" simultaneously would
	// otherwise both spawn recoverFromFatalError on the same entry.
	// Pin recovering = true after the first spawn and confirm a
	// second tick (which only proceeds because the first spawn does
	// not clear the flag) does not double-spawn.
	f := newTargetWatchdogFixture(t, 4)
	// Override recoverFn to NOT clear the recovering flag: this
	// pins entry.recovering=true after the first spawn, so the
	// flusher's recovering.Load() check at the top of the tick
	// short-circuits every subsequent tick. The watchdog block
	// never runs again, regardless of how many ticks elapse.
	f.r.recoverFn = func(types.NamespacedName) {
		f.spawnCh <- struct{}{}
		// Intentionally do NOT clear recovering: simulates a slow
		// recovery still in flight when the next flusher tick fires.
	}

	f.startFlusher(t)
	defer f.stopFlusher(t)

	// First spawn must land.
	select {
	case <-f.spawnCh:
	case <-time.After(time.Second):
		t.Fatal("first spawn did not fire")
	}

	// No second spawn within a generous window of further ticks.
	select {
	case <-f.spawnCh:
		t.Fatal("second spawn fired while a previous recovery was still " +
			"in flight; CompareAndSwap must serialize the spawn against " +
			"concurrent observers")
	case <-time.After(100 * time.Millisecond):
	}

	assert.Equal(t, uint32(1), f.entry.recoveryAttempts.Load(),
		"recoveryAttempts must be exactly 1 after a single spawn; a higher "+
			"value would mean two ticks raced past the CAS gate")
}

// armPostHandshakeWedge configures the fixture so the watchdog's
// post-handshake branch trips on the next tick: one commit landed
// after the fabric side opened, then commits stopped while
// mirror.Status.LastSentAt kept advancing past the stuck-handshake
// window. fabricOpenedAt sits inside the window so the
// neverHandshook predicate stays false; the discrimination has to
// come from the cross-side LastSentAt delta alone.
//
// The mirror status is re-fetched from the fake client before each
// update so concurrent flusher writes (and a re-arm inside the
// recoverFn callback) do not collide on resourceVersion.
func (f *targetWatchdogFixture) armPostHandshakeWedge(t *testing.T, sentAfterCommit time.Duration) {
	t.Helper()
	now := time.Now()
	openedAt := now.Add(-100 * time.Millisecond)
	f.entry.fabricOpenedAt.Store(&openedAt)
	f.entry.commitsAtFabricOpen.Store(0)
	f.entry.commits.Store(1)
	commitAt := now.Add(-sentAfterCommit - 50*time.Millisecond)
	f.entry.lastCommitAt.Store(&commitAt)

	sentAt := commitAt.Add(sentAfterCommit)
	for i := 0; i < 5; i++ {
		var current mxlv1alpha1.MxlFlowMirror
		require.NoError(t, f.c.Get(context.Background(), f.key, &current))
		current.Status.LastSentAt = &metav1.Time{Time: sentAt}
		if err := f.c.Status().Update(context.Background(), &current); err == nil {
			return
		}
	}
	t.Fatal("armPostHandshakeWedge: status update raced concurrent writes after 5 attempts")
}

func TestRunFlusher_PostHandshakeWedge_TriggersRecovery(t *testing.T) {
	// PR #85's neverHandshook discriminator (commits == commitsAtFabricOpen)
	// flips to false the moment the first grain commits, leaving the
	// watchdog blind to a wedge that develops *after* the handshake -
	// the exact pattern kind testcase 60 hits when a fresh target
	// receives one grain, the fabric stalls, and recovery never fires.
	// LastSentAt > lastCommitAt + window means the source is still
	// sending; the target must rebuild.
	f := newTargetWatchdogFixture(t, 4)
	f.r.StuckHandshakeAfter = 10 * time.Millisecond
	f.armPostHandshakeWedge(t, 5*time.Second)
	f.startFlusher(t)
	defer f.stopFlusher(t)

	select {
	case <-f.spawnCh:
	case <-time.After(time.Second):
		t.Fatal("watchdog did not spawn recovery on a post-handshake wedge; " +
			"the discriminator must also trigger when lastSentAt outpaces " +
			"lastCommitAt by more than the stuck-handshake window")
	}

	require.Eventually(t, func() bool {
		return f.entry.recoveryAttempts.Load() >= 1
	}, time.Second, 5*time.Millisecond,
		"a post-handshake wedge must increment recoveryAttempts so the cap "+
			"branch can still terminate a permanently wedged target")
}

func TestRunFlusher_PostHandshakeIdleFlow_DoesNotTrigger(t *testing.T) {
	// The discriminator must not fire when the source is genuinely
	// idle: LastSentAt absent (or trailing lastCommitAt) means no
	// grains are being sent, and rebuilding the fabric side in that
	// state would churn perfectly healthy mirrors on every quiet
	// window. Codifies the "idle is not a wedge" half of the design.
	f := newTargetWatchdogFixture(t, 4)
	f.r.StuckHandshakeAfter = 10 * time.Millisecond
	// One commit landed after open; lastSentAt is left unset on the
	// mirror status (the typical "source has stopped pushing" shape).
	now := time.Now()
	openedAt := now.Add(-100 * time.Millisecond)
	f.entry.fabricOpenedAt.Store(&openedAt)
	f.entry.commitsAtFabricOpen.Store(0)
	f.entry.commits.Store(1)
	commitAt := now.Add(-time.Second)
	f.entry.lastCommitAt.Store(&commitAt)

	f.startFlusher(t)
	defer f.stopFlusher(t)

	select {
	case <-f.spawnCh:
		t.Fatal("watchdog spawned recovery on an idle flow; the post-handshake " +
			"branch must require mirror.Status.LastSentAt to outpace " +
			"lastCommitAt before triggering a rebuild")
	case <-time.After(100 * time.Millisecond):
	}

	assert.Equal(t, uint32(0), f.entry.recoveryAttempts.Load(),
		"an idle flow must leave recoveryAttempts at zero; otherwise a "+
			"long quiet window followed by a real wedge would trip the cap "+
			"on the first true rebuild and surface a false terminal Failed")
}

func TestRunFlusher_PostHandshakeWedge_RespectsCapAndPublishesFailed(t *testing.T) {
	// The post-handshake branch must share the same cap budget as
	// neverHandshook: after maxStuckRebuilds spawns the flusher
	// publishes Phase=Failed with StuckHandshakeCapReached and exits.
	// Without sharing the cap a permanently wedged target would loop
	// rebuild forever and never surface a terminal state to operators.
	f := newTargetWatchdogFixture(t, int(maxStuckRebuilds)+2)
	f.r.StuckHandshakeAfter = 10 * time.Millisecond
	f.armPostHandshakeWedge(t, 5*time.Second)
	// Re-arm only the entry-side state on every spawn: the mirror
	// status already carries a LastSentAt far ahead of lastCommitAt
	// from the initial armPostHandshakeWedge, so the post-handshake
	// branch keeps tripping without further Status writes (which
	// would race the flusher's fetchMirror on the fake client).
	f.r.recoverFn = func(types.NamespacedName) {
		f.spawnCh <- struct{}{}
		now := time.Now()
		openedAt := now.Add(-100 * time.Millisecond)
		f.entry.fabricOpenedAt.Store(&openedAt)
		f.entry.recovering.Store(false)
	}
	f.startFlusher(t)

	for i := uint32(0); i < maxStuckRebuilds; i++ {
		select {
		case <-f.spawnCh:
		case <-time.After(time.Second):
			t.Fatalf("post-handshake watchdog did not spawn recovery attempt %d", i+1)
		}
	}

	select {
	case <-f.spawnCh:
		t.Fatal("post-handshake watchdog spawned more than maxStuckRebuilds " +
			"recoveries; the cap branch must take over before another spawn")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Fatal("flusher did not exit after the post-handshake cap branch fired")
	}
	f.cancel = nil

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.c.Get(context.Background(), f.key, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorFailed, got.Status.Phase,
		"the cap branch must publish Phase=Failed regardless of which "+
			"wedge shape (neverHandshook vs postHandshakeWedge) drove it")

	foundCond := false
	for _, cond := range got.Status.Conditions {
		if cond.Type == mxlv1alpha1.ConditionTypeTargetProgress &&
			cond.Reason == ReasonStuckHandshakeCapReached {
			foundCond = true
			break
		}
	}
	assert.True(t, foundCond,
		"post-handshake cap must carry reason StuckHandshakeCapReached so "+
			"operators see the same terminal signal whether the handshake "+
			"never landed or only landed once before wedging")
}
