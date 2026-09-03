package mirror

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A target whose fabric device has gone away leaves its progress loop
// parked inside libmxl-fabrics: the cancel lands, but the call the
// loop is blocked in never returns, so its done channel never closes.
// These tests pin the property that decides whether such a mirror can
// still be removed from the cluster.

// wedgedLoop returns the cancel/done pair of a progress loop that
// acknowledges a cancel and then never returns. cancelled is closed on
// the first cancel so a test can tell "never asked to stop" from
// "asked, did not stop".
func wedgedLoop() (cancel func(), done chan struct{}, cancelled chan struct{}) {
	cancelled = make(chan struct{})
	ctx, stop := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		close(cancelled)
	}()
	// done is never closed: this stands for the loop that cannot come
	// back, which is the whole point of the fixture.
	return stop, make(chan struct{}), cancelled
}

func TestTarget_DeletionCompletesWhenProgressLoopNeverExits(t *testing.T) {
	// Teardown used to receive on the progress loop's done channel
	// unbuffered before dropping the finalizer. A loop that never
	// returns therefore made the MxlFlowMirror permanently
	// undeletable, and no client-side escape exists: a forced delete
	// removes no finalizers, only the controller that owns one can.
	// Deletion has to be able to finish even when the handles cannot
	// be released.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := deletingMirror("node-x", "node-a")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	cancel, done, cancelled := wedgedLoop()
	entry := &targetEntry{writer: &mxl.Writer{}}
	entry.cancel = cancel
	entry.done = done

	r := &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		TeardownGrace: 50 * time.Millisecond,
		targets:       map[types.NamespacedName]*targetEntry{key: entry},
		attempts:      attemptTable[targetOpenInputs]{},
	}

	returned := make(chan error, 1)
	go func() {
		_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
		returned <- err
	}()

	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Reconcile never returned: the deletion path must bound its wait " +
			"on the progress loop, otherwise a mirror whose fabric device is " +
			"wedged can never be deleted")
	}

	select {
	case <-cancelled:
	default:
		t.Fatal("a bounded teardown must still ask the progress loop to stop; " +
			"giving up without cancelling leaks a loop that would have exited")
	}

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.False(t, controllerutil.ContainsFinalizer(&got, TargetFinalizerName),
		"the finalizer must come off even when the handles could not be "+
			"released; gating it on a teardown that cannot finish is what makes "+
			"the object undeletable")

	r.mu.Lock()
	_, live := r.targets[key]
	r.mu.Unlock()
	assert.False(t, live,
		"a mirror the gateway has given up on must leave the live set, or the "+
			"next Reconcile operates on handles nothing owns")
}

func TestTarget_DeletionDuringRecoveryDoesNotDeadlockOrLeaveALoop(t *testing.T) {
	// recoverFromFatalError cancels the progress loop, waits for it,
	// rebuilds the fabric side and then installs a fresh loop. A
	// deletion landing inside that window used to read the entry's
	// goroutine handles without any synchronisation against the swap,
	// so it could take the old generation's cancel and the new
	// generation's done channel and wait forever on a loop nobody had
	// asked to stop. Worse, the rebuild carried on afterwards and
	// started a progress loop against an entry already dropped from
	// r.targets, which nothing could ever cancel again.
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := deletingMirror("node-x", "node-a")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	// Generation one: a real goroutine that exits when cancelled, so
	// the recovery's wait-for-loop is satisfied and it proceeds to the
	// rebuild rather than parking.
	gen1Ctx, gen1Cancel := context.WithCancel(context.Background())
	gen1Done := make(chan struct{})
	go func() {
		<-gen1Ctx.Done()
		close(gen1Done)
	}()

	entry := &targetEntry{writer: &mxl.Writer{}}
	entry.cancel = gen1Cancel
	entry.done = gen1Done

	rebuilding := make(chan struct{})
	releaseRebuild := make(chan struct{})
	r := &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		TeardownGrace: 2 * time.Second,
		targets:       map[types.NamespacedName]*targetEntry{key: entry},
		attempts:      attemptTable[targetOpenInputs]{},
		openFabricSideFn: func(*mxl.Writer, fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
			close(rebuilding)
			<-releaseRebuild
			// A zero-value Target stands for the handles a rebuild
			// hands back; nothing here dials libmxl-fabrics.
			return &fabrics.Target{}, nil, "info-2", nil
		},
	}

	recovered := make(chan struct{})
	go func() {
		r.recoverFromFatalError(key)
		close(recovered)
	}()

	select {
	case <-rebuilding:
	case <-time.After(10 * time.Second):
		t.Fatal("recovery never reached the fabric-side rebuild")
	}

	// The deletion lands while the rebuild is in flight: the entry is
	// live, its old fabric side is already torn down, and its next
	// progress loop does not exist yet.
	deleted := make(chan error, 1)
	go func() {
		_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
		deleted <- err
	}()

	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Reconcile never returned: a deletion landing mid-recovery must " +
			"not block on the loop generation the rebuild is about to install")
	}

	close(releaseRebuild)

	select {
	case <-recovered:
	case <-time.After(10 * time.Second):
		t.Fatal("recoverFromFatalError never returned after the rebuild completed")
	}

	entry.lifecycle.Lock()
	installed := entry.done
	closed := entry.closed
	entry.lifecycle.Unlock()

	assert.True(t, closed, "the deletion must mark the entry torn down, which is "+
		"what tells an in-flight rebuild that its handles are no longer wanted")
	assert.Equal(t, (chan struct{})(gen1Done), installed,
		"the rebuild must not arm a progress loop on a mirror that has been "+
			"deleted: nothing is left to cancel it, so it runs for the life of "+
			"the gateway")

	r.mu.Lock()
	_, live := r.targets[key]
	r.mu.Unlock()
	assert.False(t, live, "a deleted mirror must not be resurrected in the live set")

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.False(t, controllerutil.ContainsFinalizer(&got, TargetFinalizerName),
		"the deletion must complete even though a rebuild was in flight")
}

func TestTarget_OpenFailureReachesTerminalStateInsteadOfSpinning(t *testing.T) {
	// maxTargetOpenAttempts moves the phase to Degraded and stops
	// there, while backoffFor caps at 30s: a mirror whose fabric
	// device has gone away therefore re-probes twice a minute for as
	// long as the CR exists, reporting the same reason as one that has
	// failed three times. Nothing in the status distinguishes a mirror
	// that is coming back from one that never will.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)
	ctx := context.Background()

	var res reconcile.Result
	for attempt := uint32(1); attempt <= terminalTargetOpenAttempts; attempt++ {
		var err error
		res, err = f.r.Reconcile(ctx, f.req)
		require.NoError(t, err)
		require.Equal(t, int(attempt), f.opens,
			"every ungated pass must attempt the open")

		got := f.mirror(t)
		require.Equal(t, int32(attempt), got.Status.TargetAttemptCount)
		if attempt < maxTargetOpenAttempts {
			require.Equal(t, mxlv1alpha1.MxlFlowMirrorMaterializing, got.Status.Phase)
		} else if attempt < terminalTargetOpenAttempts {
			require.Equal(t, mxlv1alpha1.MxlFlowMirrorDegraded, got.Status.Phase,
				"between the two thresholds the mirror is Degraded and still "+
					"being retried at full rate")
		}

		// Past the cap the wait is terminalRetryInterval, so releasing
		// the gate has to clear whichever of the two is longer.
		f.advance(terminalRetryInterval + time.Second)
	}

	got := f.mirror(t)
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorFailed, got.Status.Phase,
		"past terminalTargetOpenAttempts the mirror must leave Degraded: "+
			"Degraded reads as 'retrying', and the gateway has stopped "+
			"expecting this open to succeed")
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, ReasonOpenTargetUnrecoverable, got.Status.Conditions[0].Reason,
		"the terminal state needs its own reason; reusing OpenTargetFailed "+
			"leaves an operator unable to tell the two apart")
	assert.Equal(t, metav1.ConditionFalse, got.Status.Conditions[0].Status)

	assert.GreaterOrEqual(t, res.RequeueAfter, terminalRetryInterval,
		"a failure the gateway has given up on must back off hard rather than "+
			"keep the 30s ceiling backoffFor tops out at")
}

func TestTarget_TerminalOpenFailureStopsRetryingAtFullRate(t *testing.T) {
	// The status write a failure makes wakes this reconciler through
	// its own watch, so the retry period has to be enforced on the way
	// in. A terminal mirror that still opened once per watch event
	// would hammer libmxl at apiserver rate under exactly the
	// condition the terminal state was introduced to stop.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)
	ctx := context.Background()

	// The clock only moves between attempts, so the run ends with the
	// terminal failure's own wait still armed.
	for i := range terminalTargetOpenAttempts {
		if i > 0 {
			f.advance(terminalRetryInterval + time.Second)
		}
		_, err := f.r.Reconcile(ctx, f.req)
		require.NoError(t, err)
	}
	require.Equal(t, mxlv1alpha1.MxlFlowMirrorFailed, f.mirror(t).Status.Phase)
	opensAtTerminal := f.opens

	// Well past backoffFor's 30s ceiling, and every pass stands for a
	// watch event the failure's own status write produced.
	f.advance(2 * time.Minute)
	for range 20 {
		res, err := f.r.Reconcile(ctx, f.req)
		require.NoError(t, err)
		assert.Positive(t, res.RequeueAfter,
			"a gated pass still has to come back when the interval expires")
	}
	assert.Equal(t, opensAtTerminal, f.opens,
		"a terminal mirror must not reopen two minutes after giving up; the "+
			"whole point of the state is that the retry rate drops")

	f.advance(terminalRetryInterval)
	_, err := f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	assert.Equal(t, opensAtTerminal+1, f.opens,
		"terminal is a hard backoff, not a stop: a device that comes back has "+
			"to be picked up without operator action")
}
