package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// notLandingEntry builds the entry state a source reaches when its
// head moves and nothing arrives at the target: the one failure that
// asks for no reopen, and the one a stalled continuous flow reaches.
func notLandingEntry() *sourceEntry {
	e := stalledEntry(42, ptrTime(time.Now()), 0, time.Time{})
	e.openedAt = time.Now().Add(-defaultReaderStallAfter - time.Second)
	e.flowID = "flow-1"
	return e
}

func TestObservedState_TransfersNotLandingAsksForNoReopen(t *testing.T) {
	// Pins the premise the release below rests on. If this state ever
	// gains rebuildReader, the flusher test stops covering the gap it
	// was written for.
	state := observedState(notLandingEntry(), defaultReaderStallAfter)
	require.Equal(t, mxlv1alpha1.ReasonTransfersNotLanding, state.reason)
	assert.False(t, state.rebuildReader)
}

func TestRunFlusher_ReleasesReaderWhenWriterGoneWithoutARebuildState(t *testing.T) {
	// A reader on a flow nothing writes is what keeps that flow from
	// being reclaimed, and TransfersNotLanding is where a stalled
	// source sits without asking for a reopen. Observed on a cluster:
	// an audio mirror held a flow whose producer had been gone for ten
	// hours, and the release never ran because it was gated on the
	// reopen states.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	entry := notLandingEntry()
	r := &SourceReconciler{
		Client:       c,
		Scheme:       scheme,
		NodeName:     "node-a",
		sources:      map[types.NamespacedName]*sourceEntry{key: entry},
		attempts:     attemptTable[sourceAddInputs]{},
		rebuilds:     map[types.NamespacedName]uint32{},
		writerLiveFn: func(string) (bool, error) { return false, nil },
		rebuildFn: func(types.NamespacedName) {
			t.Error("a flow with no writer must be released, not reopened")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry, time.Millisecond)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the flusher must return so the entry can be closed")
	}

	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.sources[key] == nil
	}, 5*time.Second, time.Millisecond,
		"the source reader must be closed so libmxl can reclaim the flow")

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, mxlv1alpha1.ReasonSourceWriterGone, got.Status.Conditions[0].Reason)
}

func TestRunFlusher_KeepsReaderWhileTheWriterIsLive(t *testing.T) {
	// The same state with a writer attached is the target's problem to
	// resolve, so the source stays open and only reports.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	entry := notLandingEntry()
	r := &SourceReconciler{
		Client:       c,
		Scheme:       scheme,
		NodeName:     "node-a",
		sources:      map[types.NamespacedName]*sourceEntry{key: entry},
		attempts:     attemptTable[sourceAddInputs]{},
		rebuilds:     map[types.NamespacedName]uint32{},
		writerLiveFn: func(string) (bool, error) { return true, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry, time.Millisecond)

	require.Eventually(t, func() bool {
		var m mxlv1alpha1.MxlFlowMirror
		if err := c.Get(context.Background(), key, &m); err != nil {
			return false
		}
		return len(m.Status.Conditions) == 1
	}, 5*time.Second, time.Millisecond)

	cancel()
	<-done

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.Equal(t, mxlv1alpha1.ReasonTransfersNotLanding, got.Status.Conditions[0].Reason)
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.NotNil(t, r.sources[key], "a live writer's reader must stay open")
}

func TestReconcile_DoesNotOpenAReaderWhileTheWriterIsGone(t *testing.T) {
	// Without this the release is undone within the second: the
	// condition the flusher publishes wakes this reconciler, which
	// opens a fresh reader and pins the flow directory again.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	opener := &fakeOpener{
		openFn: func(string, string, fabrics.Provider) (*sourceEntry, error) {
			t.Error("no reader may be opened on a flow with no writer")
			return &sourceEntry{infoStr: "info-1"}, nil
		},
	}
	events := record.NewFakeRecorder(10)
	r := &SourceReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		opener:        opener,
		Recorder:      events,
		FlushInterval: time.Hour,
		sources:       map[types.NamespacedName]*sourceEntry{},
		attempts:      attemptTable[sourceAddInputs]{},
		writerLiveFn:  func(string) (bool, error) { return false, nil },
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, writerAbsentRetry, res.RequeueAfter,
		"the mirror has to ask again, because a writer that re-attaches to a surviving flow directory rotates nothing")
	assert.Zero(t, opener.calls.Load())

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, mxlv1alpha1.ReasonSourceWriterGone, got.Status.Conditions[0].Reason)

	// Parked, not rewriting: the retry re-reads its own condition
	// rather than re-publishing it and emitting another event.
	_, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Len(t, events.Events, 1, "a parked mirror must emit one event, not one per retry")
}

func TestReconcile_OpensAReaderOnceTheWriterIsBack(t *testing.T) {
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	opener := &fakeOpener{
		openFn: func(string, string, fabrics.Provider) (*sourceEntry, error) {
			return &sourceEntry{infoStr: "info-1"}, nil
		},
	}
	live := false
	r := &SourceReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		opener:        opener,
		FlushInterval: time.Hour,
		sources:       map[types.NamespacedName]*sourceEntry{},
		attempts:      attemptTable[sourceAddInputs]{},
		writerLiveFn:  func(string) (bool, error) { return live, nil },
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Zero(t, opener.calls.Load())

	live = true
	_, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	t.Cleanup(func() { r.closeEntry(key) })
	assert.Equal(t, int32(1), opener.calls.Load(),
		"a returning producer must get its mirror back without an operator")
}
