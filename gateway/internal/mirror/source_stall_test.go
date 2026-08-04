package mirror

import (
	"context"
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

func stalledEntry(head uint64, headAt *time.Time, transfers uint64) *sourceEntry {
	e := &sourceEntry{}
	e.lastHead.Store(head)
	if headAt != nil {
		t := *headAt
		e.headAdvancedAt.Store(&t)
	}
	for i := uint64(0); i < transfers; i++ {
		e.recordTransfer(i, time.Now())
	}
	return e
}

func TestObservedState_ReaderNotAdvancing(t *testing.T) {
	// A source whose reader head never moves transfers nothing, and
	// the transfer loop's inner range never runs, so it emits neither
	// an error nor an aged-out skip. Reporting SourceProgress=True
	// there describes a wedged mirror as healthy.
	const window = 20 * time.Second
	stale := time.Now().Add(-window - time.Second)
	fresh := time.Now()

	tests := []struct {
		name       string
		entry      *sourceEntry
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "never probed",
			entry:      stalledEntry(0, nil, 0),
			wantStatus: metav1.ConditionTrue,
			wantReason: mxlv1alpha1.ReasonHandshakeComplete,
		},
		{
			name:       "head advanced within window",
			entry:      stalledEntry(42, &fresh, 0),
			wantStatus: metav1.ConditionTrue,
			wantReason: mxlv1alpha1.ReasonHandshakeComplete,
		},
		{
			name:       "head frozen past window without transfers",
			entry:      stalledEntry(42, &stale, 0),
			wantStatus: metav1.ConditionFalse,
			wantReason: mxlv1alpha1.ReasonReaderNotAdvancing,
		},
		{
			name:       "head frozen past window after transfers",
			entry:      stalledEntry(42, &stale, 3),
			wantStatus: metav1.ConditionTrue,
			wantReason: mxlv1alpha1.ReasonRecovered,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := observedState(tc.entry, window)
			assert.Equal(t, tc.wantStatus, got.status)
			assert.Equal(t, tc.wantReason, got.reason)
		})
	}
}

func TestRecordHead_StampsOnlyOnChange(t *testing.T) {
	// headAdvancedAt has to measure how long the reader has stood
	// still, not how long ago it was last polled: the loop probes
	// every ProgressInterval regardless of whether the head moved.
	e := &sourceEntry{}
	t0 := time.Now()

	e.recordHead(100, t0)
	first := e.headAdvancedAt.Load()
	require.NotNil(t, first)
	require.Equal(t, uint64(100), e.lastHead.Load())

	e.recordHead(100, t0.Add(time.Minute))
	assert.True(t, e.headAdvancedAt.Load().Equal(*first),
		"an unchanged head must not refresh the stall clock")

	e.recordHead(101, t0.Add(2*time.Minute))
	assert.True(t, e.headAdvancedAt.Load().After(*first),
		"a changed head must refresh the stall clock")
	assert.Equal(t, uint64(101), e.lastHead.Load())
}

func TestRunTransferLoop_RecordsProbedHead(t *testing.T) {
	// The flusher can only see a frozen reader if the loop reports
	// every head it probes, including the initial attach probe.
	tracker := &recordingTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	probes := make(chan uint64, 4)
	probes <- 7
	probes <- 7
	probes <- 7
	close(probes)

	go runTransferLoop(ctx, done, "flow-1",
		func() (uint64, error) {
			if h, ok := <-probes; ok {
				return h, nil
			}
			cancel()
			return 7, nil
		},
		func(uint64) (bool, error) { return true, nil },
		func() error { return nil },
		time.Millisecond, tracker)

	<-done
	heads := tracker.headSnapshot()
	require.NotEmpty(t, heads)
	for _, h := range heads {
		assert.Equal(t, uint64(7), h)
	}
	transfers, _ := tracker.snapshot()
	assert.Empty(t, transfers,
		"a head that never advances leaves nothing to transfer")
}

func TestReconcile_PreservesLastSentAtAcrossReopen(t *testing.T) {
	// publishSourceProgress omits lastSentAt when the entry carries
	// none, and SSA releases an omitted field, so the apiserver
	// strips the published value. A reopened entry starts with zero
	// transfers, so without seeding, a reopen erases the timestamp
	// the target-side stuck-handshake watchdog discriminates on.
	scheme := newSourceTestScheme(t)
	flowID := "flow-1"
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", flowID, "info-1")
	sentAt := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	mirror.Status.LastSentAt = &sentAt

	opened := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{
				NodeName:     "node-a",
				Phase:        mxlv1alpha1.MxlFlowLocationOrigin,
				LastObserved: &opened,
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, flow).
		Build()

	opener := &fakeOpener{
		openFn: func(string, string, fabrics.Provider) (*sourceEntry, error) {
			return &sourceEntry{infoStr: "info-1"}, nil
		},
	}
	r := &SourceReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		opener:        opener,
		FlushInterval: time.Hour,
		sources:       map[types.NamespacedName]*sourceEntry{},
		attempts:      map[types.NamespacedName]uint32{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	t.Cleanup(func() { r.closeEntry(key) })

	r.mu.Lock()
	entry := r.sources[key]
	r.mu.Unlock()
	require.NotNil(t, entry)

	got := entry.lastSentAt.Load()
	require.NotNil(t, got, "a rebuilt entry must inherit the published lastSentAt")
	assert.True(t, got.Equal(sentAt.Time))

	state := observedState(entry, defaultReaderStallAfter)
	require.NotNil(t, state.lastSentAt,
		"the next publish must still carry lastSentAt so SSA does not strip it")
	assert.True(t, state.lastSentAt.Equal(sentAt.Time))
}

func TestRunFlusher_ReopensWedgedReader(t *testing.T) {
	// The wedge neither side resolves alone: the target's watchdog is
	// gated on status.lastSentAt postdating its own fabric open, and a
	// reader that has transferred nothing never advances lastSentAt,
	// so the target treats the mirror as an idle producer. Publishing
	// ReaderNotAdvancing and waiting leaves it Degraded forever.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	rebuilt := make(chan types.NamespacedName, 4)
	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: map[types.NamespacedName]uint32{},
		rebuilds: map[types.NamespacedName]uint32{},
		rebuildFn: func(key types.NamespacedName) {
			rebuilt <- key
		},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	entry := stalledEntry(42, ptrTime(time.Now().Add(-time.Minute)), 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry, time.Millisecond)

	select {
	case got := <-rebuilt:
		assert.Equal(t, key, got)
	case <-time.After(5 * time.Second):
		t.Fatal("a wedged reader must be reopened, not just reported")
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the flusher must return before the reopen tears the entry down")
	}

	assert.Equal(t, uint32(1), r.rebuilds[key],
		"the reopen must consume exactly one unit of budget")

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, mxlv1alpha1.ReasonReaderNotAdvancing, got.Status.Conditions[0].Reason,
		"the wedge stays visible on status while the reopen runs")
}

func TestRunFlusher_StopsReopeningAtCap(t *testing.T) {
	// A flow whose producer is genuinely gone would otherwise cost a
	// reopen every stall window for as long as the mirror exists.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: map[types.NamespacedName]uint32{},
		rebuilds: map[types.NamespacedName]uint32{key: maxReaderRebuilds},
		rebuildFn: func(types.NamespacedName) {
			t.Error("no reopen may be spawned once the budget is spent")
		},
	}

	entry := stalledEntry(42, ptrTime(time.Now().Add(-time.Minute)), 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry, time.Millisecond)

	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		if err := c.Get(context.Background(), key, &got); err != nil {
			return false
		}
		return len(got.Status.Conditions) == 1 &&
			got.Status.Conditions[0].Reason == ReasonReaderRebuildCapReached
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	<-done
}

func TestRunFlusher_ClearsRebuildBudgetOnProgress(t *testing.T) {
	// The cap counts consecutive failed reopens. A mirror that
	// recovered and wedges again later must get a full budget rather
	// than inherit the spent one.
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: map[types.NamespacedName]uint32{},
		rebuilds: map[types.NamespacedName]uint32{key: 3},
	}

	entry := stalledEntry(42, ptrTime(time.Now()), 5)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry, time.Millisecond)

	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		_, still := r.rebuilds[key]
		return !still
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	<-done
}

func ptrTime(t time.Time) *time.Time { return &t }
