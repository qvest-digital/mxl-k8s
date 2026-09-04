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

// stalledEntry builds a sourceEntry whose head last moved at headAt
// and which has delivered transfers grains, the last of them at
// sentAt. A zero sentAt leaves the entry with no delivery on record.
func stalledEntry(head uint64, headAt *time.Time, transfers uint64, sentAt time.Time) *sourceEntry {
	e := &sourceEntry{}
	e.lastHead.Store(head)
	if headAt != nil {
		t := *headAt
		e.headAdvancedAt.Store(&t)
	}
	for i := uint64(0); i < transfers; i++ {
		e.recordTransfer(i, sentAt)
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
		name        string
		entry       *sourceEntry
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantRebuild bool
	}{
		{
			name:       "never probed",
			entry:      stalledEntry(0, nil, 0, time.Time{}),
			wantStatus: metav1.ConditionTrue,
			wantReason: mxlv1alpha1.ReasonHandshakeComplete,
		},
		{
			name:       "head advanced within window",
			entry:      stalledEntry(42, &fresh, 0, time.Time{}),
			wantStatus: metav1.ConditionTrue,
			wantReason: mxlv1alpha1.ReasonHandshakeComplete,
		},
		{
			name:        "head frozen past window without transfers",
			entry:       stalledEntry(42, &stale, 0, time.Time{}),
			wantStatus:  metav1.ConditionFalse,
			wantReason:  mxlv1alpha1.ReasonReaderNotAdvancing,
			wantRebuild: true,
		},
		{
			// The state a mirror lands in when the producer on its
			// source node restarts: the reader stays on the flow
			// directory that went away, so the head it can see never
			// moves again. Judged on the lifetime transfer count this
			// read as Recovered, which is how a mirror that had
			// delivered for hours could then sit dead for hours with
			// neither side reopening anything.
			name:        "head frozen past window after transfers stopped",
			entry:       stalledEntry(42, &stale, 3, stale),
			wantStatus:  metav1.ConditionFalse,
			wantReason:  mxlv1alpha1.ReasonReaderNotAdvancing,
			wantRebuild: true,
		},
		{
			// A backlog drained after the head stopped moving is a
			// reader doing its job, not a wedge.
			name:       "head frozen past window while transfers land",
			entry:      stalledEntry(42, &stale, 3, fresh),
			wantStatus: metav1.ConditionTrue,
			wantReason: mxlv1alpha1.ReasonRecovered,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := observedState(tc.entry, window)
			assert.Equal(t, tc.wantStatus, got.status)
			assert.Equal(t, tc.wantReason, got.reason)
			assert.Equal(t, tc.wantRebuild, got.rebuildReader)
		})
	}
}

func TestObservedState_AgedOutDoesNotOutliveItself(t *testing.T) {
	// The aged-out skip used to be reported for the life of the entry
	// and ahead of every other state, so one fall-behind pinned
	// SourceProgress at ReaderAgedOut whatever the reader did next.
	// That hid both stall detectors behind it: the flusher never saw
	// ReaderNotAdvancing, so it never reopened the reader, and the
	// target's stuck-handshake watchdog never saw TransfersNotLanding,
	// so it never rebuilt its endpoint. The mirror kept reporting a
	// fault that had already passed and nothing recovered it.
	const window = 20 * time.Second
	stale := time.Now().Add(-window - time.Second)
	fresh := time.Now()

	skipped := stalledEntry(42, &fresh, 1, fresh)
	skipped.recordAgedOut(fresh)
	state := observedState(skipped, window)
	assert.Equal(t, mxlv1alpha1.ReasonReaderAgedOut, state.reason,
		"a recent skip stays visible while the reader delivers")
	assert.False(t, state.rebuildReader,
		"a reader that skipped and caught up needs no reopen")

	recovered := stalledEntry(42, &fresh, 1, fresh)
	recovered.recordAgedOut(stale)
	assert.Equal(t, mxlv1alpha1.ReasonRecovered,
		observedState(recovered, window).reason,
		"a skip a whole window old is history, not the current state")

	wedged := stalledEntry(42, &fresh, 1, stale)
	wedged.recordAgedOut(fresh)
	state = observedState(wedged, window)
	assert.Equal(t, metav1.ConditionFalse, state.status)
	assert.Equal(t, mxlv1alpha1.ReasonReaderAgedOut, state.reason)
	assert.True(t, state.rebuildReader,
		"skipping to the head is the loop's own recovery, so a reader "+
			"still aging out a window after its last grain cannot reach "+
			"the live tail at all")
}

func TestObservedState_TransfersStoppedWithLiveHead(t *testing.T) {
	// The head keeps moving, so the reader is demonstrably reading and
	// reopening it fixes nothing; the target owns this recovery and
	// gates it on seeing this reason. Judged on the lifetime transfer
	// count instead, a mirror that delivered and then stopped reported
	// Recovered, and the target went on reading it as an idle producer.
	const window = 20 * time.Second
	entry := stalledEntry(4242, ptrTime(time.Now()), 3,
		time.Now().Add(-window-time.Second))
	entry.openedAt = time.Now().Add(-time.Hour)

	state := observedState(entry, window)
	assert.Equal(t, metav1.ConditionFalse, state.status)
	assert.Equal(t, mxlv1alpha1.ReasonTransfersNotLanding, state.reason)
	assert.False(t, state.rebuildReader,
		"a reader whose head advances is not the broken half")
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
		func(uint64, bool) (bool, error) { return true, nil },
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

func TestRunSampleTransferLoop_RecordsProbedHead(t *testing.T) {
	// observedState reads a nil headAdvancedAt as "never probed" and
	// cannot report ReaderNotAdvancing without it, so a continuous
	// flow whose producer stops had no state that asks for a reopen
	// and no state the writer-liveness check ran on.
	tracker := &recordingTracker{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	probes := make(chan uint64, 3)
	probes <- 4800
	probes <- 4800
	close(probes)

	go runSampleTransferLoop(ctx, done, "flow-audio",
		func() (uint64, error) {
			if h, ok := <-probes; ok {
				return h, nil
			}
			cancel()
			return 4800, nil
		},
		func(uint64, int) error {
			t.Error("a head that never advances leaves nothing to transfer")
			return nil
		},
		func() error { return nil },
		480, time.Millisecond, tracker)

	<-done
	heads := tracker.headSnapshot()
	require.NotEmpty(t, heads, "the sample loop must report the head it already probes")
	for _, h := range heads {
		assert.Equal(t, uint64(4800), h)
	}

	// What the recorded head buys: the state machine can now see the
	// stall the sample path could not previously express.
	entry := stalledEntry(heads[0], ptrTime(time.Now().Add(-defaultReaderStallAfter-time.Second)), 0, time.Time{})
	state := observedState(entry, defaultReaderStallAfter)
	assert.Equal(t, mxlv1alpha1.ReasonReaderNotAdvancing, state.reason)
	assert.True(t, state.rebuildReader)
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
		attempts:      attemptTable[sourceAddInputs]{},
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
		attempts: attemptTable[sourceAddInputs]{},
		rebuilds: map[types.NamespacedName]uint32{},
		rebuildFn: func(key types.NamespacedName) {
			rebuilt <- key
		},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	entry := stalledEntry(42, ptrTime(time.Now().Add(-time.Minute)), 0, time.Time{})

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

func TestRunFlusher_ReopensReaderStuckOutsideTheRing(t *testing.T) {
	// Observed in a running deployment: a mirror sat at
	// SourceProgress=ReaderAgedOut with TargetProgress=NoGrains for
	// hours while its producer wrote continuously, and came back
	// within a minute and a half of being deleted by hand. Deleting it
	// amounts to a fresh reader at the current head, which is what the
	// reopen does without an operator.
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
		attempts: attemptTable[sourceAddInputs]{},
		rebuilds: map[types.NamespacedName]uint32{},
		rebuildFn: func(key types.NamespacedName) {
			rebuilt <- key
		},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	// A live head, a delivery a stall window old, and a skip the loop
	// is still performing: the reader keeps landing outside the ring.
	entry := stalledEntry(42, ptrTime(time.Now()), 1,
		time.Now().Add(-defaultReaderStallAfter-time.Second))
	entry.recordAgedOut(time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry, time.Millisecond)

	select {
	case got := <-rebuilt:
		assert.Equal(t, key, got)
	case <-time.After(5 * time.Second):
		t.Fatal("an aged-out reader that cannot reach the tail must be reopened")
	}
	<-done

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, mxlv1alpha1.ReasonReaderAgedOut, got.Status.Conditions[0].Reason)
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
		attempts: attemptTable[sourceAddInputs]{},
		rebuilds: map[types.NamespacedName]uint32{key: maxReaderRebuilds},
		rebuildFn: func(types.NamespacedName) {
			t.Error("no reopen may be spawned once the budget is spent")
		},
	}

	entry := stalledEntry(42, ptrTime(time.Now().Add(-time.Minute)), 0, time.Time{})
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
		attempts: attemptTable[sourceAddInputs]{},
		rebuilds: map[types.NamespacedName]uint32{key: 3},
	}

	entry := stalledEntry(42, ptrTime(time.Now()), 5, time.Now())
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
