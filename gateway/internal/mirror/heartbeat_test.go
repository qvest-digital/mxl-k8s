package mirror

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func heartbeatKey(mirror types.NamespacedName, side mxlv1alpha1.MirrorSide) types.NamespacedName {
	return types.NamespacedName{
		Namespace: mxlv1alpha1.LeaseNamespace,
		Name:      mxlv1alpha1.MirrorLeaseName(mirror.Namespace, mirror.Name, side),
	}
}

func readHeartbeat(t *testing.T, c client.Client, mirror types.NamespacedName, side mxlv1alpha1.MirrorSide) *coordinationv1.Lease {
	t.Helper()
	var lease coordinationv1.Lease
	err := c.Get(context.Background(), heartbeatKey(mirror, side), &lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	require.NoError(t, err)
	return &lease
}

// writeCounter counts the writes a flusher sends: status writes to the
// mirror and heartbeat writes to Leases.
type writeCounter struct {
	mu        sync.Mutex
	status    int
	heartbeat int
}

func (w *writeCounter) counts() (int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, w.heartbeat
}

func (w *writeCounter) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			w.mu.Lock()
			if obj.GetObjectKind().GroupVersionKind().Kind == "Lease" {
				w.heartbeat++
			}
			w.mu.Unlock()
			return cl.Patch(ctx, obj, patch, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			w.mu.Lock()
			w.status++
			w.mu.Unlock()
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
}

// A healthy mirror's commit times move on every tick. Written into the
// status they rewrote the whole mirror, descriptor included, and woke
// every controller watching mirrors, every few seconds per mirror; the
// heartbeat Lease takes them at the same resolution instead, and the
// status is written only when the phase or reason changes.
func TestTarget_FlusherBeatsTheLeaseNotTheStatus(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{Phase: mxlv1alpha1.MxlFlowMirrorReady, TargetInfo: "info-1"})
	var w writeCounter
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).WithInterceptorFuncs(w.funcs()).Build()
	r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a",
		FlushInterval: time.Millisecond, targets: map[types.NamespacedName]*targetEntry{}}

	entry := &targetEntry{infoStr: "info-1"}
	start := time.Now()
	entry.lastCommitAt.Store(&start)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry)
	defer func() { cancel(); <-done }()

	require.Eventually(t, func() bool { s, h := w.counts(); return s == 1 && h == 1 },
		time.Second, time.Millisecond, "the first observation goes to both")

	// Commit times a second apart, as a live flow's are, for two
	// quanta.
	for at := time.Second; at <= 2*statusQuantum; at += time.Second {
		next := start.Add(at)
		entry.lastCommitAt.Store(&next)
		time.Sleep(3 * time.Millisecond)
	}
	require.Eventually(t, func() bool { _, h := w.counts(); return h == 3 },
		time.Second, time.Millisecond, "one heartbeat per quantum of progress")
	s, h := w.counts()
	assert.Equal(t, 1, s, "progress alone does not write the mirror's status")
	assert.Equal(t, 3, h)

	lease := readHeartbeat(t, c, key, mxlv1alpha1.MirrorSideTarget)
	require.NotNil(t, lease)
	require.NotNil(t, lease.Spec.RenewTime)
	assert.WithinDuration(t, start.Add(2*statusQuantum), lease.Spec.RenewTime.Time, time.Millisecond)
	assert.Equal(t, "node-a", *lease.Spec.HolderIdentity)
	assert.Equal(t, string(mxlv1alpha1.MirrorSideTarget), lease.Labels[mxlv1alpha1.LabelMirrorSide])
	assert.Equal(t, key.String(), lease.Annotations[mxlv1alpha1.AnnotationMirror])
}

// The source side does the same with the time of its last completed
// transfer.
func TestSource_FlusherBeatsTheLeaseNotTheStatus(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	scheme := newSourceTestScheme(t)
	var w writeCounter
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1")).
		WithInterceptorFuncs(w.funcs()).Build()
	r := &SourceReconciler{Client: c, Scheme: scheme, NodeName: "node-a",
		sources: map[types.NamespacedName]*sourceEntry{}, attempts: attemptTable[sourceAddInputs]{},
		rebuilds: map[sourceKey]uint32{}}

	start := time.Now()
	entry := stalledEntry(42, ptrTime(start), 3, start)
	entry.key = key
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, entry, time.Millisecond)
	defer func() { cancel(); <-done }()

	require.Eventually(t, func() bool { s, h := w.counts(); return s == 1 && h == 1 },
		time.Second, time.Millisecond, "the first observation goes to both")
	for at := 100 * time.Millisecond; at <= statusQuantum; at += 100 * time.Millisecond {
		next := start.Add(at)
		entry.lastSentAt.Store(&next)
		entry.headAdvancedAt.Store(&next)
		time.Sleep(time.Millisecond)
	}
	require.Eventually(t, func() bool { _, h := w.counts(); return h == 2 },
		time.Second, time.Millisecond, "a quantum of progress renews the heartbeat")
	s, _ := w.counts()
	assert.Equal(t, 1, s, "progress alone does not write the mirror's status")

	lease := readHeartbeat(t, c, key, mxlv1alpha1.MirrorSideSource)
	require.NotNil(t, lease)
	assert.WithinDuration(t, start.Add(statusQuantum), lease.Spec.RenewTime.Time, time.Millisecond)
}

// heartbeatAt takes the later of the Lease and the status, so a target
// reads a source that predates the Lease, and a Lease left behind by an
// earlier holder does not hide a newer status.
func TestHeartbeatAt_TakesTheLaterOfLeaseAndStatus(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1")
	c := fake.NewClientBuilder().WithScheme(newSourceTestScheme(t)).Build()
	ctx := context.Background()

	assert.Nil(t, heartbeatAt(ctx, c, mirror, mxlv1alpha1.MirrorSideSource), "neither is set")

	status := time.Now().Add(-time.Minute).Truncate(time.Second)
	mirror.Status.LastSentAt = &metav1.Time{Time: status}
	got := heartbeatAt(ctx, c, mirror, mxlv1alpha1.MirrorSideSource)
	require.NotNil(t, got)
	assert.True(t, got.Equal(status), "a source without a Lease is read from the status")

	beat := status.Add(30 * time.Second)
	require.NoError(t, applyHeartbeat(ctx, c, key, mxlv1alpha1.MirrorSideSource, sourceFieldOwner, "node-a", beat))
	got = heartbeatAt(ctx, c, mirror, mxlv1alpha1.MirrorSideSource)
	require.NotNil(t, got)
	assert.WithinDuration(t, beat, *got, time.Microsecond, "a newer Lease wins")

	mirror.Status.LastSentAt = &metav1.Time{Time: beat.Add(time.Minute).Truncate(time.Second)}
	got = heartbeatAt(ctx, c, mirror, mxlv1alpha1.MirrorSideSource)
	assert.True(t, got.Equal(mirror.Status.LastSentAt.Time), "a newer status wins")

	assert.Nil(t, heartbeatAt(ctx, c, mirror, mxlv1alpha1.MirrorSideTarget),
		"each side reads its own Lease and field")
}

// The post-handshake wedge watchdog reads the source's progress from its
// heartbeat Lease. A watchdog still reading lastSentAt from the status
// would see a source that renews only its Lease as idle and never
// rebuild a wedged fabric side.
func TestRunFlusher_PostHandshakeWedge_ReadsTheSourceHeartbeat(t *testing.T) {
	f := newTargetWatchdogFixture(t, 4)
	f.r.StuckHandshakeAfter = 10 * time.Millisecond
	now := time.Now()
	openedAt := now.Add(-100 * time.Millisecond)
	f.entry.fabricOpenedAt.Store(&openedAt)
	f.entry.commitsAtFabricOpen.Store(0)
	f.entry.commits.Store(1)
	commitAt := now.Add(-5 * time.Second)
	f.entry.lastCommitAt.Store(&commitAt)
	require.NoError(t, applyHeartbeat(context.Background(), f.c, f.key, mxlv1alpha1.MirrorSideSource,
		sourceFieldOwner, "node-x", commitAt.Add(4*time.Second)))
	require.Nil(t, getMirrorStatus(t, f.c, f.key).LastSentAt, "the status carries no lastSentAt")

	f.startFlusher(t)
	defer f.stopFlusher(t)
	select {
	case <-f.spawnCh:
	case <-time.After(time.Second):
		t.Fatal("a source heartbeat seconds past the last commit is a wedge")
	}
}

// The never-handshook watchdog is gated on the source sending into the
// fabric side, which it also reads from the source's heartbeat Lease.
func TestRunFlusher_NeverHandshook_ReadsTheSourceHeartbeat(t *testing.T) {
	f := newTargetWatchdogFixture(t, 4)
	require.NoError(t, applyHeartbeat(context.Background(), f.c, f.key, mxlv1alpha1.MirrorSideSource,
		sourceFieldOwner, "node-x", time.Now()))

	f.startFlusher(t)
	defer f.stopFlusher(t)
	select {
	case <-f.spawnCh:
	case <-time.After(time.Second):
		t.Fatal("a source heartbeat after the fabric open is a source sending into it")
	}
}

// The Reconcile fast path reads the target's own heartbeat. A mirror
// whose status carries a lastGrainAt from its last transition, long
// ago, is still live while its Lease is fresh, and falling through
// would open a second fabric side for it.
func TestReconcile_FastPathReadsTheTargetHeartbeat(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	old := metav1.NewTime(time.Now().Add(-time.Hour))
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{
		Phase:       mxlv1alpha1.MxlFlowMirrorReady,
		TargetInfo:  "info-1",
		LastGrainAt: &old,
	})
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).WithObjects(mirror).Build()
	require.NoError(t, applyHeartbeat(context.Background(), c, key, mxlv1alpha1.MirrorSideTarget,
		targetFieldOwner, "node-a", time.Now()))
	r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a",
		DegradedAfter: 10 * time.Second, targets: map[types.NamespacedName]*targetEntry{key: {}}}

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorReady, getMirrorStatus(t, c, key).Phase,
		"a fresh heartbeat keeps the fast path engaged; the MxlFlow is absent, "+
			"so falling through would have set Materializing")
}

// The Lease lives in another namespace than most mirrors, so nothing
// collects it with the mirror: each gateway deletes its own when it
// releases the mirror.
func TestTeardown_DeletesTheHeartbeat(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	now := metav1.Now()
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{})
	mirror.Finalizers = append(mirror.Finalizers, SourceFinalizerName)
	mirror.Spec.SourceNode = "node-a"
	mirror.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).WithObjects(mirror).Build()
	ctx := context.Background()
	for _, side := range []mxlv1alpha1.MirrorSide{mxlv1alpha1.MirrorSideSource, mxlv1alpha1.MirrorSideTarget} {
		require.NoError(t, applyHeartbeat(ctx, c, key, side, "test", "node-a", time.Now()))
	}

	tr := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a",
		targets: map[types.NamespacedName]*targetEntry{}, attempts: attemptTable[targetOpenInputs]{}}
	_, err := tr.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Nil(t, readHeartbeat(t, c, key, mxlv1alpha1.MirrorSideTarget), "the target deletes its heartbeat")
	assert.NotNil(t, readHeartbeat(t, c, key, mxlv1alpha1.MirrorSideSource), "and only its own")

	sr := &SourceReconciler{Client: c, Scheme: scheme, NodeName: "node-a",
		sources: map[types.NamespacedName]*sourceEntry{}, attempts: attemptTable[sourceAddInputs]{}}
	_, err = sr.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Nil(t, readHeartbeat(t, c, key, mxlv1alpha1.MirrorSideSource), "the source deletes its heartbeat")
}

func getMirrorStatus(t *testing.T, c client.Client, key types.NamespacedName) mxlv1alpha1.MxlFlowMirrorStatus {
	t.Helper()
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &m))
	return m.Status
}

// The source's Origin Lease watch leaves the heartbeats out: they
// renew every few seconds per mirror side and name no flow.
func TestLeasePredicate_IgnoresMirrorHeartbeats(t *testing.T) {
	p := leaseInMxlSystem()
	beat := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace: mxlv1alpha1.LeaseNamespace,
		Name:      mxlv1alpha1.MirrorLeaseName("ns1", "m1", mxlv1alpha1.MirrorSideTarget),
		Labels:    map[string]string{mxlv1alpha1.LabelMirrorSide: string(mxlv1alpha1.MirrorSideTarget)}}}
	origin := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: mxlv1alpha1.LeaseNamespace, Name: "mxl-flow-x"}}
	assert.False(t, p.Generic(event.GenericEvent{Object: beat}))
	assert.True(t, p.Generic(event.GenericEvent{Object: origin}))
}

// A recovery's phase-only writes release TargetProgress and lastGrainAt
// under the flusher's field manager. A moving commit time no longer
// writes the status, so the flusher has to publish its state again once
// the recovery is done, or the mirror reads Ready with neither until its
// next phase change.
func TestRunFlusher_RepublishesAfterRecovery(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{Phase: mxlv1alpha1.MxlFlowMirrorReady, TargetInfo: "info-1"})
	var w writeCounter
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).WithInterceptorFuncs(w.funcs()).Build()
	r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a",
		FlushInterval: time.Millisecond, targets: map[types.NamespacedName]*targetEntry{}}
	entry := &targetEntry{infoStr: "info-1"}
	now := time.Now()
	entry.lastCommitAt.Store(&now)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry)
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { s, _ := w.counts(); return s == 1 }, time.Second, time.Millisecond)

	entry.recovering.Store(true)
	time.Sleep(20 * time.Millisecond)
	entry.recovering.Store(false)
	require.Eventually(t, func() bool { s, _ := w.counts(); return s == 2 }, time.Second, time.Millisecond,
		"the unchanged state is published again after the recovery")
}

// The Lease delete runs ahead of a finalizer's removal and must not hold
// it: a leaked Lease is collected by the operator, a finalizer that
// never comes off leaves a mirror no delete can clear.
func TestTeardown_FailedHeartbeatDeleteKeepsTheFinalizerMoving(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	now := metav1.Now()
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{})
	mirror.Finalizers = append(mirror.Finalizers, "test/hold")
	mirror.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).WithObjects(mirror).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*coordinationv1.Lease); ok {
					return apierrors.NewForbidden(coordinationv1.Resource("leases"), obj.GetName(), errors.New("denied"))
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).Build()
	r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a",
		targets: map[types.NamespacedName]*targetEntry{}, attempts: attemptTable[targetOpenInputs]{}}

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	assert.NotContains(t, got.Finalizers, TargetFinalizerName)
}

// Every phase-only status write releases TargetProgress and lastGrainAt,
// not only a rebuild's: the slow path's Materializing and Ready for a
// mirror whose entry is live do the same. The flusher publishes its
// state again after any of them.
func TestRunFlusher_RepublishesAfterAPhaseOnlyWrite(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{Phase: mxlv1alpha1.MxlFlowMirrorReady, TargetInfo: "info-1"})
	var w writeCounter
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).WithInterceptorFuncs(w.funcs()).Build()
	entry := &targetEntry{infoStr: "info-1"}
	now := time.Now()
	entry.lastCommitAt.Store(&now)
	r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a", FlushInterval: time.Millisecond,
		targets: map[types.NamespacedName]*targetEntry{key: entry}, attempts: attemptTable[targetOpenInputs]{}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry)
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { s, _ := w.counts(); return s == 1 }, time.Second, time.Millisecond)

	require.NoError(t, r.applyTargetStatus(context.Background(), mirror, mxlv1alpha1.MxlFlowMirrorReady, nil, nil))
	require.Eventually(t, func() bool {
		st := getMirrorStatus(t, c, key)
		return st.LastGrainAt != nil && len(st.Conditions) == 1
	}, time.Second, time.Millisecond, "the released condition and lastGrainAt are published again")
}
