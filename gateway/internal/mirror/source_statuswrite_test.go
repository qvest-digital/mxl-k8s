package mirror

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// newFlakySource wires a SourceReconciler whose status writes fail
// while failing is set, and counts every attempt.
func newFlakySource(t *testing.T, mirror *mxlv1alpha1.MxlFlowMirror) (*SourceReconciler, client.Client, *atomic.Int32, *atomic.Bool) {
	t.Helper()
	scheme := newSourceTestScheme(t)
	var attempts atomic.Int32
	var failing atomic.Bool
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
			) error {
				attempts.Add(1)
				if failing.Load() {
					return errors.New("etcdserver: mvcc: database space exceeded")
				}
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
		rebuilds: map[sourceKey]uint32{},
	}
	return r, c, &attempts, &failing
}

// The source flusher retried a rejected publish on every tick, once a
// second for every mirror, for as long as the apiserver kept rejecting
// writes. It backs off instead, and the state still lands once writes
// go through again.
func TestSource_FlusherBacksOffOnAFailedPublish(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	r, c, attempts, failing := newFlakySource(t, mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1"))
	failing.Store(true)

	entry := stalledEntry(42, ptrTime(time.Now()), 3, time.Now())
	entry.key = key
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, entry, time.Millisecond)
	defer func() { cancel(); <-done }()

	time.Sleep(300 * time.Millisecond)
	n := attempts.Load()
	assert.GreaterOrEqual(t, n, int32(2), "a failed publish is retried")
	assert.LessOrEqual(t, n, int32(12),
		"retries back off instead of following the 1 ms flush tick")

	failing.Store(false)
	require.Eventually(t, func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		return c.Get(context.Background(), key, &got) == nil &&
			len(got.Status.Conditions) == 1 &&
			got.Status.Conditions[0].Reason == mxlv1alpha1.ReasonRecovered
	}, 2*time.Second, time.Millisecond,
		"the state lands once writes go through again")
}

// SourceProgress keeps its transition time for as long as its status
// holds. Stamping now on every publish moved it with every heartbeat.
func TestSource_ProgressKeepsTransitionTimeWhileStatusHolds(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	since := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1")
	mirror.Status.Conditions = []metav1.Condition{{
		Type:               mxlv1alpha1.ConditionTypeSourceProgress,
		Status:             metav1.ConditionTrue,
		Reason:             mxlv1alpha1.ReasonHandshakeComplete,
		Message:            "initiator running",
		LastTransitionTime: since,
	}}
	r, c, _, _ := newFlakySource(t, mirror)
	sent := time.Now()

	require.NoError(t, r.publishSourceProgress(context.Background(), key, sourceProgressState{
		status:     metav1.ConditionTrue,
		reason:     mxlv1alpha1.ReasonRecovered,
		message:    "grain progress observed",
		lastSentAt: &sent,
	}))
	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	cond := got.Status.Conditions[0]
	assert.Equal(t, mxlv1alpha1.ReasonRecovered, cond.Reason)
	assert.True(t, cond.LastTransitionTime.Equal(&since),
		"a heartbeat under an unchanged status keeps the transition time")

	require.NoError(t, r.publishSourceProgress(context.Background(), key, sourceProgressState{
		status:     metav1.ConditionFalse,
		reason:     mxlv1alpha1.ReasonTransfersNotLanding,
		message:    "nothing reaches the target",
		lastSentAt: &sent,
	}))
	require.NoError(t, c.Get(context.Background(), key, &got))
	cond = got.Status.Conditions[0]
	assert.WithinDuration(t, time.Now(), cond.LastTransitionTime.Time, 2*time.Second,
		"True to False is a transition and is stamped as one")
}

// The source reads the mirror from the cache before it publishes, and the
// cache can trail its own last write. A flip back to the status the cache
// still shows takes a new transition time.
func TestSource_FlipBackWithAStaleCacheIsATransition(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	old := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	stale := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1")
	stale.Status.Conditions = []metav1.Condition{{
		Type: mxlv1alpha1.ConditionTypeSourceProgress, Status: metav1.ConditionTrue,
		Reason: mxlv1alpha1.ReasonHandshakeComplete, LastTransitionTime: old,
	}}
	r, c, _, _ := newFlakySource(t, stale)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if m, ok := obj.(*mxlv1alpha1.MxlFlowMirror); ok {
				stale.DeepCopyInto(m)
				return nil
			}
			return cl.Get(ctx, k, obj, opts...)
		},
	})
	ctx := context.Background()
	require.NoError(t, r.publishSourceProgress(ctx, key, sourceProgressState{
		status: metav1.ConditionFalse, reason: mxlv1alpha1.ReasonTransfersNotLanding, message: "m"}))
	require.NoError(t, r.publishSourceProgress(ctx, key, sourceProgressState{
		status: metav1.ConditionTrue, reason: mxlv1alpha1.ReasonRecovered, message: "m"}))

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, key, &got))
	assert.WithinDuration(t, time.Now(), got.Status.Conditions[0].LastTransitionTime.Time, 2*time.Second,
		"the flip back to True is stamped now, not carried from the stale cache")
}

// Only a mirror that is not there seeds the transition time with now.
// Any other read error fails the publish, which the flusher retries,
// rather than restamping a condition whose status has not changed.
func TestSource_PublishFailsOnAnUnreadableMirror(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	r, c, attempts, _ := newFlakySource(t, mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-1"))
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("cache not synced")
		},
	})
	err := r.publishSourceProgress(context.Background(), key, sourceProgressState{
		status: metav1.ConditionTrue, reason: mxlv1alpha1.ReasonRecovered, message: "m"})
	require.Error(t, err)
	assert.Zero(t, attempts.Load(), "nothing is written on a mirror that could not be read")
}
