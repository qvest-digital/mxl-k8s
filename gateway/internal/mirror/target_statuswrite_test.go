package mirror

import (
	"context"
	"encoding/json"
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

// capturedPatch is one server-side-apply payload the reconciler sent,
// decoded far enough to assert which manager wrote which keys.
type capturedPatch struct {
	owner  string
	status map[string]any
}

func (c capturedPatch) has(key string) bool {
	_, ok := c.status[key]
	return ok
}

// newCapturingTarget wires a TargetReconciler whose status writes are
// recorded rather than merely applied, so the split between the two
// field managers can be asserted on the payloads themselves. The fake
// client does not implement SSA ownership, so inspecting the object
// afterwards cannot prove which manager owns what; the payload can.
func newCapturingTarget(t *testing.T, mirror *mxlv1alpha1.MxlFlowMirror) (*TargetReconciler, *[]capturedPatch) {
	t.Helper()
	scheme := newSourceTestScheme(t)
	var seen []capturedPatch
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
			) error {
				raw, err := patch.Data(obj)
				require.NoError(t, err)
				var doc struct {
					Status map[string]any `json:"status"`
				}
				require.NoError(t, json.Unmarshal(raw, &doc))
				o := &client.SubResourcePatchOptions{}
				for _, opt := range opts {
					opt.ApplyToSubResourcePatch(o)
				}
				seen = append(seen, capturedPatch{owner: string(o.FieldManager), status: doc.Status})
				return cl.Status().Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	return &TargetReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{},
		attempts: attemptTable[targetOpenInputs]{},
	}, &seen
}

func TestTarget_ProgressWritesCarryNoTargetInfo(t *testing.T) {
	// The descriptor is the largest field on the object - a 12-channel
	// audio flow serialises around 23 kB of bounce-buffer regions - and
	// it does not change while the fabric side stays up. While one
	// field manager owned both it and the progress fields, SSA's
	// release-on-omit rule forced every progress write to re-stamp the
	// whole descriptor, so a healthy mirror rewrote it into etcd on
	// every flusher tick.
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{})
	r, seen := newCapturingTarget(t, mirror)

	require.NoError(t, r.applyTargetInfo(context.Background(), mirror, "info-1", 0, ""))
	commit := time.Now()
	require.NoError(t, r.applyTargetStatus(context.Background(), mirror,
		mxlv1alpha1.MxlFlowMirrorReady, &commit, &metav1.Condition{
			Type:               mxlv1alpha1.ConditionTypeTargetProgress,
			Status:             metav1.ConditionTrue,
			Reason:             mxlv1alpha1.ReasonHandshakeComplete,
			Message:            "grain commits observed",
			LastTransitionTime: metav1.Now(),
		}))

	require.Len(t, *seen, 2)
	info, progress := (*seen)[0], (*seen)[1]

	assert.Equal(t, targetInfoFieldOwner, info.owner)
	assert.True(t, info.has("targetInfo"),
		"the descriptor write must carry the descriptor")
	assert.False(t, info.has("phase"),
		"the descriptor manager must not claim the phase; a manager that "+
			"claims a field it later omits releases it, and the phase "+
			"changes far more often than the descriptor")

	assert.Equal(t, targetFieldOwner, progress.owner)
	assert.False(t, progress.has("targetInfo"),
		"a progress write must never carry the descriptor; re-stamping it "+
			"per tick is the write volume this split removes")
	assert.True(t, progress.has("phase"))
	assert.True(t, progress.has("lastGrainAt"))
}

func TestTarget_StatusIgnoresMovingCommitTimes(t *testing.T) {
	// A moving commit time is the target's heartbeat, which goes to its
	// heartbeat Lease. In the status it rewrote the whole mirror every
	// few seconds, however far it moved.
	base := targetProgressState{
		phase:   mxlv1alpha1.MxlFlowMirrorReady,
		status:  metav1.ConditionTrue,
		reason:  mxlv1alpha1.ReasonHandshakeComplete,
		message: "grain commits observed",
	}
	now := time.Now()
	a, b := base, base
	t0 := now
	a.lastCommitAt = &t0

	later := now.Add(time.Hour)
	b.lastCommitAt = &later
	assert.True(t, targetStateEqual(a, b),
		"a moving commit time alone must not write the mirror's status")

	b.lastCommitAt = nil
	assert.False(t, targetStateEqual(a, b),
		"the first commit time is published with the status")

	assert.Less(t, statusQuantum, defaultStuckHandshakeAfter,
		"heartbeats move at the quantum, which has to stay inside the "+
			"stuck-handshake window the watchdog compares them against")
}

func TestTarget_PhaseChangeIsNeverQuantised(t *testing.T) {
	// Coarsening applies to the timestamp only. A phase or reason
	// transition is the signal operators alert on and has to reach the
	// API on the tick it happens.
	now := time.Now()
	ready := targetProgressState{
		phase:        mxlv1alpha1.MxlFlowMirrorReady,
		status:       metav1.ConditionTrue,
		reason:       mxlv1alpha1.ReasonHandshakeComplete,
		message:      "grain commits observed",
		lastCommitAt: &now,
	}
	degraded := ready
	degraded.phase = mxlv1alpha1.MxlFlowMirrorDegraded
	degraded.status = metav1.ConditionFalse
	degraded.reason = mxlv1alpha1.ReasonNoGrains

	assert.False(t, targetStateEqual(ready, degraded),
		"a Ready->Degraded transition sharing one timestamp must still "+
			"publish; suppressing it would hide the failure the status "+
			"exists to report")
}

// The source bounds a sample batch by the target's sync hint, so it
// must never read a hint that belongs to another descriptor than the
// one it dials: both go in one payload under one manager. A target
// without a writer config publishes no hint, which the source reads as
// libmxl's default.
func TestTarget_TargetInfoCarriesTheWritersSyncHint(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{})
	r, seen := newCapturingTarget(t, mirror)

	require.NoError(t, r.applyTargetInfo(context.Background(), mirror, "info-1", 960, ""))
	require.NoError(t, r.applyTargetInfo(context.Background(), mirror, "info-2", 0, ""))

	require.Len(t, *seen, 2)
	assert.Equal(t, targetInfoFieldOwner, (*seen)[0].owner)
	assert.Equal(t, "info-1", (*seen)[0].status["targetInfo"])
	assert.EqualValues(t, 960, (*seen)[0].status["targetMaxSyncBatchSizeHint"])
	assert.False(t, (*seen)[1].has("targetMaxSyncBatchSizeHint"))
	assert.False(t, (*seen)[1].has("targetInterface"))
}

// The address the target endpoint is bound to rides with the
// descriptor it is bound for.
func TestTarget_TargetInfoCarriesTheBoundInterface(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{})
	r, seen := newCapturingTarget(t, mirror)

	require.NoError(t, r.applyTargetInfo(context.Background(), mirror, "info-1", 0, "192.0.2.129"))
	require.Len(t, *seen, 1)
	assert.Equal(t, targetInfoFieldOwner, (*seen)[0].owner)
	assert.Equal(t, "192.0.2.129", (*seen)[0].status["targetInterface"])
}

// TargetProgress keeps its transition time for as long as its status
// holds. Stamping now on every publish moved it with every heartbeat.
func TestTarget_ProgressKeepsTransitionTimeWhileStatusHolds(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	since := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{
			Phase: mxlv1alpha1.MxlFlowMirrorReady,
			Conditions: []metav1.Condition{{
				Type:               mxlv1alpha1.ConditionTypeTargetProgress,
				Status:             metav1.ConditionTrue,
				Reason:             mxlv1alpha1.ReasonHandshakeComplete,
				Message:            "grain commits observed",
				LastTransitionTime: since,
			}},
		})
	r, seen := newCapturingTarget(t, mirror)
	entry := &targetEntry{infoStr: "info-1"}
	commit := time.Now()
	ready := targetProgressState{
		phase:        mxlv1alpha1.MxlFlowMirrorReady,
		status:       metav1.ConditionTrue,
		reason:       mxlv1alpha1.ReasonRecovered,
		message:      "grain commits resumed after stall",
		lastCommitAt: &commit,
	}
	require.NoError(t, r.publishTargetProgress(context.Background(), key, ready, entry))

	degraded := ready
	degraded.phase = mxlv1alpha1.MxlFlowMirrorDegraded
	degraded.status = metav1.ConditionFalse
	degraded.reason = mxlv1alpha1.ReasonNoGrains
	require.NoError(t, r.publishTargetProgress(context.Background(), key, degraded, entry))

	require.Len(t, *seen, 2)
	transition := func(p capturedPatch) string {
		conds := p.status["conditions"].([]any)
		return conds[0].(map[string]any)["lastTransitionTime"].(string)
	}
	assert.Equal(t, since.UTC().Format(time.RFC3339), transition((*seen)[0]),
		"a heartbeat under an unchanged status keeps the transition time")
	assert.NotEqual(t, since.UTC().Format(time.RFC3339), transition((*seen)[1]),
		"Ready to Degraded is a transition and is stamped as one")
}

// A publish the apiserver rejects is retried on a backoff, not on
// every tick and not only once the state changes again. A Degraded
// mirror's state stops changing, so a lost Degraded write used to stay
// lost for as long as the mirror was down.
func TestTarget_FlusherRetriesALostPublishOnABackoff(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{Phase: mxlv1alpha1.MxlFlowMirrorReady, TargetInfo: "info-1"})
	var attempts atomic.Int32
	var failing atomic.Bool
	failing.Store(true)
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
	r := &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		FlushInterval: time.Millisecond,
		DegradedAfter: 10 * time.Millisecond,
		targets:       map[types.NamespacedName]*targetEntry{},
	}
	entry := &targetEntry{infoStr: "info-1"}
	stale := time.Now().Add(-time.Minute)
	entry.lastCommitAt.Store(&stale)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, key, entry)
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
			got.Status.Phase == mxlv1alpha1.MxlFlowMirrorDegraded
	}, 2*time.Second, time.Millisecond,
		"the Degraded state is published once writes go through again, "+
			"although it has not changed since the publish that failed")
}

// The flusher publishes against the mirror the cache hands it, which can
// trail the gateway's own last write. Degraded and then Ready again,
// published while the cache still shows the Ready from before, is two
// transitions: the second takes a new transition time.
func TestTarget_FlipBackWithAStaleCacheIsATransition(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	old := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	stale := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1",
		mxlv1alpha1.MxlFlowMirrorStatus{
			Phase: mxlv1alpha1.MxlFlowMirrorReady,
			Conditions: []metav1.Condition{{
				Type: mxlv1alpha1.ConditionTypeTargetProgress, Status: metav1.ConditionTrue,
				Reason: mxlv1alpha1.ReasonHandshakeComplete, LastTransitionTime: old,
			}},
		})
	r, seen := newCapturingTarget(t, stale)
	ctx := context.Background()
	cond := func(status metav1.ConditionStatus, reason string) *metav1.Condition {
		return &metav1.Condition{Type: mxlv1alpha1.ConditionTypeTargetProgress, Status: status, Reason: reason, Message: "m"}
	}
	require.NoError(t, r.applyTargetStatus(ctx, stale.DeepCopy(), mxlv1alpha1.MxlFlowMirrorDegraded, nil,
		cond(metav1.ConditionFalse, mxlv1alpha1.ReasonNoGrains)))
	require.NoError(t, r.applyTargetStatus(ctx, stale.DeepCopy(), mxlv1alpha1.MxlFlowMirrorReady, nil,
		cond(metav1.ConditionTrue, mxlv1alpha1.ReasonRecovered)))

	require.Len(t, *seen, 2)
	ltt := func(p capturedPatch) time.Time {
		v := p.status["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"].(string)
		at, err := time.Parse(time.RFC3339, v)
		require.NoError(t, err)
		return at
	}
	assert.WithinDuration(t, time.Now(), ltt((*seen)[1]), 2*time.Second,
		"the flip back to True is stamped now, not carried from the stale cache")
}
