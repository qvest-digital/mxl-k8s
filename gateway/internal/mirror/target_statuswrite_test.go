package mirror

import (
	"context"
	"encoding/json"
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

	require.NoError(t, r.applyTargetInfo(context.Background(), mirror, "info-1"))
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

func TestTarget_StatusQuantumSuppressesSubQuantumTicks(t *testing.T) {
	// Grains commit far faster than anything reading this status
	// reacts, so a timestamp that moved a few milliseconds is not worth
	// an etcd write and a watch fan-out to every controller.
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

	nudged := now.Add(20 * time.Millisecond)
	b.lastCommitAt = &nudged
	assert.True(t, targetStateEqual(a, b),
		"a sub-quantum move must not trigger a publish")

	later := now.Add(statusQuantum)
	b.lastCommitAt = &later
	assert.False(t, targetStateEqual(a, b),
		"a move of a full quantum must publish: an observer has to be able "+
			"to tell a live mirror from a stopped one")

	assert.Less(t, statusQuantum, defaultDegradedAfter,
		"the quantum must stay inside the degraded window, or a healthy "+
			"mirror could be demoted for a timestamp merely waiting to be "+
			"published")
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
