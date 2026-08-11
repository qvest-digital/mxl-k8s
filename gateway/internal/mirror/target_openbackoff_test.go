package mirror

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// fixedClock returns a clock the test moves by hand, so the backoff is
// exercised without sleeping for it.
func fixedClock(t0 time.Time) (func() time.Time, func(time.Duration)) {
	now := t0
	return func() time.Time { return now }, func(d time.Duration) { now = now.Add(d) }
}

func TestTarget_OpenBackoffGatesRepeatedAttempts(t *testing.T) {
	// A failed open publishes status on the mirror, and this
	// reconciler watches that same object, so its own write wakes it
	// again immediately. The RequeueAfter the failure returns is no
	// defence: the watch event arrives long before it is due. Without
	// an authoritative gate the open retries at apiserver rate and
	// rewrites status every pass, which is what drives the attempt
	// counter into the hundreds inside a single run.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)
	clock, advance := fixedClock(time.Now())
	f.r.nowFn = clock
	ctx := context.Background()

	res, err := f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	require.Equal(t, 1, f.opens, "the first pass must attempt the open")
	require.Positive(t, res.RequeueAfter)

	settled := f.mirror(t)
	require.Equal(t, int32(1), settled.Status.TargetAttemptCount)

	// Every one of these stands for a watch event the failure's own
	// status write produced.
	for range 20 {
		res, err := f.r.Reconcile(ctx, f.req)
		require.NoError(t, err)
		assert.Positive(t, res.RequeueAfter,
			"a gated pass still has to come back when the backoff expires")
	}

	assert.Equal(t, 1, f.opens,
		"inside the backoff the target must not be reopened; a wake that "+
			"retries immediately is the hot loop")

	after := f.mirror(t)
	assert.Equal(t, int32(1), after.Status.TargetAttemptCount,
		"a gated pass must not advance the failure count")
	assert.Equal(t, settled.ResourceVersion, after.ResourceVersion,
		"a gated pass must not write status: the write is what wakes this "+
			"reconciler, so writing on a gated pass sustains the loop")

	advance(backoffFor(1))
	_, err = f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	assert.Equal(t, 2, f.opens,
		"once the backoff has elapsed the retry must happen; the gate "+
			"delays attempts, it does not stop them")
	assert.Equal(t, int32(2), f.mirror(t).Status.TargetAttemptCount)
}

func TestTarget_OpenBackoffReleasedWhenFlowDefinitionChanges(t *testing.T) {
	// The backoff answers "this exact attempt failed, do not repeat it
	// yet". A republished flow definition is a different attempt, and
	// making it wait out a delay earned by the old one would leave a
	// mirror unopened for up to the 30s cap after the very change that
	// fixes it.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)
	clock, _ := fixedClock(time.Now())
	f.r.nowFn = clock
	ctx := context.Background()

	_, err := f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	require.Equal(t, 1, f.opens)

	_, err = f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	require.Equal(t, 1, f.opens, "same inputs inside the backoff stay gated")

	var flow mxlv1alpha1.MxlFlow
	require.NoError(t, f.r.Get(ctx, types.NamespacedName{Name: openFailureFlowID}, &flow))
	flow.Spec.Definition = runtime.RawExtension{
		Raw: []byte(`{"id":"` + openFailureFlowID + `","grain_rate":{"numerator":50}}`),
	}
	require.NoError(t, f.r.Update(ctx, &flow))

	_, err = f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	assert.Equal(t, 2, f.opens,
		"a changed flow definition must be attempted at once rather than "+
			"serving out the previous attempt's backoff")
}

func TestTarget_OpenBackoffClearedOnSuccessAndTeardown(t *testing.T) {
	// The gate is per mirror and lives in a map. An entry that outlives
	// the failure run it belongs to would delay a later open for a
	// mirror that is working, and one that outlives the mirror is a
	// leak on a gateway that sees many short-lived mirrors.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)
	clock, advance := fixedClock(time.Now())
	f.r.nowFn = clock
	ctx := context.Background()

	_, err := f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)

	f.r.mu.Lock()
	_, gated := f.r.openBackoff[f.key]
	f.r.mu.Unlock()
	require.True(t, gated, "a failed open must record a backoff")

	f.r.openTargetFn = func(types.NamespacedName, string, fabrics.Provider) (*targetEntry, error) {
		return &targetEntry{infoStr: "info-1"}, nil
	}
	// The gate is doing its job, so the open that succeeds only
	// happens once the previous failure's backoff has elapsed.
	advance(backoffFor(1))
	t.Cleanup(func() { f.r.closeEntry(f.key, keepFlow) })

	_, err = f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)

	f.r.mu.Lock()
	_, stillGated := f.r.openBackoff[f.key]
	f.r.mu.Unlock()
	assert.False(t, stillGated,
		"an open target must drop its backoff, or the next failure run "+
			"starts already delayed")

	f.r.closeEntry(f.key, keepFlow)
	f.r.mu.Lock()
	n := len(f.r.openBackoff)
	f.r.mu.Unlock()
	assert.Zero(t, n, "teardown must not leave the mirror's backoff behind")
}
