package mirror

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

type addBackoffFixture struct {
	r       *SourceReconciler
	opener  *fakeOpener
	key     types.NamespacedName
	req     reconcile.Request
	advance func(time.Duration)
}

func newAddBackoffFixture(t *testing.T, targetInfo string) *addBackoffFixture {
	t.Helper()
	scheme := newSourceTestScheme(t)
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", "flow-1", targetInfo)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	opener := &fakeOpener{
		openFn: func(string, string, fabrics.Provider) (*sourceEntry, error) {
			return nil, errors.Join(errAddTargetFailed, errors.New("connect: target offline"))
		},
	}
	clock, advance := fixedClock(time.Now())
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	return &addBackoffFixture{
		r: &SourceReconciler{
			Client:   c,
			Scheme:   scheme,
			NodeName: "node-a",
			nowFn:    clock,
			opener:   opener,
			sources:  map[types.NamespacedName]*sourceEntry{},
			attempts: attemptTable[sourceAddInputs]{},
		},
		opener:  opener,
		key:     key,
		req:     reconcile.Request{NamespacedName: key},
		advance: advance,
	}
}

func (f *addBackoffFixture) mirror(t *testing.T) mxlv1alpha1.MxlFlowMirror {
	t.Helper()
	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.r.Get(context.Background(), f.key, &got))
	return got
}

func TestSource_AddTargetBackoffGatesRepeatedAttempts(t *testing.T) {
	// A failed AddTarget publishes SourceProgress on the mirror this
	// reconciler watches, so the write wakes it again at once and the
	// RequeueAfter never applies. Left advisory, an unreachable target
	// is retried at apiserver rate and the initiator is torn down and
	// rebuilt on every pass.
	f := newAddBackoffFixture(t, "info-1")
	ctx := context.Background()

	res, err := f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	require.Positive(t, res.RequeueAfter)
	require.Equal(t, int32(1), f.opener.calls.Load())

	settled := f.mirror(t)
	require.Equal(t, int32(1), settled.Status.AttemptCount)

	for range 20 {
		res, err := f.r.Reconcile(ctx, f.req)
		require.NoError(t, err)
		assert.Positive(t, res.RequeueAfter)
	}

	assert.Equal(t, int32(1), f.opener.calls.Load(),
		"inside the backoff the initiator must not be rebuilt")

	after := f.mirror(t)
	assert.Equal(t, int32(1), after.Status.AttemptCount,
		"a gated pass must not advance the failure count")
	assert.Equal(t, settled.ResourceVersion, after.ResourceVersion,
		"a gated pass must not publish status: that write is what wakes "+
			"this reconciler")

	f.advance(backoffFor(1))
	_, err = f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	assert.Equal(t, int32(2), f.opener.calls.Load(),
		"the retry must happen once the backoff has elapsed")
	assert.Equal(t, int32(2), f.mirror(t).Status.AttemptCount)
}

func TestSource_AddTargetBackoffReleasedWhenTargetInfoRotates(t *testing.T) {
	// A rotated TargetInfo is the target side reporting a rebuilt
	// endpoint. That is a new attempt, and holding it behind a delay
	// earned against the dead endpoint would keep the transfer down for
	// up to the 30s cap after the very thing that fixes it.
	f := newAddBackoffFixture(t, "info-1")
	ctx := context.Background()

	_, err := f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	require.Equal(t, int32(1), f.opener.calls.Load())

	_, err = f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	require.Equal(t, int32(1), f.opener.calls.Load(),
		"the same endpoint inside the backoff stays gated")

	m := f.mirror(t)
	m.Status.TargetInfo = "info-2"
	require.NoError(t, f.r.Status().Update(ctx, &m))

	_, err = f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	assert.Equal(t, int32(2), f.opener.calls.Load(),
		"a rotated TargetInfo must be attempted at once")
}
