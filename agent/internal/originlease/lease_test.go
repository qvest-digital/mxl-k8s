package originlease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testFlowID = "11111111-2222-3333-4444-555555555555"
	testNode   = "n1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	return s
}

func TestManager_RenewCreatesAndUpdatesLease(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	m := New(c, testNode)

	require.NoError(t, m.Renew(ctx, testFlowID),
		"first Renew must Create the Lease when none exists")

	var first coordinationv1.Lease
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Namespace: LeaseNamespace,
		Name:      LeaseName(testFlowID, testNode),
	}, &first))
	require.NotNil(t, first.Spec.HolderIdentity)
	assert.Equal(t, testNode, *first.Spec.HolderIdentity,
		"HolderIdentity must match the node so the operator can verify "+
			"the Lease belongs to the Origin location's owner")
	require.NotNil(t, first.Spec.LeaseDurationSeconds)
	assert.Equal(t, int32(30), *first.Spec.LeaseDurationSeconds)
	require.NotNil(t, first.Spec.RenewTime)
	firstRenew := first.Spec.RenewTime.Time

	// Sleep a moment so the second renewal stamps a strictly newer time.
	time.Sleep(2 * time.Millisecond)

	require.NoError(t, m.Renew(ctx, testFlowID),
		"second Renew must Update the existing Lease without erroring")
	var second coordinationv1.Lease
	require.NoError(t, c.Get(ctx, types.NamespacedName{
		Namespace: LeaseNamespace,
		Name:      LeaseName(testFlowID, testNode),
	}, &second))
	require.NotNil(t, second.Spec.RenewTime)
	assert.True(t, second.Spec.RenewTime.Time.After(firstRenew),
		"the second Renew must advance RenewTime; without that, downstream "+
			"freshness checks would treat the Lease as stuck at first-publish")
}

func TestManager_ReleaseDeletesLease(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	m := New(c, testNode)
	require.NoError(t, m.Renew(ctx, testFlowID))

	require.NoError(t, m.Release(ctx, testFlowID))
	var lease coordinationv1.Lease
	err := c.Get(ctx, types.NamespacedName{
		Namespace: LeaseNamespace,
		Name:      LeaseName(testFlowID, testNode),
	}, &lease)
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err),
		"Release on a present Lease must remove it so consumers stop "+
			"trusting the now-vanished Origin location")

	// A second Release must not error even though the Lease is gone.
	require.NoError(t, m.Release(ctx, testFlowID),
		"Release must be idempotent: a vanish event that arrives twice "+
			"would otherwise spam the agent log on every redelivery")
}

func TestManager_IsFreshReturnsFalseForExpired(t *testing.T) {
	ctx := context.Background()
	// Pre-create a Lease whose RenewTime is well outside the duration
	// window. IsFresh must report it stale.
	expired := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: LeaseNamespace,
			Name:      LeaseName(testFlowID, testNode),
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr(testNode),
			LeaseDurationSeconds: ptrInt32(30),
			RenewTime: func() *metav1.MicroTime {
				t := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
				return &t
			}(),
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(expired).
		Build()
	m := New(c, testNode)

	fresh, err := m.IsFresh(ctx, testFlowID, testNode)
	require.NoError(t, err)
	assert.False(t, fresh,
		"a Lease whose RenewTime+duration is in the past must report "+
			"unfresh; the operator relies on this to demote a partitioned "+
			"node's Origin location")
}

func TestManager_IsFreshReturnsTrueForRecentRenewal(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	m := New(c, testNode)
	require.NoError(t, m.Renew(ctx, testFlowID))

	fresh, err := m.IsFresh(ctx, testFlowID, testNode)
	require.NoError(t, err)
	assert.True(t, fresh,
		"a Lease renewed inside the duration window must be fresh; "+
			"otherwise the receiver would skip every healthy Origin")
}

func TestManager_IsFreshMissingLeaseReturnsFalse(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	m := New(c, testNode)

	fresh, err := m.IsFresh(ctx, testFlowID, testNode)
	require.NoError(t, err,
		"a missing Lease is a normal state, not an error: the agent on the "+
			"Origin node may not have published yet, or may have already "+
			"Released the Lease on vanish")
	assert.False(t, fresh)
}

// blockingClient wraps a controller-runtime client and parks every
// Get call until the caller's context cancels. The fake client never
// blocks on its own; the wrapper supplies the "API server is
// unreachable" behaviour the bounded-timeout assertion requires.
type blockingClient struct {
	client.Client
}

func (b *blockingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRenew_BoundedTimeout(t *testing.T) {
	// Shrink the renew budget so the test does not actually wait
	// five seconds for the bound to fire. Restore the production
	// value on exit so a later test in the same binary sees the
	// real timeout.
	prev := renewTimeout
	renewTimeout = 50 * time.Millisecond
	t.Cleanup(func() { renewTimeout = prev })

	inner := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	m := New(&blockingClient{Client: inner}, testNode)

	// Hand Renew a caller ctx with a generous deadline so the bound
	// the function applies internally is the only thing that can
	// fire. Without the bound a partition would let RetryOnConflict
	// burn the full caller ctx and overrun the renew interval.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := m.Renew(ctx, testFlowID)
	elapsed := time.Since(start)

	require.Error(t, err,
		"a Renew that hits an unresponsive API server must surface "+
			"an error; otherwise a partitioned agent would silently "+
			"believe its Lease was renewed")
	assert.True(t, errors.Is(err, context.DeadlineExceeded),
		"Renew must wrap context.DeadlineExceeded so callers can "+
			"distinguish a timeout-induced failure from a genuine "+
			"conflict or apiserver error; got %v", err)
	assert.Less(t, elapsed, time.Second,
		"Renew must honour its internal timeout, not the caller's "+
			"five-second deadline; took %s", elapsed)
}

func ptr[T any](v T) *T       { return &v }
func ptrInt32(v int32) *int32 { return &v }
