package domainpublisher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// goleak checks every test in this package starts and ends with the
// same set of goroutines. RunRefreshLoop is the only goroutine
// producer here; the assertion catches a regression that forgot to
// honour ctx cancellation.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func staticStats(cap, free int64) FilesystemStats {
	return func(_ string) (int64, int64, error) { return cap, free, nil }
}

func TestEnsureExists_CreatesMxlDomainOnce(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{
		Client:        c,
		NodeName:      "n1",
		HostPath:      "/run/mxl/domain",
		Stats:         staticStats(1024, 512),
		FanotifyReady: func() bool { return true },
	}

	require.NoError(t, p.EnsureExists(context.Background()))

	var got mxlv1alpha1.MxlDomain
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got))
	assert.Equal(t, "n1", got.Spec.NodeName)
	assert.Equal(t, "/run/mxl/domain", got.Spec.HostPath)
	assert.Empty(t, got.Status.LastSeen,
		"EnsureExists only creates; status is left for Refresh, so the "+
			"agent can split creation from periodic refresh on cold start")

	// Second EnsureExists must be idempotent.
	require.NoError(t, p.EnsureExists(context.Background()))
}

func TestRefresh_UpdatesStatusFromStatsAndFanotifyReady(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       mxlv1alpha1.MxlDomainSpec{NodeName: "n1", HostPath: "/run/mxl/domain"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlDomain{}).
		WithObjects(existing).
		Build()

	fanotify := false
	p := &Publisher{
		Client:        c,
		NodeName:      "n1",
		HostPath:      "/run/mxl/domain",
		Stats:         staticStats(2048, 1024),
		FanotifyReady: func() bool { return fanotify },
	}

	require.NoError(t, p.Refresh(context.Background()))

	var got mxlv1alpha1.MxlDomain
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got))
	assert.Equal(t, int64(2048), got.Status.CapacityBytes)
	assert.Equal(t, int64(1024), got.Status.FreeBytes)
	assert.False(t, got.Status.FanotifyReady,
		"FanotifyReady=false at the agent must surface in CR status; "+
			"otherwise on-demand mirror materialization stays broken without "+
			"the operator noticing")
	require.NotNil(t, got.Status.LastSeen)

	// Toggle the readiness signal and refresh again: the new value
	// must replace the old one (no silent stickiness).
	fanotify = true
	require.NoError(t, p.Refresh(context.Background()))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got))
	assert.True(t, got.Status.FanotifyReady)
}

func TestRefresh_PropagatesStatsError(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlDomain{}).
		WithObjects(existing).
		Build()

	want := errors.New("statfs broke")
	p := &Publisher{
		Client:        c,
		NodeName:      "n1",
		HostPath:      "/run/mxl/domain",
		Stats:         func(_ string) (int64, int64, error) { return 0, 0, want },
		FanotifyReady: func() bool { return true },
	}

	err := p.Refresh(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, want,
		"the underlying statfs error must be wrapped, not swallowed; "+
			"the agent must surface 'cannot read disk' to logs so an "+
			"alert can fire")
}

func TestRefresh_MissingMxlDomain_Errors(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{
		Client:        c,
		NodeName:      "n1",
		Stats:         staticStats(1, 1),
		FanotifyReady: func() bool { return true },
	}
	err := p.Refresh(context.Background())
	require.Error(t, err)
}

func TestRunRefreshLoop_CancelsOnContextDone(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlDomain{}).
		WithObjects(existing).
		Build()
	p := &Publisher{
		Client:        c,
		NodeName:      "n1",
		HostPath:      "/run/mxl/domain",
		Stats:         staticStats(1, 1),
		FanotifyReady: func() bool { return true },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.RunRefreshLoop(ctx, 10*time.Millisecond)
		close(done)
	}()

	// Wait long enough for at least one tick to land, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunRefreshLoop did not return after ctx cancel")
	}
	// goleak in TestMain catches any leftover goroutine.
}
