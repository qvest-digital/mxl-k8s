package capabilities

import (
	"context"
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

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

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

func TestEnsureExists_CreatesOnce(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}
	require.NoError(t, p.EnsureExists(context.Background()))

	var got mxlv1alpha1.MxlNodeCapabilities
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got))
	assert.Equal(t, "n1", got.Spec.NodeName)
	assert.Empty(t, got.Status.Providers,
		"EnsureExists must leave status to Refresh; the gateway reports "+
			"capabilities only after the manager cache has synced")

	require.NoError(t, p.EnsureExists(context.Background()),
		"a second EnsureExists must be a no-op; first-pod-up wins on a "+
			"cold cluster and any retry must not error out")
}

func TestRefresh_PublishesProvidersAndLastSeen(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: "n1"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlNodeCapabilities{}).
		WithObjects(existing).
		Build()

	p := &Publisher{
		Client:    c,
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP, fabrics.ProviderVerbs},
	}

	require.NoError(t, p.Refresh(context.Background()))

	var got mxlv1alpha1.MxlNodeCapabilities
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got))
	require.NotNil(t, got.Status.LastSeen)
	require.Len(t, got.Status.Providers, 2)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, got.Status.Providers[0].Name)
	assert.Equal(t, mxlv1alpha1.ProviderVerbs, got.Status.Providers[1].Name,
		"the order of the providers status must mirror the configured list; "+
			"future scheduling logic in the operator may rely on the first "+
			"entry being the gateway's preferred provider")
}

func TestRefresh_RewritesProvidersExactly(t *testing.T) {
	// A previous run advertised tcp + verbs. Re-running with only
	// tcp configured must replace the slice (no leftover verbs).
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: "n1"},
		Status: mxlv1alpha1.MxlNodeCapabilitiesStatus{
			Providers: []mxlv1alpha1.MxlFabricsProviderCapability{
				{Name: mxlv1alpha1.ProviderTCP},
				{Name: mxlv1alpha1.ProviderVerbs},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlNodeCapabilities{}).
		WithObjects(existing).
		Build()

	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}
	require.NoError(t, p.Refresh(context.Background()))

	var got mxlv1alpha1.MxlNodeCapabilities
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got))
	require.Len(t, got.Status.Providers, 1)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, got.Status.Providers[0].Name,
		"the publisher must own the providers slice entirely; merging would "+
			"surface a removed provider as still-available and mislead the "+
			"operator's mirror scheduling")
}

func TestRefresh_ErrorsWhenCRMissing(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}

	err := p.Refresh(context.Background())
	require.Error(t, err)
}

func TestRunRefreshLoop_CancelsCleanlyOnCtxDone(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: "n1"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlNodeCapabilities{}).
		WithObjects(existing).
		Build()

	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.RunRefreshLoop(ctx, 10*time.Millisecond)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunRefreshLoop did not return on ctx cancel")
	}
}
