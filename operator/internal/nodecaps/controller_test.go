package nodecaps

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// The nodecaps reconciler is an observer stub: the gateway owns the
// status (probed libmxl-fabrics providers). The operator must not
// write to it.

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func TestReconcile_ExistingNodeCapabilities_IsObservedWithoutMutation(t *testing.T) {
	scheme := newScheme(t)
	nc := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: "n1"},
		Status: mxlv1alpha1.MxlNodeCapabilitiesStatus{
			Providers: []mxlv1alpha1.MxlFabricsProviderCapability{
				{Name: mxlv1alpha1.ProviderTCP, DeviceCount: 1},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlNodeCapabilities{}).
		WithObjects(nc.DeepCopy()).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)

	var after mxlv1alpha1.MxlNodeCapabilities
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &after))
	assert.Equal(t, nc.Status, after.Status,
		"the gateway is the sole writer of MxlNodeCapabilities.status; "+
			"an operator-side update would race the provider-probe loop")
}

func TestReconcile_MissingNodeCapabilities_NoError(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
}
