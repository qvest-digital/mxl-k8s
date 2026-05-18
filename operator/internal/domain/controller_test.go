package domain

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

// The domain reconciler is an observer stub: the per-node agent
// owns the status (capacity, free bytes, fanotify state). The
// operator must not write to it.

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func TestReconcile_ExistingDomain_IsObservedWithoutMutation(t *testing.T) {
	scheme := newScheme(t)
	d := &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       mxlv1alpha1.MxlDomainSpec{NodeName: "n1", HostPath: "/run/mxl/domain"},
		Status: mxlv1alpha1.MxlDomainStatus{
			CapacityBytes: 1 << 30,
			FanotifyReady: true,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlDomain{}).
		WithObjects(d.DeepCopy()).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)

	var after mxlv1alpha1.MxlDomain
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &after))
	assert.Equal(t, d.Status, after.Status,
		"the agent is the sole writer of MxlDomain.status; a non-empty "+
			"diff here would race fanotify-ready transitions")
}

func TestReconcile_MissingDomain_NoError(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
}
