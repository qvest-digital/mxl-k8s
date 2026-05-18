package mirror

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

// The mirror reconciler is a placeholder: it observes MxlFlowMirror
// events and logs. The gateway, not the operator, drives the data
// plane. The contract: read-only, no requeue, no panic on missing.

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func TestReconcile_ExistingMirror_IsObservedWithoutMutation(t *testing.T) {
	scheme := newScheme(t)
	m := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m1"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     "11111111-2222-3333-4444-555555555555",
			SourceNode: "n1",
			TargetNode: "n2",
			Provider:   mxlv1alpha1.ProviderTCP,
		},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{
			Phase:      mxlv1alpha1.MxlFlowMirrorReady,
			TargetInfo: "info",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(m.DeepCopy()).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "m1"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)

	var after mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "m1"}, &after))
	assert.Equal(t, m.Spec, after.Spec)
	assert.Equal(t, m.Status, after.Status,
		"the operator must not write MxlFlowMirror.status; the gateway "+
			"owns it through the OpenMirror gRPC. A status change here "+
			"would race the data-plane state machine")
}

func TestReconcile_MissingMirror_NoError(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "missing"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
}
