package flow

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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// The flow reconciler is an observer stub: the agent owns MxlFlow
// status, the operator only logs the event. The contract that needs
// to hold is therefore narrow: do not mutate the CR, do not requeue,
// do not crash on missing objects. A future change that quietly turns
// the reconciler into a writer would be caught by the no-mutation
// assertion.

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func TestReconcile_ExistingFlow_IsObservedWithoutMutation(t *testing.T) {
	scheme := newScheme(t)
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "11111111-2222-3333-4444-555555555555"},
		Spec: mxlv1alpha1.MxlFlowSpec{
			ID: "11111111-2222-3333-4444-555555555555",
		},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy()).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: flow.Name},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)

	var after mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: flow.Name}, &after))
	assert.Equal(t, flow.Spec, after.Spec)
	assert.Equal(t, flow.Status, after.Status,
		"the operator must not write MxlFlow.status; the agent owns it. "+
			"Any non-empty diff here is a contract violation that would "+
			"race the agent's status updates")
}

func TestReconcile_MissingFlow_NoError(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
}

// Compile-time guarantee that the package still ships an obvious
// Reconcile entry point. If the public Reconcile method ever moves
// to a different receiver, every consumer of the package breaks at
// build time; this declaration keeps that obvious in the test
// binary too.
var _ = (*Reconciler)(nil).Reconcile

// silence unused-import diagnostics when the test file is the only
// importer of a package.
var _ = client.IgnoreNotFound
