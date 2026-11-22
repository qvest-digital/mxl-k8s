package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// notFoundOnUpdate models the race on deletion: the other side's
// gateway removes the last finalizer, the object is gone, and this
// side's Update lands on nothing.
func notFoundOnUpdate(t *testing.T, mirror *mxlv1alpha1.MxlFlowMirror) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newSourceTestScheme(t)).
		WithObjects(mirror).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
				return apierrors.NewNotFound(
					schema.GroupResource{Group: "mxl.qvest-digital.com", Resource: "mxlflowmirrors"},
					obj.GetName())
			},
		}).
		Build()
}

func TestSource_FinalizerRemovalOnVanishedMirrorIsNotAnError(t *testing.T) {
	r := &SourceReconciler{
		Client:   notFoundOnUpdate(t, deletingMirror("node-a", "node-c")),
		Scheme:   newSourceTestScheme(t),
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err, "a mirror deleted under the Update means teardown finished")
	assert.Equal(t, ctrl.Result{}, res)
}

func TestTarget_FinalizerRemovalOnVanishedMirrorIsNotAnError(t *testing.T) {
	r := &TargetReconciler{
		Client:   notFoundOnUpdate(t, deletingMirror("node-x", "node-a")),
		Scheme:   newSourceTestScheme(t),
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{},
		attempts: attemptTable[targetOpenInputs]{},
	}
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err, "a mirror deleted under the Update means teardown finished")
	assert.Equal(t, ctrl.Result{}, res)
}
