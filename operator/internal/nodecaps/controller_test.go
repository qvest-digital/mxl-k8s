package nodecaps

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		WithObjects(node("n1"), nc.DeepCopy()).
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

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")}}
}

func caps(name string) *mxlv1alpha1.MxlNodeCapabilities {
	return &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-caps-uid")},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: name},
	}
}

// Resources created before the gateway stamped an owner reference have
// nothing to collect them: no gateway will ever run on that node name
// again, so the entry survives every reconcile of everything else.
func TestReconcile_DeletesCapabilitiesOfDepartedNode(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(caps("gone")).Build()
	r := &Reconciler{Client: c, Scheme: newScheme(t)}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gone"},
	})
	require.NoError(t, err)

	var got mxlv1alpha1.MxlNodeCapabilities
	err = c.Get(context.Background(), types.NamespacedName{Name: "gone"}, &got)
	assert.True(t, apierrors.IsNotFound(err), "expected deletion, got %v", err)
}

func TestReconcile_KeepsCapabilitiesWhileTheNodeExists(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(node("live"), caps("live")).Build()
	r := &Reconciler{Client: c, Scheme: newScheme(t)}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "live"},
	})
	require.NoError(t, err)

	var got mxlv1alpha1.MxlNodeCapabilities
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "live"}, &got))
}

// A node that rejoins between the read and the delete has a gateway
// rebuilding its resource. Deleting against the observed UID means the
// stale decision cannot remove the new one.
func TestReconcile_DeleteIsScopedToTheObservedUID(t *testing.T) {
	stale := caps("recycled")
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(stale).Build()
	r := &Reconciler{Client: c, Scheme: newScheme(t)}

	replacement := caps("recycled")
	replacement.UID = types.UID("rebuilt-uid")
	require.NoError(t, c.Delete(context.Background(), stale))
	require.NoError(t, c.Create(context.Background(), replacement))

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "recycled"},
	})
	require.NoError(t, err)
}

func TestNodeToCapabilities_EnqueuesOnlyTheDepartedNode(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(caps("a"), caps("b")).Build()
	r := &Reconciler{Client: c, Scheme: newScheme(t)}

	reqs := r.nodeToCapabilities(context.Background(), node("a"))
	require.Len(t, reqs, 1)
	assert.Equal(t, "a", reqs[0].Name)
}
