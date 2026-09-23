package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func reconcile(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlDomain{}).WithObjects(objs...).Build()
	r := &Reconciler{Client: c, Scheme: scheme}
	for _, o := range objs {
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: o.GetName()}})
		require.NoError(t, err)
	}
	return c
}

func domain(name string, nodes ...mxlv1alpha1.MxlDomainNodeStatus) *mxlv1alpha1.MxlDomain {
	return &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: mxlv1alpha1.MxlDomainSpec{
			ID: "1ac254d9-a5eb-475f-a2b6-3d02a5cfbc82", Directory: "domain"},
		Status: mxlv1alpha1.MxlDomainStatus{Nodes: nodes},
	}
}

func condition(t *testing.T, c client.Client, name string) *metav1.Condition {
	t.Helper()
	var d mxlv1alpha1.MxlDomain
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: name}, &d))
	return meta.FindStatusCondition(d.Status.Conditions, mxlv1alpha1.ConditionTypeMaterialised)
}

// An object without an id is the agent's old per-node record. Left in
// place it would be listed as if a node were a domain.
func TestReconcileDeletesThePerNodeShape(t *testing.T) {
	legacy := &mxlv1alpha1.MxlDomain{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}
	c := reconcile(t, legacy)

	err := c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &mxlv1alpha1.MxlDomain{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestMaterialisedSummarisesTheNodes(t *testing.T) {
	c := reconcile(t,
		domain("none"),
		domain("half",
			mxlv1alpha1.MxlDomainNodeStatus{NodeName: "n2", Ready: false},
			mxlv1alpha1.MxlDomainNodeStatus{NodeName: "n1", Ready: true}),
		domain("all",
			mxlv1alpha1.MxlDomainNodeStatus{NodeName: "n1", Ready: true},
			mxlv1alpha1.MxlDomainNodeStatus{NodeName: "n2", Ready: true}))

	none := condition(t, c, "none")
	require.NotNil(t, none)
	assert.Equal(t, metav1.ConditionFalse, none.Status)
	assert.Equal(t, "NoNodes", none.Reason)

	half := condition(t, c, "half")
	require.NotNil(t, half)
	assert.Equal(t, metav1.ConditionFalse, half.Status)
	assert.Contains(t, half.Message, "n2")

	all := condition(t, c, "all")
	require.NotNil(t, all)
	assert.Equal(t, metav1.ConditionTrue, all.Status)
}

// The per-node entries are the agents'; the operator writes only the
// condition.
func TestReconcileLeavesTheNodeEntriesAlone(t *testing.T) {
	d := domain("studio", mxlv1alpha1.MxlDomainNodeStatus{NodeName: "n1", Ready: true, Mirrored: true})
	c := reconcile(t, d)

	var after mxlv1alpha1.MxlDomain
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "studio"}, &after))
	assert.Equal(t, d.Status.Nodes, after.Status.Nodes)
}

func TestReconcileMissingDomainIsNoError(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"}})
	require.NoError(t, err)
}
