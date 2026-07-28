package flow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// The agent owns MxlFlow status for its own node. The operator's one
// write is removing entries whose node has left the cluster, which no
// agent can do for itself once its node is gone. Everything else must
// stay untouched: a location on a live node, the spec, the requeue
// result. The assertions below fence that narrow permission in, so a
// change that widens the reconciler into a general status writer
// fails here.

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func newFlow(locs ...mxlv1alpha1.MxlFlowLocation) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "11111111-2222-3333-4444-555555555555"},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: "11111111-2222-3333-4444-555555555555"},
		Status:     mxlv1alpha1.MxlFlowStatus{Locations: locs},
	}
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestReconcile_LiveNodeLocations_AreNotMutated(t *testing.T) {
	scheme := newScheme(t)
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	})
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("n1")).
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
		"a location whose node is still registered belongs to that node's "+
			"agent; writing it here would race the agent's own updates")
}

// The incident this guards: spot capacity is reclaimed, the agent
// dies with the node, and the entry it published outlives both. On a
// cluster that recycles nodes the list grows without bound, and every
// stranded entry is one a consumer cannot tell apart from a quiet
// node.
func TestReconcile_DepartedNodeLocation_IsPruned(t *testing.T) {
	scheme := newScheme(t)
	flow := newFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "gone", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
		mxlv1alpha1.MxlFlowLocation{NodeName: "alive", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("alive")).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: flow.Name},
	})
	require.NoError(t, err)

	var after mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: flow.Name}, &after))
	require.Len(t, after.Status.Locations, 1,
		"the entry for the departed node must go; nothing else can remove it")
	assert.Equal(t, "alive", after.Status.Locations[0].NodeName)
}

// An Origin entry on a departed node is the dangerous shape: it reads
// as a live producer to anything that does not also consult the
// Lease.
func TestReconcile_DepartedOriginLocation_IsPruned(t *testing.T) {
	scheme := newScheme(t)
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "gone", Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	})
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy()).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: flow.Name},
	})
	require.NoError(t, err)

	var after mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: flow.Name}, &after))
	assert.Empty(t, after.Status.Locations)
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

func TestNodeToFlows_EnqueuesOnlyReferencingFlows(t *testing.T) {
	scheme := newScheme(t)
	referencing := newFlow(mxlv1alpha1.MxlFlowLocation{NodeName: "gone"})
	unrelated := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "99999999-2222-3333-4444-555555555555"},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: "99999999-2222-3333-4444-555555555555"},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{NodeName: "elsewhere"}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(referencing, unrelated).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}
	got := r.nodeToFlows(context.Background(), node("gone"))
	require.Len(t, got, 1,
		"a Node deletion raises no event on the flows referencing it, so the "+
			"mapping is what makes the prune happen at all")
	assert.Equal(t, referencing.Name, got[0].Name)
}

func TestNodeToFlows_NonNodeObjectReturnsNil(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: c, Scheme: scheme}
	assert.Nil(t, r.nodeToFlows(context.Background(), newFlow()))
}

// Node objects churn constantly on heartbeats and conditions. Only a
// departure can strand a location, so everything else is filtered
// out before it reaches the queue.
func TestNodeDeletedOnly_AcceptsDeletesOnly(t *testing.T) {
	p := nodeDeletedOnly()
	assert.True(t, p.Delete(event.DeleteEvent{Object: node("n1")}))
	assert.False(t, p.Create(event.CreateEvent{Object: node("n1")}))
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: node("n1"), ObjectNew: node("n1")}))
	assert.False(t, p.Generic(event.GenericEvent{Object: node("n1")}))
}

var _ = (*Reconciler)(nil).Reconcile

var (
	_ = client.IgnoreNotFound
	_ reconcile.Request
)
