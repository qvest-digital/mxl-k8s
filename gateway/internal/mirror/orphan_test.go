package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// deletingMirror carries both gateway finalizers so each test can
// assert a reconciler drops only its own.
func deletingMirror(sourceNode, targetNode string) *mxlv1alpha1.MxlFlowMirror {
	now := metav1.Now()
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "m1",
			Namespace:         "ns1",
			DeletionTimestamp: &now,
			Finalizers: []string{
				SourceFinalizerName,
				TargetFinalizerName,
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     "flow-1",
			SourceNode: sourceNode,
			TargetNode: targetNode,
		},
	}
}

// Without the reaper a mirror whose source node was removed is
// undeletable: no surviving gateway claims it and nothing else owns
// the finalizer.
func TestReconcile_SourceFinalizerReapedWhenSourceNodeGone(t *testing.T) {
	scheme := newSourceTestScheme(t)
	mirror := deletingMirror("node-reclaimed", "node-c")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &live))
	assert.False(t, controllerutil.ContainsFinalizer(&live, SourceFinalizerName),
		"a deleting mirror whose spec.sourceNode names a node that no longer "+
			"exists has no gateway left to tear it down; the source-side "+
			"finalizer must be reaped or the object never deletes")
	assert.True(t, controllerutil.ContainsFinalizer(&live, TargetFinalizerName),
		"the source reconciler must not touch the target side's finalizer")
}

// The guard on the reaper: a registered node still has a gateway
// that owns the teardown, and stripping the finalizer would race it
// into leaking an open initiator.
func TestReconcile_SourceFinalizerKeptWhileSourceNodeExists(t *testing.T) {
	scheme := newSourceTestScheme(t)
	mirror := deletingMirror("node-b", "node-c")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror, node).
		Build()

	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, orphanRecheckInterval, res.RequeueAfter,
		"a Node deletion raises no event on the mirror, so a deleting "+
			"foreign mirror has to be re-examined on a timer")

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &live))
	assert.True(t, controllerutil.ContainsFinalizer(&live, SourceFinalizerName),
		"while the source node is registered its gateway owns the teardown")
}

// Target-side mirror image: the same ownership gate orphans the same
// way when a target node is reclaimed.
func TestReconcile_TargetFinalizerReapedWhenTargetNodeGone(t *testing.T) {
	scheme := newSourceTestScheme(t)
	mirror := deletingMirror("node-b", "node-reclaimed")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &TargetReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &live))
	assert.False(t, controllerutil.ContainsFinalizer(&live, TargetFinalizerName),
		"a deleting mirror whose spec.targetNode names a node that no longer "+
			"exists has no gateway left to tear it down")
	assert.True(t, controllerutil.ContainsFinalizer(&live, SourceFinalizerName),
		"the target reconciler must not touch the source side's finalizer")
}

// Target-side counterpart of the source guard.
func TestReconcile_TargetFinalizerKeptWhileTargetNodeExists(t *testing.T) {
	scheme := newSourceTestScheme(t)
	mirror := deletingMirror("node-b", "node-c")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-c"}}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror, node).
		Build()

	r := &TargetReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, orphanRecheckInterval, res.RequeueAfter)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &live))
	assert.True(t, controllerutil.ContainsFinalizer(&live, TargetFinalizerName),
		"while the target node is registered its gateway owns the teardown")
}

// The end state the incident needs: reaping the last finalizer lets
// the deletion complete.
func TestReconcile_LastOrphanedFinalizerCompletesDeletion(t *testing.T) {
	scheme := newSourceTestScheme(t)
	now := metav1.Now()
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "m1",
			Namespace:         "ns1",
			DeletionTimestamp: &now,
			Finalizers:        []string{SourceFinalizerName},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     "flow-1",
			SourceNode: "node-reclaimed",
			TargetNode: "node-c",
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)

	var live mxlv1alpha1.MxlFlowMirror
	err = c.Get(context.Background(), key, &live)
	assert.True(t, apierrors.IsNotFound(err),
		"dropping the last finalizer must let the deletion complete, got %v", err)
}
