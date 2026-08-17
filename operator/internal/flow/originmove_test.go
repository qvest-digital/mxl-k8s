package flow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// flowWithOrigin builds a flow whose authoritative copy sits on node.
func flowWithOrigin(id, node string, others ...string) *mxlv1alpha1.MxlFlow {
	locs := []mxlv1alpha1.MxlFlowLocation{
		{NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	for _, o := range others {
		locs = append(locs, mxlv1alpha1.MxlFlowLocation{
			NodeName: o, Phase: mxlv1alpha1.MxlFlowLocationReady,
		})
	}
	f := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: id},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: id},
		Status:     mxlv1alpha1.MxlFlowStatus{Locations: locs},
	}
	return f
}

func newFlowReconciler(t *testing.T, objs ...*mxlv1alpha1.MxlFlow) (*Reconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := newScheme(t)
	b := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{})
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	rec := record.NewFakeRecorder(8)
	return &Reconciler{Client: b.Build(), Scheme: scheme, Recorder: rec}, rec
}

func TestRecordOriginMove_FirstOriginIsRecordedWithoutAPrevious(t *testing.T) {
	f := flowWithOrigin("f1", "n1")
	r, rec := newFlowReconciler(t, f)

	require.NoError(t, r.recordOriginMove(context.Background(), f))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &got))
	assert.Equal(t, "n1", got.Status.OriginNode)
	assert.Empty(t, got.Status.PreviousOriginNode,
		"a first origin has nowhere to have moved from")
	require.NotNil(t, got.Status.OriginChangedAt)
	assert.Contains(t, <-rec.Events, ReasonOriginMoved)
}

func TestRecordOriginMove_RecordsBothEndsOfTheMove(t *testing.T) {
	// This is the field that was missing when a mass degradation had to
	// be told apart from a convergence window: without it the objects
	// say where the origin is, never when it got there.
	f := flowWithOrigin("f1", "n1")
	r, rec := newFlowReconciler(t, f)
	require.NoError(t, r.recordOriginMove(context.Background(), f))
	<-rec.Events

	var moved mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &moved))
	moved.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
		{NodeName: "n5", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	require.NoError(t, r.Status().Update(context.Background(), &moved))

	require.NoError(t, r.recordOriginMove(context.Background(), &moved))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &got))
	assert.Equal(t, "n5", got.Status.OriginNode)
	assert.Equal(t, "n1", got.Status.PreviousOriginNode,
		"the node the origin left is the one worth naming; a mirror still "+
			"sourcing from it is the thing being diagnosed")
	require.NotNil(t, got.Status.OriginChangedAt)

	ev := <-rec.Events
	assert.Contains(t, ev, "n1")
	assert.Contains(t, ev, "n5")
}

func TestRecordOriginMove_AnOriginVanishingIsNotAMove(t *testing.T) {
	// Every producer restart briefly leaves the flow with no Origin.
	// Treating that as a transition would stamp a move on each restart
	// and overwrite PreviousOriginNode with an empty string, losing the
	// one node worth knowing about.
	f := flowWithOrigin("f1", "n1")
	r, rec := newFlowReconciler(t, f)
	require.NoError(t, r.recordOriginMove(context.Background(), f))
	<-rec.Events

	var gone mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &gone))
	gone.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
	}
	require.NoError(t, r.Status().Update(context.Background(), &gone))

	require.NoError(t, r.recordOriginMove(context.Background(), &gone))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &got))
	assert.Equal(t, "n1", got.Status.OriginNode,
		"the last known origin is kept while the flow has none")
	select {
	case ev := <-rec.Events:
		t.Fatalf("an origin vanishing must raise no move event, got %q", ev)
	default:
	}
}

func TestRecordOriginMove_IsIdempotent(t *testing.T) {
	// Reconcile runs on every watch event. Re-recording an unchanged
	// origin would rewrite originChangedAt and make a months-old origin
	// look like it moved seconds ago.
	f := flowWithOrigin("f1", "n1")
	r, rec := newFlowReconciler(t, f)
	require.NoError(t, r.recordOriginMove(context.Background(), f))
	<-rec.Events

	var first mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &first))
	stamp := first.Status.OriginChangedAt

	require.NoError(t, r.recordOriginMove(context.Background(), &first))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &got))
	assert.True(t, got.Status.OriginChangedAt.Equal(stamp),
		"an unchanged origin must not restamp the move time")
	select {
	case ev := <-rec.Events:
		t.Fatalf("an unchanged origin must raise no event, got %q", ev)
	default:
	}
}

func TestRecordOriginMove_NilRecorderIsUsable(t *testing.T) {
	f := flowWithOrigin("f1", "n1")
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(f).Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	require.NoError(t, r.recordOriginMove(context.Background(), f))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "f1"}, &got))
	assert.Equal(t, "n1", got.Status.OriginNode,
		"the record is written whether or not events can be published")
}
