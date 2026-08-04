package flow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A flow the agent demoted to Stale describes nothing: no node holds a
// copy, so resolveSourceNode cannot answer from it and no consumer can
// be routed to it. Nothing removed those before, so the flow list grew
// for the life of the cluster.

type fakeLease struct {
	fresh    map[string]bool
	deadline time.Time
	err      error
}

func (f *fakeLease) IsFresh(_ context.Context, flowID, nodeName string) (bool, time.Time, error) {
	if f.err != nil {
		return false, time.Time{}, f.err
	}
	return f.fresh[flowID+"/"+nodeName], f.deadline, nil
}

func staleFlow(locs ...mxlv1alpha1.MxlFlowLocation) *mxlv1alpha1.MxlFlow {
	return newFlow(locs...)
}

func mirrorFor(flowID string) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns1"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID: flowID, SourceNode: "n1", TargetNode: "n2",
		},
	}
}

func TestCollect_DeletesFlowWithNoCopyAndNoMirror(t *testing.T) {
	scheme := newScheme(t)
	flow := staleFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
		mxlv1alpha1.MxlFlowLocation{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationStale},
	)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("n1"), node("n2")).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme, Lease: &fakeLease{}, GracePeriod: time.Hour}
	key := types.NamespacedName{Name: flow.Name}
	req := ctrl.Request{NamespacedName: key}

	// First sighting only starts the clock: a producer rolling over
	// looks exactly like this between releasing and republishing.
	res, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "the grace period has to be waited out")
	require.NoError(t, c.Get(context.Background(), key, &mxlv1alpha1.MxlFlow{}),
		"the flow must survive its first collectable sighting")

	// Reaching back past the grace period stands in for waiting it out.
	r.mu.Lock()
	r.firstCollectable[flow.Name] = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	err = c.Get(context.Background(), key, &mxlv1alpha1.MxlFlow{})
	assert.True(t, apierrors.IsNotFound(err), "want the flow collected, got %v", err)
}

func TestCollect_KeepsFlowWithLiveOrigin(t *testing.T) {
	scheme := newScheme(t)
	flow := staleFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
		mxlv1alpha1.MxlFlowLocation{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationStale},
	)
	deadline := time.Now().Add(30 * time.Second)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("n1"), node("n2")).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme, GracePeriod: time.Nanosecond, Lease: &fakeLease{
		fresh:    map[string]bool{flow.Spec.ID + "/n1": true},
		deadline: deadline,
	}}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: flow.Name}})
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: flow.Name}, &mxlv1alpha1.MxlFlow{}))
	assert.Positive(t, res.RequeueAfter,
		"a flow held alive by a Lease has to be re-examined when it lapses")
}

func TestCollect_KeepsFlowWithStaleOriginButLiveMirror(t *testing.T) {
	// The target gateway reads spec.definition to open its writer, so
	// deleting a mirrored flow would break the consumer the mirror
	// exists for, producer or no producer.
	scheme := newScheme(t)
	flow := staleFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
	)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("n1"), mirrorFor(flow.Spec.ID)).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme, Lease: &fakeLease{}, GracePeriod: time.Nanosecond}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: flow.Name}})
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: flow.Name}, &mxlv1alpha1.MxlFlow{}))
}

func TestCollect_KeepsFlowWithReadyLocation(t *testing.T) {
	scheme := newScheme(t)
	flow := staleFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
		mxlv1alpha1.MxlFlowLocation{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationReady},
	)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("n1"), node("n2")).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme, Lease: &fakeLease{}, GracePeriod: time.Nanosecond}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: flow.Name}})
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: flow.Name}, &mxlv1alpha1.MxlFlow{}))
}

func TestCollect_ClockRestartsWhenTheFlowComesBack(t *testing.T) {
	// A producer that returns inside the grace period must not be
	// judged on the window it was away for.
	scheme := newScheme(t)
	flow := staleFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
	)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("n1")).
		Build()

	lease := &fakeLease{}
	r := &Reconciler{Client: c, Scheme: scheme, Lease: lease, GracePeriod: time.Hour}
	key := types.NamespacedName{Name: flow.Name}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	r.mu.Lock()
	_, marked := r.firstCollectable[flow.Name]
	r.mu.Unlock()
	require.True(t, marked)

	var live mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), key, &live))
	live.Status.Locations = []mxlv1alpha1.MxlFlowLocation{{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	}}
	require.NoError(t, c.Status().Update(context.Background(), &live))
	lease.fresh = map[string]bool{flow.Spec.ID + "/n1": true}
	lease.deadline = time.Now().Add(30 * time.Second)

	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	r.mu.Lock()
	_, stillMarked := r.firstCollectable[flow.Name]
	r.mu.Unlock()
	assert.False(t, stillMarked, "a flow that came back must lose its collectable mark")
}

func TestMirrorToFlow(t *testing.T) {
	got := mirrorToFlow(context.Background(), mirrorFor("flow-1"))
	require.Len(t, got, 1)
	assert.Equal(t, "flow-1", got[0].Name)
	assert.Empty(t, mirrorToFlow(context.Background(), node("n1")))
}

func TestCollect_StaleReadDoesNotDeleteARepublishedFlow(t *testing.T) {
	// The grace period is decided against a read of the flow; an agent
	// republishing between that read and the delete must keep its
	// flow.
	scheme := newScheme(t)
	flow := staleFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
	)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow.DeepCopy(), node("n1")).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme, Lease: &fakeLease{}, GracePeriod: time.Hour}
	key := types.NamespacedName{Name: flow.Name}

	var read mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), key, &read))
	r.mu.Lock()
	r.firstCollectable = map[string]time.Time{flow.Name: time.Now().Add(-2 * time.Hour)}
	r.mu.Unlock()

	// Republish under the collector's feet.
	var live mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), key, &live))
	live.Status.Locations = []mxlv1alpha1.MxlFlowLocation{{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	}}
	require.NoError(t, c.Status().Update(context.Background(), &live))

	_, err := r.collect(context.Background(), &read)
	require.Error(t, err, "a delete against a superseded version must not succeed")
	require.NoError(t, c.Get(context.Background(), key, &mxlv1alpha1.MxlFlow{}))
}
