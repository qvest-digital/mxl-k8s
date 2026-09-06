package flow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A flow nothing holds a copy of describes nothing: no consumer can be
// routed to it and no producer will publish to it again. Nothing
// removed those before, so the flow list grew for the life of the
// cluster and every entry in it was a name the origin resolver walked
// and a condition the operator kept evaluating.

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

func mirrorFor(flowID string) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns1"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID: flowID, SourceNode: "n1", TargetNode: "n2",
		},
	}
}

// collector builds a reconciler over the given objects, with every
// node the locations name present so the prune has nothing to do.
func collector(t *testing.T, grace time.Duration, lease LeaseChecker, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	b := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(node("n1"), node("n2"))
	b = b.WithObjects(objs...)
	c := b.Build()
	return &Reconciler{
		Client:      c,
		Recorder:    record.NewFakeRecorder(16),
		Lease:       lease,
		GracePeriod: grace,
	}, c
}

func runReconcile(t *testing.T, r *Reconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	return res
}

func getFlow(t *testing.T, c client.Client, name string) *mxlv1alpha1.MxlFlow {
	t.Helper()
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: name}, &f))
	return &f
}

func flowGone(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var f mxlv1alpha1.MxlFlow
	err := c.Get(context.Background(), types.NamespacedName{Name: name}, &f)
	return apierrors.IsNotFound(err)
}

// backdate rewrites the Live condition's transition time so a test can
// reach the far side of the grace period without sleeping through it.
// That the clock lives on the object rather than in the operator's
// memory is the point: it is what makes the grace survive a restart.
func backdate(t *testing.T, c client.Client, name string, by time.Duration) {
	t.Helper()
	f := getFlow(t, c, name)
	for i := range f.Status.Conditions {
		if f.Status.Conditions[i].Type == mxlv1alpha1.ConditionTypeLive {
			f.Status.Conditions[i].LastTransitionTime =
				metav1.NewTime(time.Now().Add(-by))
		}
	}
	require.NoError(t, c.Status().Update(context.Background(), f))
}

func TestCollect_FlowWithNoCopyAndNoMirror_IsDeletedAfterGrace(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale,
	})
	r, c := collector(t, time.Hour, &fakeLease{}, flow.DeepCopy())

	res := runReconcile(t, r, flow.Name)
	assert.False(t, flowGone(t, c, flow.Name),
		"a flow must not be collected on the first pass that finds it "+
			"unreferenced: a producer rolling over has no Origin between "+
			"the old pod releasing the flow and the new one publishing it")
	assert.Positive(t, res.RequeueAfter,
		"nothing raises an event when a grace period elapses, so the pass "+
			"that starts the clock has to book the one that acts on it")

	cond := meta.FindStatusCondition(getFlow(t, c, flow.Name).Status.Conditions,
		mxlv1alpha1.ConditionTypeLive)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonNoLiveCopy, cond.Reason)

	backdate(t, c, flow.Name, 2*time.Hour)
	runReconcile(t, r, flow.Name)
	assert.True(t, flowGone(t, c, flow.Name),
		"once the flow has read Live=False for longer than the grace "+
			"period nothing is coming back for it")
}

func TestCollect_LiveOrigin_IsKept(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	})
	lease := &fakeLease{
		fresh:    map[string]bool{flow.Spec.ID + "/n1": true},
		deadline: time.Now().Add(30 * time.Second),
	}
	r, c := collector(t, time.Nanosecond, lease, flow.DeepCopy())

	res := runReconcile(t, r, flow.Name)
	assert.False(t, flowGone(t, c, flow.Name))
	assert.InDelta(t, 30*time.Second, res.RequeueAfter, float64(2*time.Second),
		"a flow held alive only by a Lease has to be looked at again when "+
			"that Lease lapses; time passing raises no event")

	cond := meta.FindStatusCondition(getFlow(t, c, flow.Name).Status.Conditions,
		mxlv1alpha1.ConditionTypeLive)
	require.NotNil(t, cond)
	assert.Equal(t, mxlv1alpha1.ReasonOriginLive, cond.Reason)
}

// The producer is gone but a mirror still carries the flow to a
// consumer. Collecting the flow would take the definition away from
// the gateway that is still serving it.
func TestCollect_StaleOriginButLiveMirror_IsKept(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale,
	})
	r, c := collector(t, time.Nanosecond, &fakeLease{},
		flow.DeepCopy(), mirrorFor(flow.Spec.ID))

	runReconcile(t, r, flow.Name)
	assert.False(t, flowGone(t, c, flow.Name))

	cond := meta.FindStatusCondition(getFlow(t, c, flow.Name).Status.Conditions,
		mxlv1alpha1.ConditionTypeLive)
	require.NotNil(t, cond)
	assert.Equal(t, mxlv1alpha1.ReasonMirrored, cond.Reason)
}

// The cycle this replaces. A Ready location is written by the node
// holding a mirror's target copy and cleared a domain sweep after the
// mirror is torn down -- and never while the mirror survives. Counting
// it as evidence meant the flow cited the copy and the mirror cited
// the flow, so neither was collected. The mirror is the only claim
// that counts, and here there is none.
func TestCollect_ReadyLocationAlone_DoesNotKeepTheFlow(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationReady,
	})
	r, c := collector(t, time.Hour, &fakeLease{}, flow.DeepCopy())

	runReconcile(t, r, flow.Name)
	cond := meta.FindStatusCondition(getFlow(t, c, flow.Name).Status.Conditions,
		mxlv1alpha1.ConditionTypeLive)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonNoLiveCopy, cond.Reason)

	backdate(t, c, flow.Name, 2*time.Hour)
	runReconcile(t, r, flow.Name)
	assert.True(t, flowGone(t, c, flow.Name),
		"a mirror target's copy is derived from the mirror; with no mirror "+
			"left to justify it, it keeps nothing alive")
}

// A producer that comes back mid-grace restarts the clock, because the
// condition transitions back to True and its timestamp moves with it.
func TestCollect_ClockRestartsWhenTheFlowComesBack(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale,
	})
	lease := &fakeLease{fresh: map[string]bool{}}
	r, c := collector(t, time.Hour, lease, flow.DeepCopy())

	runReconcile(t, r, flow.Name)
	backdate(t, c, flow.Name, 59*time.Minute)

	// The producer republishes just short of the deadline.
	live := getFlow(t, c, flow.Name)
	live.Status.Locations[0].Phase = mxlv1alpha1.MxlFlowLocationOrigin
	require.NoError(t, c.Status().Update(context.Background(), live))
	lease.fresh[flow.Spec.ID+"/n1"] = true
	runReconcile(t, r, flow.Name)

	// And goes away again. The wait starts over rather than resuming.
	live = getFlow(t, c, flow.Name)
	live.Status.Locations[0].Phase = mxlv1alpha1.MxlFlowLocationStale
	require.NoError(t, c.Status().Update(context.Background(), live))
	delete(lease.fresh, flow.Spec.ID+"/n1")
	runReconcile(t, r, flow.Name)

	assert.False(t, flowGone(t, c, flow.Name),
		"the grace measures how long the flow has been unreferenced, not "+
			"how long ago it first looked that way")
}

// The delete is preconditioned on the resourceVersion the decision was
// made against, so an agent republishing between the read and the
// delete keeps its flow.
func TestCollect_StaleReadDoesNotDeleteARepublishedFlow(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale,
	})
	r, c := collector(t, time.Hour, &fakeLease{}, flow.DeepCopy())
	runReconcile(t, r, flow.Name)
	backdate(t, c, flow.Name, 2*time.Hour)

	// The decision is taken against this version of the object.
	stale := getFlow(t, c, flow.Name)

	// An agent republishes the flow in the meantime, moving the
	// resourceVersion on.
	live := getFlow(t, c, flow.Name)
	live.Status.Locations = append(live.Status.Locations, mxlv1alpha1.MxlFlowLocation{
		NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	})
	require.NoError(t, c.Status().Update(context.Background(), live))

	res, err := r.collect(context.Background(), stale,
		verdict{Reason: mxlv1alpha1.ReasonNoLiveCopy})
	require.NoError(t, err)
	assert.True(t, res.Requeue,
		"the conflict has to come back to the queue so the flow is judged "+
			"again against what it now says")
	assert.False(t, flowGone(t, c, flow.Name),
		"deleting on a stale read would drop a flow that had just come back")
}

func TestCollect_LeaseErrorIsSurfaced(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	})
	r, c := collector(t, time.Nanosecond, &fakeLease{err: assert.AnError}, flow.DeepCopy())

	_, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: flow.Name}})
	require.Error(t, err,
		"a Lease read that failed says nothing about the producer; treating "+
			"it as absent would collect flows on an API-server hiccup")
	assert.False(t, flowGone(t, c, flow.Name))
}

func TestMirrorToFlow(t *testing.T) {
	reqs := mirrorToFlow(context.Background(), mirrorFor("flow-1"))
	require.Len(t, reqs, 1)
	assert.Equal(t, "flow-1", reqs[0].Name)

	assert.Empty(t, mirrorToFlow(context.Background(), node("n1")))
	assert.Empty(t, mirrorToFlow(context.Background(), &mxlv1alpha1.MxlFlowMirror{}),
		"a mirror with no flow id names no flow to enqueue")
}

func TestLeaseToFlow_ParsesTheFlowIDOutOfTheName(t *testing.T) {
	lease := &metav1.PartialObjectMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mxlv1alpha1.LeaseNamespace,
			Name:      mxlv1alpha1.LeaseName("11111111-2222-3333-4444-555555555555", "n1"),
		},
	}
	reqs := leaseToFlow(context.Background(), lease)
	require.Len(t, reqs, 1)
	assert.Equal(t, "11111111-2222-3333-4444-555555555555", reqs[0].Name)

	lease.Name = "kube-controller-manager"
	assert.Empty(t, leaseToFlow(context.Background(), lease),
		"a Lease that is not one of ours names no flow")
}
