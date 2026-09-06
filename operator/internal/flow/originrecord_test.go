package flow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// status.originNode has to equal the locations list. It used to be a
// remembered value that only ever moved forward, so a flow whose
// Origin location had been pruned kept naming the node it was pruned
// for -- on a dev cluster, seven flows named a node that had left
// hours earlier, which is the first thing anyone reads when asking
// where a flow lives.

func withOrigin(node string) *mxlv1alpha1.MxlFlow {
	return newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationOrigin,
	})
}

func TestOrigin_FirstClaimIsRecordedWithoutAPrevious(t *testing.T) {
	flow := withOrigin("n1")
	lease := &fakeLease{fresh: map[string]bool{flow.Spec.ID + "/n1": true}}
	r, c := collector(t, time.Hour, lease, flow.DeepCopy())

	runReconcile(t, r, flow.Name)

	got := getFlow(t, c, flow.Name)
	assert.Equal(t, "n1", got.Status.OriginNode)
	assert.Empty(t, got.Status.PreviousOriginNode)
	assert.NotNil(t, got.Status.OriginChangedAt)
}

func TestOrigin_MoveRecordsBothEnds(t *testing.T) {
	flow := withOrigin("n1")
	lease := &fakeLease{fresh: map[string]bool{flow.Spec.ID + "/n1": true}}
	r, c := collector(t, time.Hour, lease, flow.DeepCopy())
	runReconcile(t, r, flow.Name)

	live := getFlow(t, c, flow.Name)
	live.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale},
		{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	require.NoError(t, c.Status().Update(t.Context(), live))
	lease.fresh = map[string]bool{flow.Spec.ID + "/n2": true}

	runReconcile(t, r, flow.Name)

	got := getFlow(t, c, flow.Name)
	assert.Equal(t, "n2", got.Status.OriginNode)
	assert.Equal(t, "n1", got.Status.PreviousOriginNode,
		"which node the flow left is the half worth knowing: it is the one "+
			"every mirror is still sourcing from")
}

// An origin disappearing empties the field. Leaving the last known
// node behind is what made status.originNode unreadable: it said a
// node held the flow while the locations list said nobody did, and the
// two disagreed for as long as the flow existed.
func TestOrigin_LossEmptiesOriginNodeAndKeepsThePrevious(t *testing.T) {
	flow := withOrigin("n1")
	lease := &fakeLease{fresh: map[string]bool{flow.Spec.ID + "/n1": true}}
	r, c := collector(t, time.Hour, lease, flow.DeepCopy())
	runReconcile(t, r, flow.Name)

	live := getFlow(t, c, flow.Name)
	live.Status.Locations[0].Phase = mxlv1alpha1.MxlFlowLocationStale
	require.NoError(t, c.Status().Update(t.Context(), live))
	lease.fresh = map[string]bool{}

	runReconcile(t, r, flow.Name)

	got := getFlow(t, c, flow.Name)
	assert.Empty(t, got.Status.OriginNode,
		"no location claims Origin, so neither may the field that mirrors "+
			"them; a consumer reading it would be sent at a node that has "+
			"already released the flow")
	assert.Equal(t, "n1", got.Status.PreviousOriginNode)
}

// Pruning a departed node's location has to take originNode with it.
// This is the dev-cluster case: the node is gone, the entry naming it
// is gone, and the field kept pointing at it.
func TestOrigin_PrunedDepartedOriginClearsOriginNode(t *testing.T) {
	flow := newFlow(
		mxlv1alpha1.MxlFlowLocation{NodeName: "gone", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
		mxlv1alpha1.MxlFlowLocation{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationReady},
	)
	flow.Status.OriginNode = "gone"
	lease := &fakeLease{fresh: map[string]bool{flow.Spec.ID + "/gone": true}}
	// collector only creates n1 and n2, so "gone" has no Node object.
	r, c := collector(t, time.Hour, lease, flow.DeepCopy())

	runReconcile(t, r, flow.Name)

	got := getFlow(t, c, flow.Name)
	require.Len(t, got.Status.Locations, 1)
	assert.Equal(t, "n2", got.Status.Locations[0].NodeName)
	assert.Empty(t, got.Status.OriginNode)
	assert.Equal(t, "gone", got.Status.PreviousOriginNode)
	cond := meta.FindStatusCondition(got.Status.Conditions, mxlv1alpha1.ConditionTypeLive)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status,
		"the departed node's Origin must not count as live for one more "+
			"pass; it is judged against the locations that survive the prune")
}

func TestOriginFresh_PublishedOnlyWhenAnOriginIsClaimed(t *testing.T) {
	t.Run("no origin at all leaves the condition alone", func(t *testing.T) {
		flow := newFlow(mxlv1alpha1.MxlFlowLocation{
			NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale,
		})
		r, c := collector(t, time.Hour, &fakeLease{}, flow.DeepCopy())
		runReconcile(t, r, flow.Name)

		assert.Nil(t, meta.FindStatusCondition(
			getFlow(t, c, flow.Name).Status.Conditions, mxlv1alpha1.ConditionTypeOriginFresh),
			"a producer that has not published yet is not a producer the "+
				"cluster has lost; stamping False on every such flow would "+
				"drown the genuine lease-expired signal")
	})

	t.Run("a claimed origin with an expired lease is False", func(t *testing.T) {
		flow := withOrigin("n1")
		r, c := collector(t, time.Hour, &fakeLease{}, flow.DeepCopy())
		runReconcile(t, r, flow.Name)

		cond := meta.FindStatusCondition(
			getFlow(t, c, flow.Name).Status.Conditions, mxlv1alpha1.ConditionTypeOriginFresh)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionFalse, cond.Status)
		assert.Equal(t, mxlv1alpha1.ReasonLeaseExpired, cond.Reason)
	})

	t.Run("a renewed lease is True", func(t *testing.T) {
		flow := withOrigin("n1")
		lease := &fakeLease{fresh: map[string]bool{flow.Spec.ID + "/n1": true}}
		r, c := collector(t, time.Hour, lease, flow.DeepCopy())
		runReconcile(t, r, flow.Name)

		cond := meta.FindStatusCondition(
			getFlow(t, c, flow.Name).Status.Conditions, mxlv1alpha1.ConditionTypeOriginFresh)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
	})
}

// The transition timestamp is the collector's clock, so a pass that
// changes nothing must not move it. Restamping on every reconcile
// would reset the grace period forever and no flow would ever be
// collected.
func TestLiveCondition_SteadyStateDoesNotRestampTheClock(t *testing.T) {
	flow := newFlow(mxlv1alpha1.MxlFlowLocation{
		NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale,
	})
	r, c := collector(t, time.Hour, &fakeLease{}, flow.DeepCopy())

	runReconcile(t, r, flow.Name)
	first := meta.FindStatusCondition(getFlow(t, c, flow.Name).Status.Conditions,
		mxlv1alpha1.ConditionTypeLive).LastTransitionTime

	backdate(t, c, flow.Name, 30*time.Minute)
	before := meta.FindStatusCondition(getFlow(t, c, flow.Name).Status.Conditions,
		mxlv1alpha1.ConditionTypeLive).LastTransitionTime
	runReconcile(t, r, flow.Name)
	after := meta.FindStatusCondition(getFlow(t, c, flow.Name).Status.Conditions,
		mxlv1alpha1.ConditionTypeLive).LastTransitionTime

	assert.True(t, before.Equal(&after),
		"the condition still reads False for the same reason, so the "+
			"instant it turned has not changed")
	assert.False(t, first.Equal(&after))
}
