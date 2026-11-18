package flowpublisher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func localPhase(t *testing.T, flow *mxlv1alpha1.MxlFlow, node string) mxlv1alpha1.MxlFlowLocationPhase {
	t.Helper()
	for _, loc := range flow.Status.Locations {
		if loc.NodeName == node {
			return loc.Phase
		}
	}
	return ""
}

// The deadlock this exists for: a producer rescheduled onto a node
// that already mirrors the same flow finds the directory in place, so
// no rename reaches fanotify and the node stays Ready. Once the old
// origin is pruned the flow has no Origin anywhere and nothing
// recovers.
func TestClaimOrigin_PromotesReadyToOrigin(t *testing.T) {
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{
				NodeName:     "n1",
				Phase:        mxlv1alpha1.MxlFlowLocationReady,
				LastObserved: &metav1.Time{Time: metav1.Now().Time},
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()

	lease := &fakeLease{}
	p := &Publisher{Client: c, WriterAttached: writerAttached, NodeName: "n1", Lease: lease}
	require.NoError(t, p.ClaimOrigin(context.Background(), validFlowID))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: validFlowID}, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, localPhase(t, &got, "n1"),
		"a producer attaching to an existing flow is the only positive "+
			"evidence that this node owns it")
	assert.Equal(t, []string{validFlowID}, lease.renewed,
		"the Lease has to follow the claim or consumers read the brand-new "+
			"Origin as stale")
}

// The shim de-duplicates per flow on a best-effort basis, so repeats
// reach here and must not churn the status or the Lease.
func TestClaimOrigin_AlreadyOriginIsNoOp(t *testing.T) {
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{
				NodeName:     "n1",
				Phase:        mxlv1alpha1.MxlFlowLocationOrigin,
				LastObserved: &metav1.Time{Time: metav1.Now().Time},
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()

	lease := &fakeLease{}
	p := &Publisher{Client: c, WriterAttached: writerAttached, NodeName: "n1", Lease: lease}
	require.NoError(t, p.ClaimOrigin(context.Background(), validFlowID))
	assert.Empty(t, lease.renewed)
}

// Another node's copy is not this node's to relabel.
func TestClaimOrigin_LeavesOtherNodesAlone(t *testing.T) {
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{
				NodeName:     "n0",
				Phase:        mxlv1alpha1.MxlFlowLocationReady,
				LastObserved: &metav1.Time{Time: metav1.Now().Time},
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()

	p := &Publisher{Client: c, WriterAttached: writerAttached, NodeName: "n1"}
	require.NoError(t, p.ClaimOrigin(context.Background(), validFlowID))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: validFlowID}, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationReady, localPhase(t, &got, "n0"))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, localPhase(t, &got, "n1"))
}

// A flow the API server has not seen yet is left to the fanotify
// pass, which creates it and publishes the location.
func TestClaimOrigin_UnknownFlowIsSkipped(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	p := &Publisher{Client: c, WriterAttached: writerAttached, NodeName: "n1"}

	require.NoError(t, p.ClaimOrigin(context.Background(), validFlowID))

	var got mxlv1alpha1.MxlFlow
	err := c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got)
	assert.Error(t, err, "ClaimOrigin must not conjure an MxlFlow it never saw")
}

// A producer attaching to the directory of a flow whose id a redundancy
// set has taken over, before the old MxlFlow is collected, is no Origin:
// claiming one would make the operator refuse the set again, after the
// readers' mirrors of the old flow were collected for it.
func TestClaimOrigin_RedundancySetOutputIsNotClaimed(t *testing.T) {
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()
	lease := &fakeLease{}
	p := &Publisher{Client: c, WriterAttached: writerAttached, NodeName: "n1", Lease: lease,
		IsOutput: func(context.Context, string, string) (bool, error) { return true, nil }}

	require.NoError(t, p.ClaimOrigin(context.Background(), validFlowID))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, localPhase(t, &got, "n1"))
	assert.Empty(t, lease.renewed)
}

// A set lookup that fails once does not cost a real producer its Origin
// until the next rescan: ClaimOrigin asks again. Claiming on a failed
// lookup instead would take the id of a set's output.
func TestClaimOrigin_RetriesAFailedOutputLookup(t *testing.T) {
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(flow).Build()
	calls := 0
	p := &Publisher{Client: c, WriterAttached: writerAttached, NodeName: "n1", Lease: &fakeLease{},
		IsOutput: func(context.Context, string, string) (bool, error) {
			calls++
			if calls == 1 {
				return false, errors.New("apiserver unreachable")
			}
			return false, nil
		}}

	require.NoError(t, p.ClaimOrigin(context.Background(), validFlowID))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, localPhase(t, &got, "n1"))
	assert.Equal(t, 2, calls)
}
