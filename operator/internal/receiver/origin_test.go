package receiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// fakeLeaseChecker is the same minimal stub used in the dispatcher
// tests, redeclared here so the operator suite can stay independent
// of the agent module.
type fakeLeaseChecker struct {
	fresh map[string]bool
}

func (f *fakeLeaseChecker) IsFresh(_ context.Context, flowID, node string) (bool, time.Time, error) {
	v, ok := f.fresh[flowID+"/"+node]
	if !ok || !v {
		return false, time.Time{}, nil
	}
	return true, time.Now().Add(30 * time.Second), nil
}

func TestResolveSourceNode_SkipsStaleOriginByLease(t *testing.T) {
	ctx := context.Background()
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-a"},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: "flow-a"},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-stale", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
				{NodeName: "n-fresh", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()
	r := &Reconciler{
		Client: c,
		Lease: &fakeLeaseChecker{fresh: map[string]bool{
			"flow-a/n-fresh": true,
		}},
	}

	res, err := r.resolveSourceNode(ctx, "flow-a")
	require.NoError(t, err)
	assert.True(t, res.Found,
		"the receiver must skip a stale Origin location and pick the next "+
			"fresh one; otherwise a crashed agent's stuck Origin would "+
			"permanently steal the Mirror source assignment")
	assert.Equal(t, "n-fresh", res.Node)
	assert.False(t, res.AllStale)
}

func TestResolveSourceNode_AllStaleRaisesAllStale(t *testing.T) {
	ctx := context.Background()
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-a"},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: "flow-a"},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-stale-1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
				{NodeName: "n-stale-2", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()
	r := &Reconciler{
		Client: c,
		Lease:  &fakeLeaseChecker{fresh: map[string]bool{}},
	}

	res, err := r.resolveSourceNode(ctx, "flow-a")
	require.NoError(t, err)
	assert.False(t, res.Found)
	assert.True(t, res.AllStale,
		"every Origin candidate was rejected by the LeaseChecker, so the "+
			"caller must learn AllStale=true and surface "+
			"OriginFresh=False on the MxlFlow")
}

func TestResolveSourceNode_NoLeaseCheckerKeepsLegacyBehavior(t *testing.T) {
	ctx := context.Background()
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-a"},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: "flow-a"},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-origin", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()
	r := &Reconciler{Client: c} // Lease nil on purpose.

	res, err := r.resolveSourceNode(ctx, "flow-a")
	require.NoError(t, err)
	assert.True(t, res.Found,
		"with Lease nil the reconciler must keep returning the first "+
			"Origin; otherwise an operator rollout without RBAC for "+
			"Leases would break every receiver")
	assert.Equal(t, "n-origin", res.Node)
}
