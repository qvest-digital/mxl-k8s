package flowpublisher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func flowWithLocations(locs ...mxlv1alpha1.MxlFlowLocation) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status:     mxlv1alpha1.MxlFlowStatus{Locations: locs},
	}
}

// The Lease is what tells a consumer the flow's producer is alive.
// Renewing it from every node holding a copy makes it mean "some node
// has this flow on disk", which every mirror target satisfies too,
// leaving nothing to distinguish a live producer from a mirrored copy
// of a dead one.
func TestLocalOrigins_OnlyOriginPhaseCounts(t *testing.T) {
	scheme := newScheme(t)

	cases := []struct {
		name  string
		phase mxlv1alpha1.MxlFlowLocationPhase
		want  bool
	}{
		{"origin renews", mxlv1alpha1.MxlFlowLocationOrigin, true},
		{"mirror target does not", mxlv1alpha1.MxlFlowLocationReady, false},
		{"stale does not", mxlv1alpha1.MxlFlowLocationStale, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
				WithObjects(flowWithLocations(
					mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: tc.phase})).
				Build()

			p := &Publisher{Client: c, NodeName: "n1"}
			got, err := p.localOrigins(context.Background())
			require.NoError(t, err)

			_, ok := got[validFlowID]
			assert.Equal(t, tc.want, ok)
		})
	}
}

// Another node's Origin says nothing about this one; the loop must
// key on this node's own entry.
func TestLocalOrigins_IgnoresOtherNodesOrigin(t *testing.T) {
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flowWithLocations(
			mxlv1alpha1.MxlFlowLocation{NodeName: "n0", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			mxlv1alpha1.MxlFlowLocation{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationReady},
		)).
		Build()

	p := &Publisher{Client: c, NodeName: "n1"}
	got, err := p.localOrigins(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
}

// A flow the API server has not seen yet is not this node's Origin to
// renew; PublishAppeared creates it and renews on the same pass.
func TestLocalOrigins_UnknownFlowIsNotOrigin(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	p := &Publisher{Client: c, NodeName: "n1"}

	got, err := p.localOrigins(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
}
