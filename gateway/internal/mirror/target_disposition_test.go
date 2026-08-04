package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// libmxl offers a flow for deletion when the writer being released is
// its last holder, and performs that deletion by path. A mirror whose
// flow has since been taken over by a local producer must therefore be
// given up without offering it, or the producer loses its flow.

func TestLocalFlowDisposition(t *testing.T) {
	const node = "node-a"
	const flowID = "11111111-2222-3333-4444-555555555555"

	tests := []struct {
		name string
		locs []mxlv1alpha1.MxlFlowLocation
		want flowDisposition
	}{
		{
			name: "origin on this node",
			locs: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
			want: keepFlow,
		},
		{
			name: "origin elsewhere",
			locs: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
				{NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationReady},
			},
			want: dropFlow,
		},
		{
			name: "this node only holds the mirror copy",
			locs: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationReady},
			},
			want: dropFlow,
		},
		{
			name: "stale local location",
			locs: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationStale},
			},
			want: dropFlow,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newSourceTestScheme(t)
			flow := &mxlv1alpha1.MxlFlow{
				ObjectMeta: metav1.ObjectMeta{Name: flowID},
				Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
				Status:     mxlv1alpha1.MxlFlowStatus{Locations: tc.locs},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(flow).Build()
			r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: node}
			assert.Equal(t, tc.want, r.localFlowDisposition(context.Background(), flowID))
		})
	}
}

func TestLocalFlowDisposition_MissingFlowDrops(t *testing.T) {
	// No MxlFlow means no producer published one, so nothing on this
	// node can be claiming the directory the mirror filled.
	scheme := newSourceTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a"}
	assert.Equal(t, dropFlow,
		r.localFlowDisposition(context.Background(), "11111111-2222-3333-4444-555555555555"))
}
