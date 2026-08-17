package topology

import (
	"strings"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func flow(name, id string, locs ...mxlv1alpha1.MxlFlowLocation) *mxlv1alpha1.MxlFlow {
	f := &mxlv1alpha1.MxlFlow{}
	f.Name = name
	f.Spec.ID = id
	f.Status.Locations = locs
	return f
}

func newCollector(t *testing.T, node string, objs ...*mxlv1alpha1.MxlFlow) *Collector {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, mxlv1alpha1.AddToScheme(scheme))
	b := fake.NewClientBuilder().WithScheme(scheme)
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return New(b.Build(), node, testr.New(t))
}

// The producing node is the one whose location phase is Origin. That is
// the whole point of this collector: without it a shared node-wide
// domain gives no way to tell a written flow from a mirrored one.
func TestCollectReportsOriginForTheLocalNode(t *testing.T) {
	c := newCollector(t, "node-a",
		flow("video", "5fbec3b1-1b0f-417d-9059-8b94a47197ed",
			mxlv1alpha1.MxlFlowLocation{NodeName: "node-a", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationReady},
		),
	)

	expected := `
# HELP mxl_flow_location_info 1 for the phase this flow is in on this node. Phase is Origin on the node the writer runs on.
# TYPE mxl_flow_location_info gauge
mxl_flow_location_info{flow_id="5fbec3b1-1b0f-417d-9059-8b94a47197ed",phase="Origin"} 1
`
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(expected), "mxl_flow_location_info"))
}

// The same flow collected on the consuming node reports Ready, not
// Origin, so a join on (flow_id, node) separates the two sides.
func TestCollectReportsReadyOnTheConsumingNode(t *testing.T) {
	c := newCollector(t, "node-b",
		flow("video", "5fbec3b1-1b0f-417d-9059-8b94a47197ed",
			mxlv1alpha1.MxlFlowLocation{NodeName: "node-a", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationReady},
		),
	)

	expected := `
# HELP mxl_flow_location_info 1 for the phase this flow is in on this node. Phase is Origin on the node the writer runs on.
# TYPE mxl_flow_location_info gauge
mxl_flow_location_info{flow_id="5fbec3b1-1b0f-417d-9059-8b94a47197ed",phase="Ready"} 1
`
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(expected), "mxl_flow_location_info"))
}

// A node holding no location for a flow emits nothing for it. The CRs
// are cluster-scoped, so without the filter every node's exporter would
// publish the whole cluster's flows as duplicate series.
func TestCollectSkipsFlowsNotOnThisNode(t *testing.T) {
	c := newCollector(t, "node-c",
		flow("video", "5fbec3b1-1b0f-417d-9059-8b94a47197ed",
			mxlv1alpha1.MxlFlowLocation{NodeName: "node-a", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
		),
	)

	require.Equal(t, 0, testutil.CollectAndCount(c, "mxl_flow_location_info"))
}
