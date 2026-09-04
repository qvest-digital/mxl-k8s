package mirror

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/go-mxl/fabrics"
)

func TestThroughputCollectorReportsBothDirections(t *testing.T) {
	// The counters are per flow so a bottleneck can be attributed to
	// one stream, and carry the transport so the same series answers
	// "what is this node pushing over verbs" by aggregation rather
	// than by a second metric.
	src := &SourceReconciler{sources: map[types.NamespacedName]*sourceEntry{}}
	sent := &sourceEntry{shared: testShared("flow-a", fabrics.ProviderVerbs), peerNode: "n02"}
	sent.bytes.Store(5529600)
	src.sources[types.NamespacedName{Namespace: "p", Name: "a"}] = sent

	tgt := &TargetReconciler{targets: map[types.NamespacedName]*targetEntry{}}
	got := &targetEntry{flowID: "flow-b", peerNode: "n03", provider: fabrics.ProviderTCP}
	got.bytes.Store(4096)
	tgt.targets[types.NamespacedName{Namespace: "p", Name: "b"}] = got

	c := &ThroughputCollector{NodeName: "n01", Source: src, Target: tgt}

	expected := `
# HELP mxl_gateway_mirror_received_bytes_total Payload bytes committed to the local flow for a mirror this node is the target of.
# TYPE mxl_gateway_mirror_received_bytes_total counter
mxl_gateway_mirror_received_bytes_total{flow_id="flow-b",node="n01",peer_node="n03",provider="tcp"} 4096
# HELP mxl_gateway_mirror_transmitted_bytes_total Payload bytes handed to the fabric for a mirror this node is the source of.
# TYPE mxl_gateway_mirror_transmitted_bytes_total counter
mxl_gateway_mirror_transmitted_bytes_total{flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 5529600
`
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(expected)))
}

func TestThroughputCollectorForgetsClosedMirrors(t *testing.T) {
	// Collecting from the live entry maps is what makes a torn-down
	// mirror stop being reported. A counter registered per flow would
	// keep exporting its last value for as long as the process runs,
	// and a stale series reads as a flow that stopped moving rather
	// than one that is gone.
	src := &SourceReconciler{sources: map[types.NamespacedName]*sourceEntry{}}
	key := types.NamespacedName{Namespace: "p", Name: "a"}
	e := &sourceEntry{shared: testShared("flow-a", fabrics.ProviderVerbs), peerNode: "n02"}
	e.bytes.Store(1024)
	src.sources[key] = e

	c := &ThroughputCollector{NodeName: "n01", Source: src}
	require.Equal(t, 1, testutil.CollectAndCount(c))

	delete(src.sources, key)
	require.Equal(t, 0, testutil.CollectAndCount(c),
		"a mirror that has been closed must leave no series behind")
}

func TestThroughputCollectorSurvivesOneFlowMirroredTwice(t *testing.T) {
	// A node mirrors one flow to several peers, so its source
	// reconciler holds several entries carrying the same flow id. Two
	// series with one label set fail the whole gather with a 500,
	// which takes every other metric on the endpoint down with it --
	// observed on sc, where three of five gateways stopped being
	// scraped at all.
	src := &SourceReconciler{sources: map[types.NamespacedName]*sourceEntry{}}
	for _, peer := range []string{"n02", "n03"} {
		e := &sourceEntry{shared: testShared("flow-a", fabrics.ProviderVerbs), peerNode: peer}
		e.bytes.Store(1000)
		src.sources[types.NamespacedName{Namespace: "p", Name: "flow-a--" + peer}] = e
	}
	c := &ThroughputCollector{NodeName: "n01", Source: src}
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(`
# HELP mxl_gateway_mirror_transmitted_bytes_total Payload bytes handed to the fabric for a mirror this node is the source of.
# TYPE mxl_gateway_mirror_transmitted_bytes_total counter
mxl_gateway_mirror_transmitted_bytes_total{flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 1000
mxl_gateway_mirror_transmitted_bytes_total{flow_id="flow-a",node="n01",peer_node="n03",provider="verbs"} 1000
`)), "one flow mirrored to two peers must yield two distinct series")

	// Same flow and same peer in two namespaces cannot be separated by
	// labels at all, so the fold has to carry it.
	dup := &sourceEntry{shared: testShared("flow-a", fabrics.ProviderVerbs), peerNode: "n02"}
	dup.bytes.Store(500)
	src.sources[types.NamespacedName{Namespace: "other", Name: "flow-a--n02"}] = dup
	require.Equal(t, 2, testutil.CollectAndCount(c),
		"a label set that repeats must fold into one series, never a duplicate")
}
