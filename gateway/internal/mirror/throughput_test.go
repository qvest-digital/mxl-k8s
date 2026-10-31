package mirror

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
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
mxl_gateway_mirror_received_bytes_total{domain="",flow_id="flow-b",node="n01",peer_node="n03",provider="tcp"} 4096
# HELP mxl_gateway_mirror_transmitted_bytes_total Payload bytes the fabric reported as delivered for a mirror this node is the source of, including chunks of a grain given up part way, which went over the wire all the same.
# TYPE mxl_gateway_mirror_transmitted_bytes_total counter
mxl_gateway_mirror_transmitted_bytes_total{domain="",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 5529600
`
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(expected),
		"mxl_gateway_mirror_received_bytes_total", "mxl_gateway_mirror_transmitted_bytes_total"))
}

// Invalid grains are counted on both halves: the source's what it did
// not send, the target's what it committed invalid, by reason.
func TestThroughputCollectorReportsInvalidGrains(t *testing.T) {
	src := &SourceReconciler{sources: map[types.NamespacedName]*sourceEntry{}}
	sent := &sourceEntry{shared: testShared("flow-a", fabrics.ProviderVerbs), peerNode: "n02"}
	sent.recordInvalid(time.Now())
	sent.recordInvalid(time.Now())
	src.sources[types.NamespacedName{Namespace: "p", Name: "a"}] = sent

	tgt := &TargetReconciler{targets: map[types.NamespacedName]*targetEntry{}}
	got := &targetEntry{flowID: "flow-b", peerNode: "n03", provider: fabrics.ProviderVerbs}
	got.recordInvalid(invalidFromSource)
	got.recordInvalid(invalidIndexMismatch)
	got.recordInvalid(invalidIndexMismatch)
	got.recordInvalid("")
	tgt.targets[types.NamespacedName{Namespace: "p", Name: "b"}] = got

	c := &ThroughputCollector{NodeName: "n01", Source: src, Target: tgt}
	expected := `
# HELP mxl_gateway_mirror_source_unsent_grains_total Grains the source half did not offer to the initiator, by reason: invalid (the producer committed a video grain invalid), resync (the loop fell more than a few grains behind the head and resumed near it) or aged_out (the producer had reused the slot). Each one is a grain no target receives. An invalid data grain is not counted here: its header is offered, and the target commits it invalid.
# TYPE mxl_gateway_mirror_source_unsent_grains_total counter
mxl_gateway_mirror_source_unsent_grains_total{domain="",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs",reason="aged_out"} 0
mxl_gateway_mirror_source_unsent_grains_total{domain="",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs",reason="invalid"} 2
mxl_gateway_mirror_source_unsent_grains_total{domain="",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs",reason="resync"} 0
# HELP mxl_gateway_mirror_target_invalid_grains_total Arrived grains the target half committed invalid: flagged invalid at the source (invalid_from_source), or whose slot header names another index than the one committed, a later write into the slot having landed (index_mismatch). The commit rewrites that header, so the later grain is flagged invalid too, not served.
# TYPE mxl_gateway_mirror_target_invalid_grains_total counter
mxl_gateway_mirror_target_invalid_grains_total{domain="",flow_id="flow-b",node="n01",peer_node="n03",provider="verbs",reason="index_mismatch"} 2
mxl_gateway_mirror_target_invalid_grains_total{domain="",flow_id="flow-b",node="n01",peer_node="n03",provider="verbs",reason="invalid_from_source"} 1
`
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(expected),
		"mxl_gateway_mirror_source_unsent_grains_total", "mxl_gateway_mirror_target_invalid_grains_total"))
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
	const transmitted = "mxl_gateway_mirror_transmitted_bytes_total"
	require.Equal(t, 1, testutil.CollectAndCount(c, transmitted))

	delete(src.sources, key)
	require.Equal(t, 0, testutil.CollectAndCount(c, transmitted),
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
# HELP mxl_gateway_mirror_transmitted_bytes_total Payload bytes the fabric reported as delivered for a mirror this node is the source of, including chunks of a grain given up part way, which went over the wire all the same.
# TYPE mxl_gateway_mirror_transmitted_bytes_total counter
mxl_gateway_mirror_transmitted_bytes_total{domain="",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 1000
mxl_gateway_mirror_transmitted_bytes_total{domain="",flow_id="flow-a",node="n01",peer_node="n03",provider="verbs"} 1000
`), "mxl_gateway_mirror_transmitted_bytes_total"),
		"one flow mirrored to two peers must yield two distinct series")

	// Same flow and same peer in two namespaces cannot be separated by
	// labels at all, so the fold has to carry it.
	dup := &sourceEntry{shared: testShared("flow-a", fabrics.ProviderVerbs), peerNode: "n02"}
	dup.bytes.Store(500)
	src.sources[types.NamespacedName{Namespace: "other", Name: "flow-a--n02"}] = dup
	require.Equal(t, 2, testutil.CollectAndCount(c, "mxl_gateway_mirror_transmitted_bytes_total"),
		"a label set that repeats must fold into one series, never a duplicate")
}

func TestThroughputCollectorReportsSkippedSamples(t *testing.T) {
	// Skipped samples are invisible to every other counter the gateway
	// keeps: the bytes it did send are counted as sent, the target's
	// head still advances over the ones it did not, and both halves
	// look fresh. This series is the only place the hole shows up.
	src := &SourceReconciler{sources: map[types.NamespacedName]*sourceEntry{}}
	e := &sourceEntry{shared: testShared("flow-a", fabrics.ProviderVerbs), peerNode: "n02"}
	e.bytes.Store(1024)
	e.skipped.Store(19200)
	src.sources[types.NamespacedName{Namespace: "p", Name: "a"}] = e

	c := &ThroughputCollector{NodeName: "n01", Source: src}
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(`
# HELP mxl_gateway_mirror_skipped_samples_total Samples the source half advanced past without sending, having left the readable window. The target's head still moves over them, so each is published as stale ring content.
# TYPE mxl_gateway_mirror_skipped_samples_total counter
mxl_gateway_mirror_skipped_samples_total{domain="",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 19200
`), "mxl_gateway_mirror_skipped_samples_total"))
}

// The same flow id in two domains is two flows. Mirrored to the same peer
// they must be two series; folded into one, a sc dashboard would read the
// sum as one flow's rate.
func TestThroughputCollectorSeparatesDomains(t *testing.T) {
	src := &SourceReconciler{sources: map[types.NamespacedName]*sourceEntry{}}
	for _, domain := range []string{"", "studio"} {
		e := &sourceEntry{
			shared:   &sharedSource{key: sourceKey{domain: domain, flowID: "flow-a", provider: fabrics.ProviderVerbs}},
			peerNode: "n02",
		}
		e.bytes.Store(1000)
		src.sources[types.NamespacedName{Namespace: "p", Name: domain + "flow-a"}] = e
	}
	c := &ThroughputCollector{NodeName: "n01", Source: src}
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(`
# HELP mxl_gateway_mirror_transmitted_bytes_total Payload bytes the fabric reported as delivered for a mirror this node is the source of, including chunks of a grain given up part way, which went over the wire all the same.
# TYPE mxl_gateway_mirror_transmitted_bytes_total counter
mxl_gateway_mirror_transmitted_bytes_total{domain="",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 1000
mxl_gateway_mirror_transmitted_bytes_total{domain="studio",flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 1000
`), "mxl_gateway_mirror_transmitted_bytes_total"))
}

// A Lease or MxlFlow event in one domain looks mirrors up by this index;
// keyed by the bare id it would also wake, and judge, the mirrors of the
// same id in another domain.
func TestIndexMirrorByFlowCarriesTheDomain(t *testing.T) {
	m := func(domain string) *mxlv1alpha1.MxlFlowMirror {
		return &mxlv1alpha1.MxlFlowMirror{Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: "flow-a", Domain: domain}}
	}
	require.Equal(t, []string{"flow-a"}, indexMirrorByFlow(m("")))
	require.Equal(t, []string{"studio.flow-a"}, indexMirrorByFlow(m("studio")))
}

// The initiator's per-target counters are published so each outcome is
// counted once: posted in a series of its own, and completed, failed
// and skipped under one outcome label that sums to what was offered
// less what is in flight. Lag carries its unit in its name, because a
// grain and a sample are not the same distance.
func TestThroughputCollectorReportsTransferCounts(t *testing.T) {
	src := &SourceReconciler{sources: map[types.NamespacedName]*sourceEntry{}}
	video := &sourceEntry{shared: testShared("flow-v", fabrics.ProviderVerbs), peerNode: "n02"}
	video.stats.Store(&fabrics.TargetStats{Posted: 10, Completed: 7, Failed: 1, Skipped: 4, InFlight: 2, Torn: 1, AccountingErrors: 3, LastCompletedIndex: 96})
	video.lastHead.Store(100)
	audioShared := testShared("flow-a", fabrics.ProviderVerbs)
	audioShared.continuous = true
	audio := &sourceEntry{shared: audioShared, peerNode: "n02"}
	audio.stats.Store(&fabrics.TargetStats{Posted: 3, Completed: 3, LastCompletedIndex: 48000})
	audio.lastHead.Store(48480)
	src.sources[types.NamespacedName{Namespace: "p", Name: "v"}] = video
	src.sources[types.NamespacedName{Namespace: "p", Name: "a"}] = audio

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(&ThroughputCollector{NodeName: "n01", Source: src}))
	families, err := reg.Gather()
	require.NoError(t, err)
	values := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			key := f.GetName()
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "flow_id", "outcome":
					key += "/" + l.GetValue()
				}
			}
			values[key] = m.GetCounter().GetValue() + m.GetGauge().GetValue()
		}
	}

	assert.Equal(t, 10.0, values["mxl_gateway_mirror_transfers_posted_total/flow-v"])
	assert.Equal(t, 7.0, values["mxl_gateway_mirror_transfers_total/flow-v/completed"])
	assert.Equal(t, 1.0, values["mxl_gateway_mirror_transfers_total/flow-v/failed"])
	assert.Equal(t, 4.0, values["mxl_gateway_mirror_transfers_total/flow-v/skipped"])
	_, postedAsOutcome := values["mxl_gateway_mirror_transfers_total/flow-v/posted"]
	assert.False(t, postedAsOutcome, "posted would be counted twice in a sum over outcomes")
	assert.Equal(t, 1.0, values["mxl_gateway_mirror_transfers_torn_total/flow-v"])
	assert.Equal(t, 3.0, values["mxl_gateway_mirror_accounting_errors_total/flow-v"])
	assert.Equal(t, 0.0, values["mxl_gateway_mirror_accounting_errors_total/flow-a"])
	assert.Equal(t, 2.0, values["mxl_gateway_mirror_transfers_in_flight/flow-v"])
	assert.Equal(t, 4.0, values["mxl_gateway_mirror_completion_lag_grains/flow-v"])
	assert.Equal(t, 480.0, values["mxl_gateway_mirror_completion_lag_samples/flow-a"])
	_, audioAsGrains := values["mxl_gateway_mirror_completion_lag_grains/flow-a"]
	assert.False(t, audioAsGrains, "an audio lag in samples published as grains")
}
