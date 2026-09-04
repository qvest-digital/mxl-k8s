package mirror

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Throughput is what one mirror half moved since this gateway started.
type Throughput struct {
	FlowID   string
	PeerNode string
	Provider string
	Bytes    uint64
}

var (
	descTransmitted = prometheus.NewDesc(
		"mxl_gateway_mirror_transmitted_bytes_total",
		"Payload bytes handed to the fabric for a mirror this node is the source of.",
		[]string{"flow_id", "node", "peer_node", "provider"}, nil)

	descReceived = prometheus.NewDesc(
		"mxl_gateway_mirror_received_bytes_total",
		"Payload bytes committed to the local flow for a mirror this node is the target of.",
		[]string{"flow_id", "node", "peer_node", "provider"}, nil)
)

// ThroughputSource reports the live mirrors one reconciler owns.
type ThroughputSource interface {
	Throughput() []Throughput
}

// ThroughputCollector publishes the byte counters both reconcilers
// keep on their live entries.
//
// Collected on scrape rather than pushed from the transfer loops: the
// loops only add to a per-entry atomic, so nothing on the path of a
// grain takes a lock or touches a label set, and a mirror that goes
// away stops being reported without any bookkeeping to unregister it.
type ThroughputCollector struct {
	NodeName string
	Source   ThroughputSource
	Target   ThroughputSource
}

func (c *ThroughputCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTransmitted
	ch <- descReceived
}

// Collect folds the live entries onto their label set before emitting.
//
// One node mirrors the same flow to several peers, so peer_node is what
// separates those series; the fold is what guarantees the label set is
// unique regardless. A duplicate reaching the registry fails the whole
// scrape with a 500, taking every other metric on the endpoint with it,
// so this cannot be left to the labels alone.
func (c *ThroughputCollector) Collect(ch chan<- prometheus.Metric) {
	type key struct{ flowID, peerNode, provider string }
	emit := func(desc *prometheus.Desc, src ThroughputSource) {
		if src == nil {
			return
		}
		totals := map[key]uint64{}
		for _, t := range src.Throughput() {
			totals[key{t.FlowID, t.PeerNode, t.Provider}] += t.Bytes
		}
		for k, bytes := range totals {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue,
				float64(bytes), k.flowID, c.NodeName, k.peerNode, k.provider)
		}
	}
	emit(descTransmitted, c.Source)
	emit(descReceived, c.Target)
}

// Throughput reports what each mirror this node sources has put on the
// fabric.
//
// Still one entry per mirror although the mirrors of one flow share an
// initiator: a transfer is enqueued to every target that initiator
// holds, so the bytes really did go to each peer, and folding them
// onto the flow would drop the peer_node label the series are told
// apart by.
func (r *SourceReconciler) Throughput() []Throughput {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Throughput, 0, len(r.sources))
	for _, e := range r.sources {
		out = append(out, Throughput{
			FlowID:   e.flowID(),
			PeerNode: e.peerNode,
			Provider: e.provider().String(),
			Bytes:    e.bytes.Load(),
		})
	}
	return out
}

// Throughput reports what each mirror this node targets has committed
// to its local flow.
func (r *TargetReconciler) Throughput() []Throughput {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Throughput, 0, len(r.targets))
	for _, e := range r.targets {
		out = append(out, Throughput{
			FlowID:   e.flowID,
			PeerNode: e.peerNode,
			Provider: e.provider.String(),
			Bytes:    e.bytes.Load(),
		})
	}
	return out
}
