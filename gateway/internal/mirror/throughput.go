package mirror

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/qvest-digital/go-mxl/fabrics"
)

// Throughput is what one mirror half moved since this gateway started.
type Throughput struct {
	// Domain is the flow's MxlDomain, empty for the primary domain;
	// the same id in two domains is two flows and two series.
	Domain   string
	FlowID   string
	PeerNode string
	Provider string
	Bytes    uint64

	// SkippedSamples is meaningful on the source half of a continuous
	// flow only, and is zero everywhere else.
	SkippedSamples uint64

	// Transfers is the initiator's account of the mirror's target, on
	// the source half only. Nil before the first report.
	Transfers *TransferCounts
}

// TransferCounts is what an initiator did with the transfers of one
// target. A transfer is a grain, or a batch of samples.
type TransferCounts struct {
	Posted, Completed, Failed, Skipped, InFlight uint64

	// Lag is how far the last completed transfer trails the source
	// head, in grains or samples, or -1 before the first completion.
	Lag float64
}

var (
	descTransmitted = prometheus.NewDesc(
		"mxl_gateway_mirror_transmitted_bytes_total",
		"Payload bytes the fabric reported as delivered for a mirror this node is the source of.",
		[]string{"domain", "flow_id", "node", "peer_node", "provider"}, nil)

	descReceived = prometheus.NewDesc(
		"mxl_gateway_mirror_received_bytes_total",
		"Payload bytes committed to the local flow for a mirror this node is the target of.",
		[]string{"domain", "flow_id", "node", "peer_node", "provider"}, nil)

	transferLabels = []string{"domain", "flow_id", "node", "peer_node", "provider"}

	descTransfers = prometheus.NewDesc(
		"mxl_gateway_mirror_transfers_total",
		"Transfers (grains, or batches of samples) the initiator handled for a mirror target, by outcome: "+
			"posted, completed, failed (error completion or lost with the connection) and skipped "+
			"(window or send queue full, or not connected; the other targets of the flow still got them).",
		append(append([]string{}, transferLabels...), "outcome"), nil)

	descInFlight = prometheus.NewDesc(
		"mxl_gateway_mirror_transfers_in_flight",
		"Transfers posted to a mirror target and not yet completed.",
		transferLabels, nil)

	descLag = prometheus.NewDesc(
		"mxl_gateway_mirror_completion_lag",
		"Source head index minus the index of the last transfer completed to the mirror target, "+
			"in grains, or in samples for a continuous flow.",
		transferLabels, nil)

	descSkipped = prometheus.NewDesc(
		"mxl_gateway_mirror_skipped_samples_total",
		"Samples the source half advanced past without sending, having left the readable window. "+
			"The target's head still moves over them, so each is published as stale ring content.",
		[]string{"domain", "flow_id", "node", "peer_node", "provider"}, nil)
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
	ch <- descSkipped
	ch <- descTransfers
	ch <- descInFlight
	ch <- descLag
}

// Collect folds the live entries onto their label set before emitting.
//
// One node mirrors the same flow to several peers, so peer_node is what
// separates those series; the fold is what guarantees the label set is
// unique regardless. A duplicate reaching the registry fails the whole
// scrape with a 500, taking every other metric on the endpoint with it,
// so this cannot be left to the labels alone.
func (c *ThroughputCollector) Collect(ch chan<- prometheus.Metric) {
	type key struct{ domain, flowID, peerNode, provider string }
	emit := func(desc *prometheus.Desc, src ThroughputSource) {
		if src == nil {
			return
		}
		totals := map[key]uint64{}
		for _, t := range src.Throughput() {
			totals[key{t.Domain, t.FlowID, t.PeerNode, t.Provider}] += t.Bytes
		}
		for k, bytes := range totals {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue,
				float64(bytes), k.domain, k.flowID, c.NodeName, k.peerNode, k.provider)
		}
	}
	emit(descTransmitted, c.Source)
	emit(descReceived, c.Target)

	if c.Source != nil {
		skipped := map[key]uint64{}
		for _, t := range c.Source.Throughput() {
			skipped[key{t.Domain, t.FlowID, t.PeerNode, t.Provider}] += t.SkippedSamples
		}
		for k, n := range skipped {
			ch <- prometheus.MustNewConstMetric(descSkipped, prometheus.CounterValue,
				float64(n), k.domain, k.flowID, c.NodeName, k.peerNode, k.provider)
		}

		// Two mirrors of one flow to one peer are two targets; their
		// counters add, and the larger lag stands for both.
		counts := map[key]TransferCounts{}
		for _, t := range c.Source.Throughput() {
			if t.Transfers == nil {
				continue
			}
			k := key{t.Domain, t.FlowID, t.PeerNode, t.Provider}
			sum, seen := counts[k]
			sum.Posted += t.Transfers.Posted
			sum.Completed += t.Transfers.Completed
			sum.Failed += t.Transfers.Failed
			sum.Skipped += t.Transfers.Skipped
			sum.InFlight += t.Transfers.InFlight
			if !seen || t.Transfers.Lag > sum.Lag {
				sum.Lag = t.Transfers.Lag
			}
			counts[k] = sum
		}
		for k, n := range counts {
			labels := []string{k.domain, k.flowID, c.NodeName, k.peerNode, k.provider}
			for outcome, v := range map[string]uint64{
				"posted": n.Posted, "completed": n.Completed, "failed": n.Failed, "skipped": n.Skipped,
			} {
				ch <- prometheus.MustNewConstMetric(descTransfers, prometheus.CounterValue,
					float64(v), append(append([]string{}, labels...), outcome)...)
			}
			ch <- prometheus.MustNewConstMetric(descInFlight, prometheus.GaugeValue, float64(n.InFlight), labels...)
			if n.Lag >= 0 {
				ch <- prometheus.MustNewConstMetric(descLag, prometheus.GaugeValue, n.Lag, labels...)
			}
		}
	}
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
			Domain:   e.sourceKey().domain,
			FlowID:   e.flowID(),
			PeerNode: e.peerNode,
			Provider: e.provider().String(),
			Bytes:    e.bytes.Load(),

			SkippedSamples: e.skipped.Load(),
			Transfers:      e.transferCounts(),
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
			Domain:   e.domain,
			FlowID:   e.flowID,
			PeerNode: e.peerNode,
			Provider: e.provider.String(),
			Bytes:    e.bytes.Load(),
		})
	}
	return out
}

// transferCounts is the entry's last initiator report as metric values,
// or nil before the first one.
func (e *sourceEntry) transferCounts() *TransferCounts {
	st := e.stats.Load()
	if st == nil {
		return nil
	}
	lag := -1.0
	if st.LastCompletedIndex != fabrics.NoIndex {
		if head := e.lastHead.Load(); head >= st.LastCompletedIndex {
			lag = float64(head - st.LastCompletedIndex)
		} else {
			lag = 0
		}
	}
	return &TransferCounts{
		Posted: st.Posted, Completed: st.Completed, Failed: st.Failed, Skipped: st.Skipped, InFlight: st.InFlight,
		Lag: lag,
	}
}
