package mirror

import (
	"github.com/prometheus/client_golang/prometheus"
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

	// ResyncGrains and AgedOutGrains are meaningful on the source half
	// of a discrete flow only: the grains it did not offer, by
	// unsentReason.
	ResyncGrains, AgedOutGrains uint64

	// InvalidFromSource and IndexMismatch are meaningful on the target
	// half only: arrivals committed invalid, by invalidReason.
	InvalidFromSource, IndexMismatch uint64

	// Transfers is what the mirror's transfer loop counted, on the
	// source half only.
	Transfers *TransferCounts
}

// TransferCounts is what became of the transfers of one mirror. A
// transfer is a grain, or a batch of samples.
type TransferCounts struct {
	// Completed transfers completed within their deadline, Late ones
	// after it, and Lost ones were outstanding when the target was
	// given up.
	Completed, Late, Lost uint64

	// Lag is how far the last completed transfer trails the source
	// head, or -1 before the first completion. Continuous says it is
	// in samples rather than grains.
	Lag        float64
	Continuous bool
}

var (
	descTransmitted = prometheus.NewDesc(
		"mxl_gateway_mirror_transmitted_bytes_total",
		"Payload bytes of completed transfers for a mirror this node is the source of.",
		[]string{"domain", "flow_id", "node", "peer_node", "provider"}, nil)

	descReceived = prometheus.NewDesc(
		"mxl_gateway_mirror_received_bytes_total",
		"Payload bytes committed to the local flow for a mirror this node is the target of.",
		[]string{"domain", "flow_id", "node", "peer_node", "provider"}, nil)

	transferLabels = []string{"domain", "flow_id", "node", "peer_node", "provider"}

	descTransfers = prometheus.NewDesc(
		"mxl_gateway_mirror_transfers_total",
		"What became of the transfers (grains or batches of their slices, or batches of samples) to a mirror target: completed within one "+
			"grain or batch interval, late (completed after it), or lost "+
			"(outstanding when the target was given up and added again). A mirror has one transfer in flight.",
		append(append([]string{}, transferLabels...), "outcome"), nil)

	descLagGrains = prometheus.NewDesc(
		"mxl_gateway_mirror_completion_lag_grains",
		"Source head index minus the index of the last grain completed to the mirror target.",
		transferLabels, nil)

	descLagSamples = prometheus.NewDesc(
		"mxl_gateway_mirror_completion_lag_samples",
		"Source head index minus the end index of the last batch of samples completed to the mirror target.",
		transferLabels, nil)

	descSkipped = prometheus.NewDesc(
		"mxl_gateway_mirror_skipped_samples_total",
		"Samples the source half advanced past without sending, having left the readable window. "+
			"The target's head still moves over them, so each is published as stale ring content.",
		[]string{"domain", "flow_id", "node", "peer_node", "provider"}, nil)

	descSourceUnsent = prometheus.NewDesc(
		"mxl_gateway_mirror_source_unsent_grains_total",
		"Grains the source half did not offer to the initiator, by reason: resync (the loop fell more than a few grains "+
			"behind the head and resumed at it) or aged_out (the producer had reused the slot). Each one is a grain no "+
			"target receives. An invalid grain is not counted here: its header is offered, and the target commits it "+
			"invalid.",
		append(append([]string{}, transferLabels...), "reason"), nil)

	descTargetInvalid = prometheus.NewDesc(
		"mxl_gateway_mirror_target_invalid_grains_total",
		"Arrived grains the target half committed invalid: flagged invalid at the source (invalid_from_source), or "+
			"whose slot header names another index than the one committed, a later write into the slot having landed "+
			"(index_mismatch). The commit rewrites that header, so the later grain is flagged invalid too, not served.",
		append(append([]string{}, transferLabels...), "reason"), nil)
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
	ch <- descSourceUnsent
	ch <- descTargetInvalid
	ch <- descTransfers
	ch <- descLagGrains
	ch <- descLagSamples
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
	// emit sums value over the entries of src that share a label set;
	// extra are label values after the shared ones.
	emit := func(desc *prometheus.Desc, src ThroughputSource, value func(Throughput) uint64, extra ...string) {
		if src == nil {
			return
		}
		totals := map[key]uint64{}
		for _, t := range src.Throughput() {
			totals[key{t.Domain, t.FlowID, t.PeerNode, t.Provider}] += value(t)
		}
		for k, n := range totals {
			labels := append([]string{k.domain, k.flowID, c.NodeName, k.peerNode, k.provider}, extra...)
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(n), labels...)
		}
	}
	emit(descTransmitted, c.Source, func(t Throughput) uint64 { return t.Bytes })
	emit(descReceived, c.Target, func(t Throughput) uint64 { return t.Bytes })
	emit(descSkipped, c.Source, func(t Throughput) uint64 { return t.SkippedSamples })
	emit(descSourceUnsent, c.Source, func(t Throughput) uint64 { return t.ResyncGrains }, string(unsentResync))
	emit(descSourceUnsent, c.Source, func(t Throughput) uint64 { return t.AgedOutGrains }, string(unsentAgedOut))
	emit(descTargetInvalid, c.Target, func(t Throughput) uint64 { return t.InvalidFromSource }, string(invalidFromSource))
	emit(descTargetInvalid, c.Target, func(t Throughput) uint64 { return t.IndexMismatch }, string(invalidIndexMismatch))

	if c.Source != nil {
		// Two mirrors of one flow to one peer are two targets; their
		// counters add, and the larger lag stands for both.
		counts := map[key]TransferCounts{}
		for _, t := range c.Source.Throughput() {
			if t.Transfers == nil {
				continue
			}
			k := key{t.Domain, t.FlowID, t.PeerNode, t.Provider}
			sum, seen := counts[k]
			sum.Completed += t.Transfers.Completed
			sum.Late += t.Transfers.Late
			sum.Lost += t.Transfers.Lost
			sum.Continuous = t.Transfers.Continuous
			if !seen || t.Transfers.Lag > sum.Lag {
				sum.Lag = t.Transfers.Lag
			}
			counts[k] = sum
		}
		for k, n := range counts {
			labels := []string{k.domain, k.flowID, c.NodeName, k.peerNode, k.provider}
			for outcome, v := range map[string]uint64{
				"completed": n.Completed, "late": n.Late, "lost": n.Lost,
			} {
				ch <- prometheus.MustNewConstMetric(descTransfers, prometheus.CounterValue,
					float64(v), append(append([]string{}, labels...), outcome)...)
			}
			if n.Lag >= 0 {
				desc := descLagGrains
				if n.Continuous {
					desc = descLagSamples
				}
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, n.Lag, labels...)
			}
		}
	}
}

// Throughput reports what each mirror this node sources has put on the
// fabric.
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
			ResyncGrains:   e.resyncGrains.Load(),
			AgedOutGrains:  e.agedOutGrains.Load(),
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

			InvalidFromSource: e.invalidFromSource.Load(),
			IndexMismatch:     e.indexMismatch.Load(),
		})
	}
	return out
}

// transferCounts is the entry's transfer counters as metric values.
func (e *sourceEntry) transferCounts() *TransferCounts {
	// late is loaded first and counted after progress, so it never
	// exceeds the progress loaded with it.
	late := e.late.Load()
	completed := e.progress.Load()
	lag := -1.0
	if completed > 0 {
		lag = 0
		if head, last := e.lastHead.Load(), e.lastCompleted.Load(); head > last {
			lag = float64(head - last)
		}
	}
	return &TransferCounts{
		Completed: completed - late, Late: late, Lost: e.lost.Load(),
		Lag: lag, Continuous: e.shared != nil && e.shared.continuous,
	}
}
