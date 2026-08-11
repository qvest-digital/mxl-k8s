package mirror

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Throughput is what one mirror half moved since this gateway started.
type Throughput struct {
	FlowID   string
	Provider string
	Bytes    uint64
}

var (
	descTransmitted = prometheus.NewDesc(
		"mxl_gateway_mirror_transmitted_bytes_total",
		"Payload bytes handed to the fabric for a mirror this node is the source of.",
		[]string{"flow_id", "node", "provider"}, nil)

	descReceived = prometheus.NewDesc(
		"mxl_gateway_mirror_received_bytes_total",
		"Payload bytes committed to the local flow for a mirror this node is the target of.",
		[]string{"flow_id", "node", "provider"}, nil)
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

func (c *ThroughputCollector) Collect(ch chan<- prometheus.Metric) {
	emit := func(desc *prometheus.Desc, src ThroughputSource) {
		if src == nil {
			return
		}
		for _, t := range src.Throughput() {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue,
				float64(t.Bytes), t.FlowID, c.NodeName, t.Provider)
		}
	}
	emit(descTransmitted, c.Source)
	emit(descReceived, c.Target)
}

// Throughput reports what each mirror this node sources has put on the
// fabric.
func (r *SourceReconciler) Throughput() []Throughput {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Throughput, 0, len(r.sources))
	for _, e := range r.sources {
		out = append(out, Throughput{
			FlowID:   e.flowID,
			Provider: e.provider.String(),
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
			Provider: e.provider.String(),
			Bytes:    e.bytes.Load(),
		})
	}
	return out
}
