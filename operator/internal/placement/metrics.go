package placement

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	placementSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "mxl_operator_mirror_placement_seconds",
		Help: "Time from a mirror's creation to the operator writing its placement.",
		// From the operator's own reconcile, milliseconds, to a mirror
		// that waited out the demand wait or a node's first probe.
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
	})
	committedBits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_port_committed_bits_per_second",
		Help: "Demand of the mirrors placed on a fabric port, per direction. A port is the device the provider names, or the address where it names none.",
	}, []string{"node", "port", "direction"})
	committedMirrors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_port_mirrors",
		Help: "Mirrors placed on a fabric port, per direction.",
	}, []string{"node", "port", "direction"})
	plannableBits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_port_plannable_bits_per_second",
		Help: "Capacity times maximum utilization of a fabric port; absent when its capacity is unknown.",
	}, []string{"node", "port"})
)

func init() {
	metrics.Registry.MustRegister(placementSeconds, committedBits, committedMirrors, plannableBits)
}

// publish replaces the interface gauges with what the ledger holds.
func publish(l *Ledger, nodes map[string]Node, s Settings) {
	committedBits.Reset()
	committedMirrors.Reset()
	plannableBits.Reset()
	for name, n := range nodes {
		seen := map[string]struct{}{}
		for _, p := range n.Status.Providers {
			for _, i := range p.Interfaces {
				port := Port(i)
				if _, dup := seen[port]; dup {
					continue
				}
				seen[port] = struct{}{}
				eg, egN := l.Egress(name, port)
				in, inN := l.Ingress(name, port)
				committedBits.WithLabelValues(name, port, "egress").Set(float64(eg))
				committedBits.WithLabelValues(name, port, "ingress").Set(float64(in))
				committedMirrors.WithLabelValues(name, port, "egress").Set(float64(egN))
				committedMirrors.WithLabelValues(name, port, "ingress").Set(float64(inN))
				if i.CapacityBitsPerSecond > 0 {
					plannableBits.WithLabelValues(name, port).Set(float64(plannable(i.CapacityBitsPerSecond, n.utilization(s.MaxUtilizationPercent))))
				}
			}
		}
	}
}
