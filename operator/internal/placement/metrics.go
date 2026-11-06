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
		Name: "mxl_operator_interface_committed_bits_per_second",
		Help: "Demand of the mirrors placed on a fabric interface, per direction.",
	}, []string{"node", "interface", "direction"})
	committedMirrors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_interface_mirrors",
		Help: "Mirrors placed on a fabric interface, per direction.",
	}, []string{"node", "interface", "direction"})
	plannableBits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_interface_plannable_bits_per_second",
		Help: "Capacity times maximum utilization of a fabric interface; absent when its capacity is unknown.",
	}, []string{"node", "interface"})
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
				if _, dup := seen[i.Address]; dup {
					continue
				}
				seen[i.Address] = struct{}{}
				eg, egN := l.Egress(name, i.Address)
				in, inN := l.Ingress(name, i.Address)
				committedBits.WithLabelValues(name, i.Address, "egress").Set(float64(eg))
				committedBits.WithLabelValues(name, i.Address, "ingress").Set(float64(in))
				committedMirrors.WithLabelValues(name, i.Address, "egress").Set(float64(egN))
				committedMirrors.WithLabelValues(name, i.Address, "ingress").Set(float64(inN))
				if i.CapacityBitsPerSecond > 0 {
					plannableBits.WithLabelValues(name, i.Address).Set(float64(plannable(i.CapacityBitsPerSecond, n.utilization(s.MaxUtilizationPercent))))
				}
			}
		}
	}
}
