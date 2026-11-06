package placement

import (
	"sync"

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

// gauges remembers the label sets it set, so a series whose port went
// away is deleted rather than the whole vector reset: a reset leaves a
// scrape that lands between it and the next set reading no series at
// all.
type gauges struct {
	mu   sync.Mutex
	last map[[3]string]struct{}
}

// publish sets the port gauges to what the ledger holds and deletes the
// series of ports no longer reported.
func (g *gauges) publish(l *Ledger, nodes map[string]Node, s Settings) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := map[[3]string]struct{}{}
	for name, n := range nodes {
		for _, p := range n.Status.Providers {
			for _, i := range p.Interfaces {
				port := Port(i)
				if _, dup := now[[3]string{name, port, "egress"}]; dup {
					continue
				}
				eg, egN := l.Egress(name, port)
				in, inN := l.Ingress(name, port)
				for dir, v := range map[string][2]float64{"egress": {float64(eg), float64(egN)}, "ingress": {float64(in), float64(inN)}} {
					committedBits.WithLabelValues(name, port, dir).Set(v[0])
					committedMirrors.WithLabelValues(name, port, dir).Set(v[1])
					now[[3]string{name, port, dir}] = struct{}{}
				}
				if i.CapacityBitsPerSecond > 0 {
					plannableBits.WithLabelValues(name, port).Set(float64(plannable(i.CapacityBitsPerSecond, n.utilization(s.MaxUtilizationPercent))))
					now[[3]string{name, port, ""}] = struct{}{}
				}
			}
		}
	}
	for k := range g.last {
		if _, ok := now[k]; ok {
			continue
		}
		if k[2] == "" {
			plannableBits.DeleteLabelValues(k[0], k[1])
			continue
		}
		committedBits.DeleteLabelValues(k[0], k[1], k[2])
		committedMirrors.DeleteLabelValues(k[0], k[1], k[2])
	}
	g.last = now
}
