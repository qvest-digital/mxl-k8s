package placement

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Every series below is bounded by the nodes reporting capabilities,
// times the ports and shared budgets each reports, times the two
// directions.
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
	capacityBits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_port_capacity_bits_per_second",
		Help: "Capacity of a fabric port per direction, as its node reports it; absent when unknown.",
	}, []string{"node", "port"})
	nodeUtilization = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_node_max_utilization_ratio",
		Help: "Share of a capacity placement commits on a node: the node's own maximum utilization, or the operator's where the node sets none.",
	}, []string{"node"})
	budgetCapacityBits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_budget_capacity_bits_per_second",
		Help: "Capacity of a shared budget a node reports.",
	}, []string{"node", "budget", "scope", "mode"})
	budgetPlannableBits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_budget_plannable_bits_per_second",
		Help: "Capacity times maximum utilization of a shared budget.",
	}, []string{"node", "budget", "scope", "mode"})
	budgetCommittedBits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mxl_operator_budget_committed_bits_per_second",
		Help: "Demand of the mirrors placed on the interfaces drawing on a shared budget, per direction. A budget's mode says which directions count against it.",
	}, []string{"node", "budget", "scope", "mode", "direction"})
)

func init() {
	metrics.Registry.MustRegister(placementSeconds, committedBits, committedMirrors, plannableBits,
		capacityBits, nodeUtilization, budgetCapacityBits, budgetPlannableBits, budgetCommittedBits)
}

// gauges remembers the label sets it set, so a series whose port went
// away is deleted rather than the whole vector reset: a reset leaves a
// scrape that lands between it and the next set reading no series at
// all.
type gauges struct {
	mu   sync.Mutex
	last map[series][]string
}

// series is one label set of one vector, the labels joined.
type series struct {
	vec    *prometheus.GaugeVec
	labels string
}

// publish sets the gauges to what the ledger holds and deletes the
// series of ports and budgets no longer reported.
func (g *gauges) publish(l *Ledger, nodes map[string]Node, s Settings) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := map[series][]string{}
	set := func(vec *prometheus.GaugeVec, v float64, labels ...string) {
		vec.WithLabelValues(labels...).Set(v)
		now[series{vec, strings.Join(labels, "\x00")}] = labels
	}
	for name, n := range nodes {
		util := n.utilization(s.MaxUtilizationPercent)
		set(nodeUtilization, float64(util)/100, name)
		for _, p := range n.Status.Providers {
			for _, i := range p.Interfaces {
				port := Port(i)
				if _, dup := now[series{committedBits, strings.Join([]string{name, port, "egress"}, "\x00")}]; dup {
					continue
				}
				eg, egN := l.Egress(name, port)
				in, inN := l.Ingress(name, port)
				set(committedBits, float64(eg), name, port, "egress")
				set(committedMirrors, float64(egN), name, port, "egress")
				set(committedBits, float64(in), name, port, "ingress")
				set(committedMirrors, float64(inN), name, port, "ingress")
				if i.CapacityBitsPerSecond > 0 {
					set(plannableBits, float64(plannable(i.CapacityBitsPerSecond, util)), name, port)
					set(capacityBits, float64(i.CapacityBitsPerSecond), name, port)
				}
			}
		}
		for _, b := range n.Status.Budgets {
			labels := []string{name, b.Name, string(b.Scope), string(b.Mode)}
			set(budgetCapacityBits, float64(b.CapacityBitsPerSecond), labels...)
			set(budgetPlannableBits, float64(plannable(b.CapacityBitsPerSecond, util)), labels...)
			eg, in := l.BudgetLoad(name, b.Name)
			set(budgetCommittedBits, float64(eg), append(labels, "egress")...)
			set(budgetCommittedBits, float64(in), append(labels, "ingress")...)
		}
	}
	for k, labels := range g.last {
		if _, ok := now[k]; !ok {
			k.vec.DeleteLabelValues(labels...)
		}
	}
	g.last = now
}
