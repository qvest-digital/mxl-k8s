package placement

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// noneLabel stands for a phase or Placed reason a mirror does not carry
// yet.
const noneLabel = "None"

var (
	descMaxUtilization = prometheus.NewDesc("mxl_operator_placement_max_utilization_ratio",
		"Share of a capacity placement commits on nodes that set no maximum utilization of their own (--placement-max-utilization).",
		nil, nil)
	descOverhead = prometheus.NewDesc("mxl_operator_placement_protocol_overhead_ratio",
		"Share added to a flow's published rate when charging it to a port (--placement-protocol-overhead).",
		nil, nil)
	descProvisional = prometheus.NewDesc("mxl_operator_placement_provisional_demand_bits_per_second",
		"Rate charged for a flow that has not published one within the demand wait (--placement-provisional-demand).",
		nil, nil)
	descDemandWait = prometheus.NewDesc("mxl_operator_placement_demand_wait_seconds",
		"How long a mirror waits for its flow's rate before it is placed on the provisional demand (--placement-demand-wait).",
		nil, nil)
	descPolicy = prometheus.NewDesc("mxl_operator_placement_policy_info",
		"Placement policy: what happens to a mirror no pair has room for (overcommit) and how interfaces pair (pairing). The value is always 1.",
		[]string{"overcommit", "pairing"}, nil)

	descMirrors = prometheus.NewDesc("mxl_operator_mirrors",
		"MxlFlowMirrors by phase and by the reason of their Placed condition. None is a mirror without either yet.",
		[]string{"phase", "placement"}, nil)
	descMirrorDemand = prometheus.NewDesc("mxl_operator_mirror_demand_bits_per_second",
		"Rate placement charges a mirror to its two ports: its flow's published rate plus protocol overhead, or the provisional demand.",
		[]string{"namespace", "mirror", "domain", "flow_id", "source_node", "target_node", "provider"}, nil)
	descMirrorPlacement = prometheus.NewDesc("mxl_operator_mirror_placement_info",
		"Where a mirror is placed: the ports, as the port gauges name them, its source sends from and its target receives on, "+
			"empty while unplaced, with the reason of its Placed condition and its phase. The value is always 1.",
		[]string{"namespace", "mirror", "domain", "flow_id", "source_node", "target_node", "provider",
			"source_port", "target_port", "placement", "phase"}, nil)
	descPathsDisjoint = prometheus.NewDesc("mxl_operator_redundancyset_paths_disjoint",
		"1 while no two member mirrors of an MxlFlowRedundancySet arriving on one node share a fabric port, 0 otherwise; "+
			"reason is the PathsDisjoint condition's.",
		[]string{"redundancyset", "reason"}, nil)
)

// StatusCollector exports the placement settings, and the state of the
// mirrors and redundancy sets, read from the manager's cache on scrape.
//
// Read on scrape rather than set from the reconcile, so a mirror that is
// deleted stops being reported without any bookkeeping. It reports
// nothing until Elected is closed: every replica's cache holds the same
// objects, and only the leader's reconciles act on them.
type StatusCollector struct {
	Reader  client.Reader
	Elected <-chan struct{}
	Log     logr.Logger

	Settings          Settings
	OverheadPercent   int32
	ProvisionalDemand int64
	DemandWait        time.Duration
	// RedundancySets says whether the MxlFlowRedundancySet kind is
	// served and read.
	RedundancySets bool
}

// Describe implements prometheus.Collector.
func (c *StatusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descMaxUtilization, descOverhead, descProvisional, descDemandWait, descPolicy,
		descMirrors, descMirrorDemand, descMirrorPlacement, descPathsDisjoint} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *StatusCollector) Collect(ch chan<- prometheus.Metric) {
	if c.Elected != nil {
		select {
		case <-c.Elected:
		default:
			return
		}
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	gauge(descMaxUtilization, float64(c.Settings.MaxUtilizationPercent)/100)
	gauge(descOverhead, float64(c.OverheadPercent)/100)
	gauge(descProvisional, float64(c.ProvisionalDemand))
	gauge(descDemandWait, c.DemandWait.Seconds())
	gauge(descPolicy, 1, string(c.Settings.Overcommit), string(c.Settings.Pairing))

	ctx := context.Background()
	c.collectMirrors(ctx, gauge)
	if c.RedundancySets {
		c.collectSets(ctx, gauge)
	}
}

func (c *StatusCollector) collectMirrors(ctx context.Context, gauge func(*prometheus.Desc, float64, ...string)) {
	var caps mxlv1alpha1.MxlNodeCapabilitiesList
	if err := c.Reader.List(ctx, &caps); err != nil {
		c.Log.Error(err, "list node capabilities")
		return
	}
	status := map[string]mxlv1alpha1.MxlNodeCapabilitiesStatus{}
	for _, n := range caps.Items {
		name := n.Spec.NodeName
		if name == "" {
			name = n.Name
		}
		status[name] = n.Status
	}
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := c.Reader.List(ctx, &mirrors, client.UnsafeDisableDeepCopy); err != nil {
		// Nothing rather than zeros: a count of zero would read as no
		// mirrors at all.
		c.Log.Error(err, "list mirrors")
		return
	}
	type state struct{ phase, placement string }
	counts := map[state]int{}
	for i := range mirrors.Items {
		m := &mirrors.Items[i]
		if !m.DeletionTimestamp.IsZero() {
			continue
		}
		s := state{phase: string(m.Status.Phase), placement: placedReason(m.Status.Conditions)}
		if s.phase == "" {
			s.phase = noneLabel
		}
		counts[s]++

		provider := string(m.Spec.Provider)
		ids := []string{m.Namespace, m.Name, m.Spec.Domain, m.Spec.FlowID, m.Spec.SourceNode, m.Spec.TargetNode, provider}
		if m.Status.DemandBitsPerSecond > 0 {
			gauge(descMirrorDemand, float64(m.Status.DemandBitsPerSecond), ids...)
		}
		var srcPort, tgtPort string
		if p := m.Spec.Placement; p != nil {
			srcPort = PortOf(status[m.Spec.SourceNode], m.Spec.Provider, p.SourceInterface)
			tgtPort = PortOf(status[m.Spec.TargetNode], m.Spec.Provider, p.TargetInterface)
		}
		gauge(descMirrorPlacement, 1, append(ids, srcPort, tgtPort, s.placement, s.phase)...)
	}
	for s, n := range counts {
		gauge(descMirrors, float64(n), s.phase, s.placement)
	}
}

func (c *StatusCollector) collectSets(ctx context.Context, gauge func(*prometheus.Desc, float64, ...string)) {
	var sets mxlv1alpha1.MxlFlowRedundancySetList
	if err := c.Reader.List(ctx, &sets); err != nil {
		c.Log.Error(err, "list redundancy sets")
		return
	}
	for _, s := range sets.Items {
		cond := meta.FindStatusCondition(s.Status.Conditions, mxlv1alpha1.ConditionTypePathsDisjoint)
		if cond == nil {
			continue
		}
		v := 0.0
		if cond.Status == metav1.ConditionTrue {
			v = 1
		}
		gauge(descPathsDisjoint, v, s.Name, cond.Reason)
	}
}

// placedReason is the reason of the Placed condition, or None.
func placedReason(conds []metav1.Condition) string {
	if c := meta.FindStatusCondition(conds, mxlv1alpha1.ConditionTypePlaced); c != nil && c.Reason != "" {
		return c.Reason
	}
	return noneLabel
}
