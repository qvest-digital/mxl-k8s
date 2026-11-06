package placement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// The port gauges show a port's capacity next to its plannable share,
// and the node's effective ceiling: its own where it sets one.
func TestGaugesPublishCapacityAndEffectiveCeiling(t *testing.T) {
	var g gauges
	a := twoPort("cap-a", "192.0.2.211", "192.0.2.212", 10*gbit)
	b := twoPort("cap-b", "192.0.2.221", "192.0.2.222", 10*gbit)
	b.Status.MaxUtilizationPercent = 50
	nodes := map[string]Node{"cap-a": a, "cap-b": b}
	g.publish(NewLedger([]Node{a, b}, nil), nodes, defaults)

	require.Equal(t, float64(10*gbit), testutil.ToFloat64(capacityBits.WithLabelValues("cap-a", "192.0.2.211")))
	require.Equal(t, float64(8*gbit), testutil.ToFloat64(plannableBits.WithLabelValues("cap-a", "192.0.2.211")))
	require.Equal(t, float64(5*gbit), testutil.ToFloat64(plannableBits.WithLabelValues("cap-b", "192.0.2.221")))
	require.InDelta(t, 0.8, testutil.ToFloat64(nodeUtilization.WithLabelValues("cap-a")), 1e-9)
	require.InDelta(t, 0.5, testutil.ToFloat64(nodeUtilization.WithLabelValues("cap-b")), 1e-9, "the node's own ceiling wins")

	// Capacity going unknown removes both capacity gauges of that port.
	a = node("cap-a", iface("192.0.2.211", railA, 0), iface("192.0.2.212", railB, 10*gbit))
	nodes["cap-a"] = a
	g.publish(NewLedger([]Node{a, b}, nil), nodes, defaults)
	require.Equal(t, 0, seriesWith(t, capacityBits, "port", "192.0.2.211"))
	require.Equal(t, 0, seriesWith(t, plannableBits, "port", "192.0.2.211"))
	require.Equal(t, 1, seriesWith(t, capacityBits, "port", "192.0.2.212"))
}

// A budget shows its capacity, plannable share and the load of the
// mirrors placed on the interfaces drawing on it, per direction.
func TestGaugesPublishBudgetLoad(t *testing.T) {
	var g gauges
	budget := mxlv1alpha1.MxlFabricBudget{Name: "host-ingress", Scope: mxlv1alpha1.FabricBudgetHost,
		Mode: mxlv1alpha1.FabricBudgetIngress, CapacityBitsPerSecond: 20 * gbit}
	tgt := node("bud-t", iface("192.0.2.231", railA, 25*gbit, "host-ingress"), iface("192.0.2.232", railB, 25*gbit, "host-ingress"))
	tgt.Status.Budgets = []mxlv1alpha1.MxlFabricBudget{budget}
	src := twoPort("bud-s", "192.0.2.241", "192.0.2.242", 25*gbit)
	l := NewLedger([]Node{src, tgt}, []Placed{
		{SourceNode: "bud-s", TargetNode: "bud-t", Provider: tcp, Placement: place("192.0.2.241", "192.0.2.231"), Demand: 3 * gbit},
		{SourceNode: "bud-s", TargetNode: "bud-t", Provider: tcp, Placement: place("192.0.2.242", "192.0.2.232"), Demand: 2 * gbit},
	})
	g.publish(l, map[string]Node{"bud-s": src, "bud-t": tgt}, defaults)

	labels := []string{"bud-t", "host-ingress", "host", "ingress"}
	require.Equal(t, float64(20*gbit), testutil.ToFloat64(budgetCapacityBits.WithLabelValues(labels...)))
	require.Equal(t, float64(16*gbit), testutil.ToFloat64(budgetPlannableBits.WithLabelValues(labels...)))
	require.Equal(t, float64(5*gbit), testutil.ToFloat64(budgetCommittedBits.WithLabelValues(append(labels, "ingress")...)))
	require.Equal(t, float64(0), testutil.ToFloat64(budgetCommittedBits.WithLabelValues(append(labels, "egress")...)))

	tgt.Status.Budgets = nil
	g.publish(l, map[string]Node{"bud-s": src, "bud-t": tgt}, defaults)
	require.Equal(t, 0, seriesWith(t, budgetCommittedBits, "node", "bud-t"), "a budget no longer reported loses its series")
}

// seriesWith counts the series of c whose label name has value.
func seriesWith(t *testing.T, c prometheus.Collector, name, value string) int {
	t.Helper()
	n := 0
	for _, m := range collect(t, c) {
		if m.labels[name] == value {
			n++
		}
	}
	return n
}

type sample struct {
	name   string
	labels map[string]string
	value  float64
}

func collect(t *testing.T, c prometheus.Collector) []sample {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	families, err := reg.Gather()
	require.NoError(t, err)
	var out []sample
	for _, f := range families {
		for _, m := range f.GetMetric() {
			s := sample{name: f.GetName(), labels: map[string]string{}}
			for _, l := range m.GetLabel() {
				s.labels[l.GetName()] = l.GetValue()
			}
			switch {
			case m.GetGauge() != nil:
				s.value = m.GetGauge().GetValue()
			case m.GetCounter() != nil:
				s.value = m.GetCounter().GetValue()
			}
			out = append(out, s)
		}
	}
	return out
}

func statusScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func placedCond(reason string) []metav1.Condition {
	return []metav1.Condition{{Type: mxlv1alpha1.ConditionTypePlaced, Status: metav1.ConditionTrue, Reason: reason,
		LastTransitionTime: metav1.Now()}}
}

func TestStatusCollectorReportsSettingsMirrorsAndSets(t *testing.T) {
	src := capsObj("src", mxlv1alpha1.MxlFabricInterface{Address: "192.0.2.1", Device: "eth1"},
		mxlv1alpha1.MxlFabricInterface{Address: "2001:db8::1", Device: "eth1"})
	tgt := capsObj("tgt", mxlv1alpha1.MxlFabricInterface{Address: "198.51.100.1"})
	placed := &mxlv1alpha1.MxlFlowMirror{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m1"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: flowID(1), SourceNode: "src", TargetNode: "tgt", Provider: tcp,
			Placement: &mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "2001:db8::1", TargetInterface: "198.51.100.1"}},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{Phase: mxlv1alpha1.MxlFlowMirrorReady, DemandBitsPerSecond: 3 * gbit,
			Conditions: placedCond(mxlv1alpha1.ReasonOvercommitted)}}
	waiting := &mxlv1alpha1.MxlFlowMirror{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m2"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: flowID(2), SourceNode: "src", TargetNode: "tgt", Provider: tcp}}
	set := &mxlv1alpha1.MxlFlowRedundancySet{ObjectMeta: metav1.ObjectMeta{Name: "set"},
		Status: mxlv1alpha1.MxlFlowRedundancySetStatus{Conditions: []metav1.Condition{{Type: mxlv1alpha1.ConditionTypePathsDisjoint,
			Status: metav1.ConditionFalse, Reason: mxlv1alpha1.ReasonSharedTargetInterface, LastTransitionTime: metav1.Now()}}}}
	c := fake.NewClientBuilder().WithScheme(statusScheme()).WithObjects(src, tgt, placed, waiting, set).Build()

	elected := make(chan struct{})
	col := &StatusCollector{Reader: c, Elected: elected, Log: logr.Discard(), Settings: defaults,
		OverheadPercent: 5, ProvisionalDemand: 2 * gbit, DemandWait: 5 * time.Second, RedundancySets: true}
	require.Empty(t, collect(t, col), "a replica that is not the leader reports nothing")

	close(elected)
	got := collect(t, col)
	value := func(name string, labels map[string]string) float64 {
		t.Helper()
		for _, s := range got {
			if s.name != name {
				continue
			}
			match := true
			for k, v := range labels {
				match = match && s.labels[k] == v
			}
			if match {
				return s.value
			}
		}
		t.Fatalf("no %s with %v in %v", name, labels, got)
		return 0
	}
	require.InDelta(t, 0.8, value("mxl_operator_placement_max_utilization_ratio", nil), 1e-9)
	require.InDelta(t, 0.05, value("mxl_operator_placement_protocol_overhead_ratio", nil), 1e-9)
	require.Equal(t, float64(2*gbit), value("mxl_operator_placement_provisional_demand_bits_per_second", nil))
	require.Equal(t, 5.0, value("mxl_operator_placement_demand_wait_seconds", nil))
	require.Equal(t, 1.0, value("mxl_operator_placement_policy_info", map[string]string{"overcommit": "place", "pairing": "prefer-same-subnet"}))

	require.Equal(t, 1.0, value("mxl_operator_mirrors", map[string]string{"phase": "Ready", "placement": "Overcommitted"}))
	require.Equal(t, 1.0, value("mxl_operator_mirrors", map[string]string{"phase": "None", "placement": "None"}))
	require.Equal(t, float64(3*gbit), value("mxl_operator_mirror_demand_bits_per_second", map[string]string{"mirror": "m1"}))
	// The source port is the device both addresses share, the target
	// port the address its node names no device for.
	require.Equal(t, 1.0, value("mxl_operator_mirror_placement_info", map[string]string{
		"mirror": "m1", "source_port": "eth1", "target_port": "198.51.100.1", "placement": "Overcommitted", "phase": "Ready"}))
	require.Equal(t, 1.0, value("mxl_operator_mirror_placement_info", map[string]string{
		"mirror": "m2", "source_port": "", "target_port": "", "placement": "None"}))
	require.Equal(t, 0.0, value("mxl_operator_redundancyset_paths_disjoint", map[string]string{
		"redundancyset": "set", "reason": "SharedTargetInterface"}))
	for _, s := range got {
		require.False(t, s.name == "mxl_operator_mirror_demand_bits_per_second" && s.labels["mirror"] == "m2",
			"a mirror charged nothing yet has no demand series")
	}
}

// A failed list reports no mirror series at all, rather than counts of
// zero that read as no mirrors.
func TestStatusCollectorOmitsMirrorsWhenTheCacheFails(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(statusScheme()).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*mxlv1alpha1.MxlFlowMirrorList); ok {
				return errors.New("cache not synced")
			}
			return cl.List(ctx, list, opts...)
		}}).Build()
	col := &StatusCollector{Reader: c, Log: logr.Discard(), Settings: defaults}
	for _, s := range collect(t, col) {
		require.False(t, strings.HasPrefix(s.name, "mxl_operator_mirror"), "unexpected %s", s.name)
	}
}
