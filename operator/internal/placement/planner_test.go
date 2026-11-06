package placement

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	gbit = int64(1_000_000_000)
	tcp  = v1alpha1.ProviderTCP
)

// iface is one interface on a 192.0.2.0/25 or 192.0.2.128/25 rail.
func iface(addr, subnet string, capacity int64, budgets ...string) v1alpha1.MxlFabricInterface {
	return v1alpha1.MxlFabricInterface{Address: addr, Subnet: subnet, CapacityBitsPerSecond: capacity, Budgets: budgets}
}

func node(name string, ifs ...v1alpha1.MxlFabricInterface) Node {
	return Node{Name: name, Status: v1alpha1.MxlNodeCapabilitiesStatus{
		Providers: []v1alpha1.MxlFabricsProviderCapability{{Name: tcp, DeviceCount: int32(len(ifs)), Interfaces: ifs}},
	}}
}

const (
	railA = "192.0.2.0/25"
	railB = "192.0.2.128/25"
)

// twoPort is a node with one interface on each rail.
func twoPort(name string, a, b string, capacity int64) Node {
	return node(name, iface(a, railA, capacity), iface(b, railB, capacity))
}

var defaults = Settings{MaxUtilizationPercent: 80, Pairing: PairingPreferSameSubnet, Overcommit: OvercommitPlace}

func place(src, tgt string) v1alpha1.MxlFlowMirrorPlacement {
	return v1alpha1.MxlFlowMirrorPlacement{SourceInterface: src, TargetInterface: tgt}
}

func TestPlaceSpreadsAcrossPorts(t *testing.T) {
	// Two 25G ports per node, mirrors of 3G each. Without placement a
	// gateway binds the first enumerated interface, which puts every
	// mirror on one port; the ledger has to move the second one over.
	nodes := []Node{
		twoPort("a", "192.0.2.1", "192.0.2.129", 25*gbit),
		twoPort("b", "192.0.2.2", "192.0.2.130", 25*gbit),
	}
	l := NewLedger(nodes, nil)
	req := Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 3 * gbit}

	first := Place(l, defaults, req)
	require.Equal(t, OutcomePlaced, first.Outcome)
	require.Equal(t, place("192.0.2.1", "192.0.2.2"), first.Placement, "ties break on lowest address")
	l.Charge(Placed{SourceNode: "a", TargetNode: "b", Placement: first.Placement, Demand: req.Demand})

	second := Place(l, defaults, req)
	require.Equal(t, OutcomePlaced, second.Outcome)
	require.Equal(t, place("192.0.2.129", "192.0.2.130"), second.Placement)
}

func TestPlacePairsWithinSubnet(t *testing.T) {
	nodes := []Node{
		twoPort("a", "192.0.2.1", "192.0.2.129", 25*gbit),
		twoPort("b", "192.0.2.2", "192.0.2.130", 25*gbit),
	}
	// Rail A busy on the source, rail B busy on the target: a cross pair
	// would have the most headroom, but crosses subnets.
	l := NewLedger(nodes, []Placed{
		{SourceNode: "a", TargetNode: "c", Placement: place("192.0.2.1", "x"), Demand: 10 * gbit},
		{SourceNode: "c", TargetNode: "b", Placement: place("y", "192.0.2.130"), Demand: 10 * gbit},
	})
	req := Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: gbit}

	d := Place(l, defaults, req)
	require.Equal(t, place("192.0.2.1", "192.0.2.2"), d.Placement,
		"same-subnet pairs only; both have 10G committed on one side")

	anyPair := defaults
	anyPair.Pairing = PairingAny
	d = Place(l, anyPair, req)
	require.Equal(t, place("192.0.2.129", "192.0.2.2"), d.Placement, "any pairing takes the idle cross pair")
}

func TestPlaceSameSubnetRequiresSharedSubnet(t *testing.T) {
	nodes := []Node{
		node("a", iface("192.0.2.1", railA, 0)),
		node("b", iface("198.51.100.1", "198.51.100.0/24", 0)),
	}
	s := defaults
	s.Pairing = PairingSameSubnet
	d := Place(NewLedger(nodes, nil), s, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: gbit})
	require.Equal(t, OutcomeNoInterface, d.Outcome)

	d = Place(NewLedger(nodes, nil), defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: gbit})
	require.Equal(t, OutcomePlaced, d.Outcome, "prefer falls back to every pair when none shares a subnet")
}

func TestPlaceSinglePortBehavesAsToday(t *testing.T) {
	// One interface each, no capacity reported: the only pair, never
	// full, however much is on it.
	nodes := []Node{node("a", iface("192.0.2.1", "", 0)), node("b", iface("192.0.2.2", "", 0))}
	l := NewLedger(nodes, []Placed{{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.1", "192.0.2.2"), Demand: 1000 * gbit}})
	d := Place(l, defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 100 * gbit})
	require.Equal(t, OutcomePlaced, d.Outcome)
	require.Equal(t, place("192.0.2.1", "192.0.2.2"), d.Placement)
	require.Equal(t, int64(math.MaxInt64), d.Headroom)
}

func TestPlaceNoInterface(t *testing.T) {
	nodes := []Node{node("a", iface("192.0.2.1", railA, 0)), {Name: "b"}}
	d := Place(NewLedger(nodes, nil), defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp})
	require.Equal(t, OutcomeNoInterface, d.Outcome)

	down := iface("192.0.2.2", railA, 0)
	down.LinkState = "down"
	nodes = []Node{node("a", iface("192.0.2.1", railA, 0)), node("b", down)}
	d = Place(NewLedger(nodes, nil), defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp})
	require.Equal(t, OutcomeNoInterface, d.Outcome, "an interface whose link is down carries nothing")
}

func TestPlaceMaxUtilization(t *testing.T) {
	nodes := []Node{node("a", iface("192.0.2.1", railA, 10*gbit)), node("b", iface("192.0.2.2", railA, 10*gbit))}
	req := Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 9 * gbit}

	d := Place(NewLedger(nodes, nil), defaults, req)
	require.Equal(t, OutcomeOvercommitted, d.Outcome, "9G does not fit under 80%% of 10G")
	require.Equal(t, place("192.0.2.1", "192.0.2.2"), d.Placement)
	require.Equal(t, -gbit, d.Headroom)

	nodes[1].Status.MaxUtilizationPercent = 95
	nodes[0].Status.MaxUtilizationPercent = 95
	d = Place(NewLedger(nodes, nil), defaults, req)
	require.Equal(t, OutcomePlaced, d.Outcome, "a node's own maxUtilizationPercent overrides the default")
}

func TestPlaceHoldPolicy(t *testing.T) {
	nodes := []Node{node("a", iface("192.0.2.1", railA, 10*gbit)), node("b", iface("192.0.2.2", railA, 10*gbit))}
	s := defaults
	s.Overcommit = OvercommitHold
	d := Place(NewLedger(nodes, nil), s, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 9 * gbit})
	require.Equal(t, OutcomeHeld, d.Outcome)
	require.Empty(t, d.Placement.SourceInterface)
}

func TestPlaceOvercommitPicksMostHeadroom(t *testing.T) {
	nodes := []Node{
		twoPort("a", "192.0.2.1", "192.0.2.129", 10*gbit),
		twoPort("b", "192.0.2.2", "192.0.2.130", 10*gbit),
	}
	l := NewLedger(nodes, []Placed{
		{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.1", "192.0.2.2"), Demand: 7 * gbit},
		{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.129", "192.0.2.130"), Demand: 5 * gbit},
	})
	d := Place(l, defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 4 * gbit})
	require.Equal(t, OutcomeOvercommitted, d.Outcome)
	require.Equal(t, place("192.0.2.129", "192.0.2.130"), d.Placement)
}

func TestPlaceDirectionsAreSeparate(t *testing.T) {
	// A port sending 7G still receives at full capacity.
	nodes := []Node{node("a", iface("192.0.2.1", railA, 10*gbit)), node("b", iface("192.0.2.2", railA, 10*gbit))}
	l := NewLedger(nodes, []Placed{{SourceNode: "b", TargetNode: "a", Placement: place("192.0.2.2", "192.0.2.1"), Demand: 7 * gbit}})
	d := Place(l, defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 7 * gbit})
	require.Equal(t, OutcomePlaced, d.Outcome)
}

func withBudget(n Node, b v1alpha1.MxlFabricBudget) Node {
	n.Status.Budgets = append(n.Status.Budgets, b)
	return n
}

func TestPlaceBudgetModes(t *testing.T) {
	// Two 25G ports sharing a 20G cap. 9G already arrives on port one
	// and 9G already leaves port two.
	shared := func(mode v1alpha1.MxlFabricBudgetMode) []Node {
		b := withBudget(node("b", iface("192.0.2.2", railA, 25*gbit, "dev"), iface("192.0.2.130", railB, 25*gbit, "dev")),
			v1alpha1.MxlFabricBudget{Name: "dev", Scope: v1alpha1.FabricBudgetNIC, Mode: mode, CapacityBitsPerSecond: 20 * gbit})
		return []Node{twoPort("a", "192.0.2.1", "192.0.2.129", 25*gbit), b, twoPort("c", "192.0.2.3", "192.0.2.131", 25*gbit)}
	}
	existing := []Placed{
		{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.1", "192.0.2.2"), Demand: 9 * gbit},
		{SourceNode: "b", TargetNode: "c", Placement: place("192.0.2.130", "192.0.2.3"), Demand: 9 * gbit},
	}
	req := Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 6 * gbit}

	// ingress: 9 + 6 = 15 > 16? no: 16 is 80% of 20, so 15 fits.
	d := Place(NewLedger(shared(v1alpha1.FabricBudgetIngress), existing), defaults, req)
	require.Equal(t, OutcomePlaced, d.Outcome)
	require.Equal(t, place("192.0.2.129", "192.0.2.130"), d.Placement, "the idle port, the budget still has room")

	d = Place(NewLedger(shared(v1alpha1.FabricBudgetIngress), existing), defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 8 * gbit})
	require.Equal(t, OutcomeOvercommitted, d.Outcome, "9 + 8 received through one device exceeds 80%% of 20G on either port")

	// combined: 9 in + 9 out + 6 = 24 > 16.
	d = Place(NewLedger(shared(v1alpha1.FabricBudgetCombined), existing), defaults, req)
	require.Equal(t, OutcomeOvercommitted, d.Outcome, "combined counts the sending side too")

	// egress: the received mirror never touches it.
	d = Place(NewLedger(shared(v1alpha1.FabricBudgetEgress), existing), defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 8 * gbit})
	require.Equal(t, OutcomePlaced, d.Outcome)
}

func TestPlaceAntiAffinity(t *testing.T) {
	nodes := []Node{
		twoPort("a", "192.0.2.1", "192.0.2.129", 25*gbit),
		twoPort("b", "192.0.2.2", "192.0.2.130", 25*gbit),
	}
	// A sibling sits on rail A both ends with little load; the idle
	// rail is less loaded anyway, so make rail B the busier one.
	l := NewLedger(nodes, []Placed{
		{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.1", "192.0.2.2"), Demand: gbit},
		{SourceNode: "a", TargetNode: "x", Placement: place("192.0.2.129", "z"), Demand: 5 * gbit},
		{SourceNode: "x", TargetNode: "b", Placement: place("z", "192.0.2.130"), Demand: 5 * gbit},
	})
	req := Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: gbit,
		AvoidSource: []string{"192.0.2.1"}, AvoidTarget: []string{"192.0.2.2"}}

	d := Place(l, defaults, req)
	require.Equal(t, place("192.0.2.129", "192.0.2.130"), d.Placement, "disjoint beats headroom")
	require.False(t, d.SharesSource)
	require.False(t, d.SharesTarget)

	// Rail B full on the target: the disjoint pair is infeasible, so
	// the member lands on the shared one and says so.
	nodes[1].Status.Providers[0].Interfaces[1].CapacityBitsPerSecond = 6 * gbit
	l = NewLedger(nodes, []Placed{
		{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.1", "192.0.2.2"), Demand: gbit},
		{SourceNode: "x", TargetNode: "b", Placement: place("z", "192.0.2.130"), Demand: 5 * gbit},
	})
	d = Place(l, defaults, req)
	require.Equal(t, OutcomePlaced, d.Outcome)
	require.Equal(t, place("192.0.2.1", "192.0.2.2"), d.Placement, "feasibility beats disjointness")
	require.True(t, d.SharesSource)
	require.True(t, d.SharesTarget)
}

func TestPlaceAntiAffinityPrefersDisjointTarget(t *testing.T) {
	// Source has one port, so every pair shares it. Of the target
	// ports, the one the sibling is not on wins.
	nodes := []Node{
		node("a", iface("192.0.2.1", railA, 25*gbit)),
		node("b", iface("192.0.2.2", railA, 25*gbit), iface("192.0.2.3", railA, 25*gbit)),
	}
	l := NewLedger(nodes, []Placed{{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.1", "192.0.2.3"), Demand: gbit}})
	d := Place(l, defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: gbit,
		AvoidSource: []string{"192.0.2.1"}, AvoidTarget: []string{"192.0.2.2"}})
	require.Equal(t, place("192.0.2.1", "192.0.2.3"), d.Placement, "shares the source, keeps the target disjoint")
	require.True(t, d.SharesSource)
	require.False(t, d.SharesTarget)
}

func TestCheck(t *testing.T) {
	nodes := []Node{node("a", iface("192.0.2.1", railA, 10*gbit)), node("b", iface("192.0.2.2", railA, 10*gbit))}
	req := Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: 2 * gbit}
	require.Equal(t, 6*gbit, Check(NewLedger(nodes, nil), defaults, req, place("192.0.2.1", "192.0.2.2")))
}

func TestDemand(t *testing.T) {
	require.Equal(t, int64(1050), Demand(1000, 5, 7))
	require.Equal(t, int64(7), Demand(0, 5, 7), "a flow with no published rate charges the provisional demand")
	require.Equal(t, int64(0), Demand(0, 5, 0))
}

func TestHas(t *testing.T) {
	n := node("a", iface("192.0.2.1", railA, 0))
	require.True(t, n.Has(tcp, "192.0.2.1"))
	require.False(t, n.Has(tcp, "192.0.2.9"))
	require.False(t, n.Has(v1alpha1.ProviderVerbs, "192.0.2.1"))
}

func dev(i v1alpha1.MxlFabricInterface, device string) v1alpha1.MxlFabricInterface {
	i.Device = device
	return i
}

// One port holding an address on each rail is one link: what leaves on
// one address is load on the other. Counted per address, it would read
// as two idle ports.
func TestPlaceChargesAddressesOfOneDeviceToOnePort(t *testing.T) {
	nodes := []Node{
		node("big", dev(iface("192.0.2.1", railA, 10*gbit), "port0"), dev(iface("192.0.2.129", railB, 10*gbit), "port0")),
		twoPort("b", "192.0.2.2", "192.0.2.130", 10*gbit),
	}
	l := NewLedger(nodes, []Placed{{SourceNode: "big", TargetNode: "b", Provider: tcp, Placement: place("192.0.2.1", "192.0.2.2"), Demand: 5 * gbit}})
	d := Place(l, defaults, Request{SourceNode: "big", TargetNode: "b", Provider: tcp, Demand: 5 * gbit})
	require.Equal(t, OutcomeOvercommitted, d.Outcome, "10G over one 10G port is past 80%% whichever address it leaves on")
	require.Equal(t, place("192.0.2.129", "192.0.2.130"), d.Placement, "the target side still has an idle port")

	eg, n := l.Egress("big", "port0")
	require.Equal(t, 5*gbit, eg)
	require.Equal(t, 1, n)
}

// A redundancy sibling on one rail address of a port shares that port
// with a member on the other rail address.
func TestPlaceAntiAffinityByPort(t *testing.T) {
	nodes := []Node{
		node("big", dev(iface("192.0.2.1", railA, 100*gbit), "port0"), dev(iface("192.0.2.129", railB, 100*gbit), "port0")),
		twoPort("b", "192.0.2.2", "192.0.2.130", 25*gbit),
	}
	d := Place(NewLedger(nodes, nil), defaults, Request{SourceNode: "big", TargetNode: "b", Provider: tcp, Demand: gbit,
		AvoidSource: []string{"port0"}, AvoidTarget: []string{"192.0.2.2"}})
	require.True(t, d.SharesSource, "both rail addresses of the source are one port")
	require.False(t, d.SharesTarget)
	require.Equal(t, "192.0.2.130", d.Placement.TargetInterface)
}

// Every node's IPv6 link-local addresses share fe80::/64, so pairing by
// subnet would read them as a rail of their own and send the second
// mirror over them.
func TestPlaceSkipsLinkLocalWhenARoutableAddressExists(t *testing.T) {
	ll := func(a string) v1alpha1.MxlFabricInterface { return iface(a, "fe80::/64", 10*gbit) }
	nodes := []Node{
		node("a", iface("192.0.2.1", railA, 10*gbit), ll("fe80::1")),
		node("b", iface("192.0.2.2", railA, 10*gbit), ll("fe80::2")),
	}
	l := NewLedger(nodes, []Placed{{SourceNode: "a", TargetNode: "b", Provider: tcp, Placement: place("192.0.2.1", "192.0.2.2"), Demand: 5 * gbit}})
	d := Place(l, defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: gbit})
	require.Equal(t, place("192.0.2.1", "192.0.2.2"), d.Placement)

	only := []Node{node("a", ll("fe80::1")), node("b", ll("fe80::2"))}
	d = Place(NewLedger(only, nil), defaults, Request{SourceNode: "a", TargetNode: "b", Provider: tcp, Demand: gbit})
	require.Equal(t, place("fe80::1", "fe80::2"), d.Placement, "link-local alone is still what the nodes offer")
}
