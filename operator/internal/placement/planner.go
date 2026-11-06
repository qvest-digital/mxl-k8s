// Package placement chooses the pair of fabric interfaces every
// MxlFlowMirror is carried on: one on the source node its initiator
// binds, one on the target node its target binds.
//
// The planner is pure. It is handed what the nodes' gateways report
// (interfaces, capacities, shared budgets) and what is already placed,
// and answers for one mirror at a time. The controller in this package
// feeds it from the API and writes its answer back.
package placement

import (
	"cmp"
	"math"
	"net/netip"
	"slices"

	"github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Pairing is how the planner pairs a source interface with a target
// interface.
type Pairing string

const (
	// PairingPreferSameSubnet keeps only pairs whose two interfaces
	// report the same subnet, when any pair does, and every pair
	// otherwise.
	PairingPreferSameSubnet Pairing = "prefer-same-subnet"
	// PairingSameSubnet keeps only pairs whose two interfaces report
	// the same subnet.
	PairingSameSubnet Pairing = "same-subnet"
	// PairingAny keeps every pair.
	PairingAny Pairing = "any"
)

// Overcommit is what the planner does with a mirror no pair has room
// for.
type Overcommit string

const (
	// OvercommitPlace places the mirror on the pair with the most
	// headroom left, which is negative, and reports it overcommitted.
	OvercommitPlace Overcommit = "place"
	// OvercommitHold leaves the mirror unplaced.
	OvercommitHold Overcommit = "hold"
)

// Settings are the planner's tunables.
type Settings struct {
	// MaxUtilizationPercent is how much of a capacity placement may
	// commit, for nodes that report no value of their own.
	MaxUtilizationPercent int32
	Pairing               Pairing
	Overcommit            Overcommit
}

// Node is one node's fabric as its MxlNodeCapabilities reports it.
type Node struct {
	Name   string
	Status v1alpha1.MxlNodeCapabilitiesStatus
}

// Interfaces returns the interfaces the node offers provider that can
// carry a mirror: every listed one whose link is not reported down.
func (n Node) Interfaces(provider v1alpha1.MxlFabricsProvider) []v1alpha1.MxlFabricInterface {
	for _, p := range n.Status.Providers {
		if p.Name != provider {
			continue
		}
		out := make([]v1alpha1.MxlFabricInterface, 0, len(p.Interfaces))
		for _, i := range p.Interfaces {
			if i.LinkState != "down" {
				out = append(out, i)
			}
		}
		return out
	}
	return nil
}

// Port is the physical port an interface is: its device as the provider
// names it, or its address when the provider names none. Two addresses
// of one device -- an IPv4 and an IPv6 address, or one address per rail
// on one port -- share the one link, so load is accounted per port.
func Port(i v1alpha1.MxlFabricInterface) string {
	if i.Device != "" {
		return i.Device
	}
	return i.Address
}

// PortOf is the port, as Port names it, of the interface at address
// that status lists under provider, or the address when it lists none.
func PortOf(status v1alpha1.MxlNodeCapabilitiesStatus, provider v1alpha1.MxlFabricsProvider, address string) string {
	return Node{Status: status}.port(provider, address)
}

// port is the port of the interface at address under provider, or the
// address when the node does not list it.
func (n Node) port(provider v1alpha1.MxlFabricsProvider, address string) string {
	for _, p := range n.Status.Providers {
		if p.Name != provider {
			continue
		}
		for _, i := range p.Interfaces {
			if i.Address == address {
				return Port(i)
			}
		}
	}
	return address
}

// Has reports whether address is an interface the node offers
// provider that can carry a mirror.
func (n Node) Has(provider v1alpha1.MxlFabricsProvider, address string) bool {
	return slices.ContainsFunc(n.Interfaces(provider), func(i v1alpha1.MxlFabricInterface) bool {
		return i.Address == address
	})
}

func (n Node) utilization(def int32) int64 {
	if u := n.Status.MaxUtilizationPercent; u > 0 {
		return int64(u)
	}
	return int64(def)
}

// Placed is a mirror whose placement counts against its interfaces.
type Placed struct {
	SourceNode, TargetNode string
	Provider               v1alpha1.MxlFabricsProvider
	Placement              v1alpha1.MxlFlowMirrorPlacement
	Demand                 int64
}

// Ledger is the load already committed on every interface and budget,
// per direction.
type Ledger struct {
	egress, ingress map[portKey]load
	budgetEgress    map[budgetKey]int64
	budgetIngress   map[budgetKey]int64
	nodes           map[string]Node
}

type portKey struct{ node, port string }

type budgetKey struct{ node, name string }

type load struct {
	bits    int64
	mirrors int
}

// NewLedger charges every placed mirror to the interfaces it names and
// the budgets those interfaces draw on, as nodes report them.
func NewLedger(nodes []Node, placed []Placed) *Ledger {
	l := &Ledger{
		egress:        map[portKey]load{},
		ingress:       map[portKey]load{},
		budgetEgress:  map[budgetKey]int64{},
		budgetIngress: map[budgetKey]int64{},
		nodes:         map[string]Node{},
	}
	for _, n := range nodes {
		l.nodes[n.Name] = n
	}
	for _, p := range placed {
		l.Charge(p)
	}
	return l
}

// Charge adds one placed mirror to the ledger.
func (l *Ledger) Charge(p Placed) {
	src := portKey{p.SourceNode, l.nodes[p.SourceNode].port(p.Provider, p.Placement.SourceInterface)}
	tgt := portKey{p.TargetNode, l.nodes[p.TargetNode].port(p.Provider, p.Placement.TargetInterface)}
	e := l.egress[src]
	l.egress[src] = load{e.bits + p.Demand, e.mirrors + 1}
	i := l.ingress[tgt]
	l.ingress[tgt] = load{i.bits + p.Demand, i.mirrors + 1}
	for _, b := range l.budgetsOf(p.SourceNode, p.Placement.SourceInterface) {
		l.budgetEgress[budgetKey{p.SourceNode, b.Name}] += p.Demand
	}
	for _, b := range l.budgetsOf(p.TargetNode, p.Placement.TargetInterface) {
		l.budgetIngress[budgetKey{p.TargetNode, b.Name}] += p.Demand
	}
}

// Egress reports the committed rate and mirror count leaving one port,
// as Port names it.
func (l *Ledger) Egress(node, port string) (int64, int) {
	v := l.egress[portKey{node, port}]
	return v.bits, v.mirrors
}

// Ingress reports the committed rate and mirror count arriving on one
// port.
func (l *Ledger) Ingress(node, port string) (int64, int) {
	v := l.ingress[portKey{node, port}]
	return v.bits, v.mirrors
}

// budgetsOf returns the budgets the node reports for the interface at
// address.
func (l *Ledger) budgetsOf(node, address string) []v1alpha1.MxlFabricBudget {
	n, ok := l.nodes[node]
	if !ok {
		return nil
	}
	var names []string
	for _, p := range n.Status.Providers {
		for _, i := range p.Interfaces {
			if i.Address == address {
				names = append(names, i.Budgets...)
			}
		}
	}
	var out []v1alpha1.MxlFabricBudget
	for _, b := range n.Status.Budgets {
		if slices.Contains(names, b.Name) {
			out = append(out, b)
		}
	}
	return out
}

// Request is one mirror to place.
type Request struct {
	SourceNode, TargetNode string
	Provider               v1alpha1.MxlFabricsProvider
	Demand                 int64
	// AvoidSource and AvoidTarget are ports, as Port names them, the
	// mirror's redundancy siblings already use on its source and target
	// node.
	AvoidSource, AvoidTarget []string
}

// Outcome is the planner's answer.
type Outcome string

const (
	// OutcomePlaced is a pair with room for the demand.
	OutcomePlaced Outcome = v1alpha1.ReasonPlaced
	// OutcomeOvercommitted is a pair without room, chosen because the
	// policy places such mirrors anyway.
	OutcomeOvercommitted Outcome = v1alpha1.ReasonOvercommitted
	// OutcomeHeld is no pair, because none had room and the policy
	// holds.
	OutcomeHeld Outcome = v1alpha1.ReasonHeld
	// OutcomeNoInterface is no pair, because a node offers no
	// interface for the provider or none pairs.
	OutcomeNoInterface Outcome = v1alpha1.ReasonNoInterface
)

// Decision is the planner's answer for one request.
type Decision struct {
	Outcome   Outcome
	Placement v1alpha1.MxlFlowMirrorPlacement
	// SharesSource and SharesTarget report that the chosen pair uses
	// an interface the request asked to avoid.
	SharesSource, SharesTarget bool
	// Headroom is what is left on the tighter side of the pair after
	// the demand is charged, in bits per second. Negative when
	// overcommitted, math.MaxInt64 when no capacity on the pair is
	// known.
	Headroom int64
}

type candidate struct {
	src, tgt                   v1alpha1.MxlFabricInterface
	headroom                   int64
	load                       int64
	mirrors                    int
	sharesSource, sharesTarget bool
}

func (c candidate) disjointRank() int {
	switch {
	case !c.sharesSource && !c.sharesTarget:
		return 0
	case !c.sharesTarget:
		return 1
	case !c.sharesSource:
		return 2
	default:
		return 3
	}
}

// Place chooses a pair for req against what the ledger has committed.
//
// Among the pairs pairing allows, a pair with room for the demand on
// both interfaces and on every budget they draw on beats one without.
// Then a pair sharing no interface with the request's siblings beats
// one sharing only a source interface, which beats one sharing only a
// target interface, which beats one sharing both: receive side
// overflow is what loses grains, so sharing it is the worse of the
// two. Then the most headroom left on the tighter side, then the least
// load on the pair, then the fewest mirrors, then the lowest
// addresses, so the answer is the same on every run.
func Place(l *Ledger, s Settings, req Request) Decision {
	src := l.nodes[req.SourceNode]
	tgt := l.nodes[req.TargetNode]
	srcIfs, tgtIfs := routable(src.Interfaces(req.Provider)), routable(tgt.Interfaces(req.Provider))
	if len(srcIfs) == 0 || len(tgtIfs) == 0 {
		return Decision{Outcome: OutcomeNoInterface}
	}

	var pairs [][2]v1alpha1.MxlFabricInterface
	var same [][2]v1alpha1.MxlFabricInterface
	for _, a := range srcIfs {
		for _, b := range tgtIfs {
			pairs = append(pairs, [2]v1alpha1.MxlFabricInterface{a, b})
			if a.Subnet != "" && a.Subnet == b.Subnet {
				same = append(same, [2]v1alpha1.MxlFabricInterface{a, b})
			}
		}
	}
	switch s.Pairing {
	case PairingAny:
	case PairingSameSubnet:
		pairs = same
	default:
		if len(same) > 0 {
			pairs = same
		}
	}
	if len(pairs) == 0 {
		return Decision{Outcome: OutcomeNoInterface}
	}

	cands := make([]candidate, 0, len(pairs))
	for _, p := range pairs {
		cands = append(cands, l.score(s, src, tgt, p[0], p[1], req))
	}
	slices.SortFunc(cands, func(a, b candidate) int {
		if fa, fb := a.headroom >= 0, b.headroom >= 0; fa != fb {
			if fa {
				return -1
			}
			return 1
		}
		return cmp.Or(
			cmp.Compare(a.disjointRank(), b.disjointRank()),
			cmp.Compare(b.headroom, a.headroom),
			cmp.Compare(a.load, b.load),
			cmp.Compare(a.mirrors, b.mirrors),
			cmp.Compare(a.src.Address, b.src.Address),
			cmp.Compare(a.tgt.Address, b.tgt.Address),
		)
	})
	best := cands[0]
	d := Decision{
		Placement:    v1alpha1.MxlFlowMirrorPlacement{SourceInterface: best.src.Address, TargetInterface: best.tgt.Address},
		SharesSource: best.sharesSource,
		SharesTarget: best.sharesTarget,
		Headroom:     best.headroom,
		Outcome:      OutcomePlaced,
	}
	if best.headroom < 0 {
		if s.Overcommit == OvercommitHold {
			return Decision{Outcome: OutcomeHeld, Headroom: best.headroom}
		}
		d.Outcome = OutcomeOvercommitted
	}
	return d
}

// Check reports the headroom an existing placement leaves for demand,
// without the mirror itself on the ledger.
func Check(l *Ledger, s Settings, req Request, p v1alpha1.MxlFlowMirrorPlacement) int64 {
	src, tgt := l.nodes[req.SourceNode], l.nodes[req.TargetNode]
	find := func(n Node, addr string) v1alpha1.MxlFabricInterface {
		for _, i := range n.Interfaces(req.Provider) {
			if i.Address == addr {
				return i
			}
		}
		return v1alpha1.MxlFabricInterface{Address: addr}
	}
	return l.score(s, src, tgt, find(src, p.SourceInterface), find(tgt, p.TargetInterface), req).headroom
}

func (l *Ledger) score(s Settings, srcNode, tgtNode Node, a, b v1alpha1.MxlFabricInterface, req Request) candidate {
	eg, egN := l.Egress(srcNode.Name, Port(a))
	in, inN := l.Ingress(tgtNode.Name, Port(b))
	c := candidate{
		src: a, tgt: b,
		headroom:     math.MaxInt64,
		load:         eg + in,
		mirrors:      egN + inN,
		sharesSource: slices.Contains(req.AvoidSource, Port(a)),
		sharesTarget: slices.Contains(req.AvoidTarget, Port(b)),
	}
	room := func(capacity int64, util int64, used int64) {
		if capacity <= 0 {
			return
		}
		h := plannable(capacity, util) - used - req.Demand
		c.headroom = min(c.headroom, h)
	}
	su, tu := srcNode.utilization(s.MaxUtilizationPercent), tgtNode.utilization(s.MaxUtilizationPercent)
	room(a.CapacityBitsPerSecond, su, eg)
	room(b.CapacityBitsPerSecond, tu, in)
	for _, bud := range l.budgetsOf(srcNode.Name, a.Address) {
		k := budgetKey{srcNode.Name, bud.Name}
		switch bud.Mode {
		case v1alpha1.FabricBudgetEgress:
			room(bud.CapacityBitsPerSecond, su, l.budgetEgress[k])
		case v1alpha1.FabricBudgetCombined:
			room(bud.CapacityBitsPerSecond, su, l.budgetEgress[k]+l.budgetIngress[k])
		}
	}
	for _, bud := range l.budgetsOf(tgtNode.Name, b.Address) {
		k := budgetKey{tgtNode.Name, bud.Name}
		switch bud.Mode {
		case v1alpha1.FabricBudgetIngress, "":
			room(bud.CapacityBitsPerSecond, tu, l.budgetIngress[k])
		case v1alpha1.FabricBudgetCombined:
			room(bud.CapacityBitsPerSecond, tu, l.budgetEgress[k]+l.budgetIngress[k])
		}
	}
	return c
}

// routable drops link-local addresses when any other remains. Every
// node's link-local addresses share one prefix, so pairing by subnet
// would read them as one network, and an address valid only on its own
// link carries no route between rails.
func routable(ifs []v1alpha1.MxlFabricInterface) []v1alpha1.MxlFabricInterface {
	out := slices.DeleteFunc(slices.Clone(ifs), func(i v1alpha1.MxlFabricInterface) bool {
		a, err := netip.ParseAddr(i.Address)
		return err == nil && (a.IsLinkLocalUnicast() || a.Unmap().IsLinkLocalUnicast())
	})
	if len(out) == 0 {
		return ifs
	}
	return out
}

// plannable is the share of capacity placement may commit.
func plannable(capacity, utilPercent int64) int64 {
	if utilPercent <= 0 || utilPercent >= 100 {
		return capacity
	}
	return capacity / 100 * utilPercent
}

// Demand is the rate a mirror charges for a flow publishing
// bitsPerSecond, with overheadPercent added for protocol framing. A
// flow publishing none charges provisional.
func Demand(bitsPerSecond int64, overheadPercent int32, provisional int64) int64 {
	if bitsPerSecond <= 0 {
		return provisional
	}
	return bitsPerSecond + bitsPerSecond/100*int64(overheadPercent)
}
