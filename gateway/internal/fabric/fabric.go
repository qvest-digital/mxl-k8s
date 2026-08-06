// Package fabric narrows a libmxl-fabrics interface enumeration to the
// interfaces an operator has declared usable for MXL traffic.
//
// A node in a broadcast plant carries several NICs with different
// jobs: ST 2110 essence, the MXL fabric, cluster traffic, out-of-band
// management. libmxl-fabrics enumerates all of them, and nothing it
// reports says which is which - that is site policy, not a property of
// the hardware. Without a declaration the gateway would advertise, and
// could bind, an interface on a network the plant reserves for
// something else.
//
// The same Selector runs on the capability probe and on the per-mirror
// setup path, so what a node advertises is exactly what it is willing
// to bind. A node cannot promise an interface a mirror would then be
// refused, and cannot hide one it would silently pick.
package fabric

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"

	"github.com/qvest-digital/go-mxl/fabrics"
)

// Attributes is the device detail this gateway reads out of the attr
// document libmxl-fabrics attaches to an enumerated interface.
//
// Every field is optional in the source: the header calls attr
// best-effort, and which keys appear depends on the provider and the
// hardware. An interface with no NIC behind it - loopback, a veth pair
// - carries the endpoint and domain keys but no link or PCI ones.
type Attributes struct {
	// Device is the name the provider reports for the underlying
	// device. The tcp provider fills it from the libfabric domain
	// name, which is the kernel netdev name.
	Device string

	// LinkState is "up", "down", or "unknown". Empty when the
	// provider reported no link attributes at all, which is not the
	// same as a link that is down.
	LinkState string

	// LinkSpeed is the link speed in bits per second, zero when the
	// provider reported none.
	LinkSpeed uint64

	// PCIAddress is domain:bus:device.function, empty unless the
	// provider reported a PCI bus attachment.
	PCIAddress string
}

// LinkStateDown is the value libmxl-fabrics reports for a link that is
// administratively or physically down.
const LinkStateDown = "down"

// rawAttributes mirrors the keys of the attr document this gateway
// reads. Pointers distinguish a key that is absent from one reported
// as zero, which is the difference between "no NIC behind this
// interface" and "a NIC that reports a zero speed". Keys outside this
// set are ignored, so libmxl-fabrics can add to the document without
// breaking the parse.
type rawAttributes struct {
	DeviceName  string   `json:"device_name"`
	LinkState   string   `json:"link_state"`
	LinkSpeed   *float64 `json:"link_speed"`
	PCIDomain   *float64 `json:"pci_domain_id"`
	PCIBus      *float64 `json:"pci_bus_id"`
	PCIDevice   *float64 `json:"pci_device_id"`
	PCIFunction *float64 `json:"pci_function_id"`
}

// ParseAttributes reads the device detail out of an interface's attr
// document. An empty document yields the zero Attributes and no error;
// a malformed one yields the zero Attributes and an error, and callers
// treat it as an interface with no detail rather than an unusable one.
func ParseAttributes(attr string) (Attributes, error) {
	if attr == "" {
		return Attributes{}, nil
	}
	var raw rawAttributes
	if err := json.Unmarshal([]byte(attr), &raw); err != nil {
		return Attributes{}, fmt.Errorf("parse interface attr: %w", err)
	}

	out := Attributes{
		Device:    raw.DeviceName,
		LinkState: raw.LinkState,
	}
	if raw.LinkSpeed != nil && *raw.LinkSpeed > 0 {
		out.LinkSpeed = uint64(*raw.LinkSpeed)
	}
	// The bus keys are written as a set or not at all, but a partial
	// document would otherwise render as "0:0:0.0" and read like a
	// real address.
	if raw.PCIDomain != nil && raw.PCIBus != nil && raw.PCIDevice != nil && raw.PCIFunction != nil {
		out.PCIAddress = fmt.Sprintf("%04x:%02x:%02x.%x",
			int(*raw.PCIDomain), int(*raw.PCIBus), int(*raw.PCIDevice), int(*raw.PCIFunction))
	}
	return out, nil
}

// Selector declares which interfaces may carry MXL traffic on this
// node. The zero Selector accepts every interface that can transfer,
// which is the behaviour of a gateway that names no fabric.
type Selector struct {
	// CIDRs restricts interfaces to those whose address falls inside
	// one of these prefixes. Empty imposes no restriction.
	CIDRs []netip.Prefix

	// Devices restricts interfaces to those the provider names in
	// this list. Empty imposes no restriction. An interface the
	// provider reports no device for is rejected when this is set,
	// since it cannot be shown to be one of the named devices.
	Devices []string

	// MinLinkSpeed rejects interfaces slower than this many bits per
	// second. Zero imposes no restriction. An interface the provider
	// reports no link speed for is rejected when this is set: it
	// cannot be shown to clear the floor, and most interfaces with
	// no NIC behind them report none.
	MinLinkSpeed uint64
}

// Rejection records one interface the Selector dropped, so an operator
// who ends up with an empty advertisement can see what was discarded
// and on which rule.
type Rejection struct {
	Provider fabrics.Provider
	Address  string
	Device   string
	Reason   string
}

func (r Rejection) String() string {
	return fmt.Sprintf("%s %s (%s): %s", r.Provider, r.Address, r.Device, r.Reason)
}

// Select keeps the interfaces that can carry a mirror and that the
// operator has declared usable, returning the survivors in their
// original order alongside everything it dropped.
func (s Selector) Select(ifaces []fabrics.InterfaceConfig) ([]fabrics.InterfaceConfig, []Rejection) {
	var (
		kept     []fabrics.InterfaceConfig
		rejected []Rejection
	)
	for _, iface := range ifaces {
		attrs, err := ParseAttributes(iface.Attr)
		reject := func(reason string) {
			rejected = append(rejected, Rejection{
				Provider: iface.Provider,
				Address:  iface.Address.Node,
				Device:   attrs.Device,
				Reason:   reason,
			})
		}
		if err != nil {
			// Not fatal on its own: an interface with no readable
			// detail still transfers. It only fails the rules that
			// need detail, below.
			attrs = Attributes{}
		}

		// Remote write is the only transfer mode libmxl-fabrics
		// implements, and a setup handed anything else is refused
		// further down.
		if iface.Caps.Flags&fabrics.InterfaceCapRemoteWrite == 0 {
			reject("no remote-write capability")
			continue
		}
		if reason, ok := s.rejectAddress(iface.Address.Node); !ok {
			reject(reason)
			continue
		}
		if len(s.Devices) > 0 && !slices.Contains(s.Devices, attrs.Device) {
			reject("device not in the fabric device list")
			continue
		}
		if attrs.LinkState == LinkStateDown {
			reject("link is down")
			continue
		}
		if s.MinLinkSpeed > 0 && attrs.LinkSpeed < s.MinLinkSpeed {
			reject(fmt.Sprintf("link speed %d below the %d floor", attrs.LinkSpeed, s.MinLinkSpeed))
			continue
		}
		kept = append(kept, iface)
	}
	return kept, rejected
}

// rejectAddress applies the rules that read the interface address.
func (s Selector) rejectAddress(node string) (string, bool) {
	addr, err := netip.ParseAddr(node)
	if err != nil {
		// The shm provider reports a hostname here rather than an
		// address. Nothing addressable is being claimed, so it only
		// survives while no CIDR list narrows the fabric.
		if len(s.CIDRs) > 0 {
			return "address is not an IP and cannot be matched against the fabric CIDRs", false
		}
		return "", true
	}
	// Every mirror the gateway sets up spans two nodes, and libfabric
	// enumerates loopback for the tcp provider on every host. A peer
	// handed a loopback address in a TargetInfo dials itself.
	if addr.IsLoopback() {
		return "loopback address cannot carry a cross-node mirror", false
	}
	if len(s.CIDRs) == 0 {
		return "", true
	}
	for _, cidr := range s.CIDRs {
		if cidr.Contains(addr) {
			return "", true
		}
	}
	return "address outside the fabric CIDRs", false
}

// Dedupe collapses interfaces that share an address, keeping the
// first. libfabric reports one entry per endpoint type and protocol,
// so a single NIC arrives several times over.
func Dedupe(ifaces []fabrics.InterfaceConfig) []fabrics.InterfaceConfig {
	seen := make(map[string]struct{}, len(ifaces))
	out := make([]fabrics.InterfaceConfig, 0, len(ifaces))
	for _, iface := range ifaces {
		if _, dup := seen[iface.Address.Node]; dup {
			continue
		}
		seen[iface.Address.Node] = struct{}{}
		out = append(out, iface)
	}
	return out
}

// DeviceCount counts the distinct devices behind ifaces, keyed by the
// name the provider reports and falling back to the address where it
// reports none. A NIC carrying an IPv4 and an IPv6 address enumerates
// twice and counts once; two NICs a provider cannot name count
// separately, which overcounts nothing that a mirror could not be set
// up on.
func DeviceCount(ifaces []fabrics.InterfaceConfig) int {
	seen := make(map[string]struct{}, len(ifaces))
	for _, iface := range ifaces {
		key := iface.Address.Node
		if attrs, err := ParseAttributes(iface.Attr); err == nil && attrs.Device != "" {
			key = attrs.Device
		}
		seen[key] = struct{}{}
	}
	return len(seen)
}
