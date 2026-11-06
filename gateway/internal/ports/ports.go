// Package ports derives what mirror placement plans against from the
// interfaces libmxl-fabrics enumerates: the capacity of each, the
// shared budgets they draw on, the subnet each address sits in and the
// NUMA node of its device.
//
// Link speed is what a provider reports, and some report none or a
// figure the host cannot sustain; the capacity rules override it.
// Interfaces of one device or one host can share a limit below the sum
// of their links; the budget rules declare it. Both are configuration,
// read from flags, and both are off unless set.
package ports

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// CapacityRule overrides the capacity of the interfaces it matches.
type CapacityRule struct {
	// Match is a device name, an IP address or a CIDR prefix.
	Match string
	Bits  int64

	addr   netip.Addr
	prefix netip.Prefix
}

// ParseCapacities reads "<match>=<quantity>,..." where match is a
// device name, an address or a CIDR and quantity is bits per second.
func ParseCapacities(value string) ([]CapacityRule, error) {
	var out []CapacityRule
	for _, item := range splitList(value) {
		match, qty, ok := strings.Cut(item, "=")
		match = strings.TrimSpace(match)
		if !ok || match == "" {
			return nil, fmt.Errorf("%q: want <device|address|cidr>=<bits per second>", item)
		}
		bits, err := quantity(qty)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", item, err)
		}
		r := CapacityRule{Match: match, Bits: bits}
		if p, err := netip.ParsePrefix(match); err == nil {
			r.prefix = p.Masked()
		} else if a, err := netip.ParseAddr(match); err == nil {
			r.addr = a.Unmap()
		}
		out = append(out, r)
	}
	return out, nil
}

func (r CapacityRule) matches(device, address string) bool {
	a, err := netip.ParseAddr(address)
	switch {
	case r.prefix.IsValid():
		return err == nil && r.prefix.Contains(a.Unmap())
	case r.addr.IsValid():
		return err == nil && r.addr == a.Unmap()
	default:
		return r.Match == device || r.Match == address
	}
}

// Capacity is the first matching rule's figure, or linkSpeed.
func Capacity(rules []CapacityRule, device, address string, linkSpeed int64) int64 {
	for _, r := range rules {
		if r.matches(device, address) {
			return r.Bits
		}
	}
	return linkSpeed
}

// BudgetRule declares one shared cap per scope instance.
type BudgetRule struct {
	Scope mxlv1alpha1.MxlFabricBudgetScope
	Mode  mxlv1alpha1.MxlFabricBudgetMode
	Bits  int64
}

// ParseBudgets reads "<scope>=<quantity>[:<mode>],..." where scope is
// nic or host and mode is ingress (the default), egress or combined.
func ParseBudgets(value string) ([]BudgetRule, error) {
	var out []BudgetRule
	for _, item := range splitList(value) {
		scope, rest, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("%q: want <nic|host>=<bits per second>[:<ingress|egress|combined>]", item)
		}
		r := BudgetRule{Scope: mxlv1alpha1.MxlFabricBudgetScope(strings.TrimSpace(scope)), Mode: mxlv1alpha1.FabricBudgetIngress}
		if r.Scope != mxlv1alpha1.FabricBudgetNIC && r.Scope != mxlv1alpha1.FabricBudgetHost {
			return nil, fmt.Errorf("%q: scope must be nic or host", item)
		}
		qty, mode, hasMode := strings.Cut(rest, ":")
		if hasMode {
			r.Mode = mxlv1alpha1.MxlFabricBudgetMode(strings.TrimSpace(mode))
			switch r.Mode {
			case mxlv1alpha1.FabricBudgetIngress, mxlv1alpha1.FabricBudgetEgress, mxlv1alpha1.FabricBudgetCombined:
			default:
				return nil, fmt.Errorf("%q: mode must be ingress, egress or combined", item)
			}
		}
		bits, err := quantity(qty)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", item, err)
		}
		if bits == 0 {
			return nil, fmt.Errorf("%q: a budget must be positive", item)
		}
		r.Bits = bits
		if slices.ContainsFunc(out, func(o BudgetRule) bool { return o.Scope == r.Scope && o.Mode == r.Mode }) {
			return nil, fmt.Errorf("%q: %s budget in %s mode given twice", item, r.Scope, r.Mode)
		}
		out = append(out, r)
	}
	return out, nil
}

// Member is what budget assignment reads of one interface.
type Member struct {
	Address    string
	PCIAddress string
}

// Assign instantiates rules over the node's interfaces. A nic rule
// makes one budget per PCI device (domain, bus and device; the
// function is what tells the ports of one card apart), over the
// interfaces whose provider reported a PCI address. A host rule makes
// one budget over every interface. It returns the budgets and, per
// address, the names of those it draws on.
func Assign(rules []BudgetRule, members []Member) ([]mxlv1alpha1.MxlFabricBudget, map[string][]string) {
	var budgets []mxlv1alpha1.MxlFabricBudget
	names := map[string][]string{}
	add := func(name string, r BudgetRule, addrs []string) {
		if len(addrs) == 0 {
			return
		}
		budgets = append(budgets, mxlv1alpha1.MxlFabricBudget{Name: name, Scope: r.Scope, Mode: r.Mode, CapacityBitsPerSecond: r.Bits})
		for _, a := range addrs {
			if !slices.Contains(names[a], name) {
				names[a] = append(names[a], name)
			}
		}
	}
	for _, r := range rules {
		switch r.Scope {
		case mxlv1alpha1.FabricBudgetHost:
			var addrs []string
			for _, m := range members {
				addrs = append(addrs, m.Address)
			}
			add(fmt.Sprintf("host-%s", r.Mode), r, addrs)
		case mxlv1alpha1.FabricBudgetNIC:
			devices := map[string][]string{}
			var order []string
			for _, m := range members {
				dev, ok := pciDevice(m.PCIAddress)
				if !ok {
					continue
				}
				if _, seen := devices[dev]; !seen {
					order = append(order, dev)
				}
				devices[dev] = append(devices[dev], m.Address)
			}
			for _, dev := range order {
				add(fmt.Sprintf("nic-%s-%s", strings.ReplaceAll(dev, ":", "-"), r.Mode), r, devices[dev])
			}
		}
	}
	return budgets, names
}

// pciDevice strips the function from a domain:bus:device.function
// address.
func pciDevice(pci string) (string, bool) {
	i := strings.LastIndex(pci, ".")
	if pci == "" || i < 0 {
		return "", false
	}
	return pci[:i], true
}

// Host reads the facts the host, not libmxl-fabrics, reports.
type Host struct {
	// SysRoot is where sysfs is mounted; empty is /sys.
	SysRoot string
	// Addrs lists the host's interface addresses; nil reads them
	// through the net package.
	Addrs func() ([]net.Addr, error)
}

// Subnet returns the prefix of the host address equal to address, in
// CIDR form, or "" when address is not an IP or no host interface
// carries it.
func (h Host) Subnet(address string) string {
	want, err := netip.ParseAddr(address)
	if err != nil {
		return ""
	}
	list := h.Addrs
	if list == nil {
		list = net.InterfaceAddrs
	}
	addrs, err := list()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(n.IP)
		if !ok || ip.Unmap() != want.Unmap() {
			continue
		}
		ones, _ := n.Mask.Size()
		return netip.PrefixFrom(ip.Unmap(), ones).Masked().String()
	}
	return ""
}

// NUMANode returns the NUMA node sysfs reports for the PCI device, or
// nil when it reports none (-1) or the device is not there.
func (h Host) NUMANode(pci string) *int32 {
	if pci == "" {
		return nil
	}
	root := h.SysRoot
	if root == "" {
		root = "/sys"
	}
	b, err := os.ReadFile(filepath.Join(root, "bus", "pci", "devices", pci, "numa_node"))
	if err != nil {
		return nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 32)
	if err != nil || n < 0 {
		return nil
	}
	v := int32(n)
	return &v
}

func quantity(s string) (int64, error) {
	q, err := resource.ParseQuantity(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	v, ok := q.AsInt64()
	if !ok || v < 0 {
		return 0, fmt.Errorf("%q is not a non-negative whole number of bits per second", s)
	}
	return v, nil
}

func splitList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
