package fabric

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
)

// The attr documents below are what the libmxl-fabrics enumeration
// reports; the tcp and shm ones are verbatim from a run against the
// go-mxl builder image, so the parse is pinned to real output rather
// than to a guess at the schema.
const (
	attrTCPeth0 = `{"device_name":"eth0","ep_addr_format":"FI_SOCKADDR_IN",` +
		`"ep_protocol":"FI_PROTO_SOCK_TCP","ep_type":"FI_EP_MSG",` +
		`"fi_domain_cq_cnt":256,"fi_domain_ep_cnt":8192,"fi_domain_mr_cnt":0,` +
		`"fi_domain_name":"eth0"}`

	attrTCPloopback = `{"device_name":"lo","ep_addr_format":"FI_SOCKADDR_IN",` +
		`"ep_protocol":"FI_PROTO_SOCK_TCP","ep_type":"FI_EP_MSG",` +
		`"fi_domain_cq_cnt":256,"fi_domain_ep_cnt":8192,"fi_domain_mr_cnt":0,` +
		`"fi_domain_name":"lo"}`

	attrSHM = `{"ep_addr_format":"FI_ADDR_STR","ep_protocol":"FI_PROTO_SHM",` +
		`"ep_type":"FI_EP_RDM","fi_domain_cq_cnt":1024,"fi_domain_ep_cnt":256,` +
		`"fi_domain_mr_cnt":0,"fi_domain_name":"shm"}`

	// A NIC-backed interface additionally carries the link and bus
	// attributes libfabric reads off fid_nic.
	attrVerbsNIC = `{"device_name":"mlx5_0","device_driver":"mlx5","link_state":"up",` +
		`"link_speed":100000000000,"link_type":"Ethernet","pci_domain_id":0,` +
		`"pci_bus_id":65,"pci_device_id":0,"pci_function_id":1,` +
		`"fi_domain_name":"mlx5_0"}`
)

func iface(provider fabrics.Provider, node, attr string) fabrics.InterfaceConfig {
	return fabrics.InterfaceConfig{
		Provider: provider,
		Caps:     fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapRemoteWrite},
		Address:  fabrics.EndpointAddress{Node: node},
		Attr:     attr,
	}
}

func TestParseAttributesReadsTheNICDetail(t *testing.T) {
	got, err := ParseAttributes(attrVerbsNIC)
	require.NoError(t, err)
	assert.Equal(t, "mlx5_0", got.Device)
	assert.Equal(t, "up", got.LinkState)
	assert.Equal(t, uint64(100000000000), got.LinkSpeed)
	assert.Equal(t, "0000:41:00.1", got.PCIAddress,
		"the PCI address is the stable identity of a NIC across renames, so the "+
			"four numeric keys have to render in the form lspci prints")
}

func TestParseAttributesToleratesAnInterfaceWithNoNIC(t *testing.T) {
	// Verified against the builder image: a container's veth and lo
	// carry the endpoint and domain keys but no fid_nic, so the link
	// and PCI keys are absent rather than zero.
	got, err := ParseAttributes(attrTCPeth0)
	require.NoError(t, err)
	assert.Equal(t, "eth0", got.Device)
	assert.Empty(t, got.LinkState,
		"an absent link_state must not read as a link that is down")
	assert.Zero(t, got.LinkSpeed)
	assert.Empty(t, got.PCIAddress)
}

func TestParseAttributesEmptyAndMalformed(t *testing.T) {
	got, err := ParseAttributes("")
	require.NoError(t, err, "an interface with no attr is not an error")
	assert.Equal(t, Attributes{}, got)

	_, err = ParseAttributes("{not json")
	require.Error(t, err)
}

func TestParseAttributesSkipsAPartialPCIAddress(t *testing.T) {
	// Rendering a half-reported address would print 0000:00:00.0 and
	// read like a real device sitting at the root of the bus.
	got, err := ParseAttributes(`{"pci_domain_id":0,"pci_bus_id":65}`)
	require.NoError(t, err)
	assert.Empty(t, got.PCIAddress)
}

func TestSelectRejectsLoopback(t *testing.T) {
	// libfabric enumerates lo for the tcp provider on every host. A
	// peer handed 127.0.0.1 in a TargetInfo dials itself, and every
	// mirror the gateway sets up spans two nodes.
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "127.0.0.1", attrTCPloopback),
		iface(fabrics.ProviderTCP, "::1", attrTCPloopback),
		iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0),
	}
	kept, rejected := Selector{}.Select(in)
	require.Len(t, kept, 1)
	assert.Equal(t, "10.20.53.13", kept[0].Address.Node)
	assert.Len(t, rejected, 2)
}

func TestSelectKeepsEverythingTransferableByDefault(t *testing.T) {
	// The zero Selector is what a gateway that names no fabric runs
	// with, and it must not start excluding hardware on its own.
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderVerbs, "10.20.53.13", attrVerbsNIC),
		iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0),
		iface(fabrics.ProviderSHM, "node-a", attrSHM),
	}
	kept, rejected := Selector{}.Select(in)
	assert.Len(t, kept, 3)
	assert.Empty(t, rejected)
}

func TestSelectRejectsInterfacesThatCannotTransfer(t *testing.T) {
	in := []fabrics.InterfaceConfig{{
		Provider: fabrics.ProviderVerbs,
		Caps:     fabrics.InterfaceCaps{Flags: fabrics.InterfaceCapSendReceive},
		Address:  fabrics.EndpointAddress{Node: "10.20.53.13"},
	}}
	kept, rejected := Selector{}.Select(in)
	assert.Empty(t, kept)
	require.Len(t, rejected, 1)
	assert.Contains(t, rejected[0].Reason, "remote-write")
}

func TestSelectNarrowsToTheFabricCIDR(t *testing.T) {
	// The case the whole package exists for: a media node carrying an
	// ST 2110 NIC, a management NIC and the MXL fabric. Only the
	// fabric may carry a mirror, and no property of the hardware says
	// which is which.
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "192.168.100.20", attrTCPeth0), // 2110
		iface(fabrics.ProviderTCP, "172.16.0.20", attrTCPeth0),    // management
		iface(fabrics.ProviderVerbs, "10.20.53.13", attrVerbsNIC), // fabric
		iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0),    // fabric
	}
	sel := Selector{CIDRs: []netip.Prefix{netip.MustParsePrefix("10.20.53.0/24")}}

	kept, rejected := sel.Select(in)
	require.Len(t, kept, 2)
	for _, k := range kept {
		assert.Equal(t, "10.20.53.13", k.Address.Node)
	}
	assert.Len(t, rejected, 2)
	assert.Contains(t, rejected[0].Reason, "outside the fabric CIDRs")
}

func TestSelectRejectsANonAddressOnlyWhenCIDRsNarrow(t *testing.T) {
	// The shm provider reports a hostname where the others report an
	// address, so it cannot be matched against a CIDR at all.
	in := []fabrics.InterfaceConfig{iface(fabrics.ProviderSHM, "node-a", attrSHM)}

	kept, _ := Selector{}.Select(in)
	assert.Len(t, kept, 1, "an unnarrowed fabric keeps shm")

	sel := Selector{CIDRs: []netip.Prefix{netip.MustParsePrefix("10.20.53.0/24")}}
	kept, rejected := sel.Select(in)
	assert.Empty(t, kept)
	require.Len(t, rejected, 1)
	assert.Contains(t, rejected[0].Reason, "not an IP")
}

func TestSelectNarrowsToTheNamedDevices(t *testing.T) {
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderVerbs, "10.20.53.13", attrVerbsNIC),
		iface(fabrics.ProviderTCP, "10.20.53.14", attrTCPeth0),
	}
	kept, rejected := Selector{Devices: []string{"mlx5_0"}}.Select(in)
	require.Len(t, kept, 1)
	assert.Equal(t, fabrics.ProviderVerbs, kept[0].Provider)
	require.Len(t, rejected, 1)
	assert.Equal(t, "eth0", rejected[0].Device)
}

func TestSelectRejectsAnUnnamedDeviceWhenDevicesNarrow(t *testing.T) {
	// Nothing shows the interface is one of the named devices, and
	// admitting it would let the declaration be silently bypassed.
	in := []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "")}
	kept, rejected := Selector{Devices: []string{"mlx5_0"}}.Select(in)
	assert.Empty(t, kept)
	assert.Len(t, rejected, 1)
}

func TestSelectRejectsADownLink(t *testing.T) {
	down := `{"device_name":"mlx5_1","link_state":"down","link_speed":100000000000}`
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderVerbs, "10.20.53.13", attrVerbsNIC),
		iface(fabrics.ProviderVerbs, "10.20.53.14", down),
	}
	kept, rejected := Selector{}.Select(in)
	require.Len(t, kept, 1)
	assert.Equal(t, "10.20.53.13", kept[0].Address.Node)
	require.Len(t, rejected, 1)
	assert.Contains(t, rejected[0].Reason, "link is down")
}

func TestSelectAppliesTheLinkSpeedFloor(t *testing.T) {
	slow := `{"device_name":"eno1","link_state":"up","link_speed":1000000000}`
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderVerbs, "10.20.53.13", attrVerbsNIC),
		iface(fabrics.ProviderTCP, "10.20.53.14", slow),
	}
	kept, rejected := Selector{MinLinkSpeed: 25_000_000_000}.Select(in)
	require.Len(t, kept, 1)
	assert.Equal(t, "mlx5_0", mustAttrs(t, kept[0]).Device)
	require.Len(t, rejected, 1)
	assert.Contains(t, rejected[0].Reason, "below the")
}

func TestSelectRejectsAnUnknownLinkSpeedAgainstAFloor(t *testing.T) {
	// An interface with no NIC behind it reports no speed, so it
	// cannot be shown to clear the floor. Admitting it would put the
	// slowest interfaces on the node through a rule meant to keep
	// them out.
	in := []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0)}
	kept, rejected := Selector{MinLinkSpeed: 25_000_000_000}.Select(in)
	assert.Empty(t, kept)
	assert.Len(t, rejected, 1)
}

func TestSelectKeepsAnInterfaceWithUnreadableAttr(t *testing.T) {
	// Detail is best-effort; an interface still transfers without it.
	in := []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "{not json")}
	kept, rejected := Selector{}.Select(in)
	assert.Len(t, kept, 1)
	assert.Empty(t, rejected)
}

func TestDedupeCollapsesAnAddressReportedSeveralTimes(t *testing.T) {
	// libfabric returns one entry per endpoint type and protocol, so
	// a single NIC arrives repeatedly. The published interface list
	// is keyed by address and would be rejected by the apiserver with
	// duplicates in it.
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0),
		iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0),
		iface(fabrics.ProviderTCP, "10.20.53.14", attrTCPeth0),
	}
	assert.Len(t, Dedupe(in), 2)
}

func TestDeviceCountCountsDevicesNotEntries(t *testing.T) {
	// One NIC with an IPv4 and an IPv6 address, enumerated twice per
	// address, is one device. Counting entries would report four.
	v6 := `{"device_name":"eth0","fi_domain_name":"eth0"}`
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0),
		iface(fabrics.ProviderTCP, "10.20.53.13", attrTCPeth0),
		iface(fabrics.ProviderTCP, "fd00::13", v6),
		iface(fabrics.ProviderTCP, "fd00::13", v6),
	}
	assert.Equal(t, 1, DeviceCount(in))
}

func TestDeviceCountFallsBackToTheAddress(t *testing.T) {
	// A provider that names no device still has to count for
	// something, or its capability would read as absent hardware.
	in := []fabrics.InterfaceConfig{
		iface(fabrics.ProviderEFA, "10.20.53.13", ""),
		iface(fabrics.ProviderEFA, "10.20.53.14", ""),
	}
	assert.Equal(t, 2, DeviceCount(in))
}

func TestDeviceCountEmpty(t *testing.T) {
	assert.Equal(t, 0, DeviceCount(nil))
}

func mustAttrs(t *testing.T, cfg fabrics.InterfaceConfig) Attributes {
	t.Helper()
	attrs, err := ParseAttributes(cfg.Attr)
	require.NoError(t, err)
	return attrs
}
