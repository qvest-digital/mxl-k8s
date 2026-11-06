package ports

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestCapacityRules(t *testing.T) {
	rules, err := ParseCapacities("192.0.2.7=10G, 198.51.100.0/24=40G,fab1=25G")
	require.NoError(t, err)

	assert.Equal(t, int64(10_000_000_000), Capacity(rules, "fab0", "192.0.2.7", 100))
	assert.Equal(t, int64(40_000_000_000), Capacity(rules, "fab0", "198.51.100.9", 100), "a CIDR matches its addresses")
	assert.Equal(t, int64(40_000_000_000), Capacity(rules, "fab0", "::ffff:198.51.100.9", 100), "mapped IPv4 is the same address")
	assert.Equal(t, int64(25_000_000_000), Capacity(rules, "fab1", "2001:db8::1", 100), "a bare name matches the device")
	assert.Equal(t, int64(100), Capacity(rules, "fab2", "2001:db8::2", 100), "no match keeps the reported link speed")
	assert.Equal(t, int64(0), Capacity(nil, "", "192.0.2.9", 0))

	// First match wins, so a specific address can carve out of a prefix.
	rules, err = ParseCapacities("192.0.2.1=1G,192.0.2.0/24=2G")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000_000), Capacity(rules, "", "192.0.2.1", 0))
	assert.Equal(t, int64(2_000_000_000), Capacity(rules, "", "192.0.2.2", 0))

	for _, bad := range []string{"fab0", "=10G", "fab0=fast", "fab0=-1"} {
		_, err := ParseCapacities(bad)
		assert.Error(t, err, bad)
	}
	rules, err = ParseCapacities("")
	require.NoError(t, err)
	assert.Empty(t, rules)
}

func TestParseBudgets(t *testing.T) {
	rules, err := ParseBudgets("nic=40G, host=60G:combined,host=45G:egress")
	require.NoError(t, err)
	assert.Equal(t, []BudgetRule{
		{Scope: mxlv1alpha1.FabricBudgetNIC, Mode: mxlv1alpha1.FabricBudgetIngress, Bits: 40_000_000_000},
		{Scope: mxlv1alpha1.FabricBudgetHost, Mode: mxlv1alpha1.FabricBudgetCombined, Bits: 60_000_000_000},
		{Scope: mxlv1alpha1.FabricBudgetHost, Mode: mxlv1alpha1.FabricBudgetEgress, Bits: 45_000_000_000},
	}, rules, "mode defaults to ingress")

	for _, bad := range []string{"rack=10G", "nic", "nic=10G:both", "nic=0", "nic=10G,nic=20G:ingress"} {
		_, err := ParseBudgets(bad)
		assert.Error(t, err, bad)
	}
}

func TestAssign(t *testing.T) {
	rules := []BudgetRule{
		{Scope: mxlv1alpha1.FabricBudgetNIC, Mode: mxlv1alpha1.FabricBudgetIngress, Bits: 40},
		{Scope: mxlv1alpha1.FabricBudgetHost, Mode: mxlv1alpha1.FabricBudgetCombined, Bits: 60},
	}
	members := []Member{
		{Address: "192.0.2.1", PCIAddress: "0000:3b:00.0"},
		{Address: "192.0.2.129", PCIAddress: "0000:3b:00.1"},
		{Address: "198.51.100.1", PCIAddress: "0000:5e:00.0"},
		{Address: "198.51.100.9"},
	}
	budgets, names := Assign(rules, members)
	assert.Equal(t, []mxlv1alpha1.MxlFabricBudget{
		{Name: "nic-0000-3b-00-ingress", Scope: mxlv1alpha1.FabricBudgetNIC, Mode: mxlv1alpha1.FabricBudgetIngress, CapacityBitsPerSecond: 40},
		{Name: "nic-0000-5e-00-ingress", Scope: mxlv1alpha1.FabricBudgetNIC, Mode: mxlv1alpha1.FabricBudgetIngress, CapacityBitsPerSecond: 40},
		{Name: "host-combined", Scope: mxlv1alpha1.FabricBudgetHost, Mode: mxlv1alpha1.FabricBudgetCombined, CapacityBitsPerSecond: 60},
	}, budgets)
	assert.Equal(t, []string{"nic-0000-3b-00-ingress", "host-combined"}, names["192.0.2.1"], "the two functions of one card share its budget")
	assert.Equal(t, []string{"nic-0000-3b-00-ingress", "host-combined"}, names["192.0.2.129"])
	assert.Equal(t, []string{"nic-0000-5e-00-ingress", "host-combined"}, names["198.51.100.1"])
	assert.Equal(t, []string{"host-combined"}, names["198.51.100.9"], "no PCI address, no device budget")

	budgets, names = Assign(nil, members)
	assert.Empty(t, budgets, "off unless configured")
	assert.Empty(t, names)
}

func TestSubnet(t *testing.T) {
	h := Host{Addrs: func() ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.0.2.130").To4(), Mask: net.CIDRMask(25, 32)},
			&net.IPNet{IP: net.ParseIP("2001:db8::5"), Mask: net.CIDRMask(64, 128)},
		}, nil
	}}
	assert.Equal(t, "192.0.2.128/25", h.Subnet("192.0.2.130"))
	assert.Equal(t, "192.0.2.128/25", h.Subnet("::ffff:192.0.2.130"))
	assert.Equal(t, "2001:db8::/64", h.Subnet("2001:db8::5"))
	assert.Equal(t, "", h.Subnet("192.0.2.1"), "an address no host interface carries")
	assert.Equal(t, "", h.Subnet("fe80::1%eth0x"), "not an address")
}

func TestNUMANode(t *testing.T) {
	root := t.TempDir()
	write := func(pci, v string) {
		dir := filepath.Join(root, "bus", "pci", "devices", pci)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "numa_node"), []byte(v), 0o644))
	}
	write("0000:3b:00.0", "1\n")
	write("0000:5e:00.0", "-1\n")
	h := Host{SysRoot: root}

	n := h.NUMANode("0000:3b:00.0")
	require.NotNil(t, n)
	assert.Equal(t, int32(1), *n)
	assert.Nil(t, h.NUMANode("0000:5e:00.0"), "-1 is no NUMA affinity")
	assert.Nil(t, h.NUMANode("0000:af:00.0"))
	assert.Nil(t, h.NUMANode(""))
}
