package portmetrics

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/go-logr/logr"
	"github.com/mdlayher/genetlink"
	"github.com/mdlayher/genetlink/genltest"
	"github.com/mdlayher/netlink"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// fakeSys builds a sysfs tree under a temporary root.
type fakeSys struct {
	t    *testing.T
	root string
}

func newFakeSys(t *testing.T) *fakeSys { return &fakeSys{t: t, root: t.TempDir()} }

func (f *fakeSys) file(rel, content string) {
	f.t.Helper()
	p := filepath.Join(f.root, rel)
	require.NoError(f.t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(f.t, os.WriteFile(p, []byte(content+"\n"), 0o644))
}

func (f *fakeSys) dir(rel string) {
	f.t.Helper()
	require.NoError(f.t, os.MkdirAll(filepath.Join(f.root, rel), 0o755))
}

// netdev adds a device-backed netdev with statistics.
func (f *fakeSys) netdev(name string, rx, tx uint64) {
	base := "class/net/" + name
	f.dir(base + "/device")
	f.file(base+"/statistics/rx_bytes", itoa(rx))
	f.file(base+"/statistics/tx_bytes", itoa(tx))
	f.file(base+"/statistics/rx_dropped", "3")
	f.file(base+"/carrier_changes", "4")
	f.file(base+"/operstate", "up")
	f.file(base+"/speed", "25000")
}

// rdma adds an RDMA device with one port bound to netdev.
func (f *fakeSys) rdma(dev, netdev string) {
	base := "class/infiniband/" + dev + "/ports/1"
	f.file(base+"/state", "4: ACTIVE")
	f.file(base+"/rate", "100 Gb/sec (4X EDR)")
	if netdev != "" {
		f.file(base+"/gid_attrs/ndevs/0", netdev)
		f.dir("class/net/" + netdev + "/device/infiniband/" + dev)
		f.dir("class/infiniband/" + dev + "/device/net/" + netdev)
	}
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

func TestResolveFindsTheLinkFromEitherSide(t *testing.T) {
	f := newFakeSys(t)
	f.netdev("eth1", 0, 0)
	f.rdma("rdma1", "eth1")
	f.rdma("rdma2", "")    // a device with no netdev
	f.dir("class/net/br0") // no device behind it
	s := Sysfs{Root: f.root, NetdevOf: func(addr string) string {
		if addr == "192.0.2.10" {
			return "eth1"
		}
		return ""
	}}

	want := Link{Name: "eth1", Netdev: "eth1", RDMADevice: "rdma1", RDMAPort: "1"}
	for _, tc := range []struct{ device, address string }{{"rdma1", ""}, {"eth1", ""}, {"", "192.0.2.10"}} {
		l, ok := s.Resolve(tc.device, tc.address)
		require.True(t, ok, tc)
		require.Equal(t, want, l, tc)
	}
	l, ok := s.Resolve("rdma2", "")
	require.True(t, ok)
	require.Equal(t, Link{Name: "rdma2", RDMADevice: "rdma2", RDMAPort: "1"}, l, "named by its RDMA device")

	_, ok = s.Resolve("br0", "")
	require.False(t, ok, "an interface without a device is not a link")
	_, ok = s.Resolve("", "198.51.100.1")
	require.False(t, ok)
}

func TestReadScalesPortDataAndFallsBackToHardwareBytes(t *testing.T) {
	f := newFakeSys(t)
	f.netdev("eth1", 100, 200)
	f.rdma("rdma1", "eth1")
	f.file("class/infiniband/rdma1/ports/1/counters/port_rcv_data", "1000")
	f.file("class/infiniband/rdma1/ports/1/counters/port_xmit_data", "2000")
	f.file("class/infiniband/rdma1/ports/1/counters/link_downed", "1")
	f.file("class/infiniband/rdma1/ports/1/counters/port_xmit_wait", "77")
	f.rdma("rdma2", "")
	f.file("class/infiniband/rdma2/ports/1/hw_counters/rx_bytes", "5")
	f.file("class/infiniband/rdma2/ports/1/hw_counters/tx_bytes", "6")
	s := Sysfs{Root: f.root}

	c := s.Read(Link{Name: "eth1", Netdev: "eth1", RDMADevice: "rdma1", RDMAPort: "1"})
	require.Equal(t, map[string]uint64{SourceNetdev: 100, SourceRDMA: 4000}, c.RxBytes, "port data counts four-octet units")
	require.Equal(t, map[string]uint64{SourceNetdev: 200, SourceRDMA: 8000}, c.TxBytes)
	require.Equal(t, uint64(3), c.Errors[[2]string{SourceNetdev, "rx_dropped"}])
	require.Equal(t, uint64(1), c.Errors[[2]string{SourceRDMA, "link_downed"}])
	require.NotContains(t, c.Errors, [2]string{SourceNetdev, "tx_errors"}, "a counter the kernel does not report is left out")
	require.True(t, c.HaveUp && c.Up)
	require.Equal(t, 25e9, c.SpeedBits, "the netdev's speed wins")
	require.Equal(t, uint64(77), c.XmitWait)

	c = s.Read(Link{Name: "rdma2", RDMADevice: "rdma2", RDMAPort: "1"})
	require.Equal(t, map[string]uint64{SourceRDMA: 5}, c.RxBytes)
	require.Equal(t, map[string]uint64{SourceRDMA: 6}, c.TxBytes)
	require.True(t, c.HaveUp && c.Up, "the RDMA port state stands in for a missing netdev")
	require.Equal(t, 100e9, c.SpeedBits)
	require.False(t, c.HaveCarrierChanges)
}

type fakeEthtool struct {
	mac      MACStats
	pause    PauseStats
	err      error
	macCalls int
}

func (f *fakeEthtool) MACStats(string) (MACStats, error) {
	f.macCalls++
	return f.mac, f.err
}
func (f *fakeEthtool) PauseStats(string) (PauseStats, error) { return f.pause, f.err }

type sample struct {
	name   string
	labels map[string]string
	value  float64
}

func gather(t *testing.T, c prometheus.Collector) []sample {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	fams, err := reg.Gather()
	require.NoError(t, err)
	var out []sample
	for _, fam := range fams {
		for _, m := range fam.GetMetric() {
			s := sample{name: fam.GetName(), labels: map[string]string{}}
			for _, l := range m.GetLabel() {
				s.labels[l.GetName()] = l.GetValue()
			}
			if m.GetCounter() != nil {
				s.value = m.GetCounter().GetValue()
			} else {
				s.value = m.GetGauge().GetValue()
			}
			out = append(out, s)
		}
	}
	return out
}

func find(t *testing.T, got []sample, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	for _, s := range got {
		if s.name != name {
			continue
		}
		ok := true
		for k, v := range labels {
			ok = ok && s.labels[k] == v
		}
		if ok {
			return s.value, true
		}
	}
	return 0, false
}

func numa(n int32) *int32 { return &n }

func TestCollectorExportsPortsLinksAndPolicy(t *testing.T) {
	f := newFakeSys(t)
	f.netdev("eth1", 100, 200)
	f.rdma("rdma1", "eth1")
	f.file("class/infiniband/rdma1/ports/1/counters/port_rcv_data", "10")
	f.file("class/infiniband/rdma1/ports/1/counters/port_xmit_data", "20")
	f.dir("class/net/br0")

	snap := Snapshot{
		MaxUtilizationPercent: 70,
		Budgets: []mxlv1alpha1.MxlFabricBudget{{Name: "host-ingress", Scope: mxlv1alpha1.FabricBudgetHost,
			Mode: mxlv1alpha1.FabricBudgetIngress, CapacityBitsPerSecond: 40e9}},
		Providers: []mxlv1alpha1.MxlFabricsProviderCapability{
			{Name: mxlv1alpha1.ProviderVerbs, Interfaces: []mxlv1alpha1.MxlFabricInterface{
				{Address: "192.0.2.10", Device: "rdma1", PCIAddress: "0000:01:00.0", NUMANode: numa(1)},
				{Address: "2001:db8::10", Device: "rdma1", PCIAddress: "0000:01:00.0", NUMANode: numa(1)},
			}},
			{Name: mxlv1alpha1.ProviderTCP, Interfaces: []mxlv1alpha1.MxlFabricInterface{
				{Address: "192.0.2.10", Device: "eth1"},
				{Address: "198.51.100.1", Device: "br0"},
			}},
		},
	}
	eth := &fakeEthtool{
		mac:   MACStats{OctetsReceived: 9000, HaveOctetsReceived: true, OctetsTransmitted: 9500, HaveOctetsTransmitted: true},
		pause: PauseStats{Tx: 2, HaveTx: true},
	}
	c := &Collector{NodeName: "n", Snapshot: func() (Snapshot, bool) { return snap, true },
		Sysfs: Sysfs{Root: f.root}, Ethtool: eth, Log: logr.Discard()}
	got := gather(t, c)

	v, ok := find(t, got, "mxl_gateway_fabric_port_info", map[string]string{"provider": "verbs", "port": "rdma1",
		"address": "2001:db8::10", "link": "eth1", "netdev": "eth1", "rdma_device": "rdma1", "rdma_port": "1", "numa_node": "1"})
	require.True(t, ok)
	require.Equal(t, 1.0, v)
	_, ok = find(t, got, "mxl_gateway_fabric_port_info", map[string]string{"provider": "tcp", "port": "br0", "link": ""})
	require.True(t, ok, "an interface without a link still has its info series")

	// verbs and tcp share eth1: one link, counted once.
	var links []string
	for _, s := range got {
		if s.name == "mxl_gateway_link_up" {
			links = append(links, s.labels["link"])
		}
	}
	require.Equal(t, []string{"eth1"}, links)

	for src, want := range map[string]float64{SourceMAC: 9000, SourceRDMA: 40, SourceNetdev: 100} {
		v, ok := find(t, got, "mxl_gateway_link_received_bytes_total", map[string]string{"link": "eth1", "source": src})
		require.True(t, ok, src)
		require.Equal(t, want, v, src)
	}
	v, _ = find(t, got, "mxl_gateway_link_pause_frames_total", map[string]string{"direction": "egress"})
	require.Equal(t, 2.0, v)
	_, ok = find(t, got, "mxl_gateway_link_pause_frames_total", map[string]string{"direction": "ingress"})
	require.False(t, ok, "a pause counter the driver does not report is left out")

	v, _ = find(t, got, "mxl_gateway_fabric_max_utilization_ratio", nil)
	require.InDelta(t, 0.7, v, 1e-9)
	v, _ = find(t, got, "mxl_gateway_fabric_budget_bits_per_second", map[string]string{"budget": "host-ingress", "mode": "ingress"})
	require.Equal(t, 40e9, v)
}

// Without ethtool statistics the link keeps its sysfs series, and the
// failure is logged once rather than on every scrape.
func TestCollectorDegradesWithoutEthtool(t *testing.T) {
	f := newFakeSys(t)
	f.netdev("eth1", 100, 200)
	snap := Snapshot{Providers: []mxlv1alpha1.MxlFabricsProviderCapability{{Name: mxlv1alpha1.ProviderTCP,
		Interfaces: []mxlv1alpha1.MxlFabricInterface{{Address: "192.0.2.10", Device: "eth1"}}}}}
	var lines int
	sink := logr.New(&countSink{n: &lines})
	c := &Collector{NodeName: "n", Snapshot: func() (Snapshot, bool) { return snap, true },
		Sysfs: Sysfs{Root: f.root}, Ethtool: &fakeEthtool{err: errors.New("operation not supported")}, Log: sink}
	for range 3 {
		got := gather(t, c)
		_, ok := find(t, got, "mxl_gateway_link_received_bytes_total", map[string]string{"source": SourceNetdev})
		require.True(t, ok)
		_, ok = find(t, got, "mxl_gateway_link_received_bytes_total", map[string]string{"source": SourceMAC})
		require.False(t, ok)
	}
	require.Equal(t, 2, lines, "one line for MAC statistics, one for pause, across three scrapes")
}

func TestCollectorReportsNothingBeforeTheFirstProbe(t *testing.T) {
	c := &Collector{NodeName: "n", Snapshot: func() (Snapshot, bool) { return Snapshot{}, false }, Log: logr.Discard()}
	require.Empty(t, gather(t, c))
}

type countSink struct{ n *int }

func (s *countSink) Init(logr.RuntimeInfo)          {}
func (s *countSink) Enabled(int) bool               { return true }
func (s *countSink) Info(int, string, ...any)       { *s.n++ }
func (s *countSink) Error(error, string, ...any)    { *s.n++ }
func (s *countSink) WithValues(...any) logr.LogSink { return s }
func (s *countSink) WithName(string) logr.LogSink   { return s }

// The netlink client asks for the eth-mac group of one netdev and reads
// the octet and FCS counters out of the group nest; pause statistics
// are requested with the stats flag.
func TestNetlinkDecodesStandardStatistics(t *testing.T) {
	family := genetlink.Family{ID: 42, Version: 1, Name: unix.ETHTOOL_GENL_NAME}
	var commands []uint8
	conn := genltest.Dial(genltest.ServeFamily(family, func(greq genetlink.Message, _ netlink.Message) ([]genetlink.Message, error) {
		commands = append(commands, greq.Header.Command)
		ad, err := netlink.NewAttributeDecoder(greq.Data)
		require.NoError(t, err)
		var dev string
		var flags uint32
		var groups []byte
		headerType := uint16(ethtoolAStatsHeader)
		if greq.Header.Command == unix.ETHTOOL_MSG_PAUSE_GET {
			headerType = unix.ETHTOOL_A_PAUSE_HEADER
		}
		for ad.Next() {
			switch {
			case ad.Type() == headerType:
				ad.Nested(func(hd *netlink.AttributeDecoder) error {
					for hd.Next() {
						switch hd.Type() {
						case unix.ETHTOOL_A_HEADER_DEV_NAME:
							dev = hd.String()
						case unix.ETHTOOL_A_HEADER_FLAGS:
							flags = hd.Uint32()
						}
					}
					return nil
				})
			case greq.Header.Command == unix.ETHTOOL_MSG_STATS_GET && ad.Type() == ethtoolAStatsGroups:
				ad.Nested(func(bd *netlink.AttributeDecoder) error {
					for bd.Next() {
						if bd.Type() == unix.ETHTOOL_A_BITSET_VALUE {
							groups = bd.Bytes()
						}
					}
					return nil
				})
			}
		}
		require.NoError(t, ad.Err())
		require.Equal(t, "eth1", dev)

		ae := netlink.NewAttributeEncoder()
		switch greq.Header.Command {
		case unix.ETHTOOL_MSG_STATS_GET:
			require.Equal(t, nlenc32(1<<ethtoolStatsEthMAC), groups)
			ae.Nested(ethtoolAStatsGrp, func(g *netlink.AttributeEncoder) error {
				g.Uint32(ethtoolAStatsGrpID, ethtoolStatsEthMAC)
				for id, v := range map[uint16]uint64{ethMACRxOctets: 1000, ethMACTxOctets: 2000, ethMACFCSErr: 3} {
					g.Nested(ethtoolAStatsGrpStat, func(s *netlink.AttributeEncoder) error {
						s.Uint64(id, v)
						return nil
					})
				}
				return nil
			})
		case unix.ETHTOOL_MSG_PAUSE_GET:
			require.Equal(t, uint32(unix.ETHTOOL_FLAG_STATS), flags)
			ae.Nested(unix.ETHTOOL_A_PAUSE_STATS, func(s *netlink.AttributeEncoder) error {
				s.Uint64(unix.ETHTOOL_A_PAUSE_STAT_RX_FRAMES, 7)
				return nil
			})
		}
		b, err := ae.Encode()
		require.NoError(t, err)
		return []genetlink.Message{{Data: b}}, nil
	}))
	n := &Netlink{Dial: func() (*genetlink.Conn, error) { return conn, nil }}
	defer n.Close()

	mac, err := n.MACStats("eth1")
	require.NoError(t, err)
	require.Equal(t, MACStats{OctetsReceived: 1000, HaveOctetsReceived: true, OctetsTransmitted: 2000,
		HaveOctetsTransmitted: true, FCSErrors: 3, HaveFCSErr: true}, mac)
	p, err := n.PauseStats("eth1")
	require.NoError(t, err)
	require.Equal(t, PauseStats{Rx: 7, HaveRx: true}, p)
	sort.Slice(commands, func(i, j int) bool { return commands[i] < commands[j] })
	require.Equal(t, []uint8{unix.ETHTOOL_MSG_PAUSE_GET, unix.ETHTOOL_MSG_STATS_GET}, commands)
}

// An error from the kernel, an unsupported command on an old kernel
// say, comes back as an error and not as zero counters.
func TestNetlinkReturnsKernelErrors(t *testing.T) {
	family := genetlink.Family{ID: 42, Version: 1, Name: unix.ETHTOOL_GENL_NAME}
	conn := genltest.Dial(genltest.ServeFamily(family, func(genetlink.Message, netlink.Message) ([]genetlink.Message, error) {
		return nil, genltest.Error(int(unix.EOPNOTSUPP))
	}))
	n := &Netlink{Dial: func() (*genetlink.Conn, error) { return conn, nil }}
	defer n.Close()
	_, err := n.MACStats("eth1")
	require.Error(t, err)
}
