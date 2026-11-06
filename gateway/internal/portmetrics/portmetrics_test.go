package portmetrics

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

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

// The kernel's numbering of the ethtool netlink interface, written out
// rather than taken from the code under test or x/sys, so a wrong
// attribute number in either fails here. From
// include/uapi/linux/ethtool_netlink_generated.h, ethtool_netlink.h and
// ethtool.h.
const (
	uapiMsgPauseGet       = 21 // ETHTOOL_MSG_PAUSE_GET
	uapiMsgStatsGet       = 32 // ETHTOOL_MSG_STATS_GET
	uapiHeaderDevName     = 2  // ETHTOOL_A_HEADER_DEV_NAME
	uapiHeaderFlags       = 3  // ETHTOOL_A_HEADER_FLAGS
	uapiFlagStats         = 4  // ETHTOOL_FLAG_STATS
	uapiBitsetValue       = 4  // ETHTOOL_A_BITSET_VALUE
	uapiPauseHeader       = 1  // ETHTOOL_A_PAUSE_HEADER
	uapiPauseStats        = 5  // ETHTOOL_A_PAUSE_STATS
	uapiPauseStatRxFrames = 3  // ETHTOOL_A_PAUSE_STAT_RX_FRAMES
	uapiStatsHeader       = 2  // ETHTOOL_A_STATS_HEADER
	uapiStatsGroups       = 3  // ETHTOOL_A_STATS_GROUPS
	uapiStatsGrp          = 4  // ETHTOOL_A_STATS_GRP
	uapiStatsGrpID        = 2  // ETHTOOL_A_STATS_GRP_ID
	uapiStatsGrpStat      = 4  // ETHTOOL_A_STATS_GRP_STAT
	uapiStatsEthMAC       = 1  // ETHTOOL_STATS_ETH_MAC
	uapiEthMACFCSErr      = 4  // ETHTOOL_A_STATS_ETH_MAC_6_FCS_ERR
	uapiEthMACTxBytes     = 6  // ETHTOOL_A_STATS_ETH_MAC_8_TX_BYTES
	uapiEthMACRxBytes     = 12 // ETHTOOL_A_STATS_ETH_MAC_14_RX_BYTES
)

// genltestConn is a genltest connection, which has no socket deadline
// to set.
type genltestConn struct{ *genetlink.Conn }

func (genltestConn) SetDeadline(time.Time) error { return nil }

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
		headerType := uint16(uapiStatsHeader)
		if greq.Header.Command == uapiMsgPauseGet {
			headerType = uapiPauseHeader
		}
		for ad.Next() {
			switch {
			case ad.Type() == headerType:
				ad.Nested(func(hd *netlink.AttributeDecoder) error {
					for hd.Next() {
						switch hd.Type() {
						case uapiHeaderDevName:
							dev = hd.String()
						case uapiHeaderFlags:
							flags = hd.Uint32()
						}
					}
					return nil
				})
			case greq.Header.Command == uapiMsgStatsGet && ad.Type() == uapiStatsGroups:
				ad.Nested(func(bd *netlink.AttributeDecoder) error {
					for bd.Next() {
						if bd.Type() == uapiBitsetValue {
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
		case uapiMsgStatsGet:
			require.Equal(t, []byte{1 << uapiStatsEthMAC, 0, 0, 0}, groups, "a host-order (little-endian here) u32 word with the eth-mac bit")
			ae.Nested(uapiStatsGrp, func(g *netlink.AttributeEncoder) error {
				g.Uint32(uapiStatsGrpID, uapiStatsEthMAC)
				for id, v := range map[uint16]uint64{uapiEthMACRxBytes: 1000, uapiEthMACTxBytes: 2000, uapiEthMACFCSErr: 3} {
					g.Nested(uapiStatsGrpStat, func(s *netlink.AttributeEncoder) error {
						s.Uint64(id, v)
						return nil
					})
				}
				return nil
			})
		case uapiMsgPauseGet:
			require.Equal(t, uint32(uapiFlagStats), flags)
			ae.Nested(uapiPauseStats, func(s *netlink.AttributeEncoder) error {
				s.Uint64(uapiPauseStatRxFrames, 7)
				return nil
			})
		}
		b, err := ae.Encode()
		require.NoError(t, err)
		return []genetlink.Message{{Data: b}}, nil
	}))
	n := &Netlink{Dial: func() (Conn, error) { return genltestConn{conn}, nil }}
	defer n.Close()

	mac, err := n.MACStats("eth1")
	require.NoError(t, err)
	require.Equal(t, MACStats{OctetsReceived: 1000, HaveOctetsReceived: true, OctetsTransmitted: 2000,
		HaveOctetsTransmitted: true, FCSErrors: 3, HaveFCSErr: true}, mac)
	p, err := n.PauseStats("eth1")
	require.NoError(t, err)
	require.Equal(t, PauseStats{Rx: 7, HaveRx: true}, p)
	sort.Slice(commands, func(i, j int) bool { return commands[i] < commands[j] })
	require.Equal(t, []uint8{uapiMsgPauseGet, uapiMsgStatsGet}, commands)
}

// An error from the kernel, an unsupported command on an old kernel
// say, comes back as an error and not as zero counters.
func TestNetlinkReturnsKernelErrors(t *testing.T) {
	family := genetlink.Family{ID: 42, Version: 1, Name: unix.ETHTOOL_GENL_NAME}
	conn := genltest.Dial(genltest.ServeFamily(family, func(genetlink.Message, netlink.Message) ([]genetlink.Message, error) {
		return nil, genltest.Error(int(unix.EOPNOTSUPP))
	}))
	n := &Netlink{Dial: func() (Conn, error) { return genltestConn{conn}, nil }}
	defer n.Close()
	_, err := n.MACStats("eth1")
	require.ErrorIs(t, err, unix.EOPNOTSUPP)
}

// stuckConn answers the family lookup and then never answers a request:
// Execute returns only when the deadline set on it passes, as a socket
// read does.
type stuckConn struct {
	deadline time.Time
	closed   bool
}

func (c *stuckConn) GetFamily(string) (genetlink.Family, error) {
	return genetlink.Family{ID: 42, Version: 1, Name: unix.ETHTOOL_GENL_NAME}, nil
}

func (c *stuckConn) Execute(genetlink.Message, uint16, netlink.HeaderFlags) ([]genetlink.Message, error) {
	if c.deadline.IsZero() {
		return nil, errors.New("request sent without a deadline")
	}
	time.Sleep(time.Until(c.deadline))
	return nil, &netlink.OpError{Op: "receive", Err: os.ErrDeadlineExceeded}
}

func (c *stuckConn) SetDeadline(t time.Time) error { c.deadline = t; return nil }
func (c *stuckConn) Close() error                  { c.closed = true; return nil }

// A kernel that does not answer, a driver holding the RTNL lock say,
// costs one request its deadline. The connection is dropped so its late
// reply is never read, and later requests fail at once for the backoff
// rather than each waiting out a deadline of its own.
func TestNetlinkTimesOutDropsTheConnectionAndBacksOff(t *testing.T) {
	var conns []*stuckConn
	n := &Netlink{Timeout: 50 * time.Millisecond, Backoff: time.Hour, Dial: func() (Conn, error) {
		c := &stuckConn{}
		conns = append(conns, c)
		return c, nil
	}}
	start := time.Now()
	_, err := n.MACStats("eth1")
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Len(t, conns, 1)
	require.True(t, conns[0].closed, "the connection is dropped after the timeout")

	start = time.Now()
	_, err = n.PauseStats("eth1")
	require.Error(t, err)
	require.Less(t, time.Since(start), 10*time.Millisecond, "fails fast during the backoff")
	require.Len(t, conns, 1, "nothing is dialled during the backoff")
}

// An error reply drops the connection too; the next request dials a
// fresh one.
func TestNetlinkRedialsAfterAnError(t *testing.T) {
	family := genetlink.Family{ID: 42, Version: 1, Name: unix.ETHTOOL_GENL_NAME}
	dials := 0
	n := &Netlink{Dial: func() (Conn, error) {
		dials++
		return genltestConn{genltest.Dial(genltest.ServeFamily(family, func(genetlink.Message, netlink.Message) ([]genetlink.Message, error) {
			return nil, genltest.Error(int(unix.EOPNOTSUPP))
		}))}, nil
	}}
	defer n.Close()
	for range 2 {
		_, err := n.PauseStats("eth1")
		require.ErrorIs(t, err, unix.EOPNOTSUPP)
	}
	require.Equal(t, 2, dials)
}

// A driver that answers it does not support a kind is asked once.
func TestCollectorStopsAskingForWhatTheDriverDoesNotSupport(t *testing.T) {
	f := newFakeSys(t)
	f.netdev("eth1", 100, 200)
	snap := Snapshot{Providers: []mxlv1alpha1.MxlFabricsProviderCapability{{Name: mxlv1alpha1.ProviderTCP,
		Interfaces: []mxlv1alpha1.MxlFabricInterface{{Address: "192.0.2.10", Device: "eth1"}}}}}
	eth := &fakeEthtool{err: &netlink.OpError{Op: "receive", Err: unix.EOPNOTSUPP}}
	c := &Collector{NodeName: "n", Snapshot: func() (Snapshot, bool) { return snap, true },
		Sysfs: Sysfs{Root: f.root}, Ethtool: eth, Log: logr.Discard()}
	for range 3 {
		gather(t, c)
	}
	require.Equal(t, 1, eth.macCalls)
}

// deadlineConn answers every request, but only one sent under a live
// deadline: a deadline left from the dial or an earlier request has
// passed by the time a later request is sent.
type deadlineConn struct {
	deadline time.Time
	requests int
}

func (c *deadlineConn) live(op string) error {
	if c.deadline.IsZero() {
		return fmt.Errorf("%s sent without a deadline", op)
	}
	if !time.Now().Before(c.deadline) {
		return fmt.Errorf("%s sent under a deadline that passed at %s", op, c.deadline.Format(time.RFC3339Nano))
	}
	return nil
}

func (c *deadlineConn) GetFamily(string) (genetlink.Family, error) {
	if err := c.live("family lookup"); err != nil {
		return genetlink.Family{}, err
	}
	return genetlink.Family{ID: 42, Version: 1, Name: unix.ETHTOOL_GENL_NAME}, nil
}

func (c *deadlineConn) Execute(genetlink.Message, uint16, netlink.HeaderFlags) ([]genetlink.Message, error) {
	if err := c.live("request"); err != nil {
		return nil, err
	}
	c.requests++
	return nil, nil
}

func (c *deadlineConn) SetDeadline(t time.Time) error { c.deadline = t; return nil }
func (c *deadlineConn) Close() error                  { return nil }

// The family lookup and every request get a deadline of their own, set
// when each is sent, not one inherited from the dial or the previous
// request.
func TestNetlinkSetsADeadlinePerRequest(t *testing.T) {
	const timeout = 30 * time.Millisecond
	conn := &deadlineConn{}
	dials := 0
	n := &Netlink{Timeout: timeout, Dial: func() (Conn, error) { dials++; return conn, nil }}

	_, err := n.MACStats("eth1")
	require.NoError(t, err)
	time.Sleep(3 * timeout)
	_, err = n.PauseStats("eth1")
	require.NoError(t, err, "the second request needs a fresh deadline")
	require.Equal(t, 2, conn.requests)
	require.Equal(t, 1, dials, "both requests ran on one connection")
}
