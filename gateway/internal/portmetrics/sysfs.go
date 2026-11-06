package portmetrics

import (
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Link is the physical port a fabric interface sends and receives on:
// a netdev, the RDMA device and port bound to it, or both.
type Link struct {
	// Name is the netdev, or the RDMA device where there is none.
	Name       string
	Netdev     string
	RDMADevice string
	// RDMAPort is the port number under the RDMA device.
	RDMAPort string
}

// Sysfs reads links and their counters under Root, /sys when empty.
type Sysfs struct {
	Root string
	// NetdevOf names the netdev holding address, or "" when none does.
	NetdevOf func(address string) string
}

func (s Sysfs) path(parts ...string) string {
	root := s.Root
	if root == "" {
		root = "/sys"
	}
	return filepath.Join(append([]string{root}, parts...)...)
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// entries lists the names in dir, or nil.
func entries(dir string) []string {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(des))
	for _, d := range des {
		out = append(out, d.Name())
	}
	return out
}

func readString(p string) (string, bool) {
	b, err := os.ReadFile(p)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

func readUint(p string) (uint64, bool) {
	s, ok := readString(p)
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	return v, err == nil
}

// Resolve finds the link of an interface the provider names device for
// (an RDMA device, a netdev, or nothing) at address. It reports false
// for an interface not backed by a device -- a bridge, a veth, a
// loopback -- since those carry no port of their own to measure.
func (s Sysfs) Resolve(device, address string) (Link, bool) {
	var l Link
	switch {
	case device != "" && exists(s.path("class", "infiniband", device)):
		l.RDMADevice = device
	case device != "" && exists(s.path("class", "net", device)):
		l.Netdev = device
	case device == "" && s.NetdevOf != nil:
		l.Netdev = s.NetdevOf(address)
	}
	if l.Netdev != "" {
		if !exists(s.path("class", "net", l.Netdev, "device")) {
			return Link{}, false
		}
		if devs := entries(s.path("class", "net", l.Netdev, "device", "infiniband")); len(devs) == 1 {
			l.RDMADevice = devs[0]
		}
	}
	if l.RDMADevice != "" {
		l.RDMAPort = s.rdmaPort(l.RDMADevice, l.Netdev)
		if l.Netdev == "" {
			l.Netdev = s.netdevOfRDMA(l.RDMADevice, l.RDMAPort)
		}
	}
	l.Name = l.Netdev
	if l.Name == "" {
		l.Name = l.RDMADevice
	}
	return l, l.Name != ""
}

// rdmaPort is the port of device whose netdev is netdev, or its only
// port.
func (s Sysfs) rdmaPort(device, netdev string) string {
	ports := entries(s.path("class", "infiniband", device, "ports"))
	if netdev != "" {
		for _, p := range ports {
			if n, ok := readString(s.path("class", "infiniband", device, "ports", p, "gid_attrs", "ndevs", "0")); ok && n == netdev {
				return p
			}
		}
	}
	if len(ports) == 1 {
		return ports[0]
	}
	return ""
}

// netdevOfRDMA is the netdev a port of device is bound to, or the only
// netdev of its PCI function; "" where it has none.
func (s Sysfs) netdevOfRDMA(device, port string) string {
	if port != "" {
		if n, ok := readString(s.path("class", "infiniband", device, "ports", port, "gid_attrs", "ndevs", "0")); ok && n != "" {
			return n
		}
	}
	if nets := entries(s.path("class", "infiniband", device, "device", "net")); len(nets) == 1 {
		return nets[0]
	}
	return ""
}

// netdevErrors are the generic netdev statistics counted as errors.
var netdevErrors = []string{"rx_dropped", "rx_missed_errors", "rx_crc_errors", "rx_errors", "tx_dropped", "tx_errors"}

// rdmaErrors are the port counters (InfiniBand PortCounters attributes)
// counted as errors. A driver answers for the ones it implements.
var rdmaErrors = []string{"port_rcv_errors", "port_xmit_discards", "port_rcv_remote_physical_errors", "symbol_error",
	"link_downed", "link_error_recovery", "excessive_buffer_overrun_errors", "local_link_integrity_errors"}

// Counters are what one read of a link's sysfs reports. A field the
// kernel did not report is absent from its map, or false.
type Counters struct {
	// Bytes by source and direction.
	RxBytes, TxBytes map[string]uint64
	// Errors by source and counter name.
	Errors map[[2]string]uint64

	Up, HaveUp         bool
	SpeedBits          float64
	HaveSpeed          bool
	CarrierChanges     uint64
	HaveCarrierChanges bool
	XmitWait           uint64
	HaveXmitWait       bool

	// pause is filled from ethtool, not sysfs.
	pause PauseStats
}

// Read reads l's counters from sysfs.
func (s Sysfs) Read(l Link) Counters {
	c := Counters{RxBytes: map[string]uint64{}, TxBytes: map[string]uint64{}, Errors: map[[2]string]uint64{}}
	if l.Netdev != "" {
		dir := s.path("class", "net", l.Netdev)
		if v, ok := readUint(filepath.Join(dir, "statistics", "rx_bytes")); ok {
			c.RxBytes[SourceNetdev] = v
		}
		if v, ok := readUint(filepath.Join(dir, "statistics", "tx_bytes")); ok {
			c.TxBytes[SourceNetdev] = v
		}
		for _, name := range netdevErrors {
			if v, ok := readUint(filepath.Join(dir, "statistics", name)); ok {
				c.Errors[[2]string{SourceNetdev, name}] = v
			}
		}
		if v, ok := readUint(filepath.Join(dir, "carrier_changes")); ok {
			c.CarrierChanges, c.HaveCarrierChanges = v, true
		}
		if st, ok := readString(filepath.Join(dir, "operstate")); ok {
			c.Up, c.HaveUp = st == "up", true
		}
		// speed reads -1, or fails, while the link is down.
		if sp, ok := readString(filepath.Join(dir, "speed")); ok {
			if mbps, err := strconv.ParseInt(sp, 10, 64); err == nil && mbps > 0 {
				c.SpeedBits, c.HaveSpeed = float64(mbps)*1e6, true
			}
		}
	}
	if l.RDMADevice != "" && l.RDMAPort != "" {
		dir := s.path("class", "infiniband", l.RDMADevice, "ports", l.RDMAPort)
		// port_rcv_data and port_xmit_data count octets divided by
		// four (sysfs-class-infiniband ABI).
		rx, rxOK := readUint(filepath.Join(dir, "counters", "port_rcv_data"))
		tx, txOK := readUint(filepath.Join(dir, "counters", "port_xmit_data"))
		if rxOK && txOK {
			c.RxBytes[SourceRDMA], c.TxBytes[SourceRDMA] = rx*4, tx*4
		} else {
			// A driver without port counters may report bytes among its
			// own hardware counters instead.
			if v, ok := readUint(filepath.Join(dir, "hw_counters", "rx_bytes")); ok {
				c.RxBytes[SourceRDMA] = v
			}
			if v, ok := readUint(filepath.Join(dir, "hw_counters", "tx_bytes")); ok {
				c.TxBytes[SourceRDMA] = v
			}
		}
		for _, name := range rdmaErrors {
			if v, ok := readUint(filepath.Join(dir, "counters", name)); ok {
				c.Errors[[2]string{SourceRDMA, name}] = v
			}
		}
		if v, ok := readUint(filepath.Join(dir, "counters", "port_xmit_wait")); ok {
			c.XmitWait, c.HaveXmitWait = v, true
		}
		if !c.HaveUp {
			if st, ok := readString(filepath.Join(dir, "state")); ok {
				c.Up, c.HaveUp = strings.HasSuffix(st, "ACTIVE"), true
			}
		}
		if !c.HaveSpeed {
			c.SpeedBits, c.HaveSpeed = parseRate(readString(filepath.Join(dir, "rate")))
		}
	}
	return c
}

// parseRate reads an RDMA port rate such as "25 Gb/sec (1X EDR)".
func parseRate(s string, ok bool) (float64, bool) {
	if !ok {
		return 0, false
	}
	f := strings.Fields(s)
	if len(f) < 2 {
		return 0, false
	}
	v, err := strconv.ParseFloat(f[0], 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	switch f[1] {
	case "Gb/sec":
		return v * 1e9, true
	case "Mb/sec":
		return v * 1e6, true
	}
	return 0, false
}

// NetdevByAddress names the host interface holding address, or "".
func NetdevByAddress(address string) string {
	want, err := netip.ParseAddr(address)
	if err != nil {
		return ""
	}
	ifs, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, i := range ifs {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			n, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ip, ok := netip.AddrFromSlice(n.IP); ok && ip.Unmap() == want.Unmap() {
				return i.Name
			}
		}
	}
	return ""
}
