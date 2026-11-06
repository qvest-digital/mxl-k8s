// Package portmetrics exports the fabric ports a gateway publishes,
// the physical links they sit on, and what the kernel counts on those
// links.
//
// A link's bytes are reported per source, because no one kernel
// interface counts the same traffic on every driver:
//
//   - mac: the IEEE 802.3 MAC octet counters of the ethtool standard
//     statistics (group eth-mac). Everything the wire carries, where
//     the driver reports them.
//   - rdma: the RDMA port's data counters (port_rcv_data and
//     port_xmit_data, in units of four octets), or the driver's own
//     rx_bytes and tx_bytes hardware counters where it has no port
//     counters. RDMA traffic only.
//   - netdev: the netdev statistics. Traffic through the kernel's
//     network stack; whether RDMA traffic is included depends on the
//     driver.
//
// Everything is read on scrape. A counter the kernel or driver does
// not report is left out, never exported as zero.
package portmetrics

import (
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Byte and error sources.
const (
	SourceMAC    = "mac"
	SourceRDMA   = "rdma"
	SourceNetdev = "netdev"
)

// Series are bounded by the interfaces the gateway publishes (port
// info), the distinct links under them, and fixed sets of sources,
// directions and counter names.
var (
	linkLabels = []string{"node", "link"}

	descPortInfo = prometheus.NewDesc("mxl_gateway_fabric_port_info",
		"A fabric interface this gateway publishes: its port as placement accounts it (the device the provider names, "+
			"or the address), and the link it sits on. link is empty for an interface not backed by a device. "+
			"The value is always 1.",
		[]string{"node", "provider", "port", "address", "link", "netdev", "rdma_device", "rdma_port", "pci_address", "numa_node"}, nil)
	descMaxUtilization = prometheus.NewDesc("mxl_gateway_fabric_max_utilization_ratio",
		"Share of a capacity placement may commit on this node, as this gateway publishes it (--fabric-max-utilization). "+
			"Absent when the gateway leaves it to the operator.",
		[]string{"node"}, nil)
	descBudget = prometheus.NewDesc("mxl_gateway_fabric_budget_bits_per_second",
		"Capacity of a shared budget this gateway publishes (--fabric-shared-budget).",
		[]string{"node", "budget", "scope", "mode"}, nil)

	descRx = prometheus.NewDesc("mxl_gateway_link_received_bytes_total",
		"Bytes received on a fabric link, by source: mac (802.3 MAC octets, the whole wire), rdma (RDMA port data, "+
			"RDMA traffic only) or netdev (kernel interface statistics; whether RDMA is included depends on the driver).",
		append(append([]string{}, linkLabels...), "source"), nil)
	descTx = prometheus.NewDesc("mxl_gateway_link_transmitted_bytes_total",
		"Bytes transmitted on a fabric link, by source, as for mxl_gateway_link_received_bytes_total.",
		append(append([]string{}, linkLabels...), "source"), nil)
	descUp = prometheus.NewDesc("mxl_gateway_link_up",
		"1 while a fabric link is up: the netdev's operstate, or the RDMA port's state where it has no netdev.",
		linkLabels, nil)
	descSpeed = prometheus.NewDesc("mxl_gateway_link_speed_bits_per_second",
		"Speed of a fabric link: the netdev's, or the RDMA port's rate where it has no netdev. Absent while unknown.",
		linkLabels, nil)
	descCarrier = prometheus.NewDesc("mxl_gateway_link_carrier_changes_total",
		"Times a fabric link's netdev carrier went up or down.",
		linkLabels, nil)
	descErrors = prometheus.NewDesc("mxl_gateway_link_errors_total",
		"Error and drop counters of a fabric link, by source and the kernel's name for the counter.",
		append(append([]string{}, linkLabels...), "source", "counter"), nil)
	descXmitWait = prometheus.NewDesc("mxl_gateway_link_xmit_wait_total",
		"Ticks an RDMA port had data to send and sent none (port_xmit_wait).",
		linkLabels, nil)
	descPause = prometheus.NewDesc("mxl_gateway_link_pause_frames_total",
		"802.3x pause frames on a fabric link: egress sent, ingress received. Per-priority flow control is not included.",
		append(append([]string{}, linkLabels...), "direction"), nil)
)

// Snapshot is what the gateway last published for its node.
type Snapshot struct {
	Providers             []mxlv1alpha1.MxlFabricsProviderCapability
	Budgets               []mxlv1alpha1.MxlFabricBudget
	MaxUtilizationPercent int32
}

// Collector exports the ports in Snapshot and their links.
type Collector struct {
	NodeName string
	// Snapshot returns the last published state, false before the
	// first probe.
	Snapshot func() (Snapshot, bool)
	Sysfs    Sysfs
	// Ethtool reads MAC and pause statistics; nil reads none.
	Ethtool Ethtool
	Log     logr.Logger

	mu     sync.Mutex
	logged map[string]struct{}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descPortInfo, descMaxUtilization, descBudget,
		descRx, descTx, descUp, descSpeed, descCarrier, descErrors, descXmitWait, descPause} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snap, ok := c.Snapshot()
	if !ok {
		return
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v uint64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}

	if snap.MaxUtilizationPercent > 0 {
		gauge(descMaxUtilization, float64(snap.MaxUtilizationPercent)/100, c.NodeName)
	}
	for _, b := range snap.Budgets {
		gauge(descBudget, float64(b.CapacityBitsPerSecond), c.NodeName, b.Name, string(b.Scope), string(b.Mode))
	}

	links := map[string]Link{}
	var order []string
	seen := map[[2]string]struct{}{}
	for _, p := range snap.Providers {
		for _, i := range p.Interfaces {
			key := [2]string{string(p.Name), i.Address}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			l, ok := c.Sysfs.Resolve(i.Device, i.Address)
			if ok {
				if _, known := links[l.Name]; !known {
					order = append(order, l.Name)
				}
				links[l.Name] = l
			}
			numa := ""
			if i.NUMANode != nil {
				numa = strconv.Itoa(int(*i.NUMANode))
			}
			port := i.Device
			if port == "" {
				port = i.Address
			}
			gauge(descPortInfo, 1, c.NodeName, string(p.Name), port, i.Address,
				l.Name, l.Netdev, l.RDMADevice, l.RDMAPort, i.PCIAddress, numa)
		}
	}

	for _, name := range order {
		l := links[name]
		labels := []string{c.NodeName, l.Name}
		with := func(extra ...string) []string { return append(append([]string{}, labels...), extra...) }

		k := c.Sysfs.Read(l)
		if l.Netdev != "" && c.Ethtool != nil {
			c.ethtool(l.Netdev, &k)
		}
		for src, v := range k.RxBytes {
			counter(descRx, v, with(src)...)
		}
		for src, v := range k.TxBytes {
			counter(descTx, v, with(src)...)
		}
		for key, v := range k.Errors {
			counter(descErrors, v, with(key[0], key[1])...)
		}
		if k.HaveUp {
			up := 0.0
			if k.Up {
				up = 1
			}
			gauge(descUp, up, labels...)
		}
		if k.HaveSpeed {
			gauge(descSpeed, k.SpeedBits, labels...)
		}
		if k.HaveCarrierChanges {
			counter(descCarrier, k.CarrierChanges, labels...)
		}
		if k.HaveXmitWait {
			counter(descXmitWait, k.XmitWait, labels...)
		}
		if k.pause.HaveTx {
			counter(descPause, k.pause.Tx, with("egress")...)
		}
		if k.pause.HaveRx {
			counter(descPause, k.pause.Rx, with("ingress")...)
		}
	}
}

// ethtool adds what the ethtool standard statistics report for netdev
// to k. A failure leaves them out and is logged once per netdev and
// kind, so a kernel or driver without them costs a line, not a line per
// scrape.
func (c *Collector) ethtool(netdev string, k *Counters) {
	if mac, err := c.Ethtool.MACStats(netdev); err != nil {
		c.once("mac/"+netdev, err, "ethtool MAC statistics unavailable; mac bytes not exported", "netdev", netdev)
	} else {
		if mac.HaveOctetsReceived {
			k.RxBytes[SourceMAC] = mac.OctetsReceived
		}
		if mac.HaveOctetsTransmitted {
			k.TxBytes[SourceMAC] = mac.OctetsTransmitted
		}
		if mac.HaveFCSErr {
			k.Errors[[2]string{SourceMAC, "FrameCheckSequenceErrors"}] = mac.FCSErrors
		}
	}
	if p, err := c.Ethtool.PauseStats(netdev); err != nil {
		c.once("pause/"+netdev, err, "ethtool pause statistics unavailable; pause frames not exported", "netdev", netdev)
	} else {
		k.pause = p
	}
}

func (c *Collector) once(key string, err error, msg string, kv ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logged == nil {
		c.logged = map[string]struct{}{}
	}
	if _, done := c.logged[key]; done {
		return
	}
	c.logged[key] = struct{}{}
	c.Log.Info(msg, append(kv, "reason", err.Error())...)
}
