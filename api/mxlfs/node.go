package mxlfs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RDMAClass is the sysfs class that lists the RDMA devices of the host.
const RDMAClass = "/sys/class/infiniband"

// OnlineNodes is the sysfs list of the host's online NUMA nodes.
const OnlineNodes = "/sys/devices/system/node/online"

// RDMANode returns the NUMA node every RDMA device in class reports for
// its device (device/numa_node). A device without one, as a software
// RDMA device that has no PCI device, is passed over. It fails when no
// device reports a node, when one reports none (-1), or when they report
// different nodes.
func RDMANode(class string) (int32, error) {
	entries, err := os.ReadDir(class)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return NodeNone, err
	}
	node := ""
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(class, e.Name(), "device", "numa_node"))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return NodeNone, err
		}
		n := strings.TrimSpace(string(b))
		if _, err := strconv.ParseUint(n, 10, 31); err != nil {
			return NodeNone, fmt.Errorf("RDMA device %s reports NUMA node %s", e.Name(), n)
		}
		if node != "" && n != node {
			return NodeNone, fmt.Errorf("RDMA devices are on NUMA nodes %s and %s", node, n)
		}
		node = n
	}
	if node == "" {
		return NodeNone, fmt.Errorf("no RDMA device in %s reports a NUMA node", class)
	}
	v, _ := strconv.ParseUint(node, 10, 31)
	return int32(v), nil
}

// SingleOnlineNode returns the node of a host whose online list (online)
// names one NUMA node. A kernel without NUMA has no such list, and node 0.
func SingleOnlineNode(online string) (int32, bool) {
	b, err := os.ReadFile(online)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, true
	}
	s := strings.TrimSpace(string(b))
	if err != nil || s == "" || strings.ContainsAny(s, ",-") {
		return NodeNone, false
	}
	n, err := strconv.ParseUint(s, 10, 31)
	if err != nil {
		return NodeNone, false
	}
	return int32(n), true
}

// HostNICNode returns the NUMA node the host's NICs are on: the node of
// a host of one online NUMA node, else the node every RDMA device
// reports. It fails on a host whose RDMA devices report no node or
// several.
func HostNICNode(class, online string) (int32, error) {
	if n, ok := SingleOnlineNode(online); ok {
		return n, nil
	}
	return RDMANode(class)
}
