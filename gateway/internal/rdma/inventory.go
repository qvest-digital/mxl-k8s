// Package rdma reports the RDMA devices the host kernel exposes.
//
// It reads sysfs rather than asking libmxl-fabrics, because the two
// answer different questions. A provider enumeration reports the
// devices libfabric found when it first initialised that provider in
// this process; sysfs reports what the kernel has now. Nothing else
// distinguishes a host that carries no RDMA hardware from a process
// that enumerated before the hardware it does carry became usable,
// and the two produce the same empty provider entry.
package rdma

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DefaultSysPath is the sysfs directory holding one entry per RDMA
// device. It is absent on a host with no RDMA hardware.
const DefaultSysPath = "/sys/class/infiniband"

// Inventory reads a host RDMA device list out of a sysfs tree.
type Inventory struct {
	// Path is the directory to read. Empty means DefaultSysPath.
	Path string
}

// ActiveDevices returns the names of the RDMA devices that have at
// least one port in the ACTIVE state, sorted.
//
// Port state is the coarsest filter that still means something: a
// device whose every port is down carries no traffic, so a provider
// reporting nothing for it is right rather than stale. The reverse
// does not hold. An active port is necessary for the verbs provider
// to offer an endpoint, not sufficient - it also needs an address on
// the matching interface - so this bounds the question rather than
// answering it, and a caller reports the difference rather than
// treating it as a fault.
//
// A missing directory is not an error: that is how a host with no
// RDMA hardware presents, and it is a legitimate answer of none.
func (i Inventory) ActiveDevices() ([]string, error) {
	root := i.Path
	if root == "" {
		root = DefaultSysPath
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", root, err)
	}

	var active []string
	for _, entry := range entries {
		name := entry.Name()
		ok, err := deviceHasActivePort(filepath.Join(root, name))
		if err != nil {
			return nil, err
		}
		if ok {
			active = append(active, name)
		}
	}
	sort.Strings(active)
	return active, nil
}

// deviceHasActivePort reports whether any port of the device at dir
// is ACTIVE.
//
// A device that has lost its ports directory between the listing and
// this read is treated as having none rather than as an error: the
// inventory describes a moving target, and a device disappearing
// mid-read is the same answer as one that was never there.
func deviceHasActivePort(dir string) (bool, error) {
	portsDir := filepath.Join(dir, "ports")
	ports, err := os.ReadDir(portsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read %s: %w", portsDir, err)
	}

	for _, port := range ports {
		raw, err := os.ReadFile(filepath.Join(portsDir, port.Name(), "state"))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, fmt.Errorf("read %s state: %w", portsDir, err)
		}
		if portIsActive(string(raw)) {
			return true, nil
		}
	}
	return false, nil
}

// portIsActive reads the port state file, whose contents are the
// numeric state and its name separated by a colon, as in "4: ACTIVE".
// Only the name is read: the numbering belongs to the kernel's enum
// and carries no promise across versions, while the names are the
// ones the InfiniBand specification defines.
func portIsActive(raw string) bool {
	_, name, found := strings.Cut(strings.TrimSpace(raw), ":")
	if !found {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(name), "ACTIVE")
}
