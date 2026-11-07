// Package mxlfsmount mounts an mxlfs on each domain directory of the
// node, before anything is written into it or watched there.
//
// A node whose kernel has no mxlfs keeps plain directories: the domain
// works as it does without mxlfs, and the agent reports why on the Node.
package mxlfsmount

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/qvest-digital/mxl-k8s/agent/internal/mxlfswatch"
)

// Options are the mount options of every domain: writable by every
// function on the node, as the plain directory is, and a path walk for
// an absent flow waits up to 5s for it to appear.
const Options = "mode=0777,wait=5000"

// ReadyFile is written to the runtime root once the domains known at
// start are mounted or left plain. /run is a tmpfs, so a reboot, which
// takes the mounts with it, takes the file too.
const ReadyFile = "mxlfs.ready"

// ReasonUnavailable is the reason of the Node event for a directory left
// plain.
const ReasonUnavailable = "MxlfsUnavailable"

// ReasonHugeRefused is the reason of the Node event for a domain mounted
// without the huge= option the module refused.
const ReasonHugeRefused = "MxlfsHugeRefused"

// ReasonNumaNodeRefused is the reason of the Node event for a domain
// mounted without the numa_node= option the module refused.
const ReasonNumaNodeRefused = "MxlfsNumaNodeRefused"

// ReasonNumaNodeUnresolved is the reason of the Node event for domains
// mounted without numa_node= because auto found no single NUMA node.
const ReasonNumaNodeUnresolved = "MxlfsNumaNodeUnresolved"

// RDMAClass is the sysfs class that lists the RDMA devices of the node.
const RDMAClass = "/sys/class/infiniband"

// Mounter decides once per directory whether it is an mxlfs.
type Mounter struct {
	// IsMxlfs reports whether dir is an mxlfs. Nil uses mxlfswatch.IsMxlfs.
	IsMxlfs func(dir string) bool
	// Mount mounts an mxlfs on dir with the given options. Nil uses the
	// mount syscall.
	Mount func(dir, options string) error
	// Huge is the mount's huge= option, or empty to leave it out. With
	// always, a file of at least 2 MiB takes a 2 MiB folio for its last
	// partial 2 MiB, so a NIC maps a registered grain with 2 MiB pages.
	// An mxlfs module without the option refuses a mount that names it;
	// the domain is then mounted without it and HugeRefused is told.
	Huge string
	// NumaNode is the mount's numa_node= option, or empty to leave it
	// out: the NUMA node every page of the domain prefers, wherever the
	// process that makes it runs. A module without the option refuses a
	// mount that names it, as does one that finds no memory on the node;
	// the domain is then mounted without it and NumaNodeRefused is told.
	NumaNode string
	// Fallback is told about every directory left plain, and why.
	Fallback func(dir string, err error)
	// HugeRefused is told about every domain mounted without Huge, and
	// why.
	HugeRefused func(dir string, err error)
	// NumaNodeRefused is told about every domain mounted without
	// NumaNode, and why.
	NumaNodeRefused func(dir string, err error)
	Log             logr.Logger

	mu      sync.Mutex
	decided map[string]bool
}

// Ensure creates dir if it is missing and mounts an mxlfs on it unless
// it is one already. A mount that fails, or would hide flows the
// directory already holds, leaves dir a plain directory; Fallback is
// told, and this process does not try dir again. The error is only that
// of creating dir.
func (m *Mounter) Ensure(dir string) error {
	dir = filepath.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.decided[dir] {
		return nil
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	isMxlfs, mount := m.IsMxlfs, m.Mount
	if isMxlfs == nil {
		isMxlfs = mxlfswatch.IsMxlfs
	}
	if mount == nil {
		mount = sysMount
	}
	if isMxlfs(dir) {
		m.Log.Info("domain is already an mxlfs", "directory", dir)
	} else if options, err := m.mountEmpty(dir, mount); err != nil {
		m.Log.Error(err, "domain stays a plain directory", "directory", dir)
		if m.Fallback != nil {
			m.Fallback(dir, err)
		}
	} else {
		m.Log.Info("mounted mxlfs", "directory", dir, "options", options)
	}
	if m.decided == nil {
		m.decided = map[string]bool{}
	}
	m.decided[dir] = true
	return nil
}

// option is a mount option beyond Options, which a module may not have.
type option struct {
	name, value string
	refused     func(dir string, err error)
}

// optional are the options set beyond Options, the oldest first.
func (m *Mounter) optional() []option {
	var o []option
	if m.Huge != "" {
		o = append(o, option{"huge", m.Huge, m.HugeRefused})
	}
	if m.NumaNode != "" {
		o = append(o, option{"numa_node", m.NumaNode, m.NumaNodeRefused})
	}
	return o
}

// join is Options with optional added.
func join(optional []option) string {
	s := Options
	for _, o := range optional {
		s += "," + o.name + "=" + o.value
	}
	return s
}

// Decided reports whether Ensure has run for dir: it is an mxlfs, or it
// stays plain.
func (m *Mounter) Decided(dir string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.decided[filepath.Clean(dir)]
}

// mountEmpty mounts on dir unless that would hide a flow: a producer
// keeps writing the hidden one, and every reader sees an empty domain. It
// returns the options the mount took.
func (m *Mounter) mountEmpty(dir string, mount func(dir, options string) error) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() && strings.HasSuffix(e.Name(), ".mxl-flow") {
			n++
		}
	}
	if n > 0 {
		return "", fmt.Errorf("%s holds %d flows a mount would hide", dir, n)
	}
	optional := m.optional()
	options := join(optional)
	err = mount(dir, options)
	// A module that predates an option refuses it as an unknown
	// parameter, and one that finds no memory on the numa_node= node
	// refuses that. Without it the domain is still an mxlfs: with folios
	// that end by EOF without huge=, with pages where the writing process
	// runs without numa_node=. The newest option goes first, as a module
	// that has it has every older one.
	var refused []option
	var refusals []error
	for err != nil && len(optional) > 0 && errors.Is(err, unix.EINVAL) {
		refused = append(refused, optional[len(optional)-1])
		refusals = append(refusals, err)
		optional = optional[:len(optional)-1]
		options = join(optional)
		err = mount(dir, options)
	}
	if err == nil {
		for i, o := range refused {
			m.Log.Error(refusals[i], "mxlfs refused "+o.name+"=; mounted without it", "directory", dir, o.name, o.value)
			if o.refused != nil {
				o.refused(dir, refusals[i])
			}
		}
	}
	if err != nil {
		if errors.Is(err, unix.ENODEV) {
			return "", fmt.Errorf("mount mxlfs on %s: %w (the kernel has no mxlfs; is the module loaded?)", dir, err)
		}
		return "", fmt.Errorf("mount mxlfs on %s: %w", dir, err)
	}
	return options, nil
}

func sysMount(dir, options string) error {
	return unix.Mount("mxlfs", dir, "mxlfs", 0, options)
}

// MarkReady writes ReadyFile to root.
func MarkReady(root string) error {
	return os.WriteFile(filepath.Join(root, ReadyFile), nil, 0o644)
}

// ClearReady removes ReadyFile from root, where an agent without mxlfs
// finds one left from an earlier run with it.
func ClearReady(root string) error {
	err := os.Remove(filepath.Join(root, ReadyFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// NodeEvent returns a Fallback that records a warning event on node.
func NodeEvent(c client.Client, node string, log logr.Logger) func(dir string, err error) {
	return nodeEvent(c, node, log, ReasonUnavailable, "%s stays a plain directory: %v")
}

// HugeRefusedEvent returns a HugeRefused that records a warning event on
// node.
func HugeRefusedEvent(c client.Client, node string, log logr.Logger) func(dir string, err error) {
	return nodeEvent(c, node, log, ReasonHugeRefused, "%s is mounted without huge=: %v")
}

// NumaNodeRefusedEvent returns a NumaNodeRefused that records a warning
// event on node.
func NumaNodeRefusedEvent(c client.Client, node string, log logr.Logger) func(dir string, err error) {
	return nodeEvent(c, node, log, ReasonNumaNodeRefused, "%s is mounted without numa_node=: %v")
}

// NumaNodeUnresolvedEvent returns an unresolved for ResolveNumaNode that
// records a warning event on node.
func NumaNodeUnresolvedEvent(c client.Client, node string, log logr.Logger) func(dir string, err error) {
	return nodeEvent(c, node, log, ReasonNumaNodeUnresolved,
		"%s gives no NUMA node; domains are mounted without numa_node= until the agent restarts: %v")
}

// ResolveNumaNode is NumaNode, once, at the agent's start. When it finds
// no node, unresolved is told why with class, and the domains are mounted
// without numa_node=: RDMA devices that appear later do not change that.
func ResolveNumaNode(value, class string, unresolved func(dir string, err error)) string {
	n, err := NumaNode(value, class)
	if err != nil {
		if unresolved != nil {
			unresolved(class, err)
		}
		return ""
	}
	return n
}

// NumaNode turns a --mxlfs-numa-node value into Mounter.NumaNode: a node
// as numa_node= takes it, auto as RDMANumaNode finds it in class, and
// empty or none as empty. The error says why auto found no node.
func NumaNode(value, class string) (string, error) {
	switch value {
	case "", "none":
		return "", nil
	case "auto":
		return RDMANumaNode(class)
	}
	n, err := strconv.ParseUint(value, 10, 31)
	if err != nil {
		return "", fmt.Errorf("NUMA node %q: %w", value, err)
	}
	// Without leading zeros, which the kernel would read as octal.
	return strconv.FormatUint(n, 10), nil
}

// RDMANumaNode returns the NUMA node every RDMA device in class reports
// for its device (device/numa_node). A device without one, as a software
// RDMA device that has no PCI device, is passed over. It fails when no
// device reports a node, when one reports none (-1), or when they report
// different nodes.
func RDMANumaNode(class string) (string, error) {
	entries, err := os.ReadDir(class)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	node := ""
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(class, e.Name(), "device", "numa_node"))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		n := strings.TrimSpace(string(b))
		if _, err := strconv.ParseUint(n, 10, 31); err != nil {
			return "", fmt.Errorf("RDMA device %s reports NUMA node %s", e.Name(), n)
		}
		if node != "" && n != node {
			return "", fmt.Errorf("RDMA devices are on NUMA nodes %s and %s", node, n)
		}
		node = n
	}
	if node == "" {
		return "", fmt.Errorf("no RDMA device in %s reports a NUMA node", class)
	}
	return node, nil
}

func nodeEvent(c client.Client, node string, log logr.Logger, reason, format string) func(dir string, err error) {
	return func(dir string, err error) {
		now := metav1.NewTime(time.Now())
		ev := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{GenerateName: node + ".", Namespace: metav1.NamespaceDefault},
			// A Node's events name it by its name as UID, as the kubelet's do.
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: node, UID: types.UID(node)},
			Reason:         reason,
			Message:        fmt.Sprintf(format, dir, err),
			Type:           corev1.EventTypeWarning,
			Source:         corev1.EventSource{Component: "mxl-domain-agent", Host: node},
			FirstTimestamp: now,
			LastTimestamp:  now,
			Count:          1,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := c.Create(ctx, ev); cerr != nil {
			log.Error(cerr, "record event", "reason", reason, "directory", dir)
		}
	}
}
