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
	// Fallback is told about every directory left plain, and why.
	Fallback func(dir string, err error)
	// HugeRefused is told about every domain mounted without Huge, and
	// why.
	HugeRefused func(dir string, err error)
	Log         logr.Logger

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

// options are the mount options of every domain.
func (m *Mounter) options() string {
	if m.Huge == "" {
		return Options
	}
	return Options + ",huge=" + m.Huge
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
	options := m.options()
	err = mount(dir, options)
	// A module that predates huge= refuses it as an unknown parameter.
	// Without it the domain is still an mxlfs, with folios that end by EOF.
	if err != nil && m.Huge != "" && errors.Is(err, unix.EINVAL) {
		refused := err
		options = Options
		if err = mount(dir, options); err == nil {
			m.Log.Error(refused, "mxlfs refused huge=; mounted without it", "directory", dir, "huge", m.Huge)
			if m.HugeRefused != nil {
				m.HugeRefused(dir, refused)
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
