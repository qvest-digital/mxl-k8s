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

// Mounter decides once per directory whether it is an mxlfs.
type Mounter struct {
	// IsMxlfs reports whether dir is an mxlfs. Nil uses mxlfswatch.IsMxlfs.
	IsMxlfs func(dir string) bool
	// Mount mounts an mxlfs on dir. Nil uses the mount syscall.
	Mount func(dir string) error
	// Fallback is told about every directory left plain, and why.
	Fallback func(dir string, err error)
	Log      logr.Logger

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
	} else if err := mountEmpty(dir, mount); err != nil {
		m.Log.Error(err, "domain stays a plain directory", "directory", dir)
		if m.Fallback != nil {
			m.Fallback(dir, err)
		}
	} else {
		m.Log.Info("mounted mxlfs", "directory", dir, "options", Options)
	}
	if m.decided == nil {
		m.decided = map[string]bool{}
	}
	m.decided[dir] = true
	return nil
}

// Decided reports whether Ensure has run for dir: it is an mxlfs, or it
// stays plain.
func (m *Mounter) Decided(dir string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.decided[filepath.Clean(dir)]
}

// mountEmpty mounts on dir unless that would hide a flow: a producer
// keeps writing the hidden one, and every reader sees an empty domain.
func mountEmpty(dir string, mount func(string) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() && strings.HasSuffix(e.Name(), ".mxl-flow") {
			n++
		}
	}
	if n > 0 {
		return fmt.Errorf("%s holds %d flows a mount would hide", dir, n)
	}
	if err := mount(dir); err != nil {
		if errors.Is(err, unix.ENODEV) {
			return fmt.Errorf("mount mxlfs on %s: %w (the kernel has no mxlfs; is the module loaded?)", dir, err)
		}
		return fmt.Errorf("mount mxlfs on %s: %w", dir, err)
	}
	return nil
}

func sysMount(dir string) error {
	return unix.Mount("mxlfs", dir, "mxlfs", 0, Options)
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
	return func(dir string, err error) {
		now := metav1.NewTime(time.Now())
		ev := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{GenerateName: node + ".", Namespace: metav1.NamespaceDefault},
			// A Node's events name it by its name as UID, as the kubelet's do.
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: node, UID: types.UID(node)},
			Reason:         ReasonUnavailable,
			Message:        fmt.Sprintf("%s stays a plain directory: %v", dir, err),
			Type:           corev1.EventTypeWarning,
			Source:         corev1.EventSource{Component: "mxl-domain-agent", Host: node},
			FirstTimestamp: now,
			LastTimestamp:  now,
			Count:          1,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := c.Create(ctx, ev); cerr != nil {
			log.Error(cerr, "record event", "reason", ReasonUnavailable, "directory", dir)
		}
	}
}
