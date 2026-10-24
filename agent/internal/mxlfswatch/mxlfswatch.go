// Package mxlfswatch starts mirrors for placeholders on an mxlfs domain.
//
// On an mxlfs domain the gateway lists every flow through a placeholder,
// so a consumer opens a flow it has no copy of without failing. What
// tells the agent the flow is wanted is the consumer opening the
// placeholder's ring: libmxl maps every grain file, or the channels file,
// when it opens a flow, and directory scans never do. A path walk that
// finds no flow at all, not even a placeholder, waits in the kernel when
// the domain is mounted with wait=, and is reported through
// MXLFS_IOC_WANTED_NEXT.
package mxlfswatch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"

	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Magic is the f_type statfs reports for an mxlfs.
const Magic = 0x6d786c66

// IsMxlfs reports whether path is on an mxlfs.
func IsMxlfs(path string) bool {
	var st unix.Statfs_t
	return unix.Statfs(path, &st) == nil && st.Type == Magic
}

// ringFile matches what libmxl maps when it opens a flow: a grain of a
// discrete flow or the channels of a continuous one.
var ringFile = regexp.MustCompile(`^(.*)/([0-9a-fA-F-]{36})\.mxl-flow/(grains/data\.[0-9]+|channels)$`)

// Materializer is the part of the intent dispatcher a trigger needs.
type Materializer interface {
	Materialize(ctx context.Context, pid int32, path string) error
}

// Trigger turns opens of placeholder rings into mirror requests.
type Trigger struct {
	Dispatcher Materializer
	// Ours reports whether pid belongs to mxl-k8s itself: the gateway
	// creating and filling placeholders, the exporter reading them.
	Ours func(ctx context.Context, pid int32) bool
	// Window is how long one process's open of one flow counts as one
	// request; libmxl opens every grain file of a flow in a burst.
	Window time.Duration
	Log    logr.Logger

	mu   sync.Mutex
	seen map[string]time.Time
}

// Handle starts a mirror for an open of a placeholder's ring, once per
// process and flow per Window. It does not wait for the mirror.
func (t *Trigger) Handle(ctx context.Context, o fanotify.Open) bool {
	m := ringFile.FindStringSubmatch(o.Path)
	if m == nil || o.PID <= 0 {
		return false
	}
	domain, id := m[1], strings.ToLower(m[2])
	if !mxlv1alpha1.IsPlaceholder(domain, id) {
		return false
	}
	key := fmt.Sprintf("%d/%s/%s", o.PID, domain, id)
	window := t.Window
	if window <= 0 {
		window = 10 * time.Second
	}
	now := time.Now()
	t.mu.Lock()
	if t.seen == nil {
		t.seen = make(map[string]time.Time)
	}
	if last, ok := t.seen[key]; ok && now.Sub(last) < window {
		t.mu.Unlock()
		return false
	}
	t.seen[key] = now
	for k, at := range t.seen {
		if now.Sub(at) >= window {
			delete(t.seen, k)
		}
	}
	t.mu.Unlock()
	if t.Ours != nil && t.Ours(ctx, o.PID) {
		return false
	}
	flowDir := filepath.Join(domain, id+".mxl-flow")
	go func() {
		if err := t.Dispatcher.Materialize(ctx, o.PID, filepath.Join(flowDir, "data")); err != nil {
			t.Log.Info("placeholder opened, no mirror", "flowID", id, "pid", o.PID, "reason", err.Error())
		}
	}()
	return true
}

// Run feeds every open on the filesystem holding domain to Handle until
// ctx ends.
func (t *Trigger) Run(ctx context.Context, domain string) error {
	w, err := fanotify.NewOpenWatcher(domain)
	if err != nil {
		return err
	}
	defer w.Close()
	opens := make(chan fanotify.Open, 256)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx, opens) }()
	for o := range opens {
		t.Handle(ctx, o)
	}
	return <-errc
}

// wantedReq mirrors struct mxlfs_wanted_req in mxlfs_uapi.h.
type wantedReq struct {
	ID        uint64
	PID       uint32
	TimeoutMs uint32
	Path      [4080]byte
}

// iocWantedNext is _IOWR('x', 5, struct mxlfs_wanted_req).
const iocWantedNext = 3<<30 | uintptr(unsafe.Sizeof(wantedReq{}))<<16 | 'x'<<8 | 5

// Mount is where this process sees the root of a filesystem.
type Mount struct {
	Root       string // the mount's root within its filesystem
	MountPoint string
}

// MountOf returns the mount of the mxlfs holding path, from
// /proc/self/mountinfo: the deepest mount point that is a prefix of path.
func MountOf(path string) (Mount, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return Mount{}, err
	}
	defer f.Close()
	var best Mount
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		sep := -1
		for i, fl := range fields {
			if fl == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || len(fields) < sep+2 || fields[sep+1] != "mxlfs" || len(fields) < 5 {
			continue
		}
		mp := unescape(fields[4])
		if (path == mp || strings.HasPrefix(path, strings.TrimSuffix(mp, "/")+"/")) && len(mp) > len(best.MountPoint) {
			best = Mount{Root: unescape(fields[3]), MountPoint: mp}
		}
	}
	if best.MountPoint == "" {
		return Mount{}, fmt.Errorf("no mxlfs mount holds %s", path)
	}
	return best, sc.Err()
}

// unescape undoes mountinfo's octal escapes of space, tab, newline and
// backslash.
func unescape(s string) string {
	r := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return r.Replace(s)
}

// Local maps a path from the root of the filesystem to where this process
// sees it, or reports false when the mount does not show it.
func (m Mount) Local(fsPath string) (string, bool) {
	root := strings.TrimSuffix(m.Root, "/")
	if fsPath != root && !strings.HasPrefix(fsPath, root+"/") {
		return "", false
	}
	return filepath.Join(m.MountPoint, strings.TrimPrefix(fsPath, root)), true
}

// RunWanted asks the mxlfs holding domain for flows path walks wait for
// and starts a mirror for each, until ctx ends. A flow nobody publishes
// yet gets none; the walk then waits until its placeholder appears, or
// fails when wait= runs out. claim, when set, is offered each wanted flow
// directory first and takes it when it reports true: a redundancy set's
// output is merged here, not mirrored.
func RunWanted(ctx context.Context, domain string, d Materializer, claim func(ctx context.Context, flowDir string) bool, l logr.Logger) error {
	mount, err := MountOf(domain)
	if err != nil {
		return err
	}
	f, err := os.Open(domain)
	if err != nil {
		return err
	}
	defer f.Close()
	for ctx.Err() == nil {
		req := wantedReq{TimeoutMs: 1000}
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), iocWantedNext, uintptr(unsafe.Pointer(&req)))
		if errno != 0 {
			if errors.Is(errno, unix.ETIMEDOUT) || errors.Is(errno, unix.EINTR) {
				continue
			}
			return fmt.Errorf("MXLFS_IOC_WANTED_NEXT: %w", errno)
		}
		fsPath := unix.ByteSliceToString(req.Path[:])
		local, ok := mount.Local(fsPath)
		if !ok {
			continue
		}
		pid := int32(req.PID)
		go func() {
			if claim != nil && claim(ctx, local) {
				return
			}
			if err := d.Materialize(ctx, pid, filepath.Join(local, "data")); err != nil {
				l.Info("wanted flow, no mirror yet", "path", local, "pid", pid, "reason", err.Error())
			}
		}()
	}
	return ctx.Err()
}

// MarkerDir creates the placeholder marker directory of domain, so its
// removals can be watched before the first placeholder exists.
func MarkerDir(domain string) (string, error) {
	dir := filepath.Join(domain, mxlv1alpha1.PlaceholderMarkerDir)
	return dir, os.MkdirAll(dir, 0o755)
}
