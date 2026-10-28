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
var ringFile = regexp.MustCompile(`/([0-9a-fA-F-]{36})\.mxl-flow/(grains/data\.[0-9]+|channels)$`)

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
	// Domain is the domain directory, as this process sees it, of the
	// filesystem the opens are on. An open's path is the opener's: a pod
	// that mounts the domain at /mxl opens /mxl/<id>.mxl-flow/..., so the
	// flow is found by its name below Domain, not by the path's prefix.
	Domain string

	// ProcessStart identifies a process's lifetime: its start time, read
	// as the kernel reports it, or false once it is gone. Nil reads
	// /proc, which the agent sees with the host's pids.
	ProcessStart func(pid int32) (string, bool)

	mu       sync.Mutex
	seen     map[string]time.Time
	asking   map[string]bool // the (pid, flow) requests still being made
	requests sync.WaitGroup  // the mirror requests Handle started
}

// minRetry is the shortest wait before a refused request is made again;
// tests shorten it.
var minRetry = time.Second

// retryable is a refusal that could pass later, as the intent
// dispatcher's burst limit reports. A zero RetryAfter is a refusal that
// waiting does not lift, and is final.
type retryable interface{ RetryAfter() time.Duration }

// processStart reads the start time of pid, field 22 of /proc/<pid>/stat,
// which tells a process apart from a later one under the same pid.
func processStart(pid int32) (string, bool) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", false
	}
	// The command name in field 2 may hold spaces; the fields after it
	// start past its closing parenthesis.
	i := strings.LastIndexByte(string(raw), ')')
	if i < 0 {
		return "", false
	}
	f := strings.Fields(string(raw[i+1:]))
	if len(f) < 20 {
		return "", false
	}
	return f[19], true
}

// request asks for a mirror of the placeholder a process opened. The
// open has already succeeded, so a request a limit refused reaches no
// one: the process maps a ring nothing fills and does not open it again.
// A request the burst limit refused is made again when that limit says
// it could pass, for as long as the process lives and the flow is still
// a placeholder here; a refusal waiting cannot lift, as by a count of
// live mirrors, is final. Each request costs API calls on the agent's one
// client, so nothing is asked again that could not pass.
func (t *Trigger) request(ctx context.Context, pid int32, domain, id string) {
	start := t.ProcessStart
	if start == nil {
		start = processStart
	}
	born, _ := start(pid)
	path := filepath.Join(domain, id+".mxl-flow", "data")
	for retries := 0; ; retries++ {
		err := t.Dispatcher.Materialize(ctx, pid, path)
		if err == nil {
			if retries > 0 {
				t.Log.Info("placeholder opened, mirror requested again and granted", "flowID", id, "pid", pid)
			}
			return
		}
		var r retryable
		if !errors.As(err, &r) || r.RetryAfter() <= 0 {
			t.Log.Info("placeholder opened, no mirror", "flowID", id, "pid", pid, "reason", err.Error())
			return
		}
		if retries == 0 {
			t.Log.Info("placeholder opened, no mirror yet; asking again while the process runs",
				"flowID", id, "pid", pid, "reason", err.Error(), "after", r.RetryAfter())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(max(r.RetryAfter(), minRetry)):
		}
		if now, ok := start(pid); !ok || now != born || !mxlv1alpha1.IsPlaceholder(domain, id) {
			return
		}
	}
}

// Handle starts a mirror for an open of a placeholder's ring, once per
// process and flow per Window. It does not wait for the mirror; the
// OpenWatch running the trigger does, before Run returns.
func (t *Trigger) Handle(ctx context.Context, o fanotify.Open) bool {
	m := ringFile.FindStringSubmatch(o.Path)
	if m == nil || o.PID <= 0 {
		return false
	}
	domain, id := t.Domain, strings.ToLower(m[1])
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
	// One request per process and flow: an open again after the window,
	// while an earlier request is still waiting to ask again, starts none.
	t.mu.Lock()
	if t.asking[key] {
		t.mu.Unlock()
		return false
	}
	if t.asking == nil {
		t.asking = make(map[string]bool)
	}
	t.asking[key] = true
	t.mu.Unlock()
	t.requests.Add(1)
	go func() {
		defer t.requests.Done()
		defer func() {
			t.mu.Lock()
			delete(t.asking, key)
			t.mu.Unlock()
		}()
		t.request(ctx, o.PID, domain, id)
	}()
	return true
}

// OpenWatch feeds a Trigger the opens on one filesystem.
type OpenWatch struct {
	t *Trigger
	// run forwards opens until ctx ends, then closes out; close releases
	// the watch. Seams for tests, which cannot mark a filesystem.
	run   func(ctx context.Context, out chan<- fanotify.Open) error
	close func() error
}

// Watch marks the filesystem holding domain for opens and makes domain
// t.Domain. domain is the root of that filesystem, as an mxlfs domain
// is. Opens are handled once Run runs.
func (t *Trigger) Watch(domain string) (*OpenWatch, error) {
	w, err := fanotify.NewOpenWatcher(domain)
	if err != nil {
		return nil, err
	}
	t.Domain = domain
	return &OpenWatch{t: t, run: w.Run, close: w.Close}, nil
}

// Run feeds every open on the watched filesystem to Handle until ctx
// ends, waits for the mirror requests Handle started, and releases the
// watch.
func (o *OpenWatch) Run(ctx context.Context) error {
	defer o.close()
	opens := make(chan fanotify.Open, 256)
	errc := make(chan error, 1)
	go func() { errc <- o.run(ctx, opens) }()
	for op := range opens {
		o.t.Handle(ctx, op)
	}
	o.t.requests.Wait()
	return <-errc
}

// Close releases a watch Run never ran.
func (o *OpenWatch) Close() error { return o.close() }

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

// Wanted is the queue of flows path walks on an mxlfs wait for.
type Wanted struct {
	mount Mount
	f     *os.File
	// next is wantedNext(f); a seam for tests, which have no mxlfs.
	next  func(req *wantedReq) error
	first *wantedReq // taken by the probe, handed to Run
}

// OpenWanted opens the queue of the mxlfs holding domain. It takes the
// first entry without waiting, so a module without MXLFS_IOC_WANTED_NEXT
// fails here rather than once Run runs.
func OpenWanted(domain string) (*Wanted, error) {
	mount, err := MountOf(domain)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(domain)
	if err != nil {
		return nil, err
	}
	w, err := newWanted(mount, f, wantedNext(f))
	if err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// wantedNext is MXLFS_IOC_WANTED_NEXT on f.
func wantedNext(f *os.File) func(req *wantedReq) error {
	return func(req *wantedReq) error {
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), iocWantedNext, uintptr(unsafe.Pointer(req)))
		if errno != 0 {
			return errno
		}
		return nil
	}
}

// newWanted probes the queue next reads, on f, the directory of mount's
// root.
func newWanted(mount Mount, f *os.File, next func(req *wantedReq) error) (*Wanted, error) {
	w := &Wanted{mount: mount, f: f, next: next}
	req := wantedReq{} // a zero timeout takes a queued entry, or none
	switch err := w.next(&req); {
	case err == nil:
		w.first = &req
	case errors.Is(err, unix.ETIMEDOUT), errors.Is(err, unix.EINTR):
	default:
		return nil, fmt.Errorf("MXLFS_IOC_WANTED_NEXT: %w", err)
	}
	return w, nil
}

// Run starts a mirror for each flow a path walk waits for, until ctx
// ends, waits for the requests it started, and closes the queue. A flow
// nobody publishes yet gets none; the walk then waits until its
// placeholder appears, or fails when wait= runs out. claim, when set, is
// offered each wanted flow directory first and takes it when it reports
// true: a redundancy set's output is merged here, not mirrored. A walk by
// a process ours reports as mxl-k8s's own is no reader and starts nothing.
func (w *Wanted) Run(ctx context.Context, d Materializer, claim func(ctx context.Context, flowDir string) bool,
	ours func(ctx context.Context, pid int32) bool, l logr.Logger) error {
	defer w.Close()
	var requests sync.WaitGroup
	defer requests.Wait()
	handle := func(req *wantedReq) {
		local, ok := w.mount.Local(unix.ByteSliceToString(req.Path[:]))
		if !ok {
			return
		}
		requests.Add(1)
		go func() {
			defer requests.Done()
			wanted(ctx, local, int32(req.PID), d, claim, ours, l)
		}()
	}
	if w.first != nil {
		handle(w.first)
	}
	for ctx.Err() == nil {
		req := wantedReq{TimeoutMs: 1000}
		if err := w.next(&req); err != nil {
			if errors.Is(err, unix.ETIMEDOUT) || errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("MXLFS_IOC_WANTED_NEXT: %w", err)
		}
		handle(&req)
	}
	return ctx.Err()
}

// Close releases a queue Run never ran.
func (w *Wanted) Close() error {
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

// wanted handles one flow a path walk waits for: nothing when the walker
// is mxl-k8s's own process, the claim when it takes the flow, a mirror
// otherwise.
func wanted(ctx context.Context, flowDir string, pid int32, d Materializer,
	claim func(ctx context.Context, flowDir string) bool, ours func(ctx context.Context, pid int32) bool, l logr.Logger) {
	if ours != nil && ours(ctx, pid) {
		return
	}
	if claim != nil && claim(ctx, flowDir) {
		return
	}
	if err := d.Materialize(ctx, pid, filepath.Join(flowDir, "data")); err != nil {
		l.Info("wanted flow, no mirror yet", "path", flowDir, "pid", pid, "reason", err.Error())
	}
}

// Locations is what a placeholder marker change asks of the publisher.
type Locations interface {
	PublishAppeared(ctx context.Context, dirName string) error
	PublishVanished(ctx context.Context, dirName string) error
}

// MarkerChanged publishes a placeholder marker change for flow id: a
// marker that goes leaves a location, a mirror's copy or a local
// producer's flow, and one that comes back, when the mirror filling the
// placeholder is deleted, leaves a directory that is no copy any more,
// though the gateway still holds it open.
func MarkerChanged(ctx context.Context, p Locations, id string, created bool) error {
	dir := id + ".mxl-flow"
	if created {
		return p.PublishVanished(ctx, dir)
	}
	return p.PublishAppeared(ctx, dir)
}

// MarkerDir creates the placeholder marker directory of domain, so its
// removals can be watched before the first placeholder exists.
func MarkerDir(domain string) (string, error) {
	dir := filepath.Join(domain, mxlv1alpha1.PlaceholderMarkerDir)
	return dir, os.MkdirAll(dir, 0o755)
}
