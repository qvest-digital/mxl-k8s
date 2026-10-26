package merge

/*
#cgo pkg-config: libmxl
#include <mxl/flowinfo.h>
#include <mxl/flow.h>
#include <mxl/dataformat.h>
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/go-logr/logr"
	"github.com/qvest-digital/go-mxl/mxl"
	"golang.org/x/sys/unix"
)

// mxlfs ioctls, from mxlfs_uapi.h.
const (
	iocViewSetSources  = 1<<30 | 40<<16 | 'x'<<8 | 1 // _IOW('x', 1, struct mxlfs_view_sources)
	iocViewSelect      = 1<<30 | 4<<16 | 'x'<<8 | 2  // _IOW('x', 2, __u32)
	iocViewSelectRange = 1<<30 | 24<<16 | 'x'<<8 | 9 // _IOW('x', 9, struct mxlfs_view_range)
	iocAllocate        = 'x'<<8 | 8                  // _IO('x', 8)
	// _IOW('x', 10, struct mxlfs_view_replace); an mxlfs without
	// MXLFS_IOC_VIEW_REPLACE_SOURCE answers it with ENOTTY.
	iocViewReplaceSource = 1<<30 | 12<<16 | 'x'<<8 | 10
	maxSources           = 8

	// maxSleep is the longest the engine waits between two passes.
	maxSleep = 20 * time.Millisecond

	sysFutexWaitv = 449 // the same number on every architecture
	futex2SizeU32 = 0x02
	futexWake     = 1
	futexWakeAll  = 1 << 30
)

// futexWaitv is struct futex_waitv.
type futexWaitv struct {
	Val      uint64
	Uaddr    uint64
	Flags    uint32
	Reserved uint32
}

type viewSources struct {
	Count uint32
	Flags uint32
	FDs   [maxSources]int32
}

type viewRange struct {
	Source uint32
	Flags  uint32
	Offset uint64
	Length uint64
}

type viewReplace struct {
	Source uint32
	FD     int32
	Flags  uint32
}

// flowState follows mxlFlowInfo in a flow's data file: the inode readers
// check the data file against, and the word they wait on.
type flowState struct {
	inode uint64
	sync  uint32
}

// header is a mapped flow data file.
type header struct {
	mem   []byte
	info  *C.mxlFlowInfo
	state *flowState
}

func mapHeader(path string, write bool) (*header, *os.File, error) {
	flag, prot := os.O_RDONLY, unix.PROT_READ
	if write {
		flag, prot = os.O_RDWR, unix.PROT_READ|unix.PROT_WRITE
	}
	f, err := os.OpenFile(path, flag, 0)
	if err != nil {
		return nil, nil, err
	}
	h, err := mapHeaderFile(f, prot)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return h, f, nil
}

// mapHeaderFile maps the flow header f holds.
func mapHeaderFile(f *os.File, prot int) (*header, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(C.sizeof_mxlFlowInfo)+int64(unsafe.Sizeof(flowState{})) {
		return nil, fmt.Errorf("%s: %d bytes is no flow header", f.Name(), st.Size())
	}
	mem, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), prot, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &header{mem: mem, info: (*C.mxlFlowInfo)(unsafe.Pointer(&mem[0])),
		state: (*flowState)(unsafe.Pointer(&mem[C.sizeof_mxlFlowInfo]))}, nil
}

func (h *header) head() uint64 {
	return atomic.LoadUint64((*uint64)(unsafe.Pointer(&h.info.runtime.headIndex)))
}

func (h *header) syncWord() *uint32 { return &h.state.sync }

func (h *header) format() uint32 { return uint32(h.info.config.common.format) }

var (
	formatAudio = uint32(C.MXL_DATA_FORMAT_AUDIO)
	formatData  = uint32(C.MXL_DATA_FORMAT_DATA)
	flagInvalid = uint32(C.MXL_GRAIN_FLAG_INVALID)
)

// discrete returns grainCount; continuous returns channelCount and
// bufferLength. The config union, which cgo does not expose, follows the
// common config: four slice sizes, then grainCount, or channelCount then
// bufferLength.
func (h *header) discrete() uint32 {
	u := unsafe.Add(unsafe.Pointer(&h.info.config), C.sizeof_mxlCommonFlowConfigInfo)
	return *(*uint32)(unsafe.Add(u, 4*C.MXL_MAX_PLANES_PER_GRAIN))
}

func (h *header) continuous() (channels, length uint32) {
	u := unsafe.Add(unsafe.Pointer(&h.info.config), C.sizeof_mxlCommonFlowConfigInfo)
	return *(*uint32)(u), *(*uint32)(unsafe.Add(u, 4))
}

// Spec is one output to serve.
type Spec struct {
	DomainDir string   // the domain directory on this node
	OutputID  string   // the output flow id
	Members   []string // the member flow ids, in preference order
	Skew      time.Duration
	Ordered   bool
	// Stalls, when set, is called from the engine's loop when a member
	// becomes stalled or moves again; it must not block.
	Stalls func()
}

// Engine serves one output.
type Engine struct {
	spec     Spec
	log      logr.Logger
	outDir   string
	out      *header
	outFile  *os.File
	members  []*header
	files    []*os.File
	views    []*os.File // one per grain slot, or the channels file
	grains   [][]*C.mxlGrainInfo
	grainMem [][][]byte // per member, per slot
	audio    *Audio
	disc     *Discrete
	rate     C.mxlRational
	period   time.Duration // of a grain
	feeds    []bool        // members the gateway reports arrivals for

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}

	// replaceable is whether the module can put another file in place of
	// a view's source; replaces hands Replace to the engine's loop.
	replaceable bool
	replaces    chan replaceReq
	closing     sync.RWMutex // held by Replace, taken by Close
	closed      bool

	waitFailed sync.Once // logs the first futex_waitv failure

	// selects counts the slots or pages the engine pointed at another
	// source, each one a remap in every reader of the output.
	selects atomic.Uint64

	// progress follows the members' heads; nil on an engine Start did
	// not build.
	progress *progress
}

func flowDir(domain, id string) string { return filepath.Join(domain, id+".mxl-flow") }

// Start builds the output from the members, which must all be present
// in the domain, and serves it until Close.
func Start(ctx context.Context, spec Spec, log logr.Logger) (*Engine, error) {
	if len(spec.Members) < 1 || len(spec.Members) > maxSources-1 {
		return nil, fmt.Errorf("%d members; 1 to %d", len(spec.Members), maxSources-1)
	}
	e := &Engine{spec: spec, log: log, outDir: flowDir(spec.DomainDir, spec.OutputID)}
	ok := false
	defer func() {
		if !ok {
			e.release()
		}
	}()
	for _, id := range spec.Members {
		path := filepath.Join(flowDir(spec.DomainDir, id), "data")
		h, f, err := mapHeader(path, false)
		if err != nil {
			return nil, err
		}
		e.members = append(e.members, h)
		e.files = append(e.files, f)
	}
	if err := e.checkAligned(); err != nil {
		return nil, err
	}
	if err := e.build(); err != nil {
		return nil, err
	}
	e.replaceable = canReplace(e.views[0])
	e.progress = newProgress(e.heads(), time.Now())
	ok = true
	rctx, cancel := context.WithCancel(ctx)
	e.cancel, e.done, e.replaces = cancel, make(chan struct{}), make(chan replaceReq)
	go e.run(rctx)
	return e, nil
}

// checkAligned refuses members that cannot be merged slot for slot: a
// different format, rate or ring geometry.
func (e *Engine) checkAligned() error {
	m0 := e.members[0]
	for i, m := range e.members[1:] {
		if !aligned(m0, m) {
			return fmt.Errorf("member %s has %s, %s %s", e.spec.Members[i+1], m.layout(), e.spec.Members[0], m0.layout())
		}
	}
	e.rate = m0.info.config.common.grainRate
	return nil
}

// build creates the output in a scaffold and renames it into the domain.
func (e *Engine) build() error {
	tmp, err := os.MkdirTemp(e.spec.DomainDir, ".mxl-tmp-merge-")
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(tmp)
		}
	}()
	src0 := flowDir(e.spec.DomainDir, e.spec.Members[0])
	def, err := os.ReadFile(filepath.Join(src0, "flow_def.json"))
	if err != nil {
		return err
	}
	def, err = withID(def, e.spec.OutputID)
	if err != nil {
		return fmt.Errorf("member %s flow_def.json: %w", e.spec.Members[0], err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "flow_def.json"), def, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmp, "access"), nil, 0o666); err != nil {
		return err
	}
	if err := e.buildHeader(tmp); err != nil {
		return err
	}
	if e.members[0].format() == formatAudio {
		err = e.buildAudio(tmp, src0)
	} else {
		err = e.buildDiscrete(tmp, src0)
	}
	if err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		return err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, tmp, unix.AT_FDCWD, e.outDir, unix.RENAME_NOREPLACE); err != nil {
		return fmt.Errorf("publish %s: %w", e.outDir, err)
	}
	cleanup = false
	return nil
}

// withID returns the flow definition def with its id set to id. Only the
// top-level id is the flow's: the same uuid can appear anywhere else in
// a definition, in parents or a label, and in either case.
func withID(def []byte, id string) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(def, &m); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(id)
	if err != nil {
		return nil, err
	}
	m["id"] = raw
	return json.Marshal(m)
}

func (e *Engine) buildHeader(tmp string) error {
	path := filepath.Join(tmp, "data")
	m0 := e.members[0]
	if err := os.WriteFile(path, m0.mem, 0o644); err != nil {
		return err
	}
	h, f, err := mapHeader(path, true)
	if err != nil {
		return err
	}
	e.out, e.outFile = h, f
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return err
	}
	id, err := parseUUID(e.spec.OutputID)
	if err != nil {
		return err
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&h.info.config.common.id[0])), 16), id[:])
	h.info.runtime.headIndex = 0
	h.info.runtime.lastReadTime = 0
	h.info.runtime.lastWriteTime = C.uint64_t(time.Now().UnixNano())
	h.state.inode = st.Ino
	h.state.sync = 0
	// A shared lock is what libmxl's garbage collection takes as a writer.
	return unix.Flock(int(f.Fd()), unix.LOCK_SH)
}

func parseUUID(s string) ([16]byte, error) {
	var out [16]byte
	hex := bytes.ReplaceAll([]byte(s), []byte("-"), nil)
	if len(hex) != 32 {
		return out, fmt.Errorf("bad flow id %q", s)
	}
	for i := range out {
		var b byte
		if _, err := fmt.Sscanf(string(hex[2*i:2*i+2]), "%02x", &b); err != nil {
			return out, fmt.Errorf("bad flow id %q", s)
		}
		out[i] = b
	}
	return out, nil
}

// view creates path as a view of size bytes over sources.
func view(path string, size int64, sources []*os.File) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	req := viewSources{Count: uint32(len(sources))}
	for i, s := range sources {
		req.FDs[i] = int32(s.Fd())
	}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), iocViewSetSources, uintptr(unsafe.Pointer(&req))); errno != 0 {
		f.Close()
		return nil, fmt.Errorf("MXLFS_IOC_VIEW_SET_SOURCES on %s: %w", path, errno)
	}
	return f, nil
}

// allocate gives f size bytes of pages. The gateway allocates lazily on
// mxlfs, so a plain fallocate leaves holes, and a view page selecting a
// hole faults SIGBUS in its readers.
func allocate(f *os.File, size int64) error {
	if err := unix.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		return err
	}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), iocAllocate, 0); errno != 0 {
		return fmt.Errorf("MXLFS_IOC_ALLOCATE on %s: %w", f.Name(), errno)
	}
	return nil
}

func selectAll(f *os.File, source int) error {
	v := uint32(source)
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), iocViewSelect, uintptr(unsafe.Pointer(&v))); errno != 0 {
		return fmt.Errorf("MXLFS_IOC_VIEW_SELECT: %w", errno)
	}
	return nil
}

func selectRange(f *os.File, source int, offset, length uint64) error {
	r := viewRange{Source: uint32(source), Offset: offset, Length: length}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), iocViewSelectRange, uintptr(unsafe.Pointer(&r))); errno != 0 {
		return fmt.Errorf("MXLFS_IOC_VIEW_SELECT_RANGE: %w", errno)
	}
	return nil
}

// canReplace reports whether the module has MXLFS_IOC_VIEW_REPLACE_SOURCE.
// It asks with flags none accepts: a module with the ioctl refuses them
// with EINVAL, one without it does not know the command.
func canReplace(v *os.File) bool {
	req := viewReplace{Flags: ^uint32(0)}
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, v.Fd(), iocViewReplaceSource, uintptr(unsafe.Pointer(&req)))
	return errno == unix.EINVAL
}

func replaceSource(v *os.File, source int, f *os.File) error {
	req := viewReplace{Source: uint32(source), FD: int32(f.Fd())}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, v.Fd(), iocViewReplaceSource, uintptr(unsafe.Pointer(&req))); errno != 0 {
		return fmt.Errorf("MXLFS_IOC_VIEW_REPLACE_SOURCE: %w", errno)
	}
	return nil
}

// mapGrainInfo maps the grain header at the start of a grain file.
func mapGrainInfo(f *os.File) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, int(C.sizeof_mxlGrainInfo), unix.PROT_READ, unix.MAP_SHARED)
}

func (e *Engine) buildDiscrete(tmp, src0 string) error {
	grains := uint(e.members[0].discrete())
	st, err := os.Stat(filepath.Join(src0, "grains", "data.0"))
	if err != nil {
		return err
	}
	size := st.Size()
	// The INVALID grain: what a slot shows when no member delivered it.
	inv, err := os.OpenFile(filepath.Join(tmp, "invalid"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer inv.Close()
	if err := allocate(inv, size); err != nil {
		return err
	}
	tmpl, err := os.ReadFile(filepath.Join(src0, "grains", "data.0"))
	if err != nil {
		return err
	}
	g := (*C.mxlGrainInfo)(unsafe.Pointer(&tmpl[0]))
	g.index = 0
	g.flags = C.uint32_t(flagInvalid)
	g.validSlices = g.totalSlices
	if _, err := inv.WriteAt(tmpl[:C.sizeof_mxlGrainInfo], 0); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(tmp, "grains"), 0o755); err != nil {
		return err
	}
	placeholder := len(e.members)
	e.grains = make([][]*C.mxlGrainInfo, len(e.members))
	e.grainMem = make([][][]byte, len(e.members))
	for slot := uint(0); slot < grains; slot++ {
		var srcs []*os.File
		for m, id := range e.spec.Members {
			f, err := os.Open(filepath.Join(flowDir(e.spec.DomainDir, id), "grains", fmt.Sprintf("data.%d", slot)))
			if err != nil {
				return err
			}
			mem, err := mapGrainInfo(f)
			if err != nil {
				f.Close()
				return err
			}
			e.grainMem[m] = append(e.grainMem[m], mem)
			e.grains[m] = append(e.grains[m], (*C.mxlGrainInfo)(unsafe.Pointer(&mem[0])))
			srcs = append(srcs, f)
		}
		srcs = append(srcs, inv)
		v, err := view(filepath.Join(tmp, "grains", fmt.Sprintf("data.%d", slot)), size, srcs)
		for _, f := range srcs[:len(srcs)-1] {
			f.Close()
		}
		if err != nil {
			return err
		}
		if err := selectAll(v, placeholder); err != nil {
			v.Close()
			return err
		}
		e.views = append(e.views, v)
	}
	e.disc = NewDiscrete(grains, placeholder)
	e.disc.Data = e.members[0].format() == formatData
	e.disc.Ordered = e.spec.Ordered
	e.period = time.Duration(float64(time.Second) * float64(e.rate.denominator) / float64(e.rate.numerator))
	e.disc.Skew = max(uint64(e.spec.Skew/e.period), 1)
	e.disc.Tolerance = ToleranceFor(e.spec.Skew)
	return nil
}

func (e *Engine) buildAudio(tmp, src0 string) error {
	channels, length := e.members[0].continuous()
	if length%PageSamples != 0 {
		return fmt.Errorf("channel buffer of %d samples is not whole pages", length)
	}
	size := int64(channels) * int64(length) * 4
	sil, err := os.OpenFile(filepath.Join(tmp, "silence"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer sil.Close()
	if err := allocate(sil, size); err != nil {
		return err
	}
	var srcs []*os.File
	for _, id := range e.spec.Members {
		f, err := os.Open(filepath.Join(flowDir(e.spec.DomainDir, id), "channels"))
		if err != nil {
			return err
		}
		defer f.Close()
		srcs = append(srcs, f)
	}
	srcs = append(srcs, sil)
	v, err := view(filepath.Join(tmp, "channels"), size, srcs)
	if err != nil {
		return err
	}
	e.views = append(e.views, v)
	silence := len(e.members)
	if err := selectAll(v, silence); err != nil {
		return err
	}
	e.audio = NewAudio(len(e.members), uint64(length)/PageSamples, silence)
	e.audio.Skew = uint64(e.spec.Skew.Seconds() * float64(e.rate.numerator) / float64(e.rate.denominator))
	e.audio.Tolerance = ToleranceFor(e.spec.Skew)
	e.audio.Ahead = uint64(maxSleep.Seconds()*float64(e.rate.numerator)/float64(e.rate.denominator)) + 1
	return nil
}

// Committed reports that member committed samples [start, end) of an
// audio flow: the gateway knows each arrival from its completion queue,
// which tells a skipped batch from two quick ones where the ring cannot.
//
// The gateway names the flow, not the ring: the directory at that id may
// since have been replaced, a placeholder retired for another geometry,
// while the engine still maps the old ring. Only samples the mapped
// ring's own head covers count. A ring nobody wrote may hold no pages on
// mxlfs, and a page selected to it faults SIGBUS in the output's readers.
func (e *Engine) Committed(member int, start, end uint64) {
	if e.audio == nil {
		return
	}
	e.mu.Lock()
	if e.feeds == nil {
		e.feeds = make([]bool, len(e.members))
	}
	e.feeds[member] = true
	if end = min(end, e.members[member].head()); end > start {
		e.audio.Committed(member, start, end)
	}
	e.mu.Unlock()
}

// MemberIndex is the position of flow id among the members, or -1.
func (e *Engine) MemberIndex(id string) int {
	for i, m := range e.spec.Members {
		if m == id {
			return i
		}
	}
	return -1
}

// ErrNoReplace is Replace on an mxlfs without
// MXLFS_IOC_VIEW_REPLACE_SOURCE.
var ErrNoReplace = errors.New("mxlfs has no MXLFS_IOC_VIEW_REPLACE_SOURCE")

// ErrNotOpened is Replace failing before it touched the engine because
// the member's new ring could not be opened. The engine serves on as it
// was.
var ErrNotOpened = errors.New("the member's ring could not be opened")

// ErrOtherLayout is Replace refusing a new ring whose layout is not the
// output's. The flow definition does not show a ring's depth, which is
// the domain's history duration times the rate: a member restarted after
// the domain's history duration changed has the output's geometry by its
// definition and still another depth. Trying again cannot change that.
// The engine serves on as it was; only an output built again can take it.
var ErrOtherLayout = errors.New("the member's ring is not of the output's layout")

var errStopped = errors.New("engine stopped")

type replaceReq struct {
	member int
	ring   *memberRing
	done   chan error
}

// memberRing is a member's ring, opened for the engine to take.
type memberRing struct {
	h    *header
	f    *os.File
	ino  uint64
	srcs []*os.File // the grain files or the channels file, one per view
	mems [][]byte   // the mapped grain headers of a discrete ring
}

// close releases what the engine did not take: the source files always,
// the mappings and the data file unless taken.
func (r *memberRing) close(taken bool) {
	for _, s := range r.srcs {
		s.Close()
	}
	if taken {
		return
	}
	for _, m := range r.mems {
		_ = unix.Munmap(m)
	}
	if r.h != nil {
		_ = unix.Munmap(r.h.mem)
	}
	if r.f != nil {
		r.f.Close()
	}
}

// Replace serves member from the ring now in its flow directory in place
// of the one it was started over, and returns the inode of the data file
// it mapped. A restarted producer recreates its flow directory, and with
// it every file: the output and the readers holding it stay as they are,
// only the member's source in each view is replaced. The ring is opened
// here, so the engine's loop is held only for the selects and the swap.
// ErrNoReplace, ErrNotOpened, ErrOtherLayout and an engine closed leave
// the engine as it was; any other error may leave it half replaced, and
// then it has to be closed. Close waits for a Replace under way.
func (e *Engine) Replace(member int) (uint64, error) {
	if !e.replaceable {
		return 0, ErrNoReplace
	}
	e.closing.RLock()
	defer e.closing.RUnlock()
	if e.closed {
		return 0, errStopped
	}
	r, err := e.openRing(member)
	if errors.Is(err, ErrOtherLayout) {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrNotOpened, err)
	}
	req := replaceReq{member: member, ring: r, done: make(chan error, 1)}
	select {
	case e.replaces <- req:
	case <-e.done:
		r.close(false)
		return 0, errStopped
	}
	if err := <-req.done; err != nil {
		return 0, err
	}
	return r.ino, nil
}

// openRing opens and maps the ring in member's flow directory. Every file
// is opened relative to the one directory, so all of them are of the one
// ring even while the directory is being replaced again.
func (e *Engine) openRing(member int) (_ *memberRing, err error) {
	dir, err := os.Open(flowDir(e.spec.DomainDir, e.spec.Members[member]))
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	open := func(name string) (*os.File, error) {
		fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", filepath.Join(dir.Name(), name), err)
		}
		return os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name)), nil
	}
	r := &memberRing{}
	defer func() {
		if err != nil {
			r.close(false)
		}
	}()
	if r.f, err = open("data"); err != nil {
		return nil, err
	}
	if r.h, err = mapHeaderFile(r.f, unix.PROT_READ); err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(r.f.Fd()), &st); err != nil {
		return nil, err
	}
	r.ino = st.Ino
	if !aligned(e.out, r.h) {
		return nil, fmt.Errorf("%w: member %s has %s, the output %s", ErrOtherLayout, e.spec.Members[member],
			r.h.layout(), e.out.layout())
	}
	if e.disc == nil {
		s, err := open("channels")
		if err != nil {
			return nil, err
		}
		r.srcs = append(r.srcs, s)
		return r, nil
	}
	for slot := range e.views {
		s, err := open(fmt.Sprintf("grains/data.%d", slot))
		if err != nil {
			return nil, err
		}
		r.srcs = append(r.srcs, s)
		mem, err := mapGrainInfo(s)
		if err != nil {
			return nil, err
		}
		r.mems = append(r.mems, mem)
	}
	return r, nil
}

// layout describes what aligned compares.
func (h *header) layout() string {
	r := h.info.config.common.grainRate
	if h.format() == formatAudio {
		c, l := h.continuous()
		return fmt.Sprintf("format %d at %d/%d, %d channels of %d samples", h.format(), r.numerator, r.denominator, c, l)
	}
	return fmt.Sprintf("format %d at %d/%d, %d grains", h.format(), r.numerator, r.denominator, h.discrete())
}

// ringLayout returns the layout of the ring in flow id's directory in
// domain. Two rings are aligned when their layouts are equal.
func ringLayout(domain, id string) (string, error) {
	h, f, err := mapHeader(filepath.Join(flowDir(domain, id), "data"), false)
	if err != nil {
		return "", err
	}
	defer f.Close()
	defer func() { _ = unix.Munmap(h.mem) }()
	return h.layout(), nil
}

// aligned reports whether member m's ring has the layout of ref's.
func aligned(ref, m *header) bool {
	if m.format() != ref.format() || m.info.config.common.grainRate != ref.info.config.common.grainRate {
		return false
	}
	if ref.format() == formatAudio {
		c0, l0 := ref.continuous()
		c, l := m.continuous()
		return c == c0 && l == l0
	}
	return m.discrete() == ref.discrete()
}

// take is Replace on the engine's loop: it moves every slot or page the
// member serves to a member that holds its grain or samples, or to the
// INVALID grain or silence, as the new ring holds none of them and a
// lazily held one may hold no pages at all; then puts the new ring in
// place of the old in every view and in the engine.
func (e *Engine) take(member int, r *memberRing) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.drop(member); err != nil {
		r.close(false)
		return err
	}
	for k, v := range e.views {
		if err := replaceSource(v, member, r.srcs[k]); err != nil {
			r.close(false)
			return fmt.Errorf("%s: %w", v.Name(), err)
		}
	}
	_ = unix.Munmap(e.members[member].mem)
	e.files[member].Close()
	e.members[member], e.files[member] = r.h, r.f
	if e.disc != nil {
		for k, m := range e.grainMem[member] {
			_ = unix.Munmap(m)
			e.grains[member][k] = (*C.mxlGrainInfo)(unsafe.Pointer(&r.mems[k][0]))
		}
		e.grainMem[member] = r.mems
	}
	r.close(true)
	e.log.Info("replaced a member's ring", "member", e.spec.Members[member], "inode", r.ino)
	return nil
}

// drop moves every slot or page member serves off it.
func (e *Engine) drop(member int) error {
	if e.disc != nil {
		sel := e.disc.Drop(e.discreteMembers(), member)
		e.selects.Add(uint64(len(sel)))
		return e.apply(sel)
	}
	sel := e.audio.Drop(member)
	e.selects.Add(uint64(len(sel)))
	return e.selectPages(sel)
}

// discreteMembers is what the discrete policy sees of the members now.
func (e *Engine) discreteMembers() []Member {
	ms := make([]Member, len(e.members))
	for i, m := range e.members {
		i := i
		ms[i] = Member{Head: m.head(), Grain: func(slot uint) Grain {
			g := e.grains[i][slot]
			return Grain{Index: uint64(atomic.LoadUint64((*uint64)(unsafe.Pointer(&g.index)))),
				Invalid:     uint32(g.flags)&flagInvalid != 0,
				ValidSlices: uint16(g.validSlices), TotalSlices: uint16(g.totalSlices)}
		}}
	}
	return ms
}

// selectPages points page slots of every channel at their sources.
func (e *Engine) selectPages(sel []PageSelect) error {
	channels, length := e.members[0].continuous()
	for _, s := range sel {
		for c := uint64(0); c < uint64(channels); c++ {
			if err := selectRange(e.views[0], s.Source, (c*uint64(length)+s.Page*PageSamples)*4, 4096); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) run(ctx context.Context) {
	defer close(e.done)
	waits := make([]futexWaitv, len(e.members))
	lastHead := make([]uint64, len(e.members))
	report := time.Now()
	for ctx.Err() == nil {
		select {
		case req := <-e.replaces:
			err := e.take(req.member, req.ring)
			if err == nil {
				lastHead[req.member] = 0 // the new ring's head is no advance
			}
			req.done <- err
		default:
		}
		if time.Since(report) >= time.Minute {
			report = time.Now()
			e.log.Info("merging", "head", e.out.head(), "selects", e.selects.Load())
		}
		timeout := maxSleep
		for i, m := range e.members {
			waits[i] = futexWaitv{Val: uint64(atomic.LoadUint32(m.syncWord())),
				Uaddr: uint64(uintptr(unsafe.Pointer(m.syncWord()))), Flags: futex2SizeU32}
		}
		now := time.Now()
		if e.progress.observe(e.heads(), now) && e.spec.Stalls != nil {
			e.spec.Stalls()
		}
		var head uint64
		var wait time.Duration
		var err error
		if e.disc != nil {
			ms := e.discreteMembers()
			var sel []Select
			sel, head, wait = e.disc.Step(ms, now)
			// Retire looks as far as the index due, so the engine must
			// pass before every grain boundary.
			due, next := retireDue(mxl.Now(), e.mxlRate(), e.retireGuard())
			timeout = min(timeout, next)
			for i, m := range e.members {
				ms[i].Head = m.head()
			}
			sel = append(sel, e.disc.Retire(ms, due)...)
			e.selects.Add(uint64(len(sel)))
			err = e.apply(sel)
		} else {
			e.mu.Lock()
			// A member the gateway does not feed, a local producer, is
			// followed by its head: what it advanced by counts as committed.
			for i, m := range e.members {
				if h := m.head(); h > lastHead[i] {
					if lastHead[i] != 0 && !e.fed(i) {
						e.audio.Committed(i, lastHead[i], h)
					}
					lastHead[i] = h
				}
			}
			var sel []PageSelect
			sel, head, wait = e.audio.Step(now)
			sel = append(sel, e.audio.Retire(mxl.CurrentIndex(e.mxlRate()))...)
			e.selects.Add(uint64(len(sel)))
			err = e.selectPages(sel)
			e.mu.Unlock()
		}
		if err != nil {
			e.log.Error(err, "select a member")
		} else if head > e.out.head() {
			e.publish(head)
		}
		if wait > 0 && wait < timeout {
			timeout = wait
		}
		e.wait(waits, timeout)
	}
}

// selectAllCall is selectAll; tests replace it.
var selectAllCall = selectAll

// apply points view slots at their sources, in the order the policy
// returned them. Readers trust the slot of index i to hold i - Grains
// until the head reaches i - 1, so a select for an index more than one
// past the published head publishes i - 1 first: the selects before it
// have set every slot up to there.
func (e *Engine) apply(sel []Select) error {
	for _, s := range sel {
		if s.Index > e.out.head()+1 {
			e.publish(s.Index - 1)
		}
		if err := selectAllCall(e.views[s.Slot], s.Source); err != nil {
			return err
		}
	}
	return nil
}

// publish moves the output's head to head and wakes its readers.
func (e *Engine) publish(head uint64) {
	e.out.info.runtime.lastWriteTime = C.uint64_t(time.Now().UnixNano())
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&e.out.info.runtime.headIndex)), head)
	atomic.AddUint32(e.out.syncWord(), 1)
	_, _, _ = unix.Syscall6(unix.SYS_FUTEX, uintptr(unsafe.Pointer(e.out.syncWord())), futexWake, futexWakeAll, 0, 0, 0)
}

// retireGuard is how long before a grain boundary the engine passes, so
// that a slot a member writing at the clock overwrites at that boundary
// moves before the member writes: it covers the wait's timer slack and
// the pass itself. It is at most a quarter of the grain period, so a
// member that commits up to three quarters of a period after its
// boundary still counts as writing at the clock.
func (e *Engine) retireGuard() time.Duration {
	return min(maxRetireGuard, e.period/4)
}

const maxRetireGuard = 3 * time.Millisecond

// retireDue returns, for a pass at MXL time now, the index Retire looks
// as far as, the one whose period the clock is in guard from now, and
// how long until the next pass, guard before the boundary after it.
func retireDue(now uint64, rate mxl.Rational, guard time.Duration) (due uint64, next time.Duration) {
	t := now + uint64(guard)
	// libmxl rounds a time to the nearest index.
	due = mxl.TimestampToIndex(rate, t)
	if mxl.IndexToTimestamp(rate, due) > t {
		due--
	}
	return due, time.Duration(mxl.IndexToTimestamp(rate, due+1) - t)
}

// futexWaitvCall waits on waits until the absolute CLOCK_MONOTONIC time
// abs; tests replace it.
var futexWaitvCall = func(waits []futexWaitv, abs *unix.Timespec) unix.Errno {
	_, _, errno := unix.Syscall6(sysFutexWaitv, uintptr(unsafe.Pointer(&waits[0])), uintptr(len(waits)), 0,
		uintptr(unsafe.Pointer(abs)), unix.CLOCK_MONOTONIC, 0)
	return errno
}

// sleep is time.Sleep; tests replace it.
var sleep = time.Sleep

// wait blocks until a member's sync word changes or timeout passes. A
// futex_waitv that fails outright, on a kernel without it (ENOSYS) or on
// arguments it refuses, returns at once every pass and would spin a
// core, so the engine sleeps the timeout instead and says so once.
func (e *Engine) wait(waits []futexWaitv, timeout time.Duration) {
	var mono unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &mono)
	abs := unix.NsecToTimespec(unix.TimespecToNsec(mono) + int64(timeout))
	switch errno := futexWaitvCall(waits, &abs); errno {
	case 0, unix.ETIMEDOUT, unix.EAGAIN, unix.EINTR:
	default:
		e.waitFailed.Do(func() {
			e.log.Error(errno, "futex_waitv failed; polling the members every pass instead")
		})
		sleep(timeout)
	}
}

func (e *Engine) mxlRate() mxl.Rational {
	return mxl.Rational{Num: int64(e.rate.numerator), Den: int64(e.rate.denominator)}
}

// heads reads every member's head.
func (e *Engine) heads() []uint64 {
	hs := make([]uint64, len(e.members))
	for i, m := range e.members {
		hs[i] = m.head()
	}
	return hs
}

// Stalled returns the members, by flow id, whose head had not moved for
// StallAfter at the loop's last pass.
func (e *Engine) Stalled() []string {
	if e.progress == nil {
		return nil
	}
	var ids []string
	for _, i := range e.progress.stalled() {
		ids = append(ids, e.spec.Members[i])
	}
	return ids
}

// fed reports whether the gateway reports member i's arrivals.
func (e *Engine) fed(i int) bool { return e.feeds != nil && e.feeds[i] }

// Close stops serving and removes the output from the domain.
func (e *Engine) Close() {
	e.closing.Lock()
	e.closed = true
	e.closing.Unlock()
	if e.cancel != nil {
		e.cancel()
		<-e.done
	}
	e.release()
	_ = os.RemoveAll(e.outDir)
}

func (e *Engine) release() {
	for _, v := range e.views {
		v.Close()
	}
	for _, ms := range e.grainMem {
		for _, m := range ms {
			_ = unix.Munmap(m)
		}
	}
	for _, m := range e.members {
		_ = unix.Munmap(m.mem)
	}
	for _, f := range e.files {
		f.Close()
	}
	if e.out != nil {
		_ = unix.Munmap(e.out.mem)
	}
	if e.outFile != nil {
		e.outFile.Close()
	}
}
