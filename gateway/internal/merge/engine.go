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
	maxSources         = 8

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
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if st.Size() < int64(C.sizeof_mxlFlowInfo)+int64(unsafe.Sizeof(flowState{})) {
		f.Close()
		return nil, nil, fmt.Errorf("%s: %d bytes is no flow header", path, st.Size())
	}
	mem, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), prot, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	h := &header{mem: mem, info: (*C.mxlFlowInfo)(unsafe.Pointer(&mem[0])),
		state: (*flowState)(unsafe.Pointer(&mem[C.sizeof_mxlFlowInfo]))}
	return h, f, nil
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
	grainMem [][]byte
	audio    *Audio
	disc     *Discrete
	rate     C.mxlRational
	period   time.Duration // of a grain
	feeds    []bool        // members the gateway reports arrivals for

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}

	waitFailed sync.Once // logs the first futex_waitv failure

	// selects counts the slots or pages the engine pointed at another
	// source, each one a remap in every reader of the output.
	selects atomic.Uint64
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
	ok = true
	rctx, cancel := context.WithCancel(ctx)
	e.cancel, e.done = cancel, make(chan struct{})
	go e.run(rctx)
	return e, nil
}

// checkAligned refuses members that cannot be merged slot for slot: a
// different format, rate or ring geometry.
func (e *Engine) checkAligned() error {
	m0 := e.members[0]
	for i, m := range e.members[1:] {
		if m.format() != m0.format() ||
			m.info.config.common.grainRate != m0.info.config.common.grainRate {
			return fmt.Errorf("member %s differs from %s in format or rate", e.spec.Members[i+1], e.spec.Members[0])
		}
		switch m0.format() {
		case formatAudio:
			c0, l0 := m0.continuous()
			c, l := m.continuous()
			if c != c0 || l != l0 {
				return fmt.Errorf("member %s has %d channels of %d samples, %s %d of %d: not sample-aligned",
					e.spec.Members[i+1], c, l, e.spec.Members[0], c0, l0)
			}
		default:
			if m.discrete() != m0.discrete() {
				return fmt.Errorf("member %s has %d grains, %s %d", e.spec.Members[i+1], m.discrete(), e.spec.Members[0], m0.discrete())
			}
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
	for slot := uint(0); slot < grains; slot++ {
		var srcs []*os.File
		for m, id := range e.spec.Members {
			f, err := os.Open(filepath.Join(flowDir(e.spec.DomainDir, id), "grains", fmt.Sprintf("data.%d", slot)))
			if err != nil {
				return err
			}
			mem, err := unix.Mmap(int(f.Fd()), 0, int(C.sizeof_mxlGrainInfo), unix.PROT_READ, unix.MAP_SHARED)
			if err != nil {
				f.Close()
				return err
			}
			e.grainMem = append(e.grainMem, mem)
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

func (e *Engine) run(ctx context.Context) {
	defer close(e.done)
	waits := make([]futexWaitv, len(e.members))
	lastHead := make([]uint64, len(e.members))
	report := time.Now()
	for ctx.Err() == nil {
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
		var head uint64
		var wait time.Duration
		var err error
		if e.disc != nil {
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
			channels, length := e.members[0].continuous()
			for _, s := range sel {
				for c := uint64(0); c < uint64(channels) && err == nil; c++ {
					err = selectRange(e.views[0], s.Source, (c*uint64(length)+s.Page*PageSamples)*4, 4096)
				}
			}
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

// fed reports whether the gateway reports member i's arrivals.
func (e *Engine) fed(i int) bool { return e.feeds != nil && e.feeds[i] }

// Close stops serving and removes the output from the domain.
func (e *Engine) Close() {
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
	for _, m := range e.grainMem {
		_ = unix.Munmap(m)
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
