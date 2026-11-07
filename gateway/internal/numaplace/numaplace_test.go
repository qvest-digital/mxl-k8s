package numaplace

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const pageSize = 4096

// fakeFS is the mxlfs the tests place rings on: per file, the pages on
// each node and what each call answers.
type fakeFS struct {
	mu    sync.Mutex
	files map[string]*fakeFile
	calls []string // "op path" in call order
	open  int      // files open now

	holdErr, setErr error
	moveErr         error
	moveResult      mxlfs.Result
	moveReason      mxlfs.Reason
	// moveGate, when set, is waited on by every Move.
	moveGate chan struct{}
}

type fakeFile struct {
	fs    *fakeFS
	path  string
	pages []uint64
	held  bool
	pref  int32
}

func (fs *fakeFS) record(op, path string) {
	fs.calls = append(fs.calls, op+" "+filepath.Base(path))
}

func (fs *fakeFS) Open(path string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := fs.files[path]
	if f == nil {
		return nil, os.ErrNotExist
	}
	fs.open++
	fs.record("open", path)
	return &fakeHandle{f: f}, nil
}

type fakeHandle struct {
	f      *fakeFile
	held   bool
	closed bool
}

func (h *fakeHandle) Hold() error {
	fs := h.f.fs
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.record("hold", h.f.path)
	if fs.holdErr != nil {
		return fs.holdErr
	}
	h.held, h.f.held = true, true
	return nil
}

func (h *fakeHandle) Pages() (mxlfs.Pages, error) {
	fs := h.f.fs
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.record("pages", h.f.path)
	return mxlfs.Pages{PerNode: append([]uint64(nil), h.f.pages...), Pref: h.f.pref}, nil
}

func (h *fakeHandle) Move(node int32) (mxlfs.Move, error) {
	fs := h.f.fs
	if fs.moveGate != nil {
		<-fs.moveGate
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.record("move", h.f.path)
	m := mxlfs.Move{Node: node, Result: mxlfs.MovedAll}
	if fs.moveErr != nil {
		m.Reason = fs.moveReason
		return m, fs.moveErr
	}
	var off uint64
	for n, v := range h.f.pages {
		if int32(n) != node {
			off += v
			h.f.pages[n] = 0
		}
	}
	if fs.moveResult == mxlfs.MovedPartial {
		// Half moves, the rest stays.
		h.f.pages[0] += off / 2
		off -= off / 2
		m.Result, m.Reason = mxlfs.MovedPartial, fs.moveReason
	}
	h.f.pages[node] += off
	m.BytesMoved = off * pageSize
	return m, nil
}

func (h *fakeHandle) SetNode(node int32) error {
	fs := h.f.fs
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.record("setnode", h.f.path)
	if fs.setErr != nil {
		return fs.setErr
	}
	h.f.pref = node
	return nil
}

func (h *fakeHandle) Close() error {
	fs := h.f.fs
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if h.closed {
		panic("double close")
	}
	h.closed = true
	fs.open--
	fs.record("close", h.f.path)
	if h.held {
		h.f.held = false
	}
	return nil
}

func (fs *fakeFS) Calls() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]string(nil), fs.calls...)
}

func (fs *fakeFS) opsOf(op string) int {
	n := 0
	for _, c := range fs.Calls() {
		if strings.HasPrefix(c, op+" ") {
			n++
		}
	}
	return n
}

func (fs *fakeFS) openNow() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.open
}

// ring makes flow id in domain with grains grain files, each with its
// pages on the nodes perNode gives.
func (fs *fakeFS) ring(t *testing.T, domain, id string, grains int, perNode ...uint64) {
	t.Helper()
	dir := filepath.Join(domain, id+".mxl-flow", "grains")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.files == nil {
		fs.files = map[string]*fakeFile{}
	}
	for i := 0; i < grains; i++ {
		path := filepath.Join(dir, fmt.Sprintf("data.%d", i))
		require.NoError(t, os.WriteFile(path, nil, 0o644))
		fs.files[path] = &fakeFile{fs: fs, path: path, pages: append([]uint64(nil), perNode...), pref: mxlfs.NodeNone}
	}
}

func (fs *fakeFS) pagesOn(domain, id string, node int) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var n uint64
	for path, f := range fs.files {
		if strings.Contains(path, id) && node < len(f.pages) {
			n += f.pages[node]
		}
	}
	return n
}

func newPlacer(fs *fakeFS) (*Placer, *record.FakeRecorder) {
	rec := record.NewFakeRecorder(32)
	p := New("n1", 1, rec, logr.Discard())
	p.open = fs.Open
	p.pageSize = pageSize
	p.memFree = func(int32) (uint64, bool) { return 0, false }
	return p, rec
}

func mirror() client.Object { return &mxlv1alpha1.MxlFlowMirror{} }

func moves(p *Placer, domain, result, reason string) float64 {
	return testutil.ToFloat64(p.moves.WithLabelValues(domain, result, reason))
}

func events(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

const flowA = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
const flowB = "6fbec3b1-1b0f-417d-9059-8b94a47197ed"

// The first acquire holds every ring file before it moves any, later
// ones only count, and the last release drops the holds.
func TestFirstAcquireHoldsThenMovesLaterOnesCount(t *testing.T) {
	fs := &fakeFS{}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 2, 512, 0)
	p, rec := newPlacer(fs)

	r1 := p.Acquire(dom, flowA, mirror())
	require.Equal(t, []string{"open data.0", "hold data.0", "open data.1", "hold data.1",
		"pages data.0", "pages data.1", "move data.0", "move data.1"}, fs.Calls())
	require.EqualValues(t, 1024, fs.pagesOn(dom, flowA, 1))
	require.Equal(t, 1.0, moves(p, dom, ResultMoved, "none"))
	ev := events(rec)
	require.Len(t, ev, 1)
	require.Contains(t, ev[0], "Normal RingMoved")

	r2 := p.Acquire(dom, flowA, mirror())
	r3 := p.Acquire(dom, flowA, mirror())
	require.Len(t, fs.Calls(), 8, "a later acquire calls into mxlfs")

	r1()
	r1() // idempotent
	r2()
	require.Equal(t, 2, fs.openNow(), "holds dropped before the last release")
	r3()
	require.Equal(t, 0, fs.openNow())
	require.Empty(t, p.flows)
}

// Moves happen before an attach's Setup; an acquire after the last
// release holds and checks again, and a ring already on the node is
// left alone.
func TestReacquireAfterReleaseFindsRingInPlace(t *testing.T) {
	fs := &fakeFS{}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 1, 0, 512)
	p, _ := newPlacer(fs)
	p.Acquire(dom, flowA, mirror())()
	p.Acquire(dom, flowA, mirror())()
	require.Equal(t, 2.0, moves(p, dom, ResultAlready, "none"))
	require.Zero(t, fs.opsOf("move"))
	require.Equal(t, 2, fs.opsOf("hold"))
}

func TestConcurrentAcquiresMoveOnce(t *testing.T) {
	fs := &fakeFS{moveGate: make(chan struct{})}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 1, 512, 0)
	p, _ := newPlacer(fs)

	var wg sync.WaitGroup
	releases := make(chan func(), 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); releases <- p.Acquire(dom, flowA, mirror()) }()
	}
	time.Sleep(20 * time.Millisecond)
	close(fs.moveGate)
	wg.Wait()
	close(releases)
	require.Equal(t, 1, fs.opsOf("move"))
	require.Equal(t, 1, fs.opsOf("hold"))
	for r := range releases {
		r()
	}
	require.Zero(t, fs.openNow())
}

// A flow being moved does not hold up the attach of another.
func TestFlowsDoNotWaitOnEachOther(t *testing.T) {
	fs := &fakeFS{}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 1, 512, 0)
	fs.ring(t, dom, flowB, 1, 0, 512)
	p, _ := newPlacer(fs)

	gate := make(chan struct{})
	fs.moveGate = gate
	doneA := make(chan struct{})
	go func() { p.Acquire(dom, flowA, mirror()); close(doneA) }()
	require.Eventually(t, func() bool { return fs.opsOf("pages") >= 1 }, time.Second, time.Millisecond)

	doneB := make(chan struct{})
	go func() { p.Acquire(dom, flowB, mirror()); close(doneB) }()
	select {
	case <-doneB:
	case <-time.After(2 * time.Second):
		t.Fatal("acquire of flow B waited on flow A's move")
	}
	close(gate)
	<-doneA
}

// A module without the ioctls answers the first HOLD with ENOTTY: the
// mirror starts, the domain is marked unsupported, and nothing calls
// into it again.
func TestUnsupportedModuleIsQuietAndFinal(t *testing.T) {
	fs := &fakeFS{holdErr: fmt.Errorf("hold: %w", syscall.ENOTTY)}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 2, 512, 0)
	fs.ring(t, dom, flowB, 2, 512, 0)
	p, rec := newPlacer(fs)

	p.Acquire(dom, flowA, mirror())()
	require.Zero(t, fs.openNow())
	require.Equal(t, 1.0, moves(p, dom, ResultUnsupported, "none"))
	require.Empty(t, events(rec), "an old module is not a warning")

	n := len(fs.Calls())
	p.Acquire(dom, flowB, mirror())()
	p.HoldTarget(dom, flowB, mirror())()
	require.Len(t, fs.Calls(), n, "a call reached an unsupported domain")
	require.Equal(t, 1.0, moves(p, dom, ResultUnsupported, "none"))

	got := testutil.CollectAndCount(p, "mxl_gateway_numa_supported")
	require.Equal(t, 1, got)
	require.NoError(t, testutil.CollectAndCompare(p, strings.NewReader(fmt.Sprintf(`
# HELP mxl_gateway_numa_supported 1 when the domain's mxlfs module has the placement ioctls, 0 when it answered that it does not.
# TYPE mxl_gateway_numa_supported gauge
mxl_gateway_numa_supported{domain=%q,node="n1"} 0
`, dom)), "mxl_gateway_numa_supported"))
}

func TestMissingCapabilityWarnsOncePerDomain(t *testing.T) {
	fs := &fakeFS{holdErr: fmt.Errorf("hold: %w", syscall.EPERM)}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 1, 512, 0)
	fs.ring(t, dom, flowB, 1, 512, 0)
	p, rec := newPlacer(fs)
	p.Acquire(dom, flowA, mirror())()
	p.Acquire(dom, flowB, mirror())()
	ev := events(rec)
	require.Len(t, ev, 1)
	require.Contains(t, ev[0], "Warning RingMoveRefused")
	require.Contains(t, ev[0], "CAP_SYS_NICE")
	require.Equal(t, 2.0, moves(p, dom, ResultRefused, "error"))
	require.Zero(t, fs.openNow())
}

// Each refusal starts the mirror on the ring where it is, keeps the
// holds, and records result, reason and event.
func TestMoveRefusals(t *testing.T) {
	for _, tc := range []struct {
		name           string
		err            error
		result         mxlfs.Result
		reason         mxlfs.Reason
		wantResult     string
		wantReason     string
		wantEventMatch string
	}{
		{"nomem", fmt.Errorf("move: %w", syscall.ENOMEM), 0, mxlfs.RefuseNomem, ResultRefused, "nomem", "Warning RingMoveRefused"},
		{"held", fmt.Errorf("move: %w", syscall.EBUSY), 0, mxlfs.RefuseHeld, ResultRefused, "held", "Warning RingMoveRefused"},
		{"signal", fmt.Errorf("move: %w", syscall.EINTR), 0, mxlfs.RefuseSignal, ResultRefused, "signal", "Warning RingMoveRefused"},
		{"partial view", nil, mxlfs.MovedPartial, mxlfs.RefuseView, ResultPartial, "view", "Warning RingMovePartial"},
		{"partial pinned", nil, mxlfs.MovedPartial, mxlfs.RefusePinned, ResultPartial, "pinned", "Warning RingMovePartial"},
		{"partial busy", nil, mxlfs.MovedPartial, mxlfs.RefuseBusy, ResultPartial, "busy", "Warning RingMovePartial"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeFS{moveErr: tc.err, moveResult: tc.result, moveReason: tc.reason}
			dom := t.TempDir()
			fs.ring(t, dom, flowA, 2, 512, 0)
			p, rec := newPlacer(fs)
			r := p.Acquire(dom, flowA, mirror())
			require.Equal(t, 1.0, moves(p, dom, tc.wantResult, tc.wantReason))
			ev := events(rec)
			require.Len(t, ev, 1)
			require.Contains(t, ev[0], tc.wantEventMatch)
			require.Equal(t, 2, fs.openNow(), "a refused move keeps the holds")
			require.Equal(t, 1.0, gauge(t, p, "mxl_gateway_numa_off_nic_rings", ""))
			r()
		})
	}
}

// The MemFree hint refuses before any MOVE is tried.
func TestMemFreeHintRefusesWithoutMoving(t *testing.T) {
	fs := &fakeFS{}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 1, 512, 0)
	p, rec := newPlacer(fs)
	p.memFree = func(n int32) (uint64, bool) { require.EqualValues(t, 1, n); return 100 * pageSize, true }
	p.Acquire(dom, flowA, mirror())()
	require.Zero(t, fs.opsOf("move"))
	require.Equal(t, 1.0, moves(p, dom, ResultRefused, "nomem"))
	require.Contains(t, events(rec)[0], "Warning RingMoveRefused")
}

func TestMissingRingIsCountedNotFatal(t *testing.T) {
	fs := &fakeFS{}
	dom := t.TempDir()
	p, _ := newPlacer(fs)
	p.Acquire(dom, flowA, mirror())()
	require.Equal(t, 1.0, moves(p, dom, ResultRefused, "error"))
}

// A target's ring gets the preference, then the hold, and is never
// moved; the release drops the holds.
func TestHoldTargetPrefersThenHolds(t *testing.T) {
	fs := &fakeFS{}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 2, 0, 0)
	p, _ := newPlacer(fs)
	r := p.HoldTarget(dom, flowA, mirror())
	require.Equal(t, []string{"open data.0", "setnode data.0", "hold data.0",
		"open data.1", "setnode data.1", "hold data.1"}, fs.Calls())
	require.Equal(t, 1.0, gauge(t, p, "mxl_gateway_numa_held_flows", "target"))
	r()
	r()
	require.Zero(t, fs.openNow())
	require.Equal(t, 0.0, gauge(t, p, "mxl_gateway_numa_held_flows", "target"))
}

func TestHoldTargetFailureDropsEverything(t *testing.T) {
	fs := &fakeFS{setErr: fmt.Errorf("set: %w", syscall.EBUSY)}
	dom := t.TempDir()
	fs.ring(t, dom, flowA, 2, 0, 0)
	p, _ := newPlacer(fs)
	p.HoldTarget(dom, flowA, mirror())()
	require.Zero(t, fs.openNow())
	require.Equal(t, 1.0, moves(p, dom, ResultRefused, "error"))
}

func TestNilPlacerDoesNothing(t *testing.T) {
	var p *Placer
	p.Acquire("/d", flowA, nil)()
	p.HoldTarget("/d", flowA, nil)()
}

func TestMemFreeFrom(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meminfo")
	require.NoError(t, os.WriteFile(path, []byte("Node 1 MemTotal:       65536 kB\nNode 1 MemFree:        2048 kB\n"), 0o644))
	got, ok := memFreeFrom(path)
	require.True(t, ok)
	require.EqualValues(t, 2048*1024, got)
	_, ok = memFreeFrom(filepath.Join(t.TempDir(), "absent"))
	require.False(t, ok)
}

// gauge returns the value of the series of name whose side label is
// side, or the only series of name when side is empty.
func gauge(t *testing.T, p *Placer, name, side string) float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(p))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := side == ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "side" && l.GetValue() == side {
					match = true
				}
			}
			if match {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("no %s{side=%q}", name, side)
	return 0
}
