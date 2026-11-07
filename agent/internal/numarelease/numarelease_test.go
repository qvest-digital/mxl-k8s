package numarelease

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/agent/internal/observed"
	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	flowA    = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
	pageSize = 4096
	podProd  = "aaaaaaaa-1111-2222-3333-444444444444"
	podCons  = "bbbbbbbb-1111-2222-3333-444444444444"
	podOurs  = "cccccccc-1111-2222-3333-444444444444"
)

// host writes a fake /proc and sysfs: two nodes of four CPUs, and per
// pid the CPUs of each thread and the memory nodes it may use.
type host struct {
	t              *testing.T
	proc, nodeRoot string
}

func newHost(t *testing.T) *host {
	h := &host{t: t, proc: t.TempDir(), nodeRoot: t.TempDir()}
	for n, cpus := range []string{"0-3", "4-7"} {
		write(t, filepath.Join(h.nodeRoot, fmt.Sprintf("node%d", n), "cpulist"), cpus+"\n")
	}
	write(t, filepath.Join(h.nodeRoot, "online"), "0-1\n")
	return h
}

func write(t *testing.T, path, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
}

// pid gives pid the memory nodes mems and one thread per CPU list.
func (h *host) pid(pid int, mems string, threads ...string) {
	dir := filepath.Join(h.proc, fmt.Sprint(pid))
	write(h.t, filepath.Join(dir, "status"), "Name:\tx\nCpus_allowed_list:\t0-7\nMems_allowed_list:\t"+mems+"\n")
	for i, cpus := range threads {
		write(h.t, filepath.Join(dir, "task", fmt.Sprint(pid+i), "status"),
			"Name:\tx\nCpus_allowed_list:\t"+cpus+"\nMems_allowed_list:\t"+mems+"\n")
	}
}

// fakeRing is the ring files of the flows the tests release.
type fakeRing struct {
	files    map[string]*fakeFile
	calls    []string
	moveErr  error
	partial  bool
	pagesErr error
	open     int
}

type fakeFile struct {
	r     *fakeRing
	path  string
	pages []uint64
	holds uint32
	pref  int32
}

func (f *fakeFile) Pages() (mxlfs.Pages, error) {
	if f.r.pagesErr != nil {
		return mxlfs.Pages{}, f.r.pagesErr
	}
	return mxlfs.Pages{PerNode: append([]uint64(nil), f.pages...), Holds: f.holds}, nil
}

func (f *fakeFile) Move(node int32) (mxlfs.Move, error) {
	f.r.calls = append(f.r.calls, "move "+filepath.Base(f.path))
	if f.r.moveErr != nil {
		return mxlfs.Move{Reason: mxlfs.RefuseHeld}, f.r.moveErr
	}
	var off uint64
	for i := range f.pages {
		if int32(i) != node {
			off += f.pages[i]
			f.pages[i] = 0
		}
	}
	m := mxlfs.Move{Result: mxlfs.MovedAll}
	if f.r.partial {
		f.pages[1-node] = off / 2
		off -= off / 2
		m.Result, m.Reason = mxlfs.MovedPartial, mxlfs.RefuseBusy
	}
	f.pages[node] += off
	m.BytesMoved = off * pageSize
	return m, nil
}

func (f *fakeFile) SetNode(node int32) error {
	f.r.calls = append(f.r.calls, "setnode "+filepath.Base(f.path))
	f.pref = node
	return nil
}

func (f *fakeFile) Close() error { f.r.open--; return nil }

func (r *fakeRing) Open(path string) (File, error) {
	f := r.files[path]
	if f == nil {
		return nil, os.ErrNotExist
	}
	r.open++
	return f, nil
}

// add makes a ring of grains files of pages each, on node.
func (r *fakeRing) add(t *testing.T, dir, id string, grains int, node int, pages uint64) {
	if r.files == nil {
		r.files = map[string]*fakeFile{}
	}
	for i := 0; i < grains; i++ {
		path := filepath.Join(dir, id+".mxl-flow", "grains", fmt.Sprintf("data.%d", i))
		write(t, path, "")
		f := &fakeFile{r: r, path: path, pages: make([]uint64, 2), pref: mxlfs.NodeNone}
		f.pages[node] = pages
		r.files[path] = f
	}
}

func (r *fakeRing) hold(n uint32) {
	for _, f := range r.files {
		f.holds = n
	}
}

func (r *fakeRing) on(node int) uint64 {
	var n uint64
	for _, f := range r.files {
		n += f.pages[node]
	}
	return n
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fixture struct {
	h      *host
	ring   *fakeRing
	clock  *clock
	loop   *Loop
	procs  []observed.ProcUse
	events []string
	slept  []time.Duration
	dir    string
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{h: newHost(t), ring: &fakeRing{}, clock: &clock{t: time.Unix(1000, 0)}, dir: t.TempDir()}
	fx.ring.add(t, fx.dir, flowA, 2, 0, 512)
	l := New("n1")
	l.procRoot, l.nodeRoot = fx.h.proc, fx.h.nodeRoot
	l.open, l.now, l.pageSize = fx.ring.Open, fx.clock.now, pageSize
	l.sleep = func(d time.Duration) { fx.slept = append(fx.slept, d) }
	l.Procs = func() ([]observed.ProcUse, error) { return fx.procs, nil }
	l.Ours = func(context.Context) (map[string]bool, error) { return map[string]bool{podOurs: true}, nil }
	l.Domains = func() map[string]string { return map[string]string{"": fx.dir} }
	l.Event = func(ref mxlv1alpha1.FlowRef, kind, reason, msg string) {
		fx.events = append(fx.events, kind+" "+reason+" "+ref.Name())
	}
	l.Delay, l.Rate, l.Log = time.Minute, 512<<20, logr.Discard()
	fx.loop = l
	return fx
}

func (fx *fixture) use(pid int, pod string, d observed.Direction) {
	fx.procs = append(fx.procs, observed.ProcUse{PID: pid, Pod: pod, FlowID: flowA, Direction: d})
}

// pass runs one pass and returns the flow's state.
func (fx *fixture) pass(t *testing.T) string {
	t.Helper()
	require.NoError(t, fx.loop.Pass(context.Background()))
	require.Zero(t, fx.ring.open, "a ring file stays open after the pass")
	for state, n := range fx.loop.last[fx.dir] {
		if n > 0 {
			return state
		}
	}
	return ""
}

// The decision table of the release loop, row by row: nothing moves
// unless every local writer and reader is confined to one node, the ring
// is unheld, has pages off that node and has been unheld for Delay.
func TestDecisionTable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(fx *fixture)
		want  string
		moved bool
	}{
		{"writer unpinned", func(fx *fixture) {
			fx.h.pid(10, "0-1", "0-7")
			fx.use(10, podProd, observed.Output)
		}, StateUnpinned, false},
		{"reader unpinned", func(fx *fixture) {
			fx.h.pid(10, "1", "4-7")
			fx.h.pid(20, "1", "4-7", "3")
			fx.use(10, podProd, observed.Output)
			fx.use(20, podCons, observed.Input)
		}, StateUnpinned, false},
		{"writer and reader disagree", func(fx *fixture) {
			fx.h.pid(10, "1", "4-7")
			fx.h.pid(20, "0", "0-3")
			fx.use(10, podProd, observed.Output)
			fx.use(20, podCons, observed.Input)
		}, StateDisagree, false},
		{"held by the gateway", func(fx *fixture) {
			fx.h.pid(10, "1", "4-7")
			fx.use(10, podProd, observed.Output)
			fx.ring.hold(1)
			fx.clock.advance(time.Hour)
		}, StateHeld, false},
		{"already on their node", func(fx *fixture) {
			fx.h.pid(10, "0", "0-3")
			fx.use(10, podProd, observed.Output)
		}, StateInPlace, false},
		{"unheld for less than the delay", func(fx *fixture) {
			fx.h.pid(10, "1", "4-7")
			fx.use(10, podProd, observed.Output)
		}, StateSettling, false},
		{"our own reader on another node is left out", func(fx *fixture) {
			fx.h.pid(10, "1", "4-7")
			fx.h.pid(30, "0", "0-3")
			fx.use(10, podProd, observed.Output)
			fx.use(30, podOurs, observed.Input)
			fx.loop.Delay = 0
		}, StateMoved, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			tc.setup(fx)
			require.Equal(t, tc.want, fx.pass(t))
			if tc.moved {
				require.EqualValues(t, 1024, fx.ring.on(1))
				return
			}
			require.Empty(t, fx.ring.calls, "a ring moved in state %s", tc.want)
			require.EqualValues(t, 1024, fx.ring.on(0))
		})
	}
}

func TestFlowWithoutLocalWriterIsNotLookedAt(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(20, "1", "4-7")
	fx.use(20, podCons, observed.Input)
	fx.loop.Delay = 0
	require.Equal(t, "", fx.pass(t))
	require.Empty(t, fx.ring.calls)
}

// The delay counts from the first pass that saw the ring unheld, and a
// hold seen in between starts it over.
func TestDelayCountsFromFirstUnheldSighting(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(10, "1", "4-7")
	fx.use(10, podProd, observed.Output)

	require.Equal(t, StateSettling, fx.pass(t))
	fx.clock.advance(50 * time.Second)
	require.Equal(t, StateSettling, fx.pass(t))

	fx.ring.hold(1)
	fx.clock.advance(5 * time.Second)
	require.Equal(t, StateHeld, fx.pass(t))
	fx.ring.hold(0)
	fx.clock.advance(5 * time.Second)
	require.Equal(t, StateSettling, fx.pass(t), "the hold restarts the delay")
	fx.clock.advance(59 * time.Second)
	require.Equal(t, StateSettling, fx.pass(t))
	fx.clock.advance(2 * time.Second)
	require.Equal(t, StateMoved, fx.pass(t))
	require.EqualValues(t, 1024, fx.ring.on(1))
	require.Equal(t, []string{"Normal RingMovedToConsumers " + flowA}, fx.events)

	fx.clock.advance(time.Minute)
	require.Equal(t, StateInPlace, fx.pass(t))
	require.Len(t, fx.events, 1)
}

// Each file's move is followed by a pause that keeps the loop at Rate.
func TestMovesArePaced(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(10, "1", "4-7")
	fx.use(10, podProd, observed.Output)
	fx.loop.Delay, fx.loop.Rate = 0, 512*pageSize // one file of 512 pages a second
	require.Equal(t, StateMoved, fx.pass(t))
	require.Equal(t, []time.Duration{time.Second, time.Second}, fx.slept)
	require.Equal(t, 2.0, testutil.ToFloat64(fx.loop.moves.WithLabelValues(fx.dir, "moved", "none")))
	require.Equal(t, float64(1024*pageSize), testutil.ToFloat64(fx.loop.movedBytes.WithLabelValues(fx.dir)))
}

// The gateway holding a file mid-pass ends the flow's pass.
func TestHeldRefusalEndsTheFlowsPass(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(10, "1", "4-7")
	fx.use(10, podProd, observed.Output)
	fx.loop.Delay = 0
	fx.ring.moveErr = fmt.Errorf("move: %w", syscall.EBUSY)
	require.Equal(t, StateHeld, fx.pass(t))
	require.Len(t, fx.ring.calls, 1)
	require.Equal(t, 1.0, testutil.ToFloat64(fx.loop.moves.WithLabelValues(fx.dir, "refused", "held")))
	require.Equal(t, []string{"Warning RingMoveFailed " + flowA}, fx.events)
}

// A partly moved ring keeps its later pages where it went.
func TestPartialMoveSetsThePreference(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(10, "1", "4-7")
	fx.use(10, podProd, observed.Output)
	fx.loop.Delay = 0
	fx.ring.partial = true
	require.Equal(t, StateMoved, fx.pass(t))
	for _, f := range fx.ring.files {
		require.EqualValues(t, 1, f.pref)
	}
	require.Equal(t, 2.0, testutil.ToFloat64(fx.loop.moves.WithLabelValues(fx.dir, "partial", "busy")))
	require.Equal(t, []string{"Warning RingMoveFailed " + flowA}, fx.events)
	require.Equal(t, StateMoved, fx.pass(t))
	require.Len(t, fx.events, 1, "the same result again is no new event")
}

func TestUnsupportedModuleTurnsTheDomainOff(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(10, "1", "4-7")
	fx.use(10, podProd, observed.Output)
	fx.ring.pagesErr = fmt.Errorf("pages: %w", syscall.ENOTTY)
	require.Equal(t, "", fx.pass(t))
	require.True(t, fx.loop.unsupported[fx.dir])
	fx.ring.pagesErr = nil
	require.Equal(t, "", fx.pass(t), "an unsupported domain is looked at again")
}

func TestFlowStatesMetric(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(10, "1", "4-7")
	fx.use(10, podProd, observed.Output)
	fx.pass(t)
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(fx.loop))
	var want strings.Builder
	want.WriteString("# HELP mxl_agent_numa_release_flows Flows with a local writer by what the last pass of the release loop found.\n")
	want.WriteString("# TYPE mxl_agent_numa_release_flows gauge\n")
	for _, s := range []string{"disagree", "held", "in_place", "moved", "settling", "unpinned"} {
		v := 0
		if s == StateSettling {
			v = 1
		}
		fmt.Fprintf(&want, "mxl_agent_numa_release_flows{domain=%q,node=\"n1\",state=%q} %d\n", fx.dir, s, v)
	}
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(want.String()), "mxl_agent_numa_release_flows"))
}

// A hold the loop never saw, because the flow had no node then, still
// restarts the delay: the loop counts unheld time only across passes
// that saw the flow unheld and agreed.
func TestHoldWhileUnpinnedRestartsTheDelay(t *testing.T) {
	fx := newFixture(t)
	fx.h.pid(10, "1", "4-7")
	fx.use(10, podProd, observed.Output)
	require.Equal(t, StateSettling, fx.pass(t))

	// The writer is unpinned while the gateway holds and drops the ring.
	fx.clock.advance(30 * time.Second)
	fx.h.pid(10, "0-1", "0-7")
	fx.ring.hold(1)
	require.Equal(t, StateUnpinned, fx.pass(t))
	fx.clock.advance(20 * time.Second)
	fx.ring.hold(0)
	dropped := fx.clock.now()
	require.Equal(t, StateUnpinned, fx.pass(t))

	// Pinned again: nothing moves until the delay has passed since then.
	fx.clock.advance(15 * time.Second)
	fx.h.pid(10, "1", "4-7")
	require.Equal(t, StateSettling, fx.pass(t))
	for fx.clock.now().Sub(dropped) < fx.loop.Delay {
		require.NotEqual(t, StateMoved, fx.pass(t), "moved %s after the hold dropped",
			fx.clock.now().Sub(dropped))
		fx.clock.advance(10 * time.Second)
	}
	require.Empty(t, fx.ring.calls)
}
