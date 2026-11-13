// Package numarelease moves the ring of a flow nothing registers with a
// NIC to the NUMA node its local writers and readers are confined to.
//
// The gateway holds the ring files of every flow it registers and moves
// them to the NICs' node first. Once the last hold is gone and stays gone
// for Delay, and every process on the node that writes or reads the flow
// is confined to one NUMA node, CPUs and memory, the ring moves to that
// node. A process whose CPUs span several nodes has no node, so without
// CPU pinning nothing moves.
package numarelease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/qvest-digital/mxl-k8s/agent/internal/observed"
	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// States of a flow in one pass, the state label of
// mxl_agent_numa_release_flows.
const (
	StateUnpinned = "unpinned" // a writer or reader is confined to no one node
	StateDisagree = "disagree" // they are confined to different nodes
	StateHeld     = "held"     // the gateway holds the ring
	StateInPlace  = "in_place" // every page is on their node
	StateSettling = "settling" // unheld for less than Delay
	StateMoved    = "moved"    // this pass moved the ring
)

var states = []string{StateUnpinned, StateDisagree, StateHeld, StateInPlace, StateSettling, StateMoved}

// Event reasons, recorded on the MxlFlow.
const (
	ReasonMovedToConsumers = "RingMovedToConsumers"
	ReasonMoveFailed       = "RingMoveFailed"
)

// File is one open ring file.
type File interface {
	Pages() (mxlfs.Pages, error)
	Move(node int32) (mxlfs.Move, error)
	SetNode(node int32) error
	Close() error
}

type osFile struct{ *os.File }

func (f osFile) Pages() (mxlfs.Pages, error)         { return mxlfs.NodePages(f.File) }
func (f osFile) Move(node int32) (mxlfs.Move, error) { return mxlfs.MoveTo(f.File, node, 0) }
func (f osFile) SetNode(node int32) error            { return mxlfs.SetNode(f.File, node) }

func openFile(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

// Loop is the release loop of one node.
type Loop struct {
	// Procs lists every process in a pod and the flows it uses.
	Procs func() ([]observed.ProcUse, error)
	// Ours returns the UIDs of the pods whose processes are left out:
	// mxl-k8s's own, which read and write every flow.
	Ours func(ctx context.Context) (map[string]bool, error)
	// Domains maps a domain name, empty for the primary, to its directory.
	Domains func() map[string]string
	// Event records an event on the MxlFlow ref names.
	Event func(ref mxlv1alpha1.FlowRef, kind, reason, message string)
	// Delay is how long a ring stays unheld before it moves, counted from
	// the first pass that saw it unheld.
	Delay time.Duration
	// Rate is how many bytes a second the loop moves.
	Rate uint64
	Log  logr.Logger

	procRoot, nodeRoot string
	open               func(path string) (File, error)
	now                func() time.Time
	sleep              func(time.Duration)
	pageSize           uint64

	moves      *prometheus.CounterVec
	movedBytes *prometheus.CounterVec
	descFlows  *prometheus.Desc

	unheldSince map[mxlv1alpha1.FlowRef]time.Time
	lastResult  map[mxlv1alpha1.FlowRef]string
	unsupported map[string]bool

	mu   sync.Mutex
	last map[string]map[string]int // flows per domain and state, last pass
}

// New returns a Loop whose metrics carry node.
func New(node string) *Loop {
	cl := prometheus.Labels{"node": node}
	return &Loop{
		procRoot: "/proc",
		nodeRoot: NodeRoot,
		open:     openFile,
		now:      time.Now,
		sleep:    time.Sleep,
		pageSize: uint64(os.Getpagesize()),
		moves: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mxl_agent_numa_release_moves_total",
			Help: "Ring files the release loop moved to the node of their flow's writers and readers, by " +
				"result, and why a block stayed where it was.",
			ConstLabels: cl,
		}, []string{"domain", "result", "reason"}),
		movedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "mxl_agent_numa_release_bytes_total",
			Help:        "Bytes of rings the release loop moved.",
			ConstLabels: cl,
		}, []string{"domain"}),
		descFlows: prometheus.NewDesc("mxl_agent_numa_release_flows",
			"Flows with a local writer by what the last pass of the release loop found.",
			[]string{"domain", "state"}, cl),
		unheldSince: map[mxlv1alpha1.FlowRef]time.Time{},
		lastResult:  map[mxlv1alpha1.FlowRef]string{},
		unsupported: map[string]bool{},
		last:        map[string]map[string]int{},
	}
}

// Run calls Pass every period until ctx is done.
func (l *Loop) Run(ctx context.Context, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := l.Pass(ctx); err != nil {
			l.Log.Error(err, "NUMA release pass")
		}
	}
}

type flowUse struct {
	ref     mxlv1alpha1.FlowRef
	pids    []int
	writers int
}

// Pass looks at every flow with a local writer once, and moves the ring
// of each that qualifies, one flow at a time.
func (l *Loop) Pass(ctx context.Context) error {
	procs, err := l.Procs()
	if err != nil {
		return err
	}
	ours, err := l.Ours(ctx)
	if err != nil {
		return err
	}
	topo, err := readTopology(l.nodeRoot)
	if err != nil {
		return err
	}
	flows := map[mxlv1alpha1.FlowRef]*flowUse{}
	for _, p := range procs {
		if ours[p.Pod] {
			continue
		}
		ref := mxlv1alpha1.FlowRef{Domain: p.Domain, ID: p.FlowID}
		f := flows[ref]
		if f == nil {
			f = &flowUse{ref: ref}
			flows[ref] = f
		}
		f.pids = append(f.pids, p.PID)
		if p.Direction == observed.Output {
			f.writers++
		}
	}
	order := make([]*flowUse, 0, len(flows))
	for _, f := range flows {
		if f.writers > 0 {
			order = append(order, f)
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i].ref.Name() < order[j].ref.Name() })

	dirs := l.Domains()
	counts := map[string]map[string]int{}
	seen := map[mxlv1alpha1.FlowRef]bool{}
	for _, f := range order {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		dir, ok := dirs[f.ref.Domain]
		if !ok || l.unsupported[dir] {
			continue
		}
		seen[f.ref] = true
		state := l.flow(f, dir, topo)
		if state == "" {
			// A hold could come and go unseen while the ring cannot be
			// read, so unheld time is counted afresh once it can.
			delete(l.unheldSince, f.ref)
			continue
		}
		if counts[dir] == nil {
			counts[dir] = map[string]int{}
		}
		counts[dir][state]++
	}
	for ref := range l.unheldSince {
		if !seen[ref] {
			delete(l.unheldSince, ref)
		}
	}
	for ref := range l.lastResult {
		if !seen[ref] {
			delete(l.lastResult, ref)
		}
	}
	l.mu.Lock()
	l.last = counts
	l.mu.Unlock()
	return nil
}

// flow decides about one flow and moves its ring when it qualifies. An
// empty state is a flow it could not look at.
func (l *Loop) flow(f *flowUse, dir string, topo topology) string {
	w := int32(-1)
	for _, pid := range f.pids {
		n, ok := procNode(l.procRoot, pid, topo)
		switch {
		case !ok:
			// A hold could come and go unseen while the flow has no
			// node, so unheld time is counted afresh once it has one.
			delete(l.unheldSince, f.ref)
			return StateUnpinned
		case w >= 0 && n != w:
			delete(l.unheldSince, f.ref)
			return StateDisagree
		}
		w = n
	}

	paths, err := mxlfs.RingFiles(filepath.Join(dir, f.ref.ID+".mxl-flow"))
	if err != nil {
		return ""
	}
	files := make([]File, 0, len(paths))
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	pages := make([]mxlfs.Pages, 0, len(paths))
	for _, path := range paths {
		file, err := l.open(path)
		if err != nil {
			return ""
		}
		files = append(files, file)
		p, err := file.Pages()
		if err != nil {
			if mxlfs.Unsupported(err) {
				l.unsupported[dir] = true
				l.Log.Info("the mxlfs module has no placement ioctls; rings stay where they are", "domain", dir)
			}
			return ""
		}
		pages = append(pages, p)
	}

	held, off := false, uint64(0)
	for _, p := range pages {
		held = held || p.Holds > 0
		off += p.Off(w)
	}
	now := l.now()
	if held {
		delete(l.unheldSince, f.ref)
		return StateHeld
	}
	since, ok := l.unheldSince[f.ref]
	if !ok {
		since = now
		l.unheldSince[f.ref] = now
	}
	switch {
	case off == 0:
		return StateInPlace
	case now.Sub(since) < l.Delay:
		return StateSettling
	}
	return l.move(f.ref, dir, w, files, pages)
}

// move moves every file with pages off w to w, paced to Rate. A file the
// gateway holds ends the flow's pass.
func (l *Loop) move(ref mxlv1alpha1.FlowRef, dir string, w int32, files []File, pages []mxlfs.Pages) string {
	var moved uint64
	result, reason := "moved", "none"
	for i, file := range files {
		if pages[i].Off(w) == 0 {
			continue
		}
		m, err := file.Move(w)
		moved += m.BytesMoved
		l.movedBytes.WithLabelValues(dir).Add(float64(m.BytesMoved))
		r := "moved"
		rs := m.Reason.String()
		switch {
		case err == nil && m.Result == mxlfs.MovedAll:
			rs = "none"
		case err == nil:
			r = "partial"
			// A ring left partly behind still allocates its future
			// pages where it was moved to.
			if serr := file.SetNode(w); serr != nil {
				l.Log.Info("set ring preference", "flowID", ref.ID, "error", serr.Error())
			}
		default:
			r = "refused"
			rs = errReason(err, m)
		}
		l.moves.WithLabelValues(dir, r, rs).Inc()
		if r != "moved" && result == "moved" {
			result, reason = r, rs
		}
		if errors.Is(err, syscall.EBUSY) {
			// The gateway took the flow.
			delete(l.unheldSince, ref)
			l.transition(ref, result, reason, w, moved)
			return StateHeld
		}
		if l.Rate > 0 && m.BytesMoved > 0 {
			l.sleep(time.Duration(float64(m.BytesMoved) / float64(l.Rate) * float64(time.Second)))
		}
	}
	l.transition(ref, result, reason, w, moved)
	return StateMoved
}

// transition records an event when the flow's result differs from the
// last one recorded for it.
func (l *Loop) transition(ref mxlv1alpha1.FlowRef, result, reason string, w int32, moved uint64) {
	key := result + "/" + reason
	if l.lastResult[ref] == key {
		return
	}
	l.lastResult[ref] = key
	if l.Event == nil {
		return
	}
	if result == "moved" {
		l.Event(ref, corev1.EventTypeNormal, ReasonMovedToConsumers, fmt.Sprintf(
			"moved %s of the ring to NUMA node %d, where its local writers and readers run", size(moved), w))
		return
	}
	l.Event(ref, corev1.EventTypeWarning, ReasonMoveFailed, fmt.Sprintf(
		"moving the ring to NUMA node %d: %s (%s), %s moved", w, result, reason, size(moved)))
}

func size(b uint64) string {
	return resource.NewQuantity(int64(b), resource.BinarySI).String()
}

func errReason(err error, m mxlfs.Move) string {
	switch {
	case errors.Is(err, syscall.ENOMEM):
		return mxlfs.RefuseNomem.String()
	case errors.Is(err, syscall.EBUSY):
		return mxlfs.RefuseHeld.String()
	case errors.Is(err, syscall.EINTR):
		return mxlfs.RefuseSignal.String()
	case m.Reason != 0:
		return m.Reason.String()
	default:
		return "error"
	}
}

// Describe implements prometheus.Collector.
func (l *Loop) Describe(ch chan<- *prometheus.Desc) {
	l.moves.Describe(ch)
	l.movedBytes.Describe(ch)
	ch <- l.descFlows
}

// Collect implements prometheus.Collector.
func (l *Loop) Collect(ch chan<- prometheus.Metric) {
	l.moves.Collect(ch)
	l.movedBytes.Collect(ch)
	l.mu.Lock()
	defer l.mu.Unlock()
	for dir, counts := range l.last {
		for _, s := range states {
			ch <- prometheus.MustNewConstMetric(l.descFlows, prometheus.GaugeValue, float64(counts[s]), dir, s)
		}
	}
}
