// Package numaplace keeps the ring of a flow the gateway registers with
// a NIC on the NICs' NUMA node, on an mxlfs domain.
//
// Before the first initiator of a flow is set up, the ring files are
// held and moved to the NICs' node, unless every page is there already; the holds drop when the last
// initiator of the flow closes, and only then may the agent move the
// ring elsewhere. A target's ring files get the NIC's node as their
// preference and a hold before the ring is allocated, so it is
// allocated there; they are never moved. A registered ring is pinned and
// cannot move, which is why both happen before registration.
//
// Every failure is recorded and leaves the mirror to start as it would
// without placement.
package numaplace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
)

// Event reasons, recorded on the MxlFlowMirror whose attach placed the
// ring.
const (
	ReasonRingMoved       = "RingMoved"
	ReasonRingMovePartial = "RingMovePartial"
	ReasonRingMoveRefused = "RingMoveRefused"
)

// Results of a first acquire, the result label of
// mxl_gateway_numa_moves_total.
const (
	ResultMoved       = "moved"
	ResultAlready     = "already"
	ResultPartial     = "partial"
	ResultRefused     = "refused"
	ResultUnsupported = "unsupported"
)

// File is one open ring file.
type File interface {
	Hold() error
	Pages() (mxlfs.Pages, error)
	Move(node int32) (mxlfs.Move, error)
	SetNode(node int32) error
	Close() error
}

type osFile struct{ *os.File }

func (f osFile) Hold() error                         { return mxlfs.Hold(f.File, true) }
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

type flowKey struct{ domain, flowID string }

// flowPin is one flow's held source ring.
type flowPin struct {
	links int // under Placer.mu

	mu    sync.Mutex // serializes the first acquire against the later ones
	done  bool
	files []File

	held, offNIC atomic.Bool
}

// Placer places and holds the rings of the flows the gateway mirrors.
// A nil Placer does nothing.
type Placer struct {
	// nic is the NUMA node of the host's NICs, which every ring the
	// gateway registers is placed on.
	nic      int32
	recorder record.EventRecorder
	log      logr.Logger

	// Seams for tests.
	open     func(path string) (File, error)
	memFree  func(node int32) (uint64, bool)
	pageSize uint64

	moves      *prometheus.CounterVec
	moveTime   *prometheus.HistogramVec
	movedBytes *prometheus.CounterVec
	descHeld   *prometheus.Desc
	descOff    *prometheus.Desc
	descSupp   *prometheus.Desc

	mu        sync.Mutex
	flows     map[flowKey]*flowPin
	targets   map[flowKey]*flowPin // held target rings
	supported map[string]bool      // by domain, once known
	denied    map[string]bool      // domains whose HOLD needed a capability
}

// New returns a Placer placing rings on the NUMA node nic, recording
// events with rec, its metrics labelled with node.
func New(node string, nic int32, rec record.EventRecorder, log logr.Logger) *Placer {
	cl := prometheus.Labels{"node": node}
	return &Placer{
		nic:      nic,
		recorder: rec,
		log:      log,
		open:     openFile,
		memFree:  nodeMemFree,
		pageSize: uint64(os.Getpagesize()),
		moves: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "mxl_gateway_numa_moves_total",
			Help:        "First acquires of a source ring by what became of its placement on the NIC's NUMA node, and why a block stayed off it.",
			ConstLabels: cl,
		}, []string{"domain", "result", "reason"}),
		moveTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "mxl_gateway_numa_move_seconds",
			Help:        "Time from holding a source ring to having moved it, per flow.",
			ConstLabels: cl,
			Buckets:     []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
		}, []string{"domain"}),
		movedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "mxl_gateway_numa_moved_bytes_total",
			Help:        "Bytes of source rings the gateway moved to the NIC's NUMA node.",
			ConstLabels: cl,
		}, []string{"domain"}),
		descHeld: prometheus.NewDesc("mxl_gateway_numa_held_flows",
			"Flows whose ring the gateway holds, by side.", []string{"domain", "side"}, cl),
		descOff: prometheus.NewDesc("mxl_gateway_numa_off_nic_rings",
			"Held source rings with pages off the NUMA node of the NIC that registers them.", []string{"domain"}, cl),
		descSupp: prometheus.NewDesc("mxl_gateway_numa_supported",
			"1 when the domain's mxlfs module has the placement ioctls, 0 when it answered that it does not.",
			[]string{"domain"}, cl),
		flows:     map[flowKey]*flowPin{},
		targets:   map[flowKey]*flowPin{},
		supported: map[string]bool{},
		denied:    map[string]bool{},
	}
}

// Acquire counts one more initiator of flowID in domain about to be set
// up. The first one holds the ring files and moves them to the NICs'
// node; the rest only count. The returned release ends the count; the
// last one drops the holds. obj receives the events of a first acquire.
func (p *Placer) Acquire(domain, flowID string, obj client.Object) (release func()) {
	if p == nil || p.isUnsupported(domain) {
		return func() {}
	}
	k := flowKey{domain, flowID}
	p.mu.Lock()
	pin := p.flows[k]
	if pin == nil {
		pin = &flowPin{}
		p.flows[k] = pin
	}
	pin.links++
	p.mu.Unlock()

	pin.mu.Lock()
	if !pin.done {
		pin.done = true
		p.place(pin, k, obj)
	}
	pin.mu.Unlock()

	var once sync.Once
	return func() { once.Do(func() { p.release(k, pin) }) }
}

// release ends one count. The last closes the files under p.mu, so an
// acquire that follows finds the holds gone. It takes pin.mu inside p.mu
// only at zero links, while place takes p.mu inside pin.mu; the two never
// meet, because a goroutine in place holds a link of that pin.
func (p *Placer) release(k flowKey, pin *flowPin) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pin.links--
	if pin.links > 0 {
		return
	}
	delete(p.flows, k)
	pin.mu.Lock()
	closeAll(pin.files)
	pin.files = nil
	pin.held.Store(false)
	pin.mu.Unlock()
}

// HoldTarget gives the ring files of the target flow flowID in domain
// the NICs' node as their preference and holds them, before the ring is
// allocated. Every target of the flow counts; the first one sets and
// holds, the last release drops the holds.
func (p *Placer) HoldTarget(domain, flowID string, obj client.Object) (release func()) {
	if p == nil || p.isUnsupported(domain) {
		return func() {}
	}
	k := flowKey{domain, flowID}
	p.mu.Lock()
	defer p.mu.Unlock()
	pin := p.targets[k]
	if pin == nil {
		files, err := p.holdTarget(k)
		if err != nil {
			p.mu.Unlock()
			p.failed(domain, err, obj, "target ring of flow "+flowID)
			p.mu.Lock()
			return func() {}
		}
		pin = &flowPin{files: files}
		p.targets[k] = pin
	}
	pin.links++
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if pin.links--; pin.links == 0 {
				delete(p.targets, k)
				closeAll(pin.files)
			}
		})
	}
}

// holdTarget opens, prefers and holds the ring files of k.
func (p *Placer) holdTarget(k flowKey) ([]File, error) {
	paths, err := mxlfs.RingFiles(flowDir(k.domain, k.flowID))
	if err != nil {
		return nil, err
	}
	files := make([]File, 0, len(paths))
	for _, path := range paths {
		f, err := p.open(path)
		if err != nil {
			closeAll(files)
			return nil, err
		}
		files = append(files, f)
		if err = f.SetNode(p.nic); err == nil {
			err = f.Hold()
		}
		if err != nil {
			closeAll(files)
			return nil, err
		}
	}
	if _, seen := p.supported[k.domain]; !seen {
		p.supported[k.domain] = true
	}
	return files, nil
}

// place holds and moves the ring of a flow's first acquire.
func (p *Placer) place(pin *flowPin, k flowKey, obj client.Object) {
	start := time.Now()
	nic := p.nic
	paths, err := mxlfs.RingFiles(flowDir(k.domain, k.flowID))
	if err != nil {
		p.log.Info("no source ring to place", "domain", k.domain, "flowID", k.flowID, "error", err.Error())
		p.moves.WithLabelValues(k.domain, ResultRefused, "error").Inc()
		return
	}
	for _, path := range paths {
		f, err := p.open(path)
		if err == nil {
			pin.files = append(pin.files, f)
			err = f.Hold()
		}
		if err != nil {
			closeAll(pin.files)
			pin.files = nil
			p.failed(k.domain, err, obj, "source ring of flow "+k.flowID)
			return
		}
	}
	pin.held.Store(true)
	p.known(k.domain)

	var off uint64
	for _, f := range pin.files {
		pages, err := f.Pages()
		if err != nil {
			p.log.Error(err, "read ring placement", "flowID", k.flowID)
			p.moves.WithLabelValues(k.domain, ResultRefused, "error").Inc()
			pin.offNIC.Store(true)
			return
		}
		off += pages.Off(nic) * p.pageSize
	}
	if off == 0 {
		p.moves.WithLabelValues(k.domain, ResultAlready, "none").Inc()
		return
	}
	if free, ok := p.memFree(nic); ok && off > free {
		pin.offNIC.Store(true)
		p.moves.WithLabelValues(k.domain, ResultRefused, mxlfs.RefuseNomem.String()).Inc()
		p.event(obj, corev1.EventTypeWarning, ReasonRingMoveRefused,
			"ring of flow %s not moved to NUMA node %d: %s off it, %s free there",
			k.flowID, nic, size(off), size(free))
		return
	}

	var moved uint64
	reason := ""
	failed := false
	for _, f := range pin.files {
		m, err := f.Move(nic)
		moved += m.BytesMoved
		r := moveReason(m, err)
		if err != nil {
			failed = true
			p.note(k.domain, err, obj, "source ring of flow "+k.flowID)
		} else if m.Result != mxlfs.MovedAll {
			failed = true
		}
		if r != "none" && reason == "" {
			reason = r
		}
	}
	p.moveTime.WithLabelValues(k.domain).Observe(time.Since(start).Seconds())
	p.movedBytes.WithLabelValues(k.domain).Add(float64(moved))
	if reason == "" {
		reason = "none"
	}
	switch {
	case !failed:
		p.moves.WithLabelValues(k.domain, ResultMoved, "none").Inc()
		p.event(obj, corev1.EventTypeNormal, ReasonRingMoved,
			"moved %s of the ring of flow %s to NUMA node %d in %s",
			size(moved), k.flowID, nic, time.Since(start).Round(time.Millisecond))
	case moved > 0:
		pin.offNIC.Store(true)
		p.moves.WithLabelValues(k.domain, ResultPartial, reason).Inc()
		p.event(obj, corev1.EventTypeWarning, ReasonRingMovePartial,
			"moved %s of the ring of flow %s to NUMA node %d, %s left off it: %s",
			size(moved), k.flowID, nic, size(off-min(moved, off)), reason)
	default:
		pin.offNIC.Store(true)
		p.moves.WithLabelValues(k.domain, ResultRefused, reason).Inc()
		p.event(obj, corev1.EventTypeWarning, ReasonRingMoveRefused,
			"ring of flow %s not moved to NUMA node %d, %s off it: %s", k.flowID, nic, size(off), reason)
	}
}

// moveReason is the reason label for one MOVE.
func moveReason(m mxlfs.Move, err error) string {
	switch {
	case err == nil:
		return m.Reason.String()
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

// failed records a failed open, hold or preference on domain: counted,
// and noted.
func (p *Placer) failed(domain string, err error, obj client.Object, what string) {
	if p.note(domain, err, obj, what) {
		p.moves.WithLabelValues(domain, ResultUnsupported, "none").Inc()
		return
	}
	if mxlfs.Unsupported(err) {
		return
	}
	p.moves.WithLabelValues(domain, ResultRefused, "error").Inc()
}

// note logs err once in the way its kind needs and reports whether it
// says the module has no placement ioctls, the first time for domain. A
// module without them turns the domain off with one log line; a missing
// capability is a Warning event once per domain.
func (p *Placer) note(domain string, err error, obj client.Object, what string) (firstUnsupported bool) {
	switch {
	case mxlfs.Unsupported(err):
		p.mu.Lock()
		ok, seen := p.supported[domain]
		first := !seen || ok
		p.supported[domain] = false
		p.mu.Unlock()
		if first {
			p.log.Info("the mxlfs module has no placement ioctls; rings stay where they are", "domain", domain)
		}
		return first
	case errors.Is(err, syscall.EPERM):
		p.mu.Lock()
		first := !p.denied[domain]
		p.denied[domain] = true
		p.mu.Unlock()
		if first {
			p.log.Error(err, "placement needs CAP_SYS_NICE", "domain", domain)
			p.event(obj, corev1.EventTypeWarning, ReasonRingMoveRefused,
				"%s not placed: %v; the gateway needs CAP_SYS_NICE", what, err)
		}
	default:
		p.log.Error(err, "place ring", "domain", domain, "what", what)
	}
	return false
}

func hasKey(m map[string]bool, k string) bool { _, ok := m[k]; return ok }

func (p *Placer) known(domain string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !hasKey(p.supported, domain) {
		p.supported[domain] = true
	}
}

func (p *Placer) isUnsupported(domain string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	ok, seen := p.supported[domain]
	return seen && !ok
}

func (p *Placer) event(obj client.Object, kind, reason, format string, args ...any) {
	if p.recorder == nil || obj == nil {
		return
	}
	p.recorder.Eventf(obj, kind, reason, format, args...)
}

// Describe implements prometheus.Collector.
func (p *Placer) Describe(ch chan<- *prometheus.Desc) {
	p.moves.Describe(ch)
	p.moveTime.Describe(ch)
	p.movedBytes.Describe(ch)
	ch <- p.descHeld
	ch <- p.descOff
	ch <- p.descSupp
}

// Collect implements prometheus.Collector.
func (p *Placer) Collect(ch chan<- prometheus.Metric) {
	p.moves.Collect(ch)
	p.moveTime.Collect(ch)
	p.movedBytes.Collect(ch)

	p.mu.Lock()
	held, off := map[string]int{}, map[string]int{}
	for k, pin := range p.flows {
		if pin.held.Load() {
			held[k.domain]++
			if pin.offNIC.Load() {
				off[k.domain]++
			}
		}
	}
	domains := map[string]bool{}
	for d, ok := range p.supported {
		domains[d] = ok
	}
	targets := map[string]int{}
	for k := range p.targets {
		targets[k.domain]++
	}
	p.mu.Unlock()

	for d, ok := range domains {
		v := 0.0
		if ok {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(p.descSupp, prometheus.GaugeValue, v, d)
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(p.descHeld, prometheus.GaugeValue, float64(held[d]), d, "source")
		ch <- prometheus.MustNewConstMetric(p.descHeld, prometheus.GaugeValue, float64(targets[d]), d, "target")
		ch <- prometheus.MustNewConstMetric(p.descOff, prometheus.GaugeValue, float64(off[d]), d)
	}
}

func flowDir(domain, flowID string) string {
	return filepath.Join(domain, flowID+".mxl-flow")
}

func closeAll(files []File) {
	for _, f := range files {
		_ = f.Close()
	}
}

func size(b uint64) string {
	return resource.NewQuantity(int64(b), resource.BinarySI).String()
}

// nodeMemFree reads MemFree of a NUMA node from sysfs.
func nodeMemFree(node int32) (uint64, bool) {
	return memFreeFrom(filepath.Join("/sys/devices/system/node", fmt.Sprintf("node%d", node), "meminfo"))
}

// memFreeFrom parses the "Node N MemFree: X kB" line of a node meminfo.
func memFreeFrom(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	var node int
	var kb uint64
	for _, line := range strings.Split(string(b), "\n") {
		if n, _ := fmt.Sscanf(line, "Node %d MemFree: %d kB", &node, &kb); n == 2 {
			return kb * 1024, true
		}
	}
	return 0, false
}
