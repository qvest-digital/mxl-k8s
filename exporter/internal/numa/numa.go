// Package numa exports where the pages of each mxlfs domain and of each
// flow's ring are, and what the module's moves did.
//
// Scan reads the module's counters on the exporter's scan period and
// caches them; a scrape reads only the cache, so no scrape calls into
// mxlfs. Ring files are opened, read and closed within one scan: an open
// file would keep a deleted flow's pages allocated. Every call needs no
// privilege. A module without the ioctls turns the domain's series off.
package numa

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// magic is the f_type statfs reports for an mxlfs.
const magic = 0x6d786c66

// flowPages is where one flow's ring is.
type flowPages struct {
	perNode []uint64
	held    bool
}

// snapshot is one domain's counters at one scan.
type snapshot struct {
	nodes []mxlfs.NodeStat
	stats mxlfs.Stats
	flows map[string]flowPages
}

// Collector scans the mxlfs domains and exports the last scan.
type Collector struct {
	log logr.Logger

	// Seams for tests.
	isMxlfs    func(path string) bool
	pages      func(*os.File) (mxlfs.Pages, error)
	mountNodes func(*os.File) ([]mxlfs.NodeStat, error)
	stats      func(*os.File) (mxlfs.Stats, error)
	pageSize   uint64

	mu          sync.Mutex
	snaps       map[string]snapshot // by domain path
	unsupported map[string]bool
}

// New returns a Collector.
func New(log logr.Logger) *Collector {
	return &Collector{
		log:         log,
		isMxlfs:     isMxlfs,
		pages:       mxlfs.NodePages,
		mountNodes:  mxlfs.MountNodes,
		stats:       mxlfs.ReadStats,
		pageSize:    uint64(os.Getpagesize()),
		snaps:       map[string]snapshot{},
		unsupported: map[string]bool{},
	}
}

func isMxlfs(path string) bool {
	var st unix.Statfs_t
	return unix.Statfs(path, &st) == nil && st.Type == magic
}

// Scan reads the counters of every domain in paths and forgets the
// domains not among them.
func (c *Collector) Scan(paths []string) {
	snaps := map[string]snapshot{}
	for _, p := range paths {
		c.mu.Lock()
		off := c.unsupported[p]
		c.mu.Unlock()
		if off || !c.isMxlfs(p) {
			continue
		}
		s, err := c.scanDomain(p)
		if mxlfs.Unsupported(err) {
			c.log.Info("the mxlfs module has no NUMA ioctls; no NUMA series for the domain", "domain", p)
			c.mu.Lock()
			c.unsupported[p] = true
			c.mu.Unlock()
			continue
		}
		if err != nil {
			c.log.Error(err, "read NUMA counters", "domain", p)
			continue
		}
		snaps[p] = s
	}
	c.mu.Lock()
	c.snaps = snaps
	c.mu.Unlock()
}

func (c *Collector) scanDomain(path string) (snapshot, error) {
	dir, err := os.Open(path)
	if err != nil {
		return snapshot{}, err
	}
	s := snapshot{flows: map[string]flowPages{}}
	s.nodes, err = c.mountNodes(dir)
	if err == nil {
		s.stats, err = c.stats(dir)
	}
	_ = dir.Close()
	if err != nil {
		return snapshot{}, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return snapshot{}, err
	}
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), ".mxl-flow")
		if !ok || !e.IsDir() || mxlv1alpha1.IsPlaceholder(path, id) {
			continue
		}
		fp, err := c.flow(filepath.Join(path, e.Name()))
		if mxlfs.Unsupported(err) {
			return snapshot{}, err
		}
		if err == nil {
			s.flows[id] = fp
		}
	}
	return s, nil
}

// flow sums the pages of a flow's ring files; a flow torn down mid-scan
// is left out.
func (c *Collector) flow(flowDir string) (flowPages, error) {
	files, err := mxlfs.RingFiles(flowDir)
	if err != nil {
		return flowPages{}, err
	}
	var fp flowPages
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return flowPages{}, err
		}
		p, err := c.pages(f)
		_ = f.Close()
		if err != nil {
			return flowPages{}, err
		}
		for len(fp.perNode) < len(p.PerNode) {
			fp.perNode = append(fp.perNode, 0)
		}
		for n, v := range p.PerNode {
			fp.perNode[n] += v
		}
		fp.held = fp.held || p.Holds > 0
	}
	return fp, nil
}

var (
	domainLabels = []string{"domain"}
	nodeLabels   = []string{"domain", "numa_node"}
	flowLabels   = []string{"flow_id", "domain"}

	descBytes = prometheus.NewDesc("mxl_domain_numa_bytes",
		"Bytes of the domain's pages on each NUMA node.", nodeLabels, nil)
	descFallback = prometheus.NewDesc("mxl_domain_numa_fallback_bytes_total",
		"Bytes allocated on another node than the NUMA node they preferred, by preferred node.", nodeLabels, nil)
	descMoves = prometheus.NewDesc("mxl_domain_numa_moves_total",
		"Ring file moves by result: all blocks moved, some, or none of those off the node.",
		[]string{"domain", "result"}, nil)
	descRefused = prometheus.NewDesc("mxl_domain_numa_move_refused_blocks_total",
		"Blocks a move left where they were, by reason.", []string{"domain", "reason"}, nil)
	descMoved = prometheus.NewDesc("mxl_domain_numa_moved_bytes_total",
		"Bytes moved between NUMA nodes.", domainLabels, nil)
	descMoveTime = prometheus.NewDesc("mxl_domain_numa_move_duration_seconds_total",
		"Time spent in moves.", domainLabels, nil)
	descWindow = prometheus.NewDesc("mxl_domain_numa_move_block_window_seconds",
		"How long one block was unmapped while it moved: the longest an access to it waited.", domainLabels, nil)
	descWindowMax = prometheus.NewDesc("mxl_domain_numa_move_block_window_max_seconds",
		"The longest block window of any move since the domain was mounted.", domainLabels, nil)
	descHolds = prometheus.NewDesc("mxl_domain_numa_holds",
		"Open files holding a ring file in place.", domainLabels, nil)
	descRingBytes = prometheus.NewDesc("mxl_flow_ring_numa_bytes",
		"Bytes of the flow's ring on each NUMA node.", append(append([]string{}, flowLabels...), "numa_node"), nil)
	descRingHeld = prometheus.NewDesc("mxl_flow_ring_held",
		"1 while an open file holds a file of the flow's ring in place.", flowLabels, nil)
)

// refuseReasons are the reasons MoveRefused is indexed by.
var refuseReasons = []mxlfs.Reason{mxlfs.RefusePinned, mxlfs.RefuseView, mxlfs.RefuseBusy,
	mxlfs.RefuseNomem, mxlfs.RefuseHeld, mxlfs.RefuseSignal}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descBytes, descFallback, descMoves, descRefused, descMoved,
		descMoveTime, descWindow, descWindowMax, descHolds, descRingBytes, descRingHeld} {
		ch <- d
	}
}

// Collect implements prometheus.Collector. It reads the last Scan only.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	snaps := c.snaps
	c.mu.Unlock()
	paths := make([]string, 0, len(snaps))
	for p := range snaps {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, dom := range paths {
		c.collectDomain(ch, dom, snaps[dom])
	}
}

func (c *Collector) collectDomain(ch chan<- prometheus.Metric, dom string, s snapshot) {
	ps := float64(c.pageSize)
	gauge := func(d *prometheus.Desc, v float64, l ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, l...)
	}
	counter := func(d *prometheus.Desc, v float64, l ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, l...)
	}
	for n, st := range s.nodes {
		node := strconv.Itoa(n)
		gauge(descBytes, float64(st.Pages)*ps, dom, node)
		counter(descFallback, float64(st.FallbackPages)*ps, dom, node)
	}
	st := s.stats
	partial, none := st.MoveCallsPartial, st.MoveCallsNone
	counter(descMoves, float64(st.MoveCalls-min(st.MoveCalls, partial+none)), dom, "all")
	counter(descMoves, float64(partial), dom, "partial")
	counter(descMoves, float64(none), dom, "none")
	for _, r := range refuseReasons {
		counter(descRefused, float64(st.MoveRefused[r]), dom, r.String())
	}
	counter(descMoved, float64(st.MoveBytes), dom)
	counter(descMoveTime, float64(st.MoveNs)/1e9, dom)
	buckets := map[float64]uint64{}
	var cum uint64
	for i, le := range mxlfs.WindowBuckets {
		cum += st.MoveWindowHist[i]
		buckets[le] = cum
	}
	cum += st.MoveWindowHist[len(mxlfs.WindowBuckets)]
	ch <- prometheus.MustNewConstHistogram(descWindow, cum, float64(st.MoveWindowNsSum)/1e9, buckets, dom)
	gauge(descWindowMax, float64(st.MoveWindowNsMax)/1e9, dom)
	gauge(descHolds, float64(st.Holds), dom)

	for id, fp := range s.flows {
		for n, pages := range fp.perNode {
			gauge(descRingBytes, float64(pages)*ps, id, dom, strconv.Itoa(n))
		}
		held := 0.0
		if fp.held {
			held = 1
		}
		gauge(descRingHeld, held, id, dom)
	}
}
