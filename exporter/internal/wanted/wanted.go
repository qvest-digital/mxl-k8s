// Package wanted exports how path walks on each mxlfs domain wait for
// flows that are missing, from the module's counters.
//
// On a domain mounted with wait=, a walk through a flow that is not
// there waits for it to appear, up to a cap per mount and per thread
// group; a walk past a cap fails with ENOENT at once. Scan reads the
// counters on the exporter's scan period and caches them; a scrape reads
// only the cache. A module without the STATS ioctl turns the domain's
// series off. A module before mxlfs 0.1.0-rc.8 has neither counter and
// reports both as 0, which no reading tells apart from no waits and no
// refusals: while nodes still run such a module, as during a rollout,
// a 0 on them says nothing.
package wanted

import (
	"os"
	"sort"
	"sync"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
)

// magic is the f_type statfs reports for an mxlfs.
const magic = 0x6d786c66

var (
	descWanted = prometheus.NewDesc("mxl_domain_wanted_flows",
		"Missing flows path walks on the domain's mxlfs wait for now; always 0 on an mxlfs before 0.1.0-rc.8.", []string{"domain"}, nil)
	descRefused = prometheus.NewDesc("mxl_domain_wanted_refused_total",
		"Path walks that found a flow missing and failed at once instead of waiting, because the mount's or the walker's cap on waits was full; always 0 on an mxlfs before 0.1.0-rc.8.",
		[]string{"domain"}, nil)
)

// Collector scans the mxlfs domains and exports the last scan.
type Collector struct {
	log logr.Logger

	// Seams for tests.
	isMxlfs func(path string) bool
	stats   func(*os.File) (mxlfs.Stats, error)

	mu          sync.Mutex
	last        map[string]mxlfs.Stats // by domain path
	unsupported map[string]bool
}

// New returns a Collector.
func New(log logr.Logger) *Collector {
	return &Collector{
		log:         log,
		isMxlfs:     isMxlfs,
		stats:       mxlfs.ReadStats,
		last:        map[string]mxlfs.Stats{},
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
	last := map[string]mxlfs.Stats{}
	for _, p := range paths {
		c.mu.Lock()
		off := c.unsupported[p]
		c.mu.Unlock()
		if off || !c.isMxlfs(p) {
			continue
		}
		s, err := c.read(p)
		if mxlfs.Unsupported(err) {
			c.log.Info("the mxlfs module has no STATS ioctl; no wanted series for the domain", "domain", p)
			c.mu.Lock()
			c.unsupported[p] = true
			c.mu.Unlock()
			continue
		}
		if err != nil {
			c.log.Error(err, "read mxlfs counters", "domain", p)
			continue
		}
		last[p] = s
	}
	c.mu.Lock()
	c.last = last
	c.mu.Unlock()
}

func (c *Collector) read(path string) (mxlfs.Stats, error) {
	dir, err := os.Open(path)
	if err != nil {
		return mxlfs.Stats{}, err
	}
	defer dir.Close()
	return c.stats(dir)
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descWanted
	ch <- descRefused
}

// Collect implements prometheus.Collector. It reads the last Scan only.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	last := c.last
	c.mu.Unlock()
	paths := make([]string, 0, len(last))
	for p := range last {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, dom := range paths {
		s := last[dom]
		ch <- prometheus.MustNewConstMetric(descWanted, prometheus.GaugeValue, float64(s.Wanted), dom)
		ch <- prometheus.MustNewConstMetric(descRefused, prometheus.CounterValue, float64(s.WantedRefused), dom)
	}
}
