package numa

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	flowA = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
	flowB = "6fbec3b1-1b0f-417d-9059-8b94a47197ed"
	flowP = "7fbec3b1-1b0f-417d-9059-8b94a47197ed"
)

func write(t testing.TB, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, nil, 0o644))
}

// fake answers the module's calls from fixed values and counts them.
type fake struct {
	calls   atomic.Int64
	err     error
	perFile map[string]mxlfs.Pages // by base name of the flow dir + file
}

func (f *fake) collector(t testing.TB) *Collector {
	c := New(logr.Discard())
	c.isMxlfs = func(string) bool { return true }
	c.pageSize = 4096
	c.pages = func(file *os.File) (mxlfs.Pages, error) {
		f.calls.Add(1)
		if f.err != nil {
			return mxlfs.Pages{}, f.err
		}
		rel := filepath.Base(filepath.Dir(filepath.Dir(file.Name()))) + "/" + filepath.Base(file.Name())
		if strings.HasSuffix(file.Name(), "channels") {
			rel = filepath.Base(filepath.Dir(file.Name())) + "/channels"
		}
		return f.perFile[rel], nil
	}
	c.mountNodes = func(*os.File) ([]mxlfs.NodeStat, error) {
		f.calls.Add(1)
		if f.err != nil {
			return nil, f.err
		}
		return []mxlfs.NodeStat{{Pages: 100, FallbackPages: 1}, {Pages: 50}}, nil
	}
	c.stats = func(*os.File) (mxlfs.Stats, error) {
		f.calls.Add(1)
		s := mxlfs.Stats{MoveCalls: 5, MoveCallsPartial: 1, MoveCallsNone: 1, MoveBytes: 8192, MoveNs: 2e9,
			MoveWindowNsSum: 300e3, MoveWindowNsMax: 9e6, Holds: 2}
		s.MoveRefused[mxlfs.RefuseView] = 3
		s.MoveWindowHist[1], s.MoveWindowHist[8] = 2, 1
		return s, nil
	}
	return c
}

func domainDir(t *testing.T) string {
	dom := t.TempDir()
	write(t, filepath.Join(dom, flowA+".mxl-flow", "grains", "data.0"))
	write(t, filepath.Join(dom, flowA+".mxl-flow", "grains", "data.1"))
	write(t, filepath.Join(dom, flowB+".mxl-flow", "channels"))
	// A placeholder holds no ring this node has.
	write(t, filepath.Join(dom, flowP+".mxl-flow", "grains", "data.0"))
	write(t, mxlv1alpha1.PlaceholderMarker(dom, flowP))
	return dom
}

func TestSeriesAndLabels(t *testing.T) {
	dom := domainDir(t)
	f := &fake{perFile: map[string]mxlfs.Pages{
		flowA + ".mxl-flow/data.0":   {PerNode: []uint64{2, 3}, Holds: 1},
		flowA + ".mxl-flow/data.1":   {PerNode: []uint64{0, 5}},
		flowB + ".mxl-flow/channels": {PerNode: []uint64{7, 0}},
	}}
	c := f.collector(t)
	c.Scan([]string{dom})

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	want := fmt.Sprintf(`
# HELP mxl_domain_numa_bytes Bytes of the domain's pages on each NUMA node.
# TYPE mxl_domain_numa_bytes gauge
mxl_domain_numa_bytes{domain=%[1]q,numa_node="0"} 409600
mxl_domain_numa_bytes{domain=%[1]q,numa_node="1"} 204800
# HELP mxl_domain_numa_fallback_bytes_total Bytes allocated on another node than the NUMA node they preferred, by preferred node.
# TYPE mxl_domain_numa_fallback_bytes_total counter
mxl_domain_numa_fallback_bytes_total{domain=%[1]q,numa_node="0"} 4096
mxl_domain_numa_fallback_bytes_total{domain=%[1]q,numa_node="1"} 0
# HELP mxl_domain_numa_moves_total Ring file moves by result: all blocks moved, some, or none of those off the node.
# TYPE mxl_domain_numa_moves_total counter
mxl_domain_numa_moves_total{domain=%[1]q,result="all"} 3
mxl_domain_numa_moves_total{domain=%[1]q,result="none"} 1
mxl_domain_numa_moves_total{domain=%[1]q,result="partial"} 1
# HELP mxl_domain_numa_move_block_window_seconds How long one block was unmapped while it moved: the longest an access to it waited.
# TYPE mxl_domain_numa_move_block_window_seconds histogram
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="6.4e-05"} 0
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="0.000128"} 2
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="0.000256"} 2
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="0.000512"} 2
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="0.001"} 2
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="0.002"} 2
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="0.004"} 2
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="0.008"} 2
mxl_domain_numa_move_block_window_seconds_bucket{domain=%[1]q,le="+Inf"} 3
mxl_domain_numa_move_block_window_seconds_sum{domain=%[1]q} 0.0003
mxl_domain_numa_move_block_window_seconds_count{domain=%[1]q} 3
# HELP mxl_flow_ring_numa_bytes Bytes of the flow's ring on each NUMA node.
# TYPE mxl_flow_ring_numa_bytes gauge
mxl_flow_ring_numa_bytes{domain=%[1]q,flow_id=%[2]q,numa_node="0"} 8192
mxl_flow_ring_numa_bytes{domain=%[1]q,flow_id=%[2]q,numa_node="1"} 32768
mxl_flow_ring_numa_bytes{domain=%[1]q,flow_id=%[3]q,numa_node="0"} 28672
mxl_flow_ring_numa_bytes{domain=%[1]q,flow_id=%[3]q,numa_node="1"} 0
# HELP mxl_flow_ring_held 1 while an open file holds a file of the flow's ring in place.
# TYPE mxl_flow_ring_held gauge
mxl_flow_ring_held{domain=%[1]q,flow_id=%[2]q} 1
mxl_flow_ring_held{domain=%[1]q,flow_id=%[3]q} 0
`, dom, flowA, flowB)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(want),
		"mxl_domain_numa_bytes", "mxl_domain_numa_fallback_bytes_total", "mxl_domain_numa_moves_total",
		"mxl_domain_numa_move_block_window_seconds", "mxl_flow_ring_numa_bytes", "mxl_flow_ring_held"))
	require.Equal(t, 3.0, sample(t, reg, "mxl_domain_numa_move_refused_blocks_total", "view"))
	require.Equal(t, 0.0, sample(t, reg, "mxl_domain_numa_move_refused_blocks_total", "pinned"))
	require.Equal(t, 8192.0, sample(t, reg, "mxl_domain_numa_moved_bytes_total", ""))
	require.Equal(t, 2.0, sample(t, reg, "mxl_domain_numa_move_duration_seconds_total", ""))
	require.Equal(t, 2.0, sample(t, reg, "mxl_domain_numa_holds", ""))
	require.Equal(t, 0.009, sample(t, reg, "mxl_domain_numa_move_block_window_max_seconds", ""))
}

// sample returns the value of name whose reason label is reason, or the
// only one when reason is empty.
func sample(t *testing.T, reg *prometheus.Registry, name, reason string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			ok := reason == ""
			for _, l := range m.GetLabel() {
				ok = ok || (l.GetName() == "reason" && l.GetValue() == reason)
			}
			if !ok {
				continue
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	t.Fatalf("no %s{reason=%q}", name, reason)
	return 0
}

// A scrape reads the cache: no call reaches the module.
func TestScrapeNeverCallsIntoMxlfs(t *testing.T) {
	f := &fake{perFile: map[string]mxlfs.Pages{}}
	c := f.collector(t)
	c.Scan([]string{domainDir(t)})
	n := f.calls.Load()
	require.NotZero(t, n)
	for i := 0; i < 3; i++ {
		testutil.CollectAndCount(c)
	}
	require.Equal(t, n, f.calls.Load())
}

// A module without the ioctls turns the domain's series off for good;
// a domain that is not an mxlfs has none.
func TestUnsupportedModuleTurnsSeriesOff(t *testing.T) {
	f := &fake{err: fmt.Errorf("ioctl: %w", syscall.ENOTTY)}
	c := f.collector(t)
	dom := domainDir(t)
	c.Scan([]string{dom})
	require.Zero(t, testutil.CollectAndCount(c))
	n := f.calls.Load()
	f.err = nil
	c.Scan([]string{dom})
	require.Equal(t, n, f.calls.Load(), "a call reached an unsupported domain")
	require.Zero(t, testutil.CollectAndCount(c))

	c = (&fake{}).collector(t)
	c.isMxlfs = func(string) bool { return false }
	c.Scan([]string{dom})
	require.Zero(t, testutil.CollectAndCount(c))
}

// A domain that leaves the list loses its series.
func TestDepartedDomainIsForgotten(t *testing.T) {
	c := (&fake{perFile: map[string]mxlfs.Pages{}}).collector(t)
	dom := domainDir(t)
	c.Scan([]string{dom})
	require.NotZero(t, testutil.CollectAndCount(c, "mxl_domain_numa_holds"))
	c.Scan(nil)
	require.Zero(t, testutil.CollectAndCount(c))
}

// BenchmarkScan100Flows is one collection of a domain of 100 flows of 40
// ring files each: per file an open, the NODE_PAGES ioctl and a close,
// as on an mxlfs. Off an mxlfs the ioctl is answered ENOTTY by the VFS,
// so the measured syscalls are real and only the module's handler, which
// reads a few counters, is missing.
func BenchmarkScan100Flows(b *testing.B) {
	dom := b.TempDir()
	for i := 0; i < 100; i++ {
		for g := 0; g < 40; g++ {
			write(b, filepath.Join(dom, fmt.Sprintf("%08x-0000-4000-8000-000000000000.mxl-flow", i), "grains", fmt.Sprintf("data.%d", g)))
		}
	}
	c := New(logr.Discard())
	c.isMxlfs = func(string) bool { return true }
	c.pages = func(f *os.File) (mxlfs.Pages, error) {
		_, _ = mxlfs.NodePages(f)
		return mxlfs.Pages{PerNode: []uint64{1, 1}}, nil
	}
	c.mountNodes = func(*os.File) ([]mxlfs.NodeStat, error) { return make([]mxlfs.NodeStat, 2), nil }
	c.stats = func(f *os.File) (mxlfs.Stats, error) { _, _ = mxlfs.ReadStats(f); return mxlfs.Stats{}, nil }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Scan([]string{dom})
	}
	b.StopTimer()
	c.mu.Lock()
	require.Len(b, c.snaps[dom].flows, 100)
	c.mu.Unlock()
}
