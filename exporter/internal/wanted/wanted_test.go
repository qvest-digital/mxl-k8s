package wanted

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
	"github.com/qvest-digital/mxl-k8s/api/mxlfs/uapitest"
)

func collector(calls *atomic.Int64, err *error) *Collector {
	c := New(logr.Discard())
	c.isMxlfs = func(string) bool { return true }
	c.stats = func(*os.File) (mxlfs.Stats, error) {
		calls.Add(1)
		if *err != nil {
			return mxlfs.Stats{}, *err
		}
		return mxlfs.Stats{Wanted: 3, WantedRefused: 17, MoveCalls: 99}, nil
	}
	return c
}

func TestSeries(t *testing.T) {
	var calls atomic.Int64
	var err error
	c := collector(&calls, &err)
	dom := t.TempDir()
	c.Scan([]string{dom})
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(fmt.Sprintf(`
# HELP mxl_domain_wanted_flows Missing flows path walks on the domain's mxlfs wait for now; always 0 on an mxlfs before 0.1.0-rc.8.
# TYPE mxl_domain_wanted_flows gauge
mxl_domain_wanted_flows{domain=%[1]q} 3
# HELP mxl_domain_wanted_refused_total Path walks that found a flow missing and failed at once instead of waiting, because the mount's or the walker's cap on waits was full; always 0 on an mxlfs before 0.1.0-rc.8.
# TYPE mxl_domain_wanted_refused_total counter
mxl_domain_wanted_refused_total{domain=%[1]q} 17
`, dom))))
}

// A scrape reads the cache: no call reaches the module.
func TestScrapeNeverCallsIntoMxlfs(t *testing.T) {
	var calls atomic.Int64
	var err error
	c := collector(&calls, &err)
	c.Scan([]string{t.TempDir()})
	require.EqualValues(t, 1, calls.Load())
	for range 3 {
		testutil.CollectAndCount(c)
	}
	require.EqualValues(t, 1, calls.Load())
}

// A module without the ioctl turns the domain's series off for good, a
// failed read leaves them out of that scan only, and a domain that is
// not an mxlfs, or left the list, has none.
func TestDomainsWithoutCounters(t *testing.T) {
	var calls atomic.Int64
	err := error(fmt.Errorf("ioctl: %w", syscall.ENOTTY))
	c := collector(&calls, &err)
	dom := t.TempDir()
	c.Scan([]string{dom})
	err = nil
	c.Scan([]string{dom})
	require.EqualValues(t, 1, calls.Load(), "a call reached an unsupported domain")
	require.Zero(t, testutil.CollectAndCount(c))

	c = collector(&calls, &err)
	err = fmt.Errorf("ioctl: %w", syscall.EIO)
	c.Scan([]string{dom})
	require.Zero(t, testutil.CollectAndCount(c))
	err = nil
	c.Scan([]string{dom})
	require.Equal(t, 2, testutil.CollectAndCount(c))
	c.Scan(nil)
	require.Zero(t, testutil.CollectAndCount(c))

	c.isMxlfs = func(string) bool { return false }
	c.Scan([]string{dom})
	require.Zero(t, testutil.CollectAndCount(c))
}

func TestMagicMatchesHeader(t *testing.T) {
	h, err := uapitest.Parse()
	require.NoError(t, err)
	require.EqualValues(t, h.Consts["MXLFS_MAGIC"], magic)
}
