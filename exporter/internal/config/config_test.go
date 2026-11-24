package config

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(discard{})
	return FromFlags(fs, args)
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func TestFromFlagsDefaults(t *testing.T) {
	cfg, err := parse(t, "--node-name=node-a", "--domain-path=/run/mxl/domain")
	require.NoError(t, err)
	require.Equal(t, ":8080", cfg.MetricsAddr)
	require.Equal(t, ":8081", cfg.ProbeAddr)
	require.True(t, cfg.TopologyEnabled)
	require.True(t, cfg.MirrorHeartbeats)
	require.Equal(t, "mxl-system", cfg.LeaseNamespace)
}

// The node name labels every series. Without it two nodes' exporters
// would publish indistinguishable samples for the same flow.
func TestFromFlagsRequiresNodeName(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	_, err := parse(t, "--domain-path=/run/mxl/domain")
	require.ErrorContains(t, err, "--node-name")
}

func TestFromFlagsRequiresAbsoluteDomainPath(t *testing.T) {
	_, err := parse(t, "--node-name=node-a", "--domain-path=relative/domain")
	require.ErrorContains(t, err, "must be absolute")
}

func TestFromFlagsRejectsNonPositiveScanPeriod(t *testing.T) {
	_, err := parse(t, "--node-name=node-a", "--domain-path=/run/mxl/domain", "--scan-period=0s")
	require.ErrorContains(t, err, "--scan-period")
}

// The NUMA counters are off unless asked for, whatever the placement.
func TestFromFlagsMxlfsNumaMetrics(t *testing.T) {
	cfg, err := parse(t, "--node-name=node-a", "--domain-path=/run/mxl/domain")
	require.NoError(t, err)
	require.False(t, cfg.MxlfsNumaMetrics)
	cfg, err = parse(t, "--node-name=node-a", "--domain-path=/run/mxl/domain", "--mxlfs-numa-metrics")
	require.NoError(t, err)
	require.True(t, cfg.MxlfsNumaMetrics)
}

// The chart passes the namespace its Role is in, so the watch never
// lists where the exporter has no access.
func TestFromFlagsLeaseNamespace(t *testing.T) {
	cfg, err := parse(t, "--node-name=node-a", "--domain-path=/run/mxl/domain", "--lease-namespace=media")
	require.NoError(t, err)
	require.Equal(t, "media", cfg.LeaseNamespace)

	_, err = parse(t, "--node-name=node-a", "--domain-path=/run/mxl/domain", "--lease-namespace=")
	require.ErrorContains(t, err, "--lease-namespace")
}

func TestFromFlagsMirrorHeartbeatsCanBeTurnedOff(t *testing.T) {
	cfg, err := parse(t, "--node-name=node-a", "--domain-path=/run/mxl/domain", "--mirror-heartbeats=false")
	require.NoError(t, err)
	require.False(t, cfg.MirrorHeartbeats)
}
