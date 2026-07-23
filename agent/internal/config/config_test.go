package config

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FromFlags parses CLI args, falls back to env vars for NodeName and
// Kubeconfig, and finally calls Validate. The tests pin defaults
// (because the operator uses them to derive cluster-wide
// readiness expectations) and every Validate rejection path.

func TestFromFlags_DefaultsAndRequiredFields(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	t.Setenv("KUBECONFIG", "")

	_, err := FromFlags(flag.NewFlagSet("agent", flag.ContinueOnError), nil)
	require.Error(t, err,
		"FromFlags must reject an empty args set; --domain-path has no default")
	assert.Contains(t, err.Error(), "--domain-path")
}

func TestFromFlags_DefaultsFireWhenArgsProvided(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("agent", flag.ContinueOnError),
		[]string{"--domain-path=/run/mxl/domain", "--node-name=n1"})
	require.NoError(t, err)
	assert.Equal(t, "/run/mxl/domain", c.DomainPath)
	assert.Equal(t, "n1", c.NodeName)
	assert.Equal(t, ":8081", c.ProbeAddr,
		"probe address default must stay :8081 because the chart's "+
			"liveness/readiness probes target that port; flipping it "+
			"silently disables every pod's probes on upgrade")
	assert.Equal(t, ":8080", c.MetricsAddr)
	assert.Equal(t, 30*time.Second, c.ResyncPeriod)
	assert.Equal(t, "/run/mxl/agent.sock", c.IntentSocketPath)
	assert.Equal(t, 5*time.Second, c.MaterializeTimeout)
	assert.Equal(t, 50.0, c.KubeAPIQPS,
		"client-go's 5 QPS fallback queues status publishes behind "+
			"second-long delays during flow appear/vanish bursts; the "+
			"default must stay well above that")
	assert.Equal(t, 100, c.KubeAPIBurst)
}

func TestFromFlags_OverridesAreRespected(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("agent", flag.ContinueOnError), []string{
		"--domain-path=/data/mxl",
		"--node-name=worker-7",
		"--kubeconfig=/etc/kube",
		"--health-probe-bind-address=:9081",
		"--metrics-bind-address=:9080",
		"--resync-period=15s",
		"--intent-socket=/tmp/mxl.sock",
		"--materialize-timeout=2s",
	})
	require.NoError(t, err)
	assert.Equal(t, "/data/mxl", c.DomainPath)
	assert.Equal(t, "worker-7", c.NodeName)
	assert.Equal(t, "/etc/kube", c.Kubeconfig)
	assert.Equal(t, ":9081", c.ProbeAddr)
	assert.Equal(t, ":9080", c.MetricsAddr)
	assert.Equal(t, 15*time.Second, c.ResyncPeriod)
	assert.Equal(t, "/tmp/mxl.sock", c.IntentSocketPath)
	assert.Equal(t, 2*time.Second, c.MaterializeTimeout)
}

func TestFromFlags_NodeNameFallsBackToEnv(t *testing.T) {
	t.Setenv("NODE_NAME", "from-env")
	t.Setenv("KUBECONFIG", "/some/path")

	c, err := FromFlags(flag.NewFlagSet("agent", flag.ContinueOnError),
		[]string{"--domain-path=/run/mxl/domain"})
	require.NoError(t, err)
	assert.Equal(t, "from-env", c.NodeName)
	assert.Equal(t, "/some/path", c.Kubeconfig,
		"downward-API env vars must be picked up; missing them on a "+
			"production node would make the agent fail to look itself up")
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		c       Config
		wantErr string
	}{
		{
			name:    "missing domain path",
			c:       Config{NodeName: "n1"},
			wantErr: "--domain-path is required",
		},
		{
			name:    "relative domain path",
			c:       Config{DomainPath: "run/mxl/domain", NodeName: "n1"},
			wantErr: "must be absolute",
		},
		{
			name:    "missing node name",
			c:       Config{DomainPath: "/run/mxl/domain"},
			wantErr: "--node-name",
		},
		{
			name:    "valid",
			c:       Config{DomainPath: "/run/mxl/domain", NodeName: "n1", KubeAPIQPS: 50, KubeAPIBurst: 100},
			wantErr: "",
		},
		{
			name:    "explicit provider override",
			c:       Config{DomainPath: "/run/mxl/domain", NodeName: "n1", Provider: "verbs", KubeAPIQPS: 50, KubeAPIBurst: 100},
			wantErr: "",
		},
		{
			name:    "auto provider is accepted (resolves per node)",
			c:       Config{DomainPath: "/run/mxl/domain", NodeName: "n1", Provider: "auto", KubeAPIQPS: 50, KubeAPIBurst: 100},
			wantErr: "",
		},
		{
			name:    "zero qps rejected",
			c:       Config{DomainPath: "/run/mxl/domain", NodeName: "n1", KubeAPIBurst: 100},
			wantErr: "--kube-api-qps",
		},
		{
			name:    "zero burst rejected",
			c:       Config{DomainPath: "/run/mxl/domain", NodeName: "n1", KubeAPIQPS: 50},
			wantErr: "--kube-api-burst",
		},
		{
			name:    "unknown provider rejected",
			c:       Config{DomainPath: "/run/mxl/domain", NodeName: "n1", Provider: "rdma"},
			wantErr: "--provider",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.c.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
