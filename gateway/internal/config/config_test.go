package config

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
)

func TestFromFlags_RequiresProviderAndDomain(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	t.Setenv("MXL_DOMAIN", "")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	_, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), nil)
	require.Error(t, err, "no node name + no domain must fail")
}

func TestFromFlags_DefaultsAndProvidersCSV(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	t.Setenv("MXL_DOMAIN", "")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--node-name=worker-1",
		"--domain-path=/run/mxl/domain",
	})
	require.NoError(t, err)

	assert.Equal(t, "worker-1", c.NodeName)
	assert.Equal(t, "/run/mxl/domain", c.DomainPath)
	assert.Empty(t, c.BindAddress, "bind address defaults to empty so libmxl-fabrics binds all interfaces")
	assert.Equal(t, ":8081", c.ProbeAddr,
		"the chart's liveness/readiness probes target :8081; flipping it disables probes silently")
	assert.Equal(t, ":8080", c.MetricsAddr)
	assert.Empty(t, c.PprofAddr,
		"pprof endpoint is opt-in; the default must leave it off so a "+
			"production gateway never serves /debug/pprof unprompted")
	assert.Equal(t, 30*time.Second, c.ResyncPeriod)
	require.Equal(t, []fabrics.Provider{fabrics.ProviderTCP}, c.Providers,
		"default provider list must be exactly [tcp]; --providers tcp is the only "+
			"setup that works out of the box without RDMA hardware on every node")
}

func TestFromFlags_MultipleProviders(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--providers=tcp,verbs, shm",
	})
	require.NoError(t, err)
	assert.Equal(t,
		[]fabrics.Provider{fabrics.ProviderTCP, fabrics.ProviderVerbs, fabrics.ProviderSHM},
		c.Providers,
		"the CSV parser must trim whitespace and preserve order; the gateway uses "+
			"the slice order to bias selection inside libmxl-fabrics")
}

func TestFromFlags_InvalidProvider(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	_, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError),
		[]string{"--providers=banana"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--providers")
}

func TestFromFlags_EmptyProvidersCSV_Rejected(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	_, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError),
		[]string{"--providers= ,, "})
	require.Error(t, err, "an effectively-empty providers list must be rejected at flag time, "+
		"not at first-reconcile time")
}

func TestFromFlags_PprofBindAddress(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--pprof-bind-address=127.0.0.1:6060",
	})
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:6060", c.PprofAddr,
		"--pprof-bind-address must round-trip through FromFlags; main.go "+
			"keys the pprof HTTP server lifecycle on cfg.PprofAddr being "+
			"non-empty")
}

func TestFromFlags_PodIPDefault(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "10.0.0.42")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.42", c.BindAddress,
		"$POD_IP must flow through to the bind address; the chart sets it from "+
			"the downward API, and missing it would make libmxl-fabrics bind to "+
			"the wrong interface in a multi-NIC pod")
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		c       Config
		wantErr string
	}{
		{"missing node", Config{DomainPath: "/d", Providers: []fabrics.Provider{fabrics.ProviderTCP}}, "--node-name"},
		{"missing domain", Config{NodeName: "n", Providers: []fabrics.Provider{fabrics.ProviderTCP}}, "--domain-path"},
		{"relative domain", Config{NodeName: "n", DomainPath: "rel", Providers: []fabrics.Provider{fabrics.ProviderTCP}}, "absolute"},
		{"empty providers", Config{NodeName: "n", DomainPath: "/d"}, "--providers"},
		{"valid", Config{NodeName: "n", DomainPath: "/d", Providers: []fabrics.Provider{fabrics.ProviderTCP}}, ""},
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
