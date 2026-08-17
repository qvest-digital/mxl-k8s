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
	assert.Equal(t, 50.0, c.KubeAPIQPS,
		"client-go's 5 QPS fallback throttles the per-mirror status "+
			"flushers at a handful of flowing mirrors; the default must "+
			"stay well above that")
	assert.Equal(t, 100, c.KubeAPIBurst)
	require.Equal(t, []fabrics.Provider{fabrics.ProviderAny}, c.Providers,
		"the flag is an upper bound on a probed list, not the list itself; "+
			"defaulting it to one provider would hide the hardware on every "+
			"node that has more, which is the whole mixed-hardware case")
	assert.Empty(t, c.FabricCIDRs,
		"an unset fabric considers every address libmxl-fabrics reports; "+
			"narrowing by default would exclude single-NIC nodes entirely")
	assert.Empty(t, c.FabricDevices)
	assert.Zero(t, c.FabricMinLinkSpeed)
}

func TestFromFlags_FabricSelection(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--fabric-cidr=10.20.53.0/24, fd00::/64",
		"--fabric-device=mlx5_0, eth2",
		"--fabric-min-link-speed=25G",
	})
	require.NoError(t, err)

	require.Len(t, c.FabricCIDRs, 2)
	assert.Equal(t, "10.20.53.0/24", c.FabricCIDRs[0].String())
	assert.Equal(t, "fd00::/64", c.FabricCIDRs[1].String())
	assert.Equal(t, []string{"mlx5_0", "eth2"}, c.FabricDevices)
	assert.Equal(t, uint64(25_000_000_000), c.FabricMinLinkSpeed,
		"libfabric reports link speed in bits per second, so a quantity "+
			"suffix has to resolve to the same unit")

	sel := c.Selector()
	assert.Equal(t, c.FabricCIDRs, sel.CIDRs)
	assert.Equal(t, c.FabricDevices, sel.Devices)
	assert.Equal(t, c.FabricMinLinkSpeed, sel.MinLinkSpeed)
}

func TestFromFlags_RejectsAnUnparseableFabric(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	// A bare address rather than a prefix is the likely typo, and
	// silently ignoring it would leave the fabric wide open on a node
	// whose operator believes it is narrowed.
	_, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--fabric-cidr=10.20.53.13",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--fabric-cidr")

	_, err = FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--fabric-min-link-speed=fast",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--fabric-min-link-speed")
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
		"the CSV parser must trim whitespace and preserve order. Order carries "+
			"no meaning of its own: the list is an upper bound tested by "+
			"membership, and the advertised order follows the probe's own "+
			"preference. Preserving it keeps the flag legible against what an "+
			"operator wrote")
}

func TestFromFlags_ProviderSentinelAliases(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--providers=any,auto",
	})
	require.NoError(t, err)
	assert.Equal(t,
		[]fabrics.Provider{fabrics.ProviderAny, fabrics.ProviderAny},
		c.Providers,
		"fabrics.ParseProvider rejects the any sentinel's string forms, so "+
			"the flag parser must map \"any\" and the legacy \"auto\" to "+
			"ProviderAny itself; existing deployments pass --providers=auto")
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
		{"zero qps", Config{NodeName: "n", DomainPath: "/d", Providers: []fabrics.Provider{fabrics.ProviderTCP}, KubeAPIBurst: 100}, "--kube-api-qps"},
		{"zero burst", Config{NodeName: "n", DomainPath: "/d", Providers: []fabrics.Provider{fabrics.ProviderTCP}, KubeAPIQPS: 50}, "--kube-api-burst"},
		{"valid", Config{NodeName: "n", DomainPath: "/d", Providers: []fabrics.Provider{fabrics.ProviderTCP}, KubeAPIQPS: 50, KubeAPIBurst: 100}, ""},
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

func TestFromFlags_PacingDefaults(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), nil)
	require.NoError(t, err)
	assert.Negative(t, c.PacingFraction,
		"an unset --pacing-fraction must arrive negative, which is how pacing is "+
			"turned off, and not as zero: the reconciler reads zero as 'not "+
			"configured' and would substitute its own default")
	assert.Equal(t, 8, c.PacingChunks)
	require.NoError(t, c.Validate())
}

func TestFromFlags_PacingRoundTrips(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	t.Setenv("MXL_DOMAIN", "/d")
	t.Setenv("POD_IP", "")
	t.Setenv("KUBECONFIG", "")

	c, err := FromFlags(flag.NewFlagSet("g", flag.ContinueOnError), []string{
		"--pacing-fraction=0.75",
		"--pacing-chunks=16",
	})
	require.NoError(t, err)
	assert.InDelta(t, 0.75, c.PacingFraction, 1e-9)
	assert.Equal(t, 16, c.PacingChunks)
	require.NoError(t, c.Validate())
}

func TestValidate_PacingFractionMustStayBelowOne(t *testing.T) {
	// At or above 1 a grain is spread over its whole interval or more,
	// so its transmission runs into the next grain's and the mirror
	// falls permanently behind. Disabling is spelled with a negative
	// value instead, which keeps zero meaning "unset".
	for _, f := range []float64{1, 1.5, 2} {
		c := &Config{
			NodeName:       "n1",
			DomainPath:     "/d",
			Providers:      []fabrics.Provider{fabrics.ProviderTCP},
			KubeAPIQPS:     50,
			KubeAPIBurst:   100,
			PacingFraction: f,
		}
		assert.Errorf(t, c.Validate(), "fraction %v must be rejected", f)
	}
}

func TestValidate_NegativePacingFractionDisables(t *testing.T) {
	c := &Config{
		NodeName:       "n1",
		DomainPath:     "/d",
		Providers:      []fabrics.Provider{fabrics.ProviderTCP},
		KubeAPIQPS:     50,
		KubeAPIBurst:   100,
		PacingFraction: -1,
	}
	assert.NoError(t, c.Validate(),
		"a negative fraction is how an operator turns pacing off")
}
