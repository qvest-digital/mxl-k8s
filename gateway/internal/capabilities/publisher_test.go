package capabilities

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/fabric"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

// fakeLister stands in for the libmxl-fabrics enumeration.
type fakeLister struct {
	got  *fabrics.InterfaceConfig
	out  []fabrics.InterfaceConfig
	err  error
	call int
}

func (f *fakeLister) Interfaces(q *fabrics.InterfaceConfig) ([]fabrics.InterfaceConfig, error) {
	f.call++
	f.got = q
	return f.out, f.err
}

func iface(provider fabrics.Provider, node, device string) fabrics.InterfaceConfig {
	cfg := fabrics.InterfaceConfig{
		Provider: provider,
		Caps: fabrics.InterfaceCaps{
			Flags:          fabrics.InterfaceCapRemoteWrite,
			MaxMessageSize: 1 << 20,
		},
		Address: fabrics.EndpointAddress{Node: node},
	}
	if device != "" {
		cfg.Attr = `{"device_name":"` + device + `","link_state":"up","link_speed":100000000000}`
	}
	return cfg
}

// existingCR is the object the publisher refreshes, as EnsureExists
// leaves it.
func existingCR() *mxlv1alpha1.MxlNodeCapabilities {
	return &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: "n1"},
	}
}

func newClient(t *testing.T, objs ...*mxlv1alpha1.MxlNodeCapabilities) *fake.ClientBuilder {
	t.Helper()
	b := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlNodeCapabilities{})
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return b
}

func getCR(t *testing.T, p *Publisher) mxlv1alpha1.MxlNodeCapabilities {
	t.Helper()
	var got mxlv1alpha1.MxlNodeCapabilities
	require.NoError(t, p.Client.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got))
	return got
}

// providerByName finds a published provider entry.
func providerByName(caps []mxlv1alpha1.MxlFabricsProviderCapability, name mxlv1alpha1.MxlFabricsProvider) *mxlv1alpha1.MxlFabricsProviderCapability {
	for i := range caps {
		if caps[i].Name == name {
			return &caps[i]
		}
	}
	return nil
}

func TestEnsureExists_CreatesOnce(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}
	require.NoError(t, p.EnsureExists(context.Background()))

	got := getCR(t, p)
	assert.Equal(t, "n1", got.Spec.NodeName)
	assert.Empty(t, got.Status.Providers,
		"EnsureExists must leave status to Refresh; the gateway reports "+
			"capabilities only after the manager cache has synced")

	require.NoError(t, p.EnsureExists(context.Background()),
		"a second EnsureExists must be a no-op; first-pod-up wins on a "+
			"cold cluster and any retry must not error out")
}

// The point of the whole change: what a node advertises comes from
// libmxl-fabrics, not from the flag it was started with.
func TestRefresh_PublishesWhatTheProbeFound(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderVerbs, "10.20.53.13", "mlx5_0"),
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
	}
	require.NoError(t, p.Refresh(context.Background()))

	got := getCR(t, p)
	require.NotNil(t, got.Status.LastSeen)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, mxlv1alpha1.ConditionTypeProbed),
		"deviceCount is only readable behind this condition, so a probe that "+
			"succeeded has to say so")

	verbs := providerByName(got.Status.Providers, mxlv1alpha1.ProviderVerbs)
	require.NotNil(t, verbs)
	assert.Equal(t, int32(1), verbs.DeviceCount)
	require.Len(t, verbs.Interfaces, 1)
	assert.Equal(t, "10.20.53.13", verbs.Interfaces[0].Address)
	assert.Equal(t, "mlx5_0", verbs.Interfaces[0].Device)
	assert.Equal(t, "up", verbs.Interfaces[0].LinkState)
	assert.Equal(t, int64(100000000000), verbs.Interfaces[0].LinkSpeedBitsPerSecond)
	assert.Equal(t, int64(1<<20), verbs.Interfaces[0].MaxMessageSize)

	assert.Equal(t, fabrics.ProviderAny, l.got.Provider,
		"one sweep answers for every provider; querying per provider would "+
			"multiply fi_getinfo calls for the same answer")
}

// The mixed-hardware case from the issue: the same DaemonSet flag on
// every node, and only the node with the adapter advertises it.
func TestRefresh_ReportsAbsentHardwareAsZero(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
	}
	require.NoError(t, p.Refresh(context.Background()))

	got := getCR(t, p)
	efa := providerByName(got.Status.Providers, mxlv1alpha1.ProviderEFA)
	require.NotNil(t, efa,
		"a provider considered but not found has to appear with a zero count; "+
			"omitting it would read as 'never considered'")
	assert.Zero(t, efa.DeviceCount)
	assert.Empty(t, efa.Interfaces)

	tcp := providerByName(got.Status.Providers, mxlv1alpha1.ProviderTCP)
	require.NotNil(t, tcp)
	assert.Equal(t, int32(1), tcp.DeviceCount)
}

func TestRefresh_ProvidersFlagIsAnUpperBound(t *testing.T) {
	// The node has EFA hardware, and the operator has excluded it.
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderEFA, "10.20.53.13", "efa0"),
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP},
		Lister:    l,
	}
	require.NoError(t, p.Refresh(context.Background()))

	got := getCR(t, p)
	assert.Nil(t, providerByName(got.Status.Providers, mxlv1alpha1.ProviderEFA),
		"a provider outside the bound must not be advertised even where the "+
			"probe found the hardware")
	require.Len(t, got.Status.Providers, 1)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, got.Status.Providers[0].Name)
}

func TestRefresh_SelectorNarrowsWhatIsAdvertised(t *testing.T) {
	// Advertising an interface the mirror path would refuse to bind
	// is what makes a node promise capacity it cannot honour.
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "192.168.100.20", "eth1"),
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
		Selector:  fabric.Selector{CIDRs: []netip.Prefix{netip.MustParsePrefix("10.20.53.0/24")}},
	}
	require.NoError(t, p.Refresh(context.Background()))

	tcp := providerByName(getCR(t, p).Status.Providers, mxlv1alpha1.ProviderTCP)
	require.NotNil(t, tcp)
	assert.Equal(t, int32(1), tcp.DeviceCount)
	require.Len(t, tcp.Interfaces, 1)
	assert.Equal(t, "10.20.53.13", tcp.Interfaces[0].Address)
}

func TestRefresh_CountsDevicesNotEnumerationEntries(t *testing.T) {
	// libfabric reports one entry per endpoint type and address
	// family. The published list is keyed by address, so duplicates
	// would also be rejected by the apiserver.
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
		iface(fabrics.ProviderTCP, "fd00::13", "eth0"),
	}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
	}
	require.NoError(t, p.Refresh(context.Background()))

	tcp := providerByName(getCR(t, p).Status.Providers, mxlv1alpha1.ProviderTCP)
	require.NotNil(t, tcp)
	assert.Equal(t, int32(1), tcp.DeviceCount, "one NIC with two addresses is one device")
	assert.Len(t, tcp.Interfaces, 2, "both addresses are still reported, deduplicated")
}

func TestRefresh_ExcludesLoopback(t *testing.T) {
	// Verified against the go-mxl builder image: the tcp provider
	// enumerates lo on every host, and a peer handed 127.0.0.1 in a
	// TargetInfo dials itself.
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "127.0.0.1", "lo"),
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
	}
	require.NoError(t, p.Refresh(context.Background()))

	tcp := providerByName(getCR(t, p).Status.Providers, mxlv1alpha1.ProviderTCP)
	require.NotNil(t, tcp)
	require.Len(t, tcp.Interfaces, 1)
	assert.Equal(t, "10.20.53.13", tcp.Interfaces[0].Address)
}

func TestRefresh_KeepsLastKnownProvidersWhenTheProbeFails(t *testing.T) {
	// Clearing them would drop every mirror on the node to the tcp
	// fallback over a transient enumeration error.
	existing := existingCR()
	existing.Status.Providers = []mxlv1alpha1.MxlFabricsProviderCapability{
		{Name: mxlv1alpha1.ProviderVerbs, DeviceCount: 1},
	}
	p := &Publisher{
		Client:    newClient(t, existing).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    &fakeLister{err: errors.New("fi_getinfo exploded")},
	}

	err := p.Refresh(context.Background())
	require.Error(t, err)

	got := getCR(t, p)
	require.Len(t, got.Status.Providers, 1)
	assert.Equal(t, mxlv1alpha1.ProviderVerbs, got.Status.Providers[0].Name)

	cond := meta.FindStatusCondition(got.Status.Conditions, mxlv1alpha1.ConditionTypeProbed)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonProbeFailed, cond.Reason)
	assert.Contains(t, cond.Message, "fi_getinfo exploded")
}

func TestRefresh_WithoutAListerReportsAFailedProbe(t *testing.T) {
	// Falling back to the configured list would put an unmeasured
	// claim behind a Probed condition, which is the state the probe
	// exists to end.
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP},
	}
	require.Error(t, p.Refresh(context.Background()))

	got := getCR(t, p)
	assert.Empty(t, got.Status.Providers)
	assert.False(t, meta.IsStatusConditionTrue(got.Status.Conditions, mxlv1alpha1.ConditionTypeProbed))
}

func TestRefresh_RewritesProvidersExactly(t *testing.T) {
	// A previous run advertised verbs. Re-running on a node where the
	// probe no longer finds it must replace the slice, not merge it.
	existing := existingCR()
	existing.Status.Providers = []mxlv1alpha1.MxlFabricsProviderCapability{
		{Name: mxlv1alpha1.ProviderVerbs, DeviceCount: 1},
		{Name: mxlv1alpha1.ProviderTCP, DeviceCount: 1},
	}
	p := &Publisher{
		Client:    newClient(t, existing).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP},
		Lister:    &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}},
	}
	require.NoError(t, p.Refresh(context.Background()))

	got := getCR(t, p)
	require.Len(t, got.Status.Providers, 1)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, got.Status.Providers[0].Name,
		"the publisher must own the providers slice entirely; merging would "+
			"surface a removed provider as still-available and mislead the "+
			"operator's mirror scheduling")
}

func TestRefresh_NarrowsTheQueryToTheBindAddress(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderAny},
		Lister:      l,
		BindAddress: "10.20.53.13",
	}
	require.NoError(t, p.Refresh(context.Background()))

	require.NotNil(t, l.got)
	assert.Equal(t, "10.20.53.13", l.got.Address.Node,
		"a gateway pinned to one address must not advertise capacity it would "+
			"never bind")
}

func TestRefresh_ErrorsWhenCRMissing(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}

	require.Error(t, p.Refresh(context.Background()))
}

func TestRunRefreshLoop_CancelsCleanlyOnCtxDone(t *testing.T) {
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP},
		Lister:    &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.RunRefreshLoop(ctx, 10*time.Millisecond)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunRefreshLoop did not return on ctx cancel")
	}
}

// A link that goes down, a driver that loads late, or an adapter
// attached after boot has to show up without restarting the gateway.
func TestRunRefreshLoop_ReprobesOnEveryTick(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.RunRefreshLoop(ctx, 10*time.Millisecond)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	assert.Greater(t, l.call, 1,
		"probing once at boot would leave a stale advertisement behind every "+
			"link and driver change for the life of the pod")
}
