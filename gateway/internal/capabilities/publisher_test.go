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

	corev1 "k8s.io/api/core/v1"

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
		WithStatusSubresource(&mxlv1alpha1.MxlNodeCapabilities{}).
		WithObjects(testNode())
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return b
}

// testNode is the Node an MxlNodeCapabilities is owned by.
func testNode() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "node-uid-1"}}
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
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(testNode()).Build()

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
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(testNode()).Build()
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

// Without an owner nothing ever deletes one, so a cluster accumulates
// a resource per node that ever existed.
func TestEnsureExists_OwnedByItsNode(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(testNode()).Build()
	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}
	require.NoError(t, p.EnsureExists(context.Background()))

	got := getCR(t, p)
	require.Len(t, got.OwnerReferences, 1)
	ref := got.OwnerReferences[0]
	assert.Equal(t, "Node", ref.Kind)
	assert.Equal(t, "n1", ref.Name)
	assert.Equal(t, types.UID("node-uid-1"), ref.UID,
		"a reference whose UID does not match the live node is treated as "+
			"dangling and the dependent is collected immediately")
	require.NotNil(t, ref.BlockOwnerDeletion)
	assert.False(t, *ref.BlockOwnerDeletion,
		"a gateway that cannot reach the API must not be able to hold a node in Terminating")
}

// Resources created before the owner reference existed have to pick
// it up, or they are never collected.
func TestEnsureExists_AdoptsAnUnownedResource(t *testing.T) {
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP},
	}
	require.Empty(t, getCR(t, p).OwnerReferences)
	require.NoError(t, p.EnsureExists(context.Background()))

	got := getCR(t, p)
	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, types.UID("node-uid-1"), got.OwnerReferences[0].UID)
}

// A node rebuilt under the same name gets a new UID; the stale
// reference would have the resource collected immediately.
func TestEnsureExists_ReplacesAStaleOwnerUID(t *testing.T) {
	stale := existingCR()
	stale.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "v1", Kind: "Node", Name: "n1", UID: "node-uid-0",
	}}
	p := &Publisher{
		Client:    newClient(t, stale).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP},
	}
	require.NoError(t, p.EnsureExists(context.Background()))

	got := getCR(t, p)
	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, types.UID("node-uid-1"), got.OwnerReferences[0].UID)
}

func TestEnsureExists_FailsWhenTheNodeIsGone(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	p := &Publisher{Client: c, NodeName: "n1", Providers: []fabrics.Provider{fabrics.ProviderTCP}}
	require.Error(t, p.EnsureExists(context.Background()))
}

// Each enumeration sweeps every provider libfabric was built with and
// warns about those it finds no device for, so the sweep is bounded
// independently of the status refresh.
func TestRefresh_ReusesTheProbeWithinTheProbePeriod(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderAny},
		Lister:      l,
		ProbePeriod: time.Hour,
	}

	for i := 0; i < 5; i++ {
		require.NoError(t, p.Refresh(context.Background()))
	}
	assert.Equal(t, 1, l.call, "five refreshes must sweep the fabric once")

	tcp := providerByName(getCR(t, p).Status.Providers, mxlv1alpha1.ProviderTCP)
	require.NotNil(t, tcp)
	assert.Equal(t, int32(1), tcp.DeviceCount,
		"a reused probe still has to publish what it found")
}

func TestRefresh_ReprobesOnceThePeriodElapses(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderAny},
		Lister:      l,
		ProbePeriod: time.Nanosecond,
	}

	require.NoError(t, p.Refresh(context.Background()))
	time.Sleep(time.Millisecond)
	require.NoError(t, p.Refresh(context.Background()))
	assert.Equal(t, 2, l.call)
}

func TestRefresh_ZeroProbePeriodSweepsEveryTime(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
	}
	require.NoError(t, p.Refresh(context.Background()))
	require.NoError(t, p.Refresh(context.Background()))
	assert.Equal(t, 2, l.call)
}

// A cached get starts an informer, which needs list and watch on
// nodes. The gateway's ClusterRole grants get alone.
func TestEnsureExists_ReadsTheNodeUncached(t *testing.T) {
	scheme := newScheme(t)
	apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testNode()).Build()
	cached := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlNodeCapabilities{}).
		Build()

	p := &Publisher{
		Client:    cached,
		APIReader: apiReader,
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderTCP},
	}
	require.NoError(t, p.EnsureExists(context.Background()))

	got := getCR(t, p)
	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, types.UID("node-uid-1"), got.OwnerReferences[0].UID)
}

// The owner reference gives the resource a real deletion path, and
// EnsureExists runs only at startup, so a refresh that cannot find it
// has to rebuild it or the node never advertises again.
func TestRefresh_RecreatesADeletedResource(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{iface(fabrics.ProviderTCP, "10.20.53.13", "eth0")}}
	p := &Publisher{
		Client:    newClient(t, existingCR()).Build(),
		NodeName:  "n1",
		Providers: []fabrics.Provider{fabrics.ProviderAny},
		Lister:    l,
	}
	require.NoError(t, p.Refresh(context.Background()))

	require.NoError(t, p.Client.Delete(context.Background(), &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
	}))

	require.NoError(t, p.Refresh(context.Background()))
	got := getCR(t, p)
	require.Len(t, got.OwnerReferences, 1)
	tcp := providerByName(got.Status.Providers, mxlv1alpha1.ProviderTCP)
	require.NotNil(t, tcp)
	assert.Equal(t, int32(1), tcp.DeviceCount)
}

// fakeHostDevices stands in for the host's RDMA device inventory.
type fakeHostDevices struct {
	out []string
	err error
}

func (f fakeHostDevices) ActiveDevices() ([]string, error) { return f.out, f.err }

// rdmaCondition returns the enumeration cross-check condition, or nil
// when the publisher left it unset.
func rdmaCondition(t *testing.T, p *Publisher) *metav1.Condition {
	t.Helper()
	got := getCR(t, p)
	return meta.FindStatusCondition(got.Status.Conditions,
		mxlv1alpha1.ConditionTypeRDMADevicesEnumerated)
}

// The defect this guards. libfabric builds a provider's device list
// once per process and offers no rebuild through the libmxl-fabrics
// API, so a gateway that enumerated before the host's RDMA devices
// were usable reports none of them for the rest of its life. The
// published entry is identical to the one a host with no RDMA
// hardware produces, and every mirror silently resolves to a
// non-RDMA provider.
func TestRefresh_ReportsHostDevicesMissingFromTheProbe(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderAny},
		Lister:      l,
		HostDevices: fakeHostDevices{out: []string{"dev0"}},
	}
	require.NoError(t, p.Refresh(context.Background()))

	cond := rdmaCondition(t, p)
	require.NotNil(t, cond, "a provider that measured nothing while the host "+
		"carries an active device is the whole condition")
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonHostDevicesUnenumerated, cond.Reason)
	assert.Contains(t, cond.Message, "dev0")
}

// A host with no RDMA hardware is represented by definition, and
// must not be reported as a discrepancy: the fix would otherwise turn
// a correct zero into a permanent fault on every node without RDMA.
func TestRefresh_HostWithoutRDMADevicesIsRepresented(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderAny},
		Lister:      l,
		HostDevices: fakeHostDevices{},
	}
	require.NoError(t, p.Refresh(context.Background()))

	cond := rdmaCondition(t, p)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonHostDevicesRepresented, cond.Reason)
}

// A probe that found the hardware is the healthy case.
func TestRefresh_EnumeratedRDMADeviceIsRepresented(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderVerbs, "10.20.53.13", "mlx5_0"),
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderAny},
		Lister:      l,
		HostDevices: fakeHostDevices{out: []string{"dev0"}},
	}
	require.NoError(t, p.Refresh(context.Background()))

	cond := rdmaCondition(t, p)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonHostDevicesRepresented, cond.Reason)
}

// A gateway configured for tcp alone advertises no RDMA provider by
// instruction rather than by measurement, so the host device list
// contradicts nothing and the condition does not apply.
func TestRefresh_TCPOnlyBoundLeavesTheConditionUnset(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderTCP},
		Lister:      l,
		HostDevices: fakeHostDevices{out: []string{"dev0"}},
	}
	require.NoError(t, p.Refresh(context.Background()))

	assert.Nil(t, rdmaCondition(t, p))
}

// Without a host device list there is nothing to compare against, and
// claiming either answer would be a guess.
func TestRefresh_NoHostDeviceListLeavesTheConditionUnset(t *testing.T) {
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

	assert.Nil(t, rdmaCondition(t, p))
}

// The providers stand on their own measurement when only the
// cross-check fails, so the condition goes Unknown rather than False
// and the probe result is published either way.
func TestRefresh_UnreadableHostDeviceListIsUnknown(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "10.20.53.13", "eth0"),
	}}
	p := &Publisher{
		Client:      newClient(t, existingCR()).Build(),
		NodeName:    "n1",
		Providers:   []fabrics.Provider{fabrics.ProviderAny},
		Lister:      l,
		HostDevices: fakeHostDevices{err: errors.New("boom")},
	}
	require.NoError(t, p.Refresh(context.Background()))

	cond := rdmaCondition(t, p)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionUnknown, cond.Status)
	assert.Equal(t, mxlv1alpha1.ReasonHostDevicesUnreadable, cond.Reason)

	got := getCR(t, p)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions,
		mxlv1alpha1.ConditionTypeProbed), "the probe itself still succeeded")
}

// recordingObserver captures what the publisher hands its host-device
// observer, which is the seam a health gate hangs on.
type recordingObserver struct {
	conds   []metav1.Condition
	applies []bool
}

func (r *recordingObserver) Observe(c metav1.Condition, applies bool) {
	r.conds = append(r.conds, c)
	r.applies = append(r.applies, applies)
}

// The gate cannot read the resource it would be judging, so the
// publisher has to hand it every comparison it makes.
func TestRefresh_HandsTheComparisonToTheObserver(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "198.51.100.7", "eth0"),
	}}
	obs := &recordingObserver{}
	p := &Publisher{
		Client:             newClient(t, existingCR()).Build(),
		NodeName:           "n1",
		Providers:          []fabrics.Provider{fabrics.ProviderAny},
		Lister:             l,
		HostDevices:        fakeHostDevices{out: []string{"dev0"}},
		HostDeviceObserver: obs,
	}
	require.NoError(t, p.Refresh(context.Background()))

	require.Len(t, obs.conds, 1)
	assert.True(t, obs.applies[0])
	assert.Equal(t, mxlv1alpha1.ReasonHostDevicesUnenumerated, obs.conds[0].Reason)
}

// A comparison that stops applying has to reach the observer too. Left
// unsaid, a gateway that loses its host device list would keep whatever
// verdict it was last given and restart on a comparison nothing is
// making any more.
func TestRefresh_TellsTheObserverWhenNoComparisonApplies(t *testing.T) {
	l := &fakeLister{out: []fabrics.InterfaceConfig{
		iface(fabrics.ProviderTCP, "198.51.100.7", "eth0"),
	}}
	obs := &recordingObserver{}
	p := &Publisher{
		Client:             newClient(t, existingCR()).Build(),
		NodeName:           "n1",
		Providers:          []fabrics.Provider{fabrics.ProviderAny},
		Lister:             l,
		HostDeviceObserver: obs,
	}
	require.NoError(t, p.Refresh(context.Background()))

	require.Len(t, obs.applies, 1)
	assert.False(t, obs.applies[0], "no host device list means no comparison ran")
}
