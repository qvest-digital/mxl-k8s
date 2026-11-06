package placement

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

type harness struct {
	t   *testing.T
	ctx context.Context
	c   client.Client
	ns  string
}

// start boots an API server and runs the placement controller against
// it in a manager.
func start(t *testing.T, r *Reconciler) *harness {
	t.Helper()
	env, err := testutil.Start()
	if err != nil {
		t.Skipf("envtest unavailable: %v", err)
	}
	t.Cleanup(env.Stop)
	skip := true
	mgr, err := ctrl.NewManager(env.Cfg, manager.Options{
		Scheme:                 env.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: &skip},
	})
	require.NoError(t, err)
	r.Client = mgr.GetClient()
	require.NoError(t, r.SetupWithManager(mgr))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return &harness{t: t, ctx: context.Background(), c: env.Client, ns: env.NewNamespace(t)}
}

func defaultReconciler() *Reconciler {
	return &Reconciler{Settings: defaults, OverheadPercent: 0, DemandWait: time.Second}
}

// caps publishes a node's interfaces as its gateway would.
func (h *harness) caps(name string, ifs ...mxlv1alpha1.MxlFabricInterface) {
	h.t.Helper()
	obj := &mxlv1alpha1.MxlNodeCapabilities{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: name}}
	err := h.c.Get(h.ctx, client.ObjectKeyFromObject(obj), obj)
	if err != nil {
		require.NoError(h.t, h.c.Create(h.ctx, obj))
	}
	obj.Status.Providers = []mxlv1alpha1.MxlFabricsProviderCapability{{Name: tcp, DeviceCount: int32(len(ifs)), Interfaces: ifs}}
	require.NoError(h.t, h.c.Status().Update(h.ctx, obj))
}

func flowID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

// flow creates an MxlFlow publishing bits per second; zero publishes no
// rate.
func (h *harness) flow(n int, bits int64) {
	h.t.Helper()
	f := &mxlv1alpha1.MxlFlow{ObjectMeta: metav1.ObjectMeta{Name: flowID(n)},
		Spec: mxlv1alpha1.MxlFlowSpec{ID: flowID(n), Definition: runtime.RawExtension{Raw: []byte(`{}`)}}}
	require.NoError(h.t, h.c.Create(h.ctx, f))
	if bits > 0 {
		h.rate(n, bits)
	}
}

func (h *harness) rate(n int, bits int64) {
	h.t.Helper()
	var f mxlv1alpha1.MxlFlow
	require.NoError(h.t, h.c.Get(h.ctx, client.ObjectKey{Name: flowID(n)}, &f))
	f.Status.BitsPerSecond = bits
	require.NoError(h.t, h.c.Status().Update(h.ctx, &f))
}

func (h *harness) mirror(n int, src, tgt string, mutate ...func(*mxlv1alpha1.MxlFlowMirror)) string {
	h.t.Helper()
	m := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: h.ns, Name: mxlv1alpha1.MirrorNameFor(mxlv1alpha1.FlowRef{ID: flowID(n)}, tgt)},
		Spec:       mxlv1alpha1.MxlFlowMirrorSpec{FlowID: flowID(n), SourceNode: src, TargetNode: tgt, Provider: tcp},
	}
	for _, f := range mutate {
		f(m)
	}
	require.NoError(h.t, h.c.Create(h.ctx, m))
	return m.Name
}

func (h *harness) get(name string) *mxlv1alpha1.MxlFlowMirror {
	h.t.Helper()
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(h.t, h.c.Get(h.ctx, client.ObjectKey{Namespace: h.ns, Name: name}, &m))
	return &m
}

// placed waits until the mirror's Placed condition carries reason and
// returns the mirror.
func (h *harness) placed(name, reason string) *mxlv1alpha1.MxlFlowMirror {
	h.t.Helper()
	var m *mxlv1alpha1.MxlFlowMirror
	require.Eventually(h.t, func() bool {
		m = h.get(name)
		c := meta.FindStatusCondition(m.Status.Conditions, mxlv1alpha1.ConditionTypePlaced)
		return c != nil && c.Reason == reason
	}, 15*time.Second, 50*time.Millisecond, "mirror %s never reported Placed=%s", name, reason)
	return m
}

func twoRails(prefix int) []mxlv1alpha1.MxlFabricInterface {
	return []mxlv1alpha1.MxlFabricInterface{
		iface(fmt.Sprintf("192.0.2.%d", prefix), railA, 10*gbit),
		iface(fmt.Sprintf("192.0.2.%d", 128+prefix), railB, 10*gbit),
	}
}

func TestControllerSpreadsMirrorsOverPorts(t *testing.T) {
	h := start(t, defaultReconciler())
	h.caps("src", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, 3*gbit)
	h.flow(2, 3*gbit)

	first := h.placed(h.mirror(1, "src", "tgt"), mxlv1alpha1.ReasonPlaced)
	require.Equal(t, int64(3*gbit), first.Status.DemandBitsPerSecond)
	second := h.placed(h.mirror(2, "src", "tgt"), mxlv1alpha1.ReasonPlaced)

	require.NotNil(t, first.Spec.Placement)
	require.NotNil(t, second.Spec.Placement)
	require.NotEqual(t, first.Spec.Placement.SourceInterface, second.Spec.Placement.SourceInterface,
		"the second mirror takes the idle rail")
	require.NotEqual(t, first.Spec.Placement.TargetInterface, second.Spec.Placement.TargetInterface)
}

func TestControllerReportsOvercommit(t *testing.T) {
	h := start(t, defaultReconciler())
	h.caps("src", iface("192.0.2.1", railA, 10*gbit))
	h.caps("tgt", iface("192.0.2.2", railA, 10*gbit))
	h.flow(1, 6*gbit)
	h.flow(2, 6*gbit)

	h.placed(h.mirror(1, "src", "tgt"), mxlv1alpha1.ReasonPlaced)
	m := h.placed(h.mirror(2, "src", "tgt"), mxlv1alpha1.ReasonOvercommitted)
	require.Equal(t, &mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "192.0.2.1", TargetInterface: "192.0.2.2"}, m.Spec.Placement,
		"place policy keeps a mirror moving on the only pair there is")
}

func TestControllerHoldPolicy(t *testing.T) {
	r := defaultReconciler()
	r.Settings.Overcommit = OvercommitHold
	h := start(t, r)
	h.caps("src", iface("192.0.2.1", railA, 10*gbit))
	h.caps("tgt", iface("192.0.2.2", railA, 10*gbit))
	h.flow(1, 9*gbit)

	m := h.placed(h.mirror(1, "src", "tgt"), mxlv1alpha1.ReasonHeld)
	require.Nil(t, m.Spec.Placement)
}

func TestControllerWaitsForDemandThenPlacesProvisionally(t *testing.T) {
	// The wait runs from CreationTimestamp, which has whole seconds, so
	// a one-second wait can be over before the first reconcile sees the
	// mirror. Five seconds leaves at least four for AwaitingDemand to be
	// observed and still places well inside placed()'s 15 s.
	r := defaultReconciler()
	r.DemandWait = 5 * time.Second
	h := start(t, r)
	h.caps("src", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, 0)

	name := h.mirror(1, "src", "tgt")
	h.placed(name, mxlv1alpha1.ReasonAwaitingDemand)
	m := h.placed(name, mxlv1alpha1.ReasonPlaced)
	require.Zero(t, m.Status.DemandBitsPerSecond, "provisional demand defaults to zero")

	// The rate arriving later is charged; the placement does not move.
	before := *m.Spec.Placement
	h.rate(1, 4*gbit)
	require.Eventually(t, func() bool { return h.get(name).Status.DemandBitsPerSecond == 4*gbit }, 15*time.Second, 50*time.Millisecond)
	require.Equal(t, before, *h.get(name).Spec.Placement)
}

func TestControllerReplacesOnPortLoss(t *testing.T) {
	h := start(t, defaultReconciler())
	h.caps("src", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, gbit)

	name := h.mirror(1, "src", "tgt")
	m := h.placed(name, mxlv1alpha1.ReasonPlaced)
	require.Equal(t, "192.0.2.2", m.Spec.Placement.TargetInterface)

	down := twoRails(2)
	down[0].LinkState = "down"
	h.caps("tgt", down...)
	require.Eventually(t, func() bool {
		p := h.get(name).Spec.Placement
		return p != nil && p.TargetInterface == "192.0.2.130" && p.SourceInterface == "192.0.2.129"
	}, 15*time.Second, 50*time.Millisecond, "a mirror whose target port went down moves to the rail still up")
}

func TestControllerKeepsPlacementWhenLoadChanges(t *testing.T) {
	h := start(t, defaultReconciler())
	h.caps("src", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, gbit)
	h.flow(2, gbit)
	a := h.placed(h.mirror(1, "src", "tgt"), mxlv1alpha1.ReasonPlaced)
	b := h.placed(h.mirror(2, "src", "tgt"), mxlv1alpha1.ReasonPlaced)

	// Mirror 1 grows past what its rail can take with room left on
	// the other: it is reported, not moved.
	h.rate(1, 20*gbit)
	m := h.placed(a.Name, mxlv1alpha1.ReasonOvercommitted)
	require.Equal(t, *a.Spec.Placement, *m.Spec.Placement)
	require.Equal(t, *b.Spec.Placement, *h.get(b.Name).Spec.Placement)
}

func TestControllerReplacesClearedPlacement(t *testing.T) {
	h := start(t, defaultReconciler())
	h.caps("src", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, gbit)
	name := h.mirror(1, "src", "tgt", func(m *mxlv1alpha1.MxlFlowMirror) {
		m.Spec.Placement = &mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "192.0.2.129", TargetInterface: "192.0.2.130"}
	})
	m := h.placed(name, mxlv1alpha1.ReasonPlaced)
	require.Equal(t, "192.0.2.129", m.Spec.Placement.SourceInterface, "a placement written by hand is kept")

	patch := client.MergeFrom(m.DeepCopy())
	m.Spec.Placement = nil
	require.NoError(t, h.c.Patch(h.ctx, m, patch))
	require.Eventually(t, func() bool {
		p := h.get(name).Spec.Placement
		return p != nil && p.SourceInterface == "192.0.2.1"
	}, 15*time.Second, 50*time.Millisecond, "a cleared placement is chosen again")
}

func TestControllerAdoptsBoundInterfaces(t *testing.T) {
	h := start(t, defaultReconciler())
	h.caps("src", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, gbit)

	// A mirror from before placement, bound by its gateways on rail B
	// but not yet charged: create it with a provider placement cannot
	// take yet, record what the gateways bound, then let it in.
	name := h.mirror(1, "src", "tgt", func(m *mxlv1alpha1.MxlFlowMirror) { m.Spec.Provider = mxlv1alpha1.ProviderAuto })
	m := h.get(name)
	m.Status.SourceInterface, m.Status.TargetInterface = "192.0.2.129", "192.0.2.130"
	require.NoError(t, h.c.Status().Update(h.ctx, m))
	patch := client.MergeFrom(m.DeepCopy())
	m.Spec.Provider = tcp
	require.NoError(t, h.c.Patch(h.ctx, m, patch))

	m = h.placed(name, mxlv1alpha1.ReasonPlaced)
	require.Equal(t, mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "192.0.2.129", TargetInterface: "192.0.2.130"}, *m.Spec.Placement,
		"the interfaces a running mirror is bound to are kept rather than moved")
}

func TestControllerNoInterface(t *testing.T) {
	h := start(t, defaultReconciler())
	h.caps("src", twoRails(1)...)
	h.flow(1, gbit)
	name := h.mirror(1, "src", "tgt")
	h.placed(name, mxlv1alpha1.ReasonNoInterface)

	h.caps("tgt", twoRails(2)...)
	h.placed(name, mxlv1alpha1.ReasonPlaced)
}

func TestControllerRedundancyMembersDisjoint(t *testing.T) {
	r := defaultReconciler()
	r.RedundancySets = true
	h := start(t, r)
	h.caps("a", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, gbit)
	h.flow(2, gbit)
	h.flow(3, gbit)
	set := &mxlv1alpha1.MxlFlowRedundancySet{
		ObjectMeta: metav1.ObjectMeta{Name: "pair"},
		Spec: mxlv1alpha1.MxlFlowRedundancySetSpec{
			Members: []mxlv1alpha1.RedundancyFlowRef{{FlowID: flowID(1)}, {FlowID: flowID(2)}},
			Output:  mxlv1alpha1.RedundancyFlowRef{FlowID: flowID(9)},
		},
	}
	require.NoError(t, h.c.Create(h.ctx, set))

	// An unrelated mirror loads rail B first, so plain least-load
	// placement would put both members on rail A.
	h.placed(h.mirror(3, "a", "tgt", func(m *mxlv1alpha1.MxlFlowMirror) {
		m.Spec.Placement = &mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "192.0.2.129", TargetInterface: "192.0.2.130"}
	}), mxlv1alpha1.ReasonPlaced)
	one := h.placed(h.mirror(1, "a", "tgt"), mxlv1alpha1.ReasonPlaced)
	two := h.placed(h.mirror(2, "a", "tgt"), mxlv1alpha1.ReasonPlaced)
	require.NotEqual(t, one.Spec.Placement.SourceInterface, two.Spec.Placement.SourceInterface)
	require.NotEqual(t, one.Spec.Placement.TargetInterface, two.Spec.Placement.TargetInterface)
}

// A mirror that waited for its flow's rate is not a mirror from before
// placement: the gateways that bound it in the meantime gave up waiting
// for a placement and took the first interface. Adopting what they bound
// would leave every such mirror on that interface, reported as Placed.
func TestControllerDoesNotAdoptAMirrorBoundWhileAwaitingDemand(t *testing.T) {
	r := defaultReconciler()
	r.DemandWait = 4 * time.Second
	h := start(t, r)
	h.caps("src", twoRails(1)...)
	h.caps("tgt", twoRails(2)...)
	h.flow(1, 0)
	// Rail A carries a mirror already, so placement puts this one on
	// rail B; the gateways bound rail A, the first interface.
	h.flow(2, 5*gbit)
	h.placed(h.mirror(2, "src", "tgt"), mxlv1alpha1.ReasonPlaced)

	name := h.mirror(1, "src", "tgt")
	m := h.placed(name, mxlv1alpha1.ReasonAwaitingDemand)
	m.Status.SourceInterface, m.Status.TargetInterface = "192.0.2.1", "192.0.2.2"
	require.NoError(t, h.c.Status().Update(h.ctx, m))

	m = h.placed(name, mxlv1alpha1.ReasonPlaced)
	require.Equal(t, mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "192.0.2.129", TargetInterface: "192.0.2.130"}, *m.Spec.Placement,
		"placed on the idle rail, not left where the gateways first bound it")
}
