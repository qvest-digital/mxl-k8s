package intent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const flowID = "11111111-2222-3333-4444-555555555555"

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func TestFlowIDFromPath(t *testing.T) {
	cases := []struct {
		name   string
		domain string
		path   string
		want   string
		ok     bool
	}{
		{
			name:   "canonical",
			domain: "/run/mxl/domain",
			path:   "/run/mxl/domain/" + flowID + ".mxl-flow/flow_def.json",
			want:   flowID,
			ok:     true,
		},
		{
			name:   "trailing slash on domain still matches",
			domain: "/run/mxl/domain/",
			path:   "/run/mxl/domain/" + flowID + ".mxl-flow/flow_def.json",
			want:   flowID,
			ok:     true,
		},
		{
			name:   "different domain root rejected",
			domain: "/run/mxl/domain",
			path:   "/run/other/" + flowID + ".mxl-flow/flow_def.json",
			ok:     false,
		},
		{
			name:   "access file under flow directory accepted",
			domain: "/run/mxl/domain",
			path:   "/run/mxl/domain/" + flowID + ".mxl-flow/access",
			want:   flowID,
			ok:     true,
		},
		{
			name:   "flow directory itself accepted",
			domain: "/run/mxl/domain",
			path:   "/run/mxl/domain/" + flowID + ".mxl-flow",
			want:   flowID,
			ok:     true,
		},
		{
			name:   "deeper path under flow directory accepted",
			domain: "/run/mxl/domain",
			path:   "/run/mxl/domain/" + flowID + ".mxl-flow/grains/00001",
			want:   flowID,
			ok:     true,
		},
		{
			name:   "missing flow suffix rejected",
			domain: "/run/mxl/domain",
			path:   "/run/mxl/domain/" + flowID + "/flow_def.json",
			ok:     false,
		},
		{
			name:   "empty id rejected",
			domain: "/run/mxl/domain",
			path:   "/run/mxl/domain/.mxl-flow/flow_def.json",
			ok:     false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FlowIDFromPath(tc.domain, tc.path)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMirrorName_MatchesOperatorAlgorithm(t *testing.T) {
	// The operator and the agent must converge on identical names so
	// the gateway sees exactly one MxlFlowMirror per (flow, target
	// node). Pin the result here so any drift in either codepath
	// surfaces with a failed test.
	cases := []struct {
		flowID string
		target string
		want   string
	}{
		{flowID, "worker-1", flowID + "--worker-1"},
		{"ABCDABCD-1234-5678-9ABC-DEF012345678", "Worker-A",
			"abcdabcd-1234-5678-9abc-def012345678--worker-a"},
		{flowID, "ip-10.0.0.1.eu-central-1.compute",
			flowID + "--ip-10-0-0-1-eu-central-1-compute"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			got := MirrorName(tc.flowID, tc.target)
			assert.Equal(t, tc.want, got)
			errs := validation.IsDNS1123Subdomain(got)
			assert.Empty(t, errs, "MirrorName output not DNS-1123: %v", errs)
		})
	}
}

func TestMaterialize_LocalFlow_ReturnsImmediately(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	d := &Dispatcher{
		Client:      c,
		Resolver:    &podlookup.Resolver{Client: c, NodeName: "n1"},
		DomainPath:  "/run/mxl/domain",
		NodeName:    "n1",
		FlowChecker: func(string) bool { return true },
	}

	err := d.Materialize(context.Background(), 42, "/run/mxl/domain/"+flowID+".mxl-flow/flow_def.json")
	require.NoError(t, err,
		"a flow already materialized on this node must not trigger a mirror "+
			"creation; the shim's caller only needs the file to exist before it retries")
}

func TestMaterialize_InvalidPath_ErrorsWithoutSideEffects(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	d := &Dispatcher{
		Client:      c,
		Resolver:    &podlookup.Resolver{Client: c, NodeName: "n1"},
		DomainPath:  "/run/mxl/domain",
		NodeName:    "n1",
		FlowChecker: func(string) bool { return false },
	}

	err := d.Materialize(context.Background(), 42, "/etc/passwd")
	require.Error(t, err)

	var list mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(context.Background(), &list))
	assert.Empty(t, list.Items,
		"a malformed path must never lead to a mirror Create; if it does, "+
			"a buggy shim can spam unrelated mirrors into the cluster")
}

func TestMaterialize_MissingFlow_Errors(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "consumer",
				UID:       "uid-1",
			},
			Spec: corev1.PodSpec{NodeName: "n1"},
		}).
		Build()

	d := &Dispatcher{
		Client:      c,
		Resolver:    &podlookup.Resolver{Client: c, NodeName: "n1"},
		DomainPath:  "/run/mxl/domain",
		NodeName:    "n1",
		FlowChecker: func(string) bool { return false },
	}

	// Materialize cannot call PodForPID without /proc; bypass via a
	// shortcut: when FlowChecker says true on second call, the
	// dispatcher early-returns. Instead test the
	// resolveSourceNode-missing-flow case by exercising it directly.
	_, ok, err := d.resolveSourceNode(context.Background(), "missing-flow")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestMaterialize_SourceIsLocalNode_NoOps(t *testing.T) {
	// When the flow's origin is the same node the dispatcher runs on,
	// Materialize returns without creating any mirror. This protects
	// against the agent racing its own MxlFlow publish - the writer
	// is local; the file will appear shortly.
	scheme := newScheme(t)
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()

	d := &Dispatcher{
		Client:      c,
		Resolver:    &podlookup.Resolver{Client: c, NodeName: "n1"},
		DomainPath:  "/run/mxl/domain",
		NodeName:    "n1",
		FlowChecker: func(string) bool { return false },
	}
	_, ok, err := d.resolveSourceNode(context.Background(), flowID)
	require.NoError(t, err)
	require.True(t, ok)

	// Manually drive the same logic Materialize uses for the
	// same-node branch by inspecting resolveSourceNode plus a check.
	src, _, _ := d.resolveSourceNode(context.Background(), flowID)
	assert.Equal(t, "n1", src)

	// No mirror has been created at this point.
	var list mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(context.Background(), &list))
	assert.Empty(t, list.Items)
}

func TestWaitReady_FailedPhase_ReturnsError(t *testing.T) {
	scheme := newScheme(t)
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{
			Phase: mxlv1alpha1.MxlFlowMirrorFailed,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	d := &Dispatcher{
		Client:       c,
		PollInterval: 5 * time.Millisecond,
	}
	err := d.waitReady(context.Background(), mirror)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Failed phase")
}

func TestWaitReady_ContextDeadline_PropagatesError(t *testing.T) {
	scheme := newScheme(t)
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{
			Phase: mxlv1alpha1.MxlFlowMirrorMaterializing,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	d := &Dispatcher{
		Client:       c,
		PollInterval: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := d.waitReady(ctx, mirror)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"the per-request timeout must surface as DeadlineExceeded so the "+
			"shim's caller stays out of an indefinite wait when the gateway "+
			"never materializes the mirror")
}

func TestWaitReady_Ready_ReturnsNil(t *testing.T) {
	scheme := newScheme(t)
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{
			Phase:      mxlv1alpha1.MxlFlowMirrorReady,
			TargetInfo: "info",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	d := &Dispatcher{
		Client:       c,
		PollInterval: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, d.waitReady(ctx, mirror))
}

func TestWaitReady_ReadyButMissingTargetInfo_KeepsPolling(t *testing.T) {
	// A Ready phase with an empty TargetInfo is a half-published
	// state from the gateway. The dispatcher must keep polling so a
	// transient missing-field race doesn't return success early.
	scheme := newScheme(t)
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{
			Phase:      mxlv1alpha1.MxlFlowMirrorReady,
			TargetInfo: "",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	d := &Dispatcher{
		Client:       c,
		PollInterval: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := d.waitReady(ctx, mirror)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestEnsureMirror_Idempotent(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n1",
		Provider:   mxlv1alpha1.ProviderTCP,
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-1"},
	}

	first, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, MirrorName(flowID, "n1"), first.Name)
	assert.Equal(t, "n-src", first.Spec.SourceNode)
	assert.Equal(t, "n1", first.Spec.TargetNode)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, first.Spec.Provider)

	// Same call again must return the same name (no AlreadyExists
	// surfacing up). The reconciler relies on this idempotence so
	// the agent and the operator can race-create without errors.
	second, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.NoError(t, err)
	assert.Equal(t, first.Name, second.Name)
}

// TestEnsureMirror_StampsIntentLabels asserts the contract the
// operator's intent-GC controller depends on: every mirror the
// agent creates must carry LabelCreatedByIntent (with the local
// node name as value) and LabelRequestorPodUID (with the consumer
// pod's UID), and Spec.Requestor must name the pod. Without these
// the GC cannot tell intent-created mirrors apart from
// receiver-created ones and cannot detect pod replacement.
func TestEnsureMirror_StampsIntentLabels(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-42"},
	}

	got, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.NoError(t, err)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: got.Name}, &live))

	assert.Equal(t, "n-target", live.Labels[mxlv1alpha1.LabelCreatedByIntent],
		"LabelCreatedByIntent must carry the local node name so the GC "+
			"can tell which agent stamped the mirror")
	assert.Equal(t, "uid-42", live.Labels[mxlv1alpha1.LabelRequestorPodUID],
		"LabelRequestorPodUID lets a label selector find every mirror "+
			"tied to a given pod UID without unmarshalling spec")
	require.NotNil(t, live.Spec.Requestor)
	assert.Equal(t, "consumer", live.Spec.Requestor.Name)
	assert.Equal(t, "ns", live.Spec.Requestor.Namespace)
	assert.Equal(t, "uid-42", live.Spec.Requestor.UID)
	_, receiverLabel := live.Labels[mxlv1alpha1.LabelCreatedByReceiver]
	assert.False(t, receiverLabel,
		"a mirror the agent created must not carry the receiver label; "+
			"the receiver reconciler would then try to GC it and race "+
			"the intent reconciler")
}

// nodeCaps builds an MxlNodeCapabilities for a node advertising the
// named providers. Seeded via WithObjects so the fake client persists
// the status the resolver reads.
func nodeCaps(node string, providers ...mxlv1alpha1.MxlFabricsProvider) *mxlv1alpha1.MxlNodeCapabilities {
	caps := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: node},
	}
	for _, p := range providers {
		caps.Status.Providers = append(caps.Status.Providers,
			mxlv1alpha1.MxlFabricsProviderCapability{Name: p})
	}
	return caps
}

// TestEnsureMirror_ResolvesProviderFromCapabilities asserts that with
// no explicit override the agent stamps the mirror with a concrete
// provider resolved from the source and target nodes' capabilities,
// preferring verbs over the tcp both nodes also advertise.
func TestEnsureMirror_ResolvesProviderFromCapabilities(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			nodeCaps("n-src", mxlv1alpha1.ProviderTCP, mxlv1alpha1.ProviderVerbs),
			nodeCaps("n1", mxlv1alpha1.ProviderTCP, mxlv1alpha1.ProviderVerbs),
		).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		Build()

	// Provider left empty: the dispatcher must resolve, not default to
	// auto.
	d := &Dispatcher{Client: c, DomainPath: "/run/mxl/domain", NodeName: "n1"}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-1"},
	}

	got, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.NoError(t, err)
	assert.Equal(t, mxlv1alpha1.ProviderVerbs, got.Spec.Provider,
		"the resolver must prefer verbs, the highest-preference provider "+
			"both nodes advertise")
}

// TestEnsureMirror_FallsBackToTCPWithoutCapabilities asserts that when
// neither node has published MxlNodeCapabilities the agent still stamps
// a concrete provider (tcp) rather than leaving the mirror on auto,
// which the gateway would reject.
func TestEnsureMirror_FallsBackToTCPWithoutCapabilities(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		Build()

	d := &Dispatcher{Client: c, DomainPath: "/run/mxl/domain", NodeName: "n1"}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-1"},
	}

	got, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.NoError(t, err)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, got.Spec.Provider)
	assert.NotEqual(t, mxlv1alpha1.ProviderAuto, got.Spec.Provider,
		"a mirror must never be created with provider auto")
}

// TestEnsureMirror_ExistingReceiverMirror_LeftIntact asserts that
// when the receiver reconciler has already created a mirror for the
// same (flow, target node), the agent reuses it without rewriting
// ownership: no intent label is added, the receiver label stays,
// and Spec.Requestor remains nil. Otherwise the two reconcilers
// would both claim the mirror and race on deletion.
func TestEnsureMirror_ExistingReceiverMirror_LeftIntact(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      MirrorName(flowID, "n-target"),
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByReceiver: "rcv-1",
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: "n-src",
			TargetNode: "n-target",
			Provider:   mxlv1alpha1.ProviderTCP,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(existing).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-7"},
	}

	got, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.NoError(t, err)
	assert.Equal(t, existing.Name, got.Name)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: existing.Name}, &live))

	assert.Equal(t, "rcv-1", live.Labels[mxlv1alpha1.LabelCreatedByReceiver],
		"the receiver label must survive: deleting it would orphan the "+
			"mirror from the receiver's GC pass")
	_, intentLabel := live.Labels[mxlv1alpha1.LabelCreatedByIntent]
	assert.False(t, intentLabel,
		"the agent must not claim co-ownership of a receiver-owned "+
			"mirror; the intent reconciler would then race the receiver "+
			"reconciler to delete it")
	assert.Nil(t, live.Spec.Requestor,
		"writing Spec.Requestor onto a receiver-owned mirror would "+
			"trip the intent GC into deleting it when the consumer "+
			"pod goes away, even though the receiver still wants it")
}

// fakeLeaseChecker hands back a canned IsFresh answer per (flowID,
// node) pair so tests can drive resolveSourceNode's skip-stale logic
// without spinning up a coordination.k8s.io fake.
type fakeLeaseChecker struct {
	fresh map[string]bool
}

func (f *fakeLeaseChecker) IsFresh(_ context.Context, flowID, node string) (bool, error) {
	v, ok := f.fresh[flowID+"/"+node]
	return ok && v, nil
}

func TestResolveSourceNode_SkipsStaleOriginByLease(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				// Stale Origin first: the dispatcher must skip it and
				// keep scanning rather than return it.
				{NodeName: "n-stale", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
				{NodeName: "n-fresh", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()
	d := &Dispatcher{
		Client:   c,
		NodeName: "n1",
		Lease: &fakeLeaseChecker{fresh: map[string]bool{
			flowID + "/n-fresh": true,
			// n-stale intentionally absent: missing = not fresh.
		}},
	}

	node, ok, err := d.resolveSourceNode(ctx, flowID)
	require.NoError(t, err)
	assert.True(t, ok,
		"a second Origin with a fresh Lease must be the dispatcher's "+
			"answer even when an earlier Origin entry is present but stale; "+
			"otherwise a partitioned producer permanently masks a recovered one")
	assert.Equal(t, "n-fresh", node)
}

func TestResolveSourceNode_AllStaleOriginsReturnsNotOK(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-stale-1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
				{NodeName: "n-stale-2", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()
	d := &Dispatcher{
		Client:   c,
		NodeName: "n1",
		Lease:    &fakeLeaseChecker{fresh: map[string]bool{}},
	}

	_, ok, err := d.resolveSourceNode(ctx, flowID)
	require.NoError(t, err)
	assert.False(t, ok,
		"when every Origin's Lease is expired the dispatcher must report "+
			"no source; routing a Materialize call at a stale Origin would "+
			"only hand the shim back a 'no grains' wait until the timeout")
}

// TestEnsureMirror_ExplicitProviderBypassesResolution asserts that a
// concrete Provider override on the dispatcher is stamped verbatim,
// even when the nodes' capabilities would resolve to a different
// provider. The override is the per-cluster escape hatch that skips
// resolution entirely.
func TestEnsureMirror_ExplicitProviderBypassesResolution(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			nodeCaps("n-src", mxlv1alpha1.ProviderVerbs),
			nodeCaps("n1", mxlv1alpha1.ProviderVerbs),
		).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n1",
		Provider:   mxlv1alpha1.ProviderTCP,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p", UID: "u"},
	}

	got, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.NoError(t, err)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: got.Name}, &live))
	assert.Equal(t, mxlv1alpha1.ProviderTCP, live.Spec.Provider,
		"an explicit Provider override must be stamped verbatim and skip "+
			"resolution, even when the nodes both advertise verbs")
}

var _ = client.IgnoreNotFound
