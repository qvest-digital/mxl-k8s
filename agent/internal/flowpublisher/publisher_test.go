package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const validFlowID = "11111111-2222-3333-4444-555555555555"

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func TestFlowIDFromDirName(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		want   string
		wantOK bool
	}{
		{"canonical", validFlowID + ".mxl-flow", validFlowID, true},
		{"empty id but suffix", ".mxl-flow", "", false},
		{"missing suffix", validFlowID, "", false},
		{"wrong suffix", validFlowID + ".mxl-flowx", "", false},
		{"empty", "", "", false},
		{"only suffix-looking", "abc.mxl-flow", "abc", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FlowIDFromDirName(tc.in)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPublishAppeared_CreatesMxlFlowAndLocationOnFirstObservation(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`","grain_size":1234}`),
		0o644))

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		Build()

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	assert.Equal(t, validFlowID, got.Spec.ID)
	assert.JSONEq(t, `{"id":"`+validFlowID+`","grain_size":1234}`, string(got.Spec.Definition.Raw))

	require.Len(t, got.Status.Locations, 1)
	assert.Equal(t, "n1", got.Status.Locations[0].NodeName)
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, got.Status.Locations[0].Phase)
	assert.NotNil(t, got.Status.Locations[0].LastObserved,
		"the agent stamps observed time so stale-detection downstream "+
			"has a reference; missing this turns Stale into a permanent state")
}

func TestPublishAppeared_IsIdempotent_DoesNotOverwriteExistingSpec(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`"}`),
		0o644))

	// Pre-existing MxlFlow with a richer spec - simulates the case
	// where another writer (operator-driven, or a different agent
	// that watched first) created the CR.
	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec: mxlv1alpha1.MxlFlowSpec{
			ID:         validFlowID,
			Definition: runtime.RawExtension{Raw: []byte(`{"id":"` + validFlowID + `","richer":true}`)},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		Build()

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	assert.Equal(t,
		`{"id":"`+validFlowID+`","richer":true}`,
		string(got.Spec.Definition.Raw),
		"the agent must not clobber a richer definition the operator "+
			"or another writer set; only the per-node location is the "+
			"agent's to own")
	require.Len(t, got.Status.Locations, 1)
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, got.Status.Locations[0].Phase)
}

func TestPublishAppeared_RejectsInvalidJSON(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`not-json-at-all`),
		0o644))

	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}

	err := p.PublishAppeared(context.Background(), validFlowID+".mxl-flow")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not valid JSON")
}

func TestPublishAppeared_MissingDefFileReturnsError(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	// Directory exists but no flow_def.json inside.
	require.NoError(t, os.Mkdir(filepath.Join(domain, validFlowID+".mxl-flow"), 0o755))

	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}

	err := p.PublishAppeared(context.Background(), validFlowID+".mxl-flow")
	require.Error(t, err)
}

func TestPublishAppeared_NonFlowEntryIsIgnoredQuietly(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{Client: c, DomainPath: t.TempDir(), NodeName: "n1"}

	require.NoError(t, p.PublishAppeared(context.Background(), "not-a-flow.txt"))
	// No CR should have been created.
	var list mxlv1alpha1.MxlFlowList
	require.NoError(t, c.List(context.Background(), &list))
	assert.Empty(t, list.Items)
}

func TestPublishVanished_MarksLocationStale(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
				{NodeName: "other", Phase: mxlv1alpha1.MxlFlowLocationReady},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		Build()
	p := &Publisher{Client: c, DomainPath: "/tmp", NodeName: "n1"}

	require.NoError(t, p.PublishVanished(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	require.Len(t, got.Status.Locations, 2)

	byNode := map[string]mxlv1alpha1.MxlFlowLocationPhase{}
	for _, l := range got.Status.Locations {
		byNode[l.NodeName] = l.Phase
	}
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, byNode["n1"],
		"vanishing on this node flips this node's phase to Stale only; "+
			"other nodes' locations must stay intact")
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationReady, byNode["other"])
}

func TestPublishVanished_MissingMxlFlowIsNoOp(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{Client: c, DomainPath: "/tmp", NodeName: "n1"}

	require.NoError(t, p.PublishVanished(context.Background(), validFlowID+".mxl-flow"),
		"a flow that never made it to the API server is the same as "+
			"one that vanished; the agent must not error out on the race")
}

func TestInitialSync_WalksDomainAndPublishesEach(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()

	// Create two valid flows + one entry to ignore.
	for _, id := range []string{
		validFlowID,
		"22222222-3333-4444-5555-666666666666",
	} {
		dir := filepath.Join(domain, id+".mxl-flow")
		require.NoError(t, os.Mkdir(dir, 0o755))
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, FlowDefName),
			[]byte(`{"id":"`+id+`"}`), 0o644))
	}
	// A non-flow directory must not produce a Create call.
	require.NoError(t, os.Mkdir(filepath.Join(domain, "junk"), 0o755))

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		Build()
	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.InitialSync(context.Background()))

	var list mxlv1alpha1.MxlFlowList
	require.NoError(t, c.List(context.Background(), &list))
	assert.Len(t, list.Items, 2,
		"InitialSync must produce one CR per .mxl-flow directory and "+
			"ignore non-matching entries; if this slips, the agent "+
			"would either skip flows on cold start or produce garbage CRs")
}

func TestPublishAppeared_MirrorTargetMarksReadyNotOrigin(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`"}`), 0o644))

	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "some-mirror",
			Namespace: "mxl-system",
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     validFlowID,
			SourceNode: "n0",
			TargetNode: "n1",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror).
		Build()

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	require.Len(t, got.Status.Locations, 1)
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationReady, got.Status.Locations[0].Phase,
		"a flow directory observed by an agent on a node that already "+
			"hosts a mirror target for the same flow is a materialized "+
			"copy, not an Origin; resolveSourceNode picks Origin to "+
			"locate the producer, so misclassifying the target as "+
			"Origin would route downstream lookups at a mirror")
}

func TestInitialSync_DemotesOriginForFlowsNoLongerOnDisk(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	// no flow dirs on disk

	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
				{NodeName: "other", Phase: mxlv1alpha1.MxlFlowLocationReady},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		Build()

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.InitialSync(context.Background()))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	byNode := map[string]mxlv1alpha1.MxlFlowLocationPhase{}
	for _, l := range got.Status.Locations {
		byNode[l.NodeName] = l.Phase
	}
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, byNode["n1"],
		"an agent that owns a Origin location must demote it on cold "+
			"start when the on-disk flow is gone; leaving the stale "+
			"Origin behind misdirects resolveSourceNode to a node that "+
			"no longer holds the flow")
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationReady, byNode["other"],
		"other-node locations are out of this agent's scope; "+
			"InitialSync must not touch them")
}

// fakeLease records Renew/Release calls and lets a test return a
// canned error from either method. Used to assert the Publisher's
// Lease wiring without exercising the real coordination.k8s.io API.
type fakeLease struct {
	renewed    []string
	released   []string
	renewErr   error
	releaseErr error
}

func (f *fakeLease) Renew(_ context.Context, flowID string) error {
	f.renewed = append(f.renewed, flowID)
	return f.renewErr
}

func (f *fakeLease) Release(_ context.Context, flowID string) error {
	f.released = append(f.released, flowID)
	return f.releaseErr
}

func TestPublisher_RenewsLeaseOnAppeared(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`"}`), 0o644))

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		Build()

	lease := &fakeLease{}
	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1", Lease: lease}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	assert.Equal(t, []string{validFlowID}, lease.renewed,
		"the publisher must renew the Origin Lease right after the location "+
			"flips to Origin; otherwise the operator would treat the brand-new "+
			"Origin as stale on first read")
	assert.Empty(t, lease.released)
}

func TestPublisher_AppearedAsMirrorTargetDoesNotRenewLease(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`"}`), 0o644))

	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "mxl-system"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     validFlowID,
			SourceNode: "n0",
			TargetNode: "n1",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror).
		Build()

	lease := &fakeLease{}
	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1", Lease: lease}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	assert.Empty(t, lease.renewed,
		"a mirror target's local copy is not the authoritative flow; "+
			"renewing a Lease for it would let the receiver pick the "+
			"mirror as Origin and direct further lookups at a copy")
}

func TestPublisher_ReleasesLeaseOnVanished(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		Build()

	lease := &fakeLease{}
	p := &Publisher{Client: c, DomainPath: "/tmp", NodeName: "n1", Lease: lease}

	require.NoError(t, p.PublishVanished(context.Background(), validFlowID+".mxl-flow"))
	assert.Equal(t, []string{validFlowID}, lease.released,
		"the publisher must release the Origin Lease when the on-disk "+
			"flow vanishes so consumers stop treating the now-Stale Origin "+
			"as a fresh source")
}
