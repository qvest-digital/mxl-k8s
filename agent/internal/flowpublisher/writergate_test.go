package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A flow directory only says a copy was here once. Whether it is a
// copy anything still writes to is the question these cover, and
// getting it wrong in the permissive direction is what let a collected
// flow come straight back with a mirror target named as its producer.

func writeFlowDir(t *testing.T, domain, flowID string) {
	t.Helper()
	dir := filepath.Join(domain, flowID+".mxl-flow")
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, FlowDefName), []byte(`{"id":"`+flowID+`"}`), 0o644))
}

// existingFlow is the MxlFlow a producer published before this node's
// copy appeared.
func existingFlow() *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{ObjectMeta: ObjectMeta(validFlowID), Spec: mxlv1alpha1.MxlFlowSpec{ID: validFlowID}}
}

// phaseOnNode reads back this node's published phase.
func phaseOnNode(t *testing.T, c client.Client, node string) mxlv1alpha1.MxlFlowLocationPhase {
	t.Helper()
	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: validFlowID}, &got))
	return localPhase(t, &got, node)
}

func TestPublishAppeared_WriterDetached_PublishesStaleAndReleasesLease(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)

	// The producer this node published as Origin has left its directory
	// behind.
	flow := existingFlow()
	flow.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(flow).
		Build()
	lease := &fakeLease{}
	p := &Publisher{
		Client: c, WriterAttached: writerDetached,
		DomainPath: domain, NodeName: "n1", Lease: lease,
	}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, phaseOnNode(t, c, "n1"),
		"a directory with no attached writer is a copy nothing feeds; "+
			"publishing it as Origin names this node as the flow's "+
			"producer and sends every consumer at a dead copy")
	assert.Empty(t, lease.renewed,
		"renewing here would keep the flow's liveness signal alive on a "+
			"copy that has no writer, which is the one thing the Lease "+
			"exists to rule out")
	assert.Equal(t, []string{validFlowID}, lease.released)
}

// A directory with no writer on a node the flow does not list is no
// copy of it, as a placeholder the gateway created and released is not.
func TestPublishAppeared_WriterDetachedOnUnlistedNode_AddsNoLocation(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existingFlow()).
		Build()
	lease := &fakeLease{}
	p := &Publisher{
		Client: c, WriterAttached: writerDetached,
		DomainPath: domain, NodeName: "n1", Lease: lease,
	}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	assert.Empty(t, got.Status.Locations,
		"a Stale entry for a node that never held the flow reads as a "+
			"copy there, so a node that was never asked for the flow "+
			"would appear to have had it")
	assert.Equal(t, []string{validFlowID}, lease.released)
}

func TestPublishAppeared_WriterAttachedAndNoMirror_ClaimsOrigin(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		Build()
	lease := &fakeLease{}
	p := &Publisher{
		Client: c, WriterAttached: writerAttached,
		DomainPath: domain, NodeName: "n1", Lease: lease,
	}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, phaseOnNode(t, c, "n1"))
	assert.Equal(t, []string{validFlowID}, lease.renewed)
}

// The two evidence sources have to be read together. A writer being
// attached does not make this node the producer -- the gateway filling
// a mirror target holds the same lock -- so the mirror is what tells
// the two apart.
func TestPublishAppeared_WriterAttachedAndMirrorTarget_IsReady(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)

	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metaNS("ns", "m1"),
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID: validFlowID, SourceNode: "n2", TargetNode: "n1",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, existingFlow()).
		Build()
	lease := &fakeLease{}
	p := &Publisher{
		Client: c, WriterAttached: writerAttached,
		DomainPath: domain, NodeName: "n1", Lease: lease,
	}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	assert.Equal(t, mxlv1alpha1.MxlFlowLocationReady, phaseOnNode(t, c, "n1"))
	assert.Empty(t, lease.renewed,
		"only the producer side renews: a Lease on a mirror target would "+
			"say a mirrored copy of a dead flow is a live one")
}

// The regression the whole gate exists for. After a mirror is
// collected the target's copy survives until libmxl's next domain
// sweep, and with the mirror gone the old classification had nothing
// left to attribute the directory to but a local producer -- so the
// rescan republished a corpse as Origin and resurrected the flow that
// had just been collected.
func TestRescan_WriterDetachedAfterMirrorTeardown_DemotesInsteadOfClaimingOrigin(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)

	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationReady},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		Build()
	lease := &fakeLease{}
	// No mirror object any more, and the target gateway has closed its
	// writer: exactly the state one domain sweep before the directory
	// is reclaimed.
	p := &Publisher{
		Client: c, WriterAttached: writerDetached,
		DomainPath: domain, NodeName: "n1", Lease: lease,
	}

	onDisk, err := p.localFlowIDs()
	require.NoError(t, err)
	held := p.heldFlowIDs(context.Background(), onDisk)
	assert.Empty(t, held, "a directory with no writer is not a flow this node holds")

	require.NoError(t, p.retireUnheldLocations(context.Background(), onDisk, held))
	require.NoError(t, p.promoteStaleLocalOrigins(context.Background(), held))

	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, phaseOnNode(t, c, "n1"))
	assert.Equal(t, []string{validFlowID}, lease.released)
}

// A probe that cannot answer must not demote: being unable to tell is
// not evidence of absence, and a wrong demote costs every consumer of
// the flow its source.
func TestHeldFlowIDs_ProbeErrorCountsAsHeld(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	p := &Publisher{
		Client:   c,
		NodeName: "n1",
		WriterAttached: func(string) (bool, error) {
			return true, assert.AnError
		},
	}
	held := p.heldFlowIDs(context.Background(), map[string]struct{}{validFlowID: {}})
	assert.Contains(t, held, validFlowID)
}

// Only the Origin creates an MxlFlow. A mirror target's copy and a copy
// nothing writes any more hold the definition the object had when they
// were made; recreating a deleted object from them brings back a
// geometry the producer may since have changed, and every mirror built
// from that object recreates the stale copy in turn.
func TestPublishAppeared_OnlyTheOriginCreatesTheMxlFlow(t *testing.T) {
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metaNS("ns", "m1"),
		Spec:       mxlv1alpha1.MxlFlowMirrorSpec{FlowID: validFlowID, SourceNode: "n2", TargetNode: "n1"},
	}
	for _, tc := range []struct {
		name     string
		attached func(string) (bool, error)
		mirror   bool
		created  bool
	}{
		{"mirror target", writerAttached, true, false},
		{"copy without a writer", writerDetached, false, false},
		{"origin", writerAttached, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			domain := t.TempDir()
			writeFlowDir(t, domain, validFlowID)
			b := fake.NewClientBuilder().WithScheme(newScheme(t)).WithStatusSubresource(&mxlv1alpha1.MxlFlow{})
			if tc.mirror {
				b = b.WithObjects(mirror.DeepCopy())
			}
			c := b.Build()
			p := &Publisher{Client: c, WriterAttached: tc.attached, DomainPath: domain, NodeName: "n1"}

			require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

			err := c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &mxlv1alpha1.MxlFlow{})
			if tc.created {
				require.NoError(t, err)
			} else {
				require.True(t, apierrors.IsNotFound(err), "a %s recreated the MxlFlow: %v", tc.name, err)
			}
		})
	}
}

// The rescan settles every location this node owns by what is on disk.
// Stale is the state of a directory nothing feeds; once the directory
// is gone the entry goes too, including the ones earlier agents left
// Stale for good, while other nodes' entries are theirs to settle.
func TestRetireUnheldLocations_StaleWhileOnDiskRemovedOnceGone(t *testing.T) {
	const goneID = "22222222-3333-4444-5555-666666666666"
	stale := func(id string, nodes ...string) *mxlv1alpha1.MxlFlow {
		f := &mxlv1alpha1.MxlFlow{ObjectMeta: ObjectMeta(id), Spec: mxlv1alpha1.MxlFlowSpec{ID: id}}
		for _, n := range nodes {
			f.Status.Locations = append(f.Status.Locations,
				mxlv1alpha1.MxlFlowLocation{NodeName: n, Phase: mxlv1alpha1.MxlFlowLocationStale})
		}
		return f
	}
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(stale(validFlowID, "n1"), stale(goneID, "n1", "n2")).
		Build()
	lease := &fakeLease{}
	p := &Publisher{Client: c, WriterAttached: writerDetached, DomainPath: domain, NodeName: "n1", Lease: lease}

	onDisk, err := p.localFlowIDs()
	require.NoError(t, err)
	require.NoError(t, p.retireUnheldLocations(context.Background(), onDisk, p.heldFlowIDs(context.Background(), onDisk)))

	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, phaseOnNode(t, c, "n1"),
		"a directory still on disk with no writer stays Stale")

	var gone mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: goneID}, &gone))
	assert.Equal(t, []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationStale},
	}, gone.Status.Locations,
		"this node's entry for a directory that is gone is removed; n2's is n2's agent's")
	assert.Equal(t, []string{goneID}, lease.released,
		"the removed location gives up its Lease; the one already Stale was written nowhere")
}

// The rescan decides from a listing taken before it writes. A flow that
// appears in between has published a fresh entry by the time the
// removal is written, and removing it would leave the new copy
// unlisted until the promote pass of the next rescan.
func TestRetireUnheldLocations_KeepsAnEntryWhoseDirectoryCameBack(t *testing.T) {
	domain := t.TempDir()
	existing := existingFlow()
	existing.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(existing).Build()
	lease := &fakeLease{}
	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1", Lease: lease}

	onDisk, err := p.localFlowIDs()
	require.NoError(t, err)
	require.Empty(t, onDisk)
	writeFlowDir(t, domain, validFlowID)

	require.NoError(t, p.retireUnheldLocations(context.Background(), onDisk, p.heldFlowIDs(context.Background(), onDisk)))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, phaseOnNode(t, c, "n1"))
	assert.Empty(t, lease.released, "a copy still on disk keeps its Lease")
}

// A vanish event for a directory that is back by the time it is handled
// demotes rather than removes, as before; the appearance queued behind
// it publishes the copy again.
func TestPublishVanished_DirectoryBackOnDiskDemotes(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)
	existing := existingFlow()
	existing.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(existing).Build()
	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1"}

	require.NoError(t, p.PublishVanished(context.Background(), validFlowID+".mxl-flow"))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, phaseOnNode(t, c, "n1"))
}

// On mxlfs a placeholder directory stays for a flow with no copy on the
// node, and its marker appearing is published as a vanish. It is no
// copy, so the entry goes as for a directory that is gone.
func TestPublishVanished_PlaceholderCountsAsGone(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)
	marker := mxlv1alpha1.PlaceholderMarker(domain, validFlowID)
	require.NoError(t, os.MkdirAll(filepath.Dir(marker), 0o755))
	require.NoError(t, os.WriteFile(marker, nil, 0o644))
	existing := existingFlow()
	existing.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationReady},
		{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(existing).Build()
	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1"}

	require.NoError(t, p.PublishVanished(context.Background(), validFlowID+".mxl-flow"))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationPhase(""), phaseOnNode(t, c, "n1"))
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, phaseOnNode(t, c, "n2"))
}
