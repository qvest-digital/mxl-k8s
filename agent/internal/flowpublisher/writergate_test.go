package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
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
		WithObjects(mirror).
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

	require.NoError(t, p.demoteVanishedLocalOrigins(context.Background(), held))
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
