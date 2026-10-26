package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A producer that restarts onto a node a mirror targets opens the
// mirror's copy and takes it over. The mirror still names this node,
// so the inference alone keeps the node Ready, the previous Origin goes
// Stale, and the flow is left with no Origin anywhere: no consumer on
// any node can resolve it, and the operator collects it. A producer
// among the writers is the evidence that the node is the Origin now.

func mirrorOnN1() *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metaNS("ns", "m1"),
		Spec:       mxlv1alpha1.MxlFlowMirrorSpec{FlowID: validFlowID, SourceNode: "n2", TargetNode: "n1"},
	}
}

// holdWriter takes, from this process, the shared lock a libmxl writer
// holds on the flow's data file, so the real lock table lists it.
func holdWriter(t *testing.T, domain string) {
	t.Helper()
	f, err := os.Create(filepath.Join(domain, validFlowID+".mxl-flow", "data"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	require.NoError(t, syscall.Flock(int(f.Fd()), syscall.LOCK_SH))
}

// thisProcess stands in for a producer's pid: the writer holdWriter
// takes is this process's.
func thisProcess(_ context.Context, pid int32) bool { return pid == int32(os.Getpid()) }

// noProducer finds every writer to be mxl-k8s's own.
func noProducer(context.Context, int32) bool { return false }

func TestPublishAppeared_ProducerInAMirrorTarget_ClaimsOrigin(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)
	holdWriter(t, domain)
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(mirrorOnN1(), existingFlow()).
		Build()
	lease := &fakeLease{}
	p := &Publisher{
		Client: c, WriterAttached: writerAttached, Producer: thisProcess,
		DomainPath: domain, NodeName: "n1", Lease: lease,
	}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, phaseOnNode(t, c, "n1"))
	assert.Equal(t, []string{validFlowID}, lease.renewed)
}

// A placeholder a mirror had already filled has no marker left to
// remove, so the takeover publishes nothing: the rescan is what has to
// find it.
func TestRescan_ProducerTookOverAMirrorCopy_PromotesToOrigin(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)
	holdWriter(t, domain)
	c := readyFlowOnN1(t)
	p := &Publisher{
		Client: c, WriterAttached: writerAttached, Producer: thisProcess,
		DomainPath: domain, NodeName: "n1",
	}

	require.NoError(t, p.promoteStaleLocalOrigins(context.Background(), map[string]struct{}{validFlowID: {}}))

	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, phaseOnNode(t, c, "n1"))
}

// Without a producer, or without the probe as off mxlfs, a mirror copy
// stays Ready and a healthy location is left alone.
func TestRescan_MirrorCopyWithoutProducer_StaysReady(t *testing.T) {
	for name, producer := range map[string]func(context.Context, int32) bool{
		"no probe":    nil,
		"no producer": noProducer,
	} {
		t.Run(name, func(t *testing.T) {
			domain := t.TempDir()
			writeFlowDir(t, domain, validFlowID)
			holdWriter(t, domain)
			c := readyFlowOnN1(t)
			p := &Publisher{
				Client: c, WriterAttached: writerAttached, Producer: producer,
				DomainPath: domain, NodeName: "n1",
			}
			require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
			require.NoError(t, p.promoteStaleLocalOrigins(context.Background(), map[string]struct{}{validFlowID: {}}))
			assert.Equal(t, mxlv1alpha1.MxlFlowLocationReady, phaseOnNode(t, c, "n1"))
		})
	}
}

// The rescan asks about a producer only where the node is not Origin
// yet. An Origin re-published every pass restamps AppearedAt, which the
// source gateway reads as the writer rotating, and reopens its reader
// once a pass.
func TestRescan_ProducerOnAnOrigin_LeavesItAlone(t *testing.T) {
	domain := t.TempDir()
	writeFlowDir(t, domain, validFlowID)
	holdWriter(t, domain)
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existingFlow()).
		Build()
	p := &Publisher{
		Client: c, WriterAttached: writerAttached, Producer: thisProcess,
		DomainPath: domain, NodeName: "n1",
	}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	require.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, phaseOnNode(t, c, "n1"))
	var before mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: validFlowID}, &before))

	require.NoError(t, p.promoteStaleLocalOrigins(context.Background(), map[string]struct{}{validFlowID: {}}))

	var after mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: validFlowID}, &after))
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion,
		"a healthy Origin must not be re-published")
}

// readyFlowOnN1 is a flow whose copy on n1 a mirror fills, published
// Ready.
func readyFlowOnN1(t *testing.T) client.Client {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(mirrorOnN1(), existingFlow()).
		Build()
	p := &Publisher{Client: c, WriterAttached: writerAttached, NodeName: "n1", DomainPath: t.TempDir()}
	writeFlowDir(t, p.DomainPath, validFlowID)
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	require.Equal(t, mxlv1alpha1.MxlFlowLocationReady, phaseOnNode(t, c, "n1"))
	return c
}
