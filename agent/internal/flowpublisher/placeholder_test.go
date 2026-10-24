package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A placeholder lists a flow on a node that has no copy of it. The
// gateway holds it open as a writer, so without the marker check it
// would be published as this node's Origin and attract consumers to a
// ring nothing feeds.
func TestPublishAppeared_IgnoresAPlaceholderUntilItsMarkerGoes(t *testing.T) {
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flowDir, FlowDefName), []byte(`{"id":"`+validFlowID+`"}`), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(domain, mxlv1alpha1.PlaceholderMarkerDir), 0o755))
	require.NoError(t, os.WriteFile(mxlv1alpha1.PlaceholderMarker(domain, validFlowID), nil, 0o644))

	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).Build()
	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1"}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	var got mxlv1alpha1.MxlFlow
	err := c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got)
	require.True(t, apierrors.IsNotFound(err), "a placeholder was published: %v", err)

	ids, err := p.localFlowIDs()
	require.NoError(t, err)
	require.NotContains(t, ids, validFlowID, "a placeholder counted as held")

	require.NoError(t, os.Remove(mxlv1alpha1.PlaceholderMarker(domain, validFlowID)))
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	require.Len(t, got.Status.Locations, 1)
}

// A redundancy set's output is merged on each node it is read on; the
// engine holds it as a writer, so without the check it would be
// published as this node's Origin and mirrored from, which its moving
// pages do not allow.
func TestPublishAppeared_IgnoresARedundancySetOutput(t *testing.T) {
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flowDir, FlowDefName), []byte(`{"id":"`+validFlowID+`"}`), 0o644))
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).Build()
	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1",
		IsOutput: func(_ context.Context, _, id string) bool { return id == validFlowID }}

	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	err := c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got)
	require.True(t, apierrors.IsNotFound(err), "a redundancy set output was published: %v", err)
}
