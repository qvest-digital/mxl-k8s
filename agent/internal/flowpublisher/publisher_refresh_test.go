package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A producer that restarts in another format rewrites flow_def.json in
// the directory it already owns. The MxlFlow then describes a format
// nothing produces, and every consumer reading it is told the wrong
// one. Measured on a showcase cluster: the object said 1296x720 at
// 30/1 while the writer had been producing 1920x1080 at 50/1 for a day.
func TestPublishAppeared_RefreshesADefinitionTheProducerChanged(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`","frame_width":1920,"grain_rate":{"numerator":50,"denominator":1}}`),
		0o644))

	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec: mxlv1alpha1.MxlFlowSpec{
			ID: validFlowID,
			Definition: runtime.RawExtension{Raw: []byte(
				`{"id":"` + validFlowID + `","frame_width":1296,"grain_rate":{"numerator":30,"denominator":1},"label":"set by the operator"}`)},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		Build()

	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	assert.JSONEq(t,
		`{"id":"`+validFlowID+`","frame_width":1920,"grain_rate":{"numerator":50,"denominator":1},"label":"set by the operator"}`,
		string(got.Spec.Definition.Raw),
		"the file wins for the keys it carries; keys only the object has survive")
}
