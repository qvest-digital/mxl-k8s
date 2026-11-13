package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A producer that comes back in another format publishes a new flow
// directory under the same name. The MxlFlow then describes a format
// nothing produces, and every consumer reading it is told the wrong
// one: the object said 1296x720 at 30/1 while the writer had been
// producing 1920x1080 at 50/1 for a day.
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

// An object that already carries the file's definition is left alone.
// The API server returns the definition with its keys sorted at every
// level while libmxl writes its own order, so a byte comparison would
// Update the spec on every observation and wake every watcher of it.
func TestPublishAppeared_LeavesAnUnchangedDefinitionAlone(t *testing.T) {
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
		Spec: mxlv1alpha1.MxlFlowSpec{ID: validFlowID, Definition: runtime.RawExtension{Raw: []byte(
			`{"frame_width":1920,"grain_rate":{"denominator":1,"numerator":50},"id":"` + validFlowID + `"}`)}},
	}
	var specUpdates atomic.Int32
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				specUpdates.Add(1)
				return cl.Update(ctx, obj, opts...)
			},
		}).
		Build()

	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	assert.Zero(t, specUpdates.Load(), "same definition in another key order is not a change")
}

// Everything that acts on a new Origin -- the gateway building
// placeholders on other nodes, the operator mirroring members of a
// redundancy set -- reads the definition when the Origin location
// appears. Written before the definition, the location had placeholders
// built in the previous geometry, which a merge engine then mapped and
// a reader of its output faulted on.
func TestPublishAppeared_RefreshesTheDefinitionBeforeClaimingOrigin(t *testing.T) {
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`","channel_count":16}`), 0o644))
	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec: mxlv1alpha1.MxlFlowSpec{ID: validFlowID,
			Definition: runtime.RawExtension{Raw: []byte(`{"id":"` + validFlowID + `","channel_count":8}`)}},
	}
	var defAtOrigin string
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				f := obj.(*mxlv1alpha1.MxlFlow)
				for _, l := range f.Status.Locations {
					if l.Phase == mxlv1alpha1.MxlFlowLocationOrigin && defAtOrigin == "" {
						var cur mxlv1alpha1.MxlFlow
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f), &cur))
						defAtOrigin = string(cur.Spec.Definition.Raw)
					}
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()

	p := &Publisher{Client: c, WriterAttached: writerAttached, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))
	require.NotEmpty(t, defAtOrigin, "no Origin location was written")
	assert.JSONEq(t, `{"id":"`+validFlowID+`","channel_count":16}`, defAtOrigin,
		"the Origin location appeared while the object still held the old definition")
}
