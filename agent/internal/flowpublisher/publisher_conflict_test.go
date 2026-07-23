package flowpublisher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestPublishAppeared_SurvivesStatusUpdateConflict(t *testing.T) {
	// status.locations of one MxlFlow is read-modify-written by every
	// node's agent, so concurrent publishes conflict routinely. The
	// fanotify dispatcher fires PublishAppeared exactly once per event
	// and only logs a failure, which turns a single 409 into a lost
	// appearance: LastObserved never advances, and the source gateway
	// never learns the origin rotated. A conflict must therefore be
	// retried inside the publisher instead of surfacing to the
	// dispatcher.
	scheme := newScheme(t)
	domain := t.TempDir()
	flowDir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(flowDir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`"}`), 0o644))

	var updates atomic.Int32
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if updates.Add(1) == 1 {
					return apierrors.NewConflict(
						schema.GroupResource{Group: mxlv1alpha1.GroupVersion.Group, Resource: "mxlflows"},
						obj.GetName(),
						errors.New("the object has been modified"))
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+FlowDirSuffix),
		"a single optimistic-concurrency conflict must not surface to the "+
			"dispatcher: the fanotify event fires once and its error is only "+
			"logged, so an unretried conflict permanently drops the appearance")

	var flow mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &flow))
	require.Len(t, flow.Status.Locations, 1)
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, flow.Status.Locations[0].Phase)
	assert.NotNil(t, flow.Status.Locations[0].LastObserved,
		"the appearance timestamp is the source gateway's only rotation "+
			"signal; losing it leaves a restarted writer's ring untailed")
}
