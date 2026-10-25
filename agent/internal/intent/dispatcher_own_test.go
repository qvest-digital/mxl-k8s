package intent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

type onePod struct{ pod *corev1.Pod }

func (o onePod) PodForPID(context.Context, int32) (*corev1.Pod, error) { return o.pod, nil }

// The open trigger skips the gateway's and the exporter's opens of a
// placeholder, but passes on a pid whose pod it could not resolve. When
// the dispatcher's own lookup then finds one of them, no mirror starts:
// one would copy a flow to this node for no reader, and keep it there.
func TestMaterialize_OwnPodGetsNoMirror(t *testing.T) {
	scheme := newScheme(t)
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{Locations: []mxlv1alpha1.MxlFlowLocation{
			{NodeName: "n-src", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
		}},
	}
	gateway := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "mxl-system", Name: "gw", UID: "uid-gw",
		Labels: map[string]string{"app.kubernetes.io/component": "gateway"}}}
	consumer := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "media", Name: "viewer", UID: "uid-viewer"}}

	for _, tc := range []struct {
		pod    *corev1.Pod
		mirror bool
	}{{gateway, false}, {consumer, true}} {
		t.Run(tc.pod.Name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&mxlv1alpha1.MxlFlow{}, &mxlv1alpha1.MxlFlowMirror{}).
				WithObjects(flow.DeepCopy()).Build()
			d := &Dispatcher{
				Client:             c,
				Resolver:           onePod{tc.pod},
				DomainPath:         "/run/mxl/domain",
				NodeName:           "n-target",
				Provider:           mxlv1alpha1.ProviderTCP,
				FlowChecker:        func(string) bool { return false },
				MaterializeTimeout: 50 * time.Millisecond,
				Own: func(p *corev1.Pod) bool {
					return p.Namespace == "mxl-system" && p.Labels["app.kubernetes.io/component"] == "gateway"
				},
			}

			err := d.Materialize(context.Background(), 42, "/run/mxl/domain/"+flowID+".mxl-flow/data")
			require.Error(t, err, "no gateway fills the mirror here, so even the consumer's wait times out")

			var list mxlv1alpha1.MxlFlowMirrorList
			require.NoError(t, c.List(context.Background(), &list))
			if tc.mirror {
				assert.Len(t, list.Items, 1, "a consumer's open still starts its mirror")
			} else {
				assert.Empty(t, list.Items)
				assert.Contains(t, err.Error(), "mxl-k8s's own")
			}
		})
	}
}
