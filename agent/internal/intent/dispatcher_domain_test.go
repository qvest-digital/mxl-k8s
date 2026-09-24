package intent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// The same flow id in two domains is two flows. A consumer in the second
// domain is resolved against that domain's MxlFlow, not the primary's, and
// its mirror carries the domain -- otherwise it would be served the primary
// domain's flow of the same id.
func TestADomainFlowResolvesAndMirrorsWithinItsDomain(t *testing.T) {
	scheme := newScheme(t)
	origin := func(name, domain, node string) *mxlv1alpha1.MxlFlow {
		f := &mxlv1alpha1.MxlFlow{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID, Domain: domain},
		}
		f.Status.Locations = []mxlv1alpha1.MxlFlowLocation{{
			NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationOrigin,
		}}
		return f
	}
	ref := mxlv1alpha1.FlowRef{Domain: "studio-b", ID: flowID}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
		WithObjects(
			origin(flowID, "", "primary-node"),
			origin(ref.Name(), "studio-b", "studio-node"),
		).Build()

	d := &Dispatcher{Client: c, NodeName: "n1"}

	res, err := d.resolveSourceNode(context.Background(), ref)
	require.NoError(t, err)
	require.True(t, res.Found)
	assert.Equal(t, "studio-node", res.Node, "the second domain's origin, not the primary's")

	pod := &metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-1"}
	m, err := d.ensureMirror(context.Background(), ref, res.Node, pod)
	require.NoError(t, err)
	assert.Equal(t, "studio-b", m.Spec.Domain)
	assert.Equal(t, flowID, m.Spec.FlowID)
	assert.Equal(t, mxlv1alpha1.MirrorNameFor(ref, "n1"), m.Name)

	var stored mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: m.Name}, &stored))
	assert.NotEqual(t, mxlv1alpha1.MirrorName(flowID, "n1"), stored.Name,
		"distinct from a mirror of the primary domain's flow of the same id")
}
