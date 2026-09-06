package intent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// flowAt builds an MxlFlow whose only Origin location is origin.
func flowAt(origin string) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: origin, Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
}

type fakeOriginClaimer struct {
	claimed []string
	err     error
}

func (f *fakeOriginClaimer) ClaimOrigin(_ context.Context, flowID string) error {
	if f.err != nil {
		return f.err
	}
	f.claimed = append(f.claimed, flowID)
	return nil
}

func TestNotifyProducerAttached_ClaimsOrigin(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	origin := &fakeOriginClaimer{}
	d := &Dispatcher{
		Client:     c,
		Resolver:   &podlookup.Resolver{Client: c, NodeName: "n-target"},
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Origin:     origin,
	}

	err := d.NotifyProducerAttached(context.Background(), 4242,
		"/run/mxl/domain/"+flowID+".mxl-flow/data")
	require.NoError(t, err)
	assert.Equal(t, []string{flowID}, origin.claimed)
}

// A path outside the domain is a shim bug, not a producer.
func TestNotifyProducerAttached_RejectsForeignPath(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	origin := &fakeOriginClaimer{}
	d := &Dispatcher{
		Client:     c,
		Resolver:   &podlookup.Resolver{Client: c, NodeName: "n-target"},
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Origin:     origin,
	}

	err := d.NotifyProducerAttached(context.Background(), 4242,
		"/etc/passwd")
	require.Error(t, err)
	assert.Empty(t, origin.claimed)
}

// A dispatcher wired without a publisher must not panic on the
// notification.
func TestNotifyProducerAttached_NoClaimerIsNoOp(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	d := &Dispatcher{
		Client:     c,
		Resolver:   &podlookup.Resolver{Client: c, NodeName: "n-target"},
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
	}

	require.NoError(t, d.NotifyProducerAttached(context.Background(), 1,
		"/run/mxl/domain/"+flowID+".mxl-flow/data"))
}
