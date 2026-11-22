package intent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestEnsureMirror_TerminatingMirrorNotReused(t *testing.T) {
	scheme := newScheme(t)
	now := metav1.Now()
	existing := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "ns",
			Name:              MirrorName(flowID, "n-target"),
			DeletionTimestamp: &now,
			Finalizers:        []string{"gateway.mxl.qvest-digital.com/source-side"},
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByIntent: "n-target",
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: "n-src",
			TargetNode: "n-target",
			Provider:   mxlv1alpha1.ProviderTCP,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(existing).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-3"},
	}

	got, err := d.ensureMirror(context.Background(), mxlv1alpha1.FlowRef{ID: flowID}, "n-src", pod)
	require.Error(t, err,
		"a terminating mirror is not a usable mirror; the shim has to retry "+
			"rather than block on an object that cannot become Ready")
	assert.Nil(t, got)
	assert.Equal(t, "mirror ns/"+existing.Name+" is terminating", err.Error())

	// The open trigger asks again only for a refusal that says when: an
	// open of a placeholder has already succeeded, so a final refusal
	// leaves the reader on a ring nothing fills once the name frees up.
	var r interface{ RetryAfter() time.Duration }
	require.True(t, errors.As(err, &r), "a terminating mirror is no final refusal")
	assert.Positive(t, r.RetryAfter())
}
