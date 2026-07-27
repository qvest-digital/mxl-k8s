package intent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Mirror names encode only (flowID, targetNode), so a consumer keeps
// resolving to the same object while the origin moves. Leaving
// spec.sourceNode at its create-time value pins the mirror to a node
// that may since have been drained or deleted.
func TestEnsureMirror_RepointsIntentMirrorAfterOriginMove(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      MirrorName(flowID, "n-target"),
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByIntent: "n-target",
				mxlv1alpha1.LabelRequestorPodUID: "uid-1",
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: "n-old",
			TargetNode: "n-target",
			Provider:   mxlv1alpha1.ProviderTCP,
			Requestor: &mxlv1alpha1.PodRef{
				Name: "consumer", Namespace: "ns", UID: "uid-1",
			},
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
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-1"},
	}

	got, err := d.ensureMirror(context.Background(), flowID, "n-new", pod)
	require.NoError(t, err)
	assert.Equal(t, existing.Name, got.Name)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: existing.Name}, &live))
	assert.Equal(t, "n-new", live.Spec.SourceNode,
		"an intent mirror must follow the flow's origin; pinning it to the "+
			"create-time node leaves it Degraded once that node goes away")
	require.NotNil(t, live.Spec.Requestor)
	assert.Equal(t, "uid-1", live.Spec.Requestor.UID,
		"the repoint patch must not disturb the agent-owned Requestor")
	assert.Equal(t, "n-target", live.Labels[mxlv1alpha1.LabelCreatedByIntent],
		"the repoint patch must not disturb the labels that decide GC ownership")
}

// Keeps the two ownership domains apart: patchMirrorIfDrifted owns
// drift on receiver-authored mirrors.
func TestEnsureMirror_ReceiverMirrorNotRepointed(t *testing.T) {
	scheme := newScheme(t)
	existing := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      MirrorName(flowID, "n-target"),
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByReceiver: "rcv-1",
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: "n-old",
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
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer", UID: "uid-2"},
	}

	_, err := d.ensureMirror(context.Background(), flowID, "n-new", pod)
	require.NoError(t, err)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: existing.Name}, &live))
	assert.Equal(t, "n-old", live.Spec.SourceNode,
		"receiver-authored mirrors are the receiver reconciler's to repoint")
}

// A mirror stuck on a finalizer occupies the only name this consumer
// can use; returning it parks the caller on an object that can never
// go Ready.
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

	got, err := d.ensureMirror(context.Background(), flowID, "n-src", pod)
	require.Error(t, err,
		"a terminating mirror is not a usable mirror; the shim has to retry "+
			"rather than block on an object that cannot become Ready")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "terminating")
}
