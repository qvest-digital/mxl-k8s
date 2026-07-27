package receiver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A mirror mid-deletion still owns its name, so the receiver cannot
// replace it yet. Adopting it instead writes an owner reference and
// a spec patch onto an object about to disappear, and lets the
// receiver report Bound against a mirror that is already gone.
func Test_ensureMirror_TerminatingMirrorIsNotAdopted(t *testing.T) {
	ctx := context.Background()
	const (
		flowID = "11111111-2222-3333-4444-555555555555"
		recvNS = "ns"
		oldSrc = "n-old"
		newSrc = "n-new"
		tgt    = "n-target"
	)

	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: recvNS, Name: "r", UID: "recv-uid"},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			FlowID:   flowID,
			Provider: mxlv1alpha1.ProviderTCP,
		},
	}

	now := metav1.Now()
	existing := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         recvNS,
			Name:              mirrorName(flowID, tgt),
			DeletionTimestamp: &now,
			Finalizers:        []string{"gateway.mxl.qvest-digital.com/source-side"},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: oldSrc,
			TargetNode: tgt,
			Provider:   mxlv1alpha1.ProviderTCP,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(existing).
		Build()
	r := &Reconciler{Client: c}

	got, err := r.ensureMirror(ctx, recv, newSrc, nodeTarget{node: tgt, namespace: recvNS})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errMirrorTerminating),
		"a terminating mirror is reported through the sentinel so Reconcile "+
			"can wait for the name instead of failing the whole receiver")
	assert.Nil(t, got)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: recvNS, Name: existing.Name}, &live))
	assert.Equal(t, oldSrc, live.Spec.SourceNode,
		"spec must not be patched onto an object that is being deleted")
	assert.Empty(t, live.OwnerReferences,
		"an owner reference on a terminating mirror buys nothing and is a "+
			"write against an object on its way out")
}
