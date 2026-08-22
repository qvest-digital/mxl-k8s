package mirror

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"github.com/qvest-digital/go-mxl/mxl"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestEventTypeForReason_HealthyReasonsAreNormal(t *testing.T) {
	for _, reason := range []string{
		mxlv1alpha1.ReasonRecovered,
		mxlv1alpha1.ReasonHandshakeComplete,
		mxlv1alpha1.ReasonProbeComplete,
	} {
		assert.Equal(t, corev1.EventTypeNormal, eventTypeForReason(reason), reason)
	}
}

func TestEventTypeForReason_EverythingElseIsAWarning(t *testing.T) {
	// Including the ones that recover on their own: a transient wedge
	// that resolved is still what an operator is looking for when they
	// ask why a consumer stuttered.
	for _, reason := range []string{
		mxlv1alpha1.ReasonNoGrains,
		mxlv1alpha1.ReasonReaderAgedOut,
		mxlv1alpha1.ReasonReaderNotAdvancing,
		mxlv1alpha1.ReasonTransfersNotLanding,
		mxlv1alpha1.ReasonSourceWriterGone,
		mxlv1alpha1.ReasonAddTargetFailed,
		mxlv1alpha1.ReasonOpenTargetFailed,
		mxlv1alpha1.ReasonProviderUnresolved,
		mxlv1alpha1.ReasonLeaseExpired,
		mxlv1alpha1.ReasonFlowDefinitionEmpty,
		ReasonReaderRebuildCapReached,
	} {
		assert.Equal(t, corev1.EventTypeWarning, eventTypeForReason(reason), reason)
	}
}

func TestRecordProgress_EmitsAgainstTheMirror(t *testing.T) {
	rec := record.NewFakeRecorder(4)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}

	recordProgress(rec, key, mxlv1alpha1.ConditionTypeSourceProgress,
		mxlv1alpha1.ReasonReaderAgedOut, "reader fell behind")

	ev := <-rec.Events
	assert.Contains(t, ev, corev1.EventTypeWarning)
	assert.Contains(t, ev, mxlv1alpha1.ReasonReaderAgedOut)
	assert.Contains(t, ev, "reader fell behind")
	assert.Contains(t, ev, mxlv1alpha1.ConditionTypeSourceProgress,
		"the condition the reason belongs to disambiguates the two progress "+
			"conditions, which share several reasons")
}

func TestRecordProgress_NilRecorderAndEmptyReasonAreNoOps(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	recordProgress(nil, key, mxlv1alpha1.ConditionTypeSourceProgress,
		mxlv1alpha1.ReasonRecovered, "fine")

	rec := record.NewFakeRecorder(2)
	recordProgress(rec, key, mxlv1alpha1.ConditionTypeSourceProgress, "", "no reason")
	select {
	case ev := <-rec.Events:
		t.Fatalf("an empty reason must record nothing, got %q", ev)
	default:
	}
}

func TestWriterGone_NilSeamAndErrorsAssumeTheWriterIsLive(t *testing.T) {
	// The check exists to stop a futile reopen. Failing to get an
	// answer is not grounds for tearing down a mirror that may be
	// perfectly healthy, so both cases have to answer "not gone".
	r := &SourceReconciler{}
	assert.False(t, r.writerGone("f1"), "a nil seam must not strand a mirror")

	r = &SourceReconciler{writerLiveFn: func(string) (bool, error) {
		return false, assert.AnError
	}}
	assert.False(t, r.writerGone("f1"), "an error must not strand a mirror")

	r = &SourceReconciler{writerLiveFn: func(string) (bool, error) {
		t.Fatal("an empty flowID must not reach libmxl")
		return false, nil
	}}
	assert.False(t, r.writerGone(""))
}

func TestWriterGone_ReportsGoneOnlyWhenLibmxlSaysSo(t *testing.T) {
	r := &SourceReconciler{writerLiveFn: func(string) (bool, error) { return true, nil }}
	assert.False(t, r.writerGone("f1"))

	r = &SourceReconciler{writerLiveFn: func(string) (bool, error) { return false, nil }}
	assert.True(t, r.writerGone("f1"),
		"libmxl reporting no active writer is the authoritative answer, not "+
			"an inference from a stalled head")
}

func TestWriterGone_ReadsAFlowLibmxlCannotFindAsGone(t *testing.T) {
	// ErrFlowNotFound is an answer, not a failure to get one: a flow
	// that is not in this node's domain has no writer here, and no
	// reader can be opened on it either. Wrapped, because the seam
	// reports the call it made.
	r := &SourceReconciler{writerLiveFn: func(string) (bool, error) {
		return false, fmt.Errorf("IsFlowActive: %w", mxl.ErrFlowNotFound)
	}}
	assert.True(t, r.writerGone("f1"),
		"a flow that is not in the domain has no writer to wait for")
}
