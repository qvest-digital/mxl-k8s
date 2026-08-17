package mirror

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Every reason the two progress conditions can carry already reaches
// the mirror's status, and both sides funnel through one publish call
// that fires only when the condition transitions. Hooking events there
// covers the grain and the sample path together, and keeps a condition
// added later from needing its own Eventf: it is emitted by virtue of
// being published.
//
// Events rather than status alone because status keeps only the
// current reason. A mirror that flapped between ReaderAgedOut and
// Recovered a dozen times looks identical to one that transitioned
// once, and the sequence is usually the diagnosis. kubectl describe
// and k9s both surface events against the object without anyone
// having to know which node's gateway logged what.

// eventTypeForReason maps a condition reason to an event type. The
// healthy reasons are Normal; everything else describes a mirror not
// doing its job and is a Warning, including the ones that recover on
// their own -- a transient wedge that resolves is still the thing an
// operator is looking for when they ask why a consumer stuttered.
func eventTypeForReason(reason string) string {
	switch reason {
	case mxlv1alpha1.ReasonRecovered,
		mxlv1alpha1.ReasonHandshakeComplete,
		mxlv1alpha1.ReasonProbeComplete:
		return corev1.EventTypeNormal
	default:
		return corev1.EventTypeWarning
	}
}

// mirrorRef is the object an event is recorded against. Both flushers
// hold only the key by the time they publish.
func mirrorRef(key types.NamespacedName) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
	}
}

// ReasonPhaseChanged is the event reason for a mirror lifecycle
// transition. The phase is the coarse answer an operator reads first,
// and Ready -> Materializing -> Ready is the flap a consumer feels;
// the conditions explain it, the phase says when.
const ReasonPhaseChanged = "PhaseChanged"

// recordPhase emits an event for a mirror phase transition. Called
// only when the phase actually differs, so a status write that merely
// refreshes counters is silent.
func recordPhase(rec record.EventRecorder, key types.NamespacedName, from, to mxlv1alpha1.MxlFlowMirrorPhase) {
	if rec == nil {
		return
	}
	kind := corev1.EventTypeNormal
	if to == mxlv1alpha1.MxlFlowMirrorFailed || to == mxlv1alpha1.MxlFlowMirrorDegraded {
		kind = corev1.EventTypeWarning
	}
	if from == "" {
		rec.Eventf(mirrorRef(key), kind, ReasonPhaseChanged, "Phase set to %s", to)
		return
	}
	rec.Eventf(mirrorRef(key), kind, ReasonPhaseChanged, "Phase %s -> %s", from, to)
}

// recordProgress emits one event for a published progress condition.
// A nil recorder or an empty reason records nothing, which is what a
// test that wires no manager wants.
func recordProgress(rec record.EventRecorder, key types.NamespacedName, condType, reason, message string) {
	if rec == nil || reason == "" {
		return
	}
	rec.Eventf(mirrorRef(key), eventTypeForReason(reason), reason,
		"%s: %s", condType, message)
}
