package mirror

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestObservedStateReportsTransfersNotLanding(t *testing.T) {
	// A source whose reader head keeps moving has a live producer, so
	// the reader-stall watchdog never fires; if nothing reaches the
	// target anyway, reporting HandshakeComplete calls the wedge
	// healthy and leaves the mirror there for good.
	const stall = 20 * time.Second

	entry := &sourceEntry{shared: testShared("flow-1", fabrics.ProviderTCP), openedAt: time.Now().Add(-time.Minute)}
	entry.lastHead.Store(4242)
	advanced := time.Now()
	entry.headAdvancedAt.Store(&advanced)

	state := observedState(entry, stall)
	require.Equal(t, metav1.ConditionFalse, state.status)
	require.Equal(t, mxlv1alpha1.ReasonTransfersNotLanding, state.reason,
		"a moving head with nothing delivered is a wedge, not a healthy "+
			"initiator")

	// A mirror still inside the window has legitimately delivered
	// nothing yet and must not be called wedged.
	fresh := &sourceEntry{shared: testShared("flow-1", fabrics.ProviderTCP), openedAt: time.Now()}
	fresh.lastHead.Store(7)
	fresh.headAdvancedAt.Store(&advanced)
	require.Equal(t, mxlv1alpha1.ReasonHandshakeComplete,
		observedState(fresh, stall).reason)

	// Once a transfer lands after the open, the wedge reading has to
	// clear on its own.
	delivered := &sourceEntry{shared: testShared("flow-1", fabrics.ProviderTCP), openedAt: time.Now().Add(-time.Minute)}
	delivered.lastHead.Store(4242)
	delivered.headAdvancedAt.Store(&advanced)
	sent := time.Now().Add(-time.Second)
	delivered.lastSentAt.Store(&sent)
	require.Equal(t, mxlv1alpha1.ReasonHandshakeComplete,
		observedState(delivered, stall).reason)
}

func TestSourceIsDeliveringAcceptsAReportedWedge(t *testing.T) {
	// The target's stuck-handshake watchdog needs to know the source is
	// trying before it rebuilds its fabric side, and it used to take
	// that only from status.lastSentAt. A source that cannot reach the
	// target never advances lastSentAt, so the gate read the mirror as
	// an idle producer and the target never rebuilt the endpoint that
	// was refusing the connection - the failure suppressed its own
	// evidence and the pair deadlocked.
	openedAt := time.Now()

	sending := &mxlv1alpha1.MxlFlowMirror{}
	sending.Status.LastSentAt = &metav1.Time{Time: openedAt.Add(time.Second)}
	require.True(t, sourceIsDelivering(sending, openedAt),
		"a transfer after the open is still the direct signal")

	idle := &mxlv1alpha1.MxlFlowMirror{}
	idle.Status.LastSentAt = &metav1.Time{Time: openedAt.Add(-time.Hour)}
	idle.Status.Conditions = []metav1.Condition{{
		Type:   mxlv1alpha1.ConditionTypeSourceProgress,
		Status: metav1.ConditionTrue,
		Reason: mxlv1alpha1.ReasonHandshakeComplete,
	}}
	require.False(t, sourceIsDelivering(idle, openedAt),
		"a producer with nothing to send must not provoke a rebuild")

	wedged := &mxlv1alpha1.MxlFlowMirror{}
	wedged.Status.LastSentAt = &metav1.Time{Time: openedAt.Add(-time.Hour)}
	wedged.Status.Conditions = []metav1.Condition{{
		Type:   mxlv1alpha1.ConditionTypeSourceProgress,
		Status: metav1.ConditionFalse,
		Reason: mxlv1alpha1.ReasonTransfersNotLanding,
	}}
	require.True(t, sourceIsDelivering(wedged, openedAt),
		"a source reporting that it cannot deliver is trying, and is the "+
			"only evidence available once lastSentAt has stopped moving")

	require.False(t, sourceIsDelivering(nil, openedAt))
}
