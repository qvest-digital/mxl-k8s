package capabilities

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// fakeClock hands the gate a time the test moves by hand, so a grace
// period spanning minutes costs the suite nothing.
type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// newGate builds a gate on a fake clock started at a fixed instant.
func newGate(grace time.Duration) (*EnumerationGate, *fakeClock) {
	c := &fakeClock{at: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
	return &EnumerationGate{Grace: grace, now: c.now}, c
}

// cond builds a host-device comparison carrying one reason. Only the
// reason is read, so the rest is left at its zero value.
func cond(reason string) metav1.Condition {
	return metav1.Condition{
		Type:   mxlv1alpha1.ConditionTypeRDMADevicesEnumerated,
		Reason: reason,
	}
}

// The gate exists for exactly one state: a host device the enumerated
// providers do not account for, still unaccounted for after the grace.
// libfabric will not revisit the enumeration inside this process, so
// the check has to fail for the container to be replaced by one that
// enumerates again.
func TestEnumerationGate_FailsOnceUnenumeratedOutlastsTheGrace(t *testing.T) {
	g, clock := newGate(10 * time.Minute)

	g.Observe(cond(mxlv1alpha1.ReasonHostDevicesUnenumerated), true)
	require.NoError(t, g.Check(nil), "the run has only just started")

	clock.advance(10 * time.Minute)
	err := g.Check(nil)
	require.Error(t, err, "a discrepancy that outlasts the grace is what the gate is for")
	assert.Contains(t, err.Error(), "fixed for the life of this process")
}

// Short of the grace the gate stays healthy, so a single enumeration
// never decides a restart.
func TestEnumerationGate_HoldsWithinTheGrace(t *testing.T) {
	g, clock := newGate(10 * time.Minute)

	g.Observe(cond(mxlv1alpha1.ReasonHostDevicesUnenumerated), true)
	clock.advance(9*time.Minute + 59*time.Second)

	assert.NoError(t, g.Check(nil))
}

// An unreadable host device list withdraws the cross-check, not the
// providers. Restarting cannot make the list readable, so failing here
// would restart a gateway whose fabric is fine.
func TestEnumerationGate_NeverFailsOnUnreadable(t *testing.T) {
	g, clock := newGate(time.Minute)

	for i := 0; i < 10; i++ {
		g.Observe(cond(mxlv1alpha1.ReasonHostDevicesUnreadable), true)
		clock.advance(time.Minute)
	}

	assert.NoError(t, g.Check(nil), "a missing cross-check is not a missing provider")
}

// A host whose devices are accounted for, including a host carrying
// none at all, reports Represented on every pass. Failing on it would
// restart every gateway on a cluster without RDMA hardware.
func TestEnumerationGate_NeverFailsOnRepresented(t *testing.T) {
	g, clock := newGate(time.Minute)

	for i := 0; i < 10; i++ {
		g.Observe(cond(mxlv1alpha1.ReasonHostDevicesRepresented), true)
		clock.advance(time.Minute)
	}

	assert.NoError(t, g.Check(nil))
}

// A gateway bound to providers that drive no RDMA hardware, or one with
// no host device list, makes no comparison at all. The publisher
// reports that as applies=false and the gate must ignore the reason
// carried alongside it.
func TestEnumerationGate_NeverFailsWhenTheComparisonDoesNotApply(t *testing.T) {
	g, clock := newGate(time.Minute)

	for i := 0; i < 10; i++ {
		g.Observe(cond(mxlv1alpha1.ReasonHostDevicesUnenumerated), false)
		clock.advance(time.Minute)
	}

	assert.NoError(t, g.Check(nil), "a comparison that never ran cannot condemn the process")
}

// Nothing is observed until a probe has succeeded, because the
// publisher computes the comparison only on that path. A gateway still
// coming up therefore has to pass, or it could never reach the state
// where it probes at all.
func TestEnumerationGate_UnobservedGatePasses(t *testing.T) {
	g, clock := newGate(time.Minute)

	clock.advance(time.Hour)

	assert.NoError(t, g.Check(nil))
}

// The run is unbroken or it is not a run. A comparison that comes back
// represented clears it, so a device that appears late leaves no credit
// toward a later restart.
func TestEnumerationGate_RecoveryClearsTheRun(t *testing.T) {
	g, clock := newGate(10 * time.Minute)

	g.Observe(cond(mxlv1alpha1.ReasonHostDevicesUnenumerated), true)
	clock.advance(9 * time.Minute)
	g.Observe(cond(mxlv1alpha1.ReasonHostDevicesRepresented), true)
	clock.advance(9 * time.Minute)
	require.NoError(t, g.Check(nil), "the earlier run was cleared")

	g.Observe(cond(mxlv1alpha1.ReasonHostDevicesUnenumerated), true)
	clock.advance(9 * time.Minute)
	assert.NoError(t, g.Check(nil), "the new run is measured from its own start")

	clock.advance(time.Minute)
	assert.Error(t, g.Check(nil))
}

// A zero grace is the degenerate setting and trips on the first
// observation, which is what makes the grace the only thing standing
// between the condition and a restart.
func TestEnumerationGate_ZeroGraceTripsImmediately(t *testing.T) {
	g, _ := newGate(0)

	g.Observe(cond(mxlv1alpha1.ReasonHostDevicesUnenumerated), true)

	assert.Error(t, g.Check(nil))
}
