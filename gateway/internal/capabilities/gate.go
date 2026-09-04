package capabilities

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// EnumerationGate turns a sustained RDMADevicesEnumerated=False into a
// failing health check, so the container is restarted rather than left
// advertising a provider set it can no longer change.
//
// It is a liveness gate and not a readiness one. Readiness withdraws a
// pod from its Service, which here would stop the per-mirror counters
// being scraped while leaving the process in exactly the state that
// produced the condition; the enumeration is fixed to the process, so
// nothing short of a restart revisits it. Liveness is the only probe
// that restarts a container.
//
// That makes a false positive expensive: a restart drops the fabric
// side of every mirror the node carries. ReasonHostDevicesUnenumerated
// is a discrepancy rather than a proven fault -- an active port does
// not oblige the verbs provider to build an endpoint on the device, and
// a device the container cannot open reads the same way from here as
// one enumerated too early. A restart clears the second and loops on
// the first. The gate is therefore off unless switched on, and when on
// it waits out a grace period, so a node whose hardware the built
// providers never drive fails visibly through the condition instead of
// restarting forever.
type EnumerationGate struct {
	// Grace is how long the condition has to hold before the gate
	// reports unhealthy. Zero trips on the first observation.
	Grace time.Duration

	// now is swapped in tests. Nil uses the wall clock.
	now func() time.Time

	mu sync.Mutex
	// since is when the current unbroken run of observations carrying
	// ReasonHostDevicesUnenumerated began, and the zero time when the
	// last observation carried anything else. Observations arrive from
	// the publisher's refresh loop and Check is served on the probe
	// listener, so the two need the lock.
	since time.Time
}

// clock reads the gate's time source.
func (g *EnumerationGate) clock() time.Time {
	if g.now != nil {
		return g.now()
	}
	return time.Now()
}

// Observe records one host-device comparison. applies is the publisher's
// report of whether the comparison ran at all: a gateway bound to
// providers that drive no RDMA hardware, or one with no host device
// list, advertises nothing for a host device to contradict and must
// never trip.
//
// Anything other than an applicable ReasonHostDevicesUnenumerated
// clears the run, so ReasonHostDevicesUnreadable and
// ReasonHostDevicesRepresented both leave the gate healthy. Unreadable
// in particular is a missing cross-check rather than a missing
// provider, and a host with no RDMA hardware reports Represented on
// every pass.
func (g *EnumerationGate) Observe(cond metav1.Condition, applies bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !applies || cond.Reason != mxlv1alpha1.ReasonHostDevicesUnenumerated {
		g.since = time.Time{}
		return
	}
	if g.since.IsZero() {
		g.since = g.clock()
	}
}

// Check reports the gate's verdict in the shape controller-runtime's
// health handlers take.
//
// A gate that has observed nothing is healthy. The first observation
// cannot arrive before a probe has succeeded, because the publisher
// computes the condition only on that path, so a gateway still coming
// up is never failed for a comparison it has not yet made.
func (g *EnumerationGate) Check(_ *http.Request) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.since.IsZero() {
		return nil
	}
	held := g.clock().Sub(g.since)
	if held < g.Grace {
		return nil
	}
	return fmt.Errorf(
		"host RDMA devices have been unaccounted for in the enumerated providers for %s; "+
			"the provider set is fixed for the life of this process", held.Truncate(time.Second))
}
