package mirror

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A post is not a delivery. Two targets of one flow are connected and
// the transfer loop posts to both; only one of them completes anything.
// The other must not read as delivering: no lastSentAt, no progress, no
// bytes. Stamped at post, it told its target side that a source was
// delivering to it, and it hid the stall from every watchdog.
func TestRunFlusher_DeliveryIsCompletionsNotPosts(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderVerbs),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderVerbs),
	)
	f.r.FlushInterval = 5 * time.Millisecond
	f.opener.setState(stateFor(f.opener, "none", fabrics.TargetConnected))
	var completed atomic.Uint64
	f.opener.setStats(func(info *fabrics.TargetInfo) (fabrics.TargetStats, error) {
		if f.opener.nameOf(info) == "info-b" {
			return fabrics.TargetStats{Posted: 50, InFlight: 8, Skipped: 42, LastCompletedIndex: fabrics.NoIndex}, nil
		}
		n := completed.Add(1)
		return fabrics.TargetStats{Posted: n, Completed: n, BytesCompleted: n * 100, LastCompletedIndex: n}, nil
	})
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")
	s := f.sharedFor("flow-1", fabrics.ProviderVerbs)
	require.NotNil(t, s)
	slow, fast := f.entry("m1"), f.entry("m2")
	require.Eventually(t, func() bool { return slow.connected.Load() && fast.connected.Load() }, time.Second, 5*time.Millisecond)

	for i := uint64(0); i < 20; i++ {
		s.recordTransfer(i, time.Now())
	}
	require.Eventually(t, func() bool { return fast.lastSentAt.Load() != nil }, time.Second, 5*time.Millisecond)

	assert.Nil(t, slow.lastSentAt.Load(), "a target that completed nothing read as delivering")
	assert.Zero(t, slow.progress.Load())
	assert.Zero(t, slow.bytes.Load())
	assert.NotZero(t, fast.bytes.Load())
}

// One initiator, two connected targets, a producer that keeps writing.
// The target that stops completing is taken off and added again on its
// own; the one that keeps completing is left alone. Before, nothing on
// the source side looked at completions, and the stalled target held
// its share of the initiator's completion queue until its connection
// failed.
func TestRunFlusher_ReaddsOnlyTheTargetThatStopsCompleting(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderVerbs),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderVerbs),
	)
	f.r.FlushInterval = 5 * time.Millisecond
	f.r.deliveryStall = 80 * time.Millisecond
	f.opener.setState(stateFor(f.opener, "none", fabrics.TargetConnected))
	var completed atomic.Uint64
	f.opener.setStats(func(info *fabrics.TargetInfo) (fabrics.TargetStats, error) {
		if f.opener.nameOf(info) == "info-b" {
			return fabrics.TargetStats{Posted: 8, InFlight: 8, LastCompletedIndex: fabrics.NoIndex}, nil
		}
		n := completed.Add(1)
		return fabrics.TargetStats{Posted: n, Completed: n, LastCompletedIndex: n}, nil
	})
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")
	s := f.sharedFor("flow-1", fabrics.ProviderVerbs)
	require.NotNil(t, s)
	stalled, healthy := f.entry("m1"), f.entry("m2")

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for head := uint64(1); ; head++ {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				s.recordHead(head, time.Now())
			}
		}
	}()

	require.Eventually(t, stalled.dropped.Load, 2*time.Second, 5*time.Millisecond,
		"a connected target that completed nothing while the head advanced was kept")
	time.Sleep(4 * f.r.deliveryStall)
	assert.False(t, healthy.dropped.Load(), "a target that keeps completing was re-added")
}

// A producer that stopped writing leaves nothing to complete, so a
// quiet target is not a stalled one; and a provider without a
// connection per target has none to re-add.
func TestDeliveryStalled(t *testing.T) {
	now := time.Now()
	entry := func(provider fabrics.Provider, head time.Time) *sourceEntry {
		e := &sourceEntry{shared: &sharedSource{key: sourceKey{flowID: "f", provider: provider}}}
		e.connected.Store(true)
		connected := now.Add(-time.Minute)
		e.connectedAt.Store(&connected)
		e.headAdvancedAt.Store(&head)
		return e
	}
	assert.True(t, deliveryStalled(now, entry(fabrics.ProviderVerbs, now), time.Second))
	assert.False(t, deliveryStalled(now, entry(fabrics.ProviderVerbs, now.Add(-time.Minute)), time.Second),
		"an idle producer")
	assert.False(t, deliveryStalled(now, entry(fabrics.ProviderEFA, now), time.Second),
		"a provider with no connection per target")

	delivered := entry(fabrics.ProviderVerbs, now)
	delivered.recordDelivered(3, now.Add(-500*time.Millisecond))
	assert.False(t, deliveryStalled(now, delivered, time.Second), "a completion inside the window")
}
