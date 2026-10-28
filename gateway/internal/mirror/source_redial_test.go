package mirror

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// nameOf is the target info a TargetInfo handle was attached with.
func (f *fakeOpener) nameOf(info *fabrics.TargetInfo) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.names[info]
}

// stateFor answers targetState with notFound for the targets attached
// with info dropped, and with state for every other one.
func stateFor(f *fakeOpener, dropped string, state fabrics.TargetState) func(*fabrics.TargetInfo) (fabrics.TargetState, error) {
	return func(info *fabrics.TargetInfo) (fabrics.TargetState, error) {
		if f.nameOf(info) == dropped {
			return 0, mxl.ErrNotFound
		}
		return state, nil
	}
}

// A tcp or verbs initiator whose connection to a target fails drops the
// target and never connects to it again, while the transfers for it are
// still accepted. Nothing on the source side noticed, so the mirror sat
// until the target side gave up on it. The flusher now asks for the
// target's state and adds a dropped target again, backing off when it
// keeps being dropped, and leaves the other targets on the same source
// alone.
func TestRunFlusher_DroppedTarget_IsAddedAgainWithBackoff(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.r.FlushInterval = 5 * time.Millisecond

	var mu sync.Mutex
	var addedAt []time.Time
	f.opener.attachFn = func(_ *sharedSource, info string) (*fabrics.TargetInfo, error) {
		if info == "info-b" {
			mu.Lock()
			addedAt = append(addedAt, time.Now())
			mu.Unlock()
		}
		return &fabrics.TargetInfo{}, nil
	}
	f.opener.setState(stateFor(f.opener, "info-b", fabrics.TargetConnected))
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	adds := func() []time.Time {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Time(nil), addedAt...)
	}
	require.Eventually(t, func() bool { return len(adds()) >= 3 }, 3*time.Second, 5*time.Millisecond,
		"a dropped target was not added again")

	// Let the last re-add settle on a connected target before the
	// fixture tears the entries down.
	f.opener.setState(stateFor(f.opener, "none", fabrics.TargetConnected))
	require.Eventually(t, func() bool {
		e := f.entry("m1")
		if e == nil || !e.connected.Load() {
			return false
		}
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		return f.r.redials[e.key] == 0
	}, 3*time.Second, 5*time.Millisecond, "the re-added target never read connected")

	got := adds()
	first, second := got[1].Sub(got[0]), got[2].Sub(got[1])
	assert.GreaterOrEqual(t, first, backoffFor(1))
	assert.Less(t, first, time.Second, "the first re-add is due within one flusher tick and its backoff")
	assert.GreaterOrEqual(t, second, backoffFor(2), "a target dropped again is added again later")

	for _, info := range f.opener.removedTargets() {
		assert.Equal(t, "info-b", info, "a target the initiator still holds was taken off")
	}
	assert.Equal(t, 1, countOf(f.opener.addedTargets(), "info-c"),
		"the connected mirror on the same source was added again")
}

func countOf(list []string, s string) int {
	n := 0
	for _, v := range list {
		if v == s {
			n++
		}
	}
	return n
}

// A target still connecting, connected or closing is held by the
// initiator, and adding it again would only tear down a connection that
// may be about to work.
func TestRunFlusher_HeldTarget_IsLeftAlone(t *testing.T) {
	for _, state := range []fabrics.TargetState{fabrics.TargetConnecting, fabrics.TargetConnected, fabrics.TargetClosing} {
		t.Run(state.String(), func(t *testing.T) {
			f := newFanoutFixture(t,
				fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderVerbs),
			)
			f.r.FlushInterval = 5 * time.Millisecond
			f.opener.setState(stateFor(f.opener, "none", state))
			f.reconcile(t, "m1")

			time.Sleep(backoffFor(1) + 100*time.Millisecond)
			assert.Equal(t, int32(1), f.opener.attaches.Load())
			assert.Equal(t, int32(0), f.opener.detaches.Load())
			assert.Equal(t, state == fabrics.TargetConnected, f.entry("m1").connected.Load())
		})
	}
}

// A transfer on a shared source counts for every attached mirror, and
// libmxl-fabrics accepts one for a tcp or verbs target it is not
// connected to. Stamping lastSentAt on such a mirror told its target a
// source was delivering to it. Providers without a connection per
// target stamp as before.
func TestSource_LastSentAtOnlyOnATargetThatReadsConnected(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderEFA),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	tcp := f.sharedFor("flow-1", fabrics.ProviderTCP)
	efa := f.sharedFor("flow-1", fabrics.ProviderEFA)
	require.NotNil(t, tcp)
	require.NotNil(t, efa)

	tcp.recordTransfer(1, time.Now())
	efa.recordTransfer(1, time.Now())
	assert.Nil(t, f.entry("m1").lastSentAt.Load(), "lastSentAt stamped on a target not known to be connected")
	assert.Equal(t, uint64(1), f.entry("m1").progress.Load())
	assert.NotNil(t, f.entry("m2").lastSentAt.Load())

	f.entry("m1").connected.Store(true)
	tcp.recordTransfer(2, time.Now())
	assert.NotNil(t, f.entry("m1").lastSentAt.Load())
}

// The flusher's state poll is what moves connected, both ways.
func TestRunFlusher_TargetStateDrivesConnected(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
	)
	f.r.FlushInterval = 5 * time.Millisecond
	f.opener.setState(stateFor(f.opener, "none", fabrics.TargetConnected))
	f.reconcile(t, "m1")
	e := f.entry("m1")
	require.NotNil(t, e)
	require.Eventually(t, e.connected.Load, time.Second, 5*time.Millisecond)

	f.opener.setState(stateFor(f.opener, "none", fabrics.TargetClosing))
	require.Eventually(t, func() bool { return !e.connected.Load() }, time.Second, 5*time.Millisecond)

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	assert.Same(t, e, f.entry(key.Name), "a held target was added again")
}
