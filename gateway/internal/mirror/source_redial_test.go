package mirror

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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
// target's state and has a dropped target added again through the
// workqueue, with a backoff that grows while it keeps being dropped, and
// leaves the other targets on the same source alone.
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
	w := f.startWorker(t)
	w.enqueue("m1")
	w.enqueue("m2")

	adds := func() []time.Time {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Time(nil), addedAt...)
	}
	require.Eventually(t, func() bool { return len(adds()) >= 4 }, 3*time.Second, 5*time.Millisecond,
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
		_, counting := f.r.redials[e.key]
		return !counting
	}, 3*time.Second, 5*time.Millisecond, "the re-added target never read connected")

	// The wait is armed when a re-add starts, and the fake records the
	// AddTarget a little later in the same pass, so the gap between two
	// recorded adds may fall short of the backoff by that much.
	const slack = 25 * time.Millisecond
	got := adds()
	gaps := []time.Duration{got[1].Sub(got[0]), got[2].Sub(got[1]), got[3].Sub(got[2])}
	assert.Less(t, gaps[0], time.Second, "the first re-add waits for nothing but the flusher's tick; gaps %v", gaps)
	assert.GreaterOrEqual(t, gaps[1], backoffFor(1)-slack, "a target dropped again is added again later; gaps %v", gaps)
	assert.GreaterOrEqual(t, gaps[2], backoffFor(2)-slack, "and later still the time after; gaps %v", gaps)

	for _, info := range f.opener.removedTargets() {
		assert.Equal(t, "info-b", info, "a target the initiator still holds was taken off")
	}
	assert.Equal(t, 1, countOf(f.opener.addedTargets(), "info-c"),
		"the connected mirror on the same source was added again")
}

// A target gateway restart drops the source's connection, so the source
// adds the old TargetInfo again, and the restarted target publishes its
// new one while that re-add is still in AddTarget. A re-add run beside
// the worker let the stale entry land after the worker had attached the
// new TargetInfo; the worker then dropped its own attach and asked for
// no second look, and the flusher publishes nothing for an entry on a
// replaced TargetInfo, so the mirror stayed dark. The mirror has to end
// on the new TargetInfo, and stay there.
func TestSource_TargetRestartDuringAReAdd_EndsOnTheNewTargetInfo(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-old", mxlv1alpha1.ProviderTCP),
	)
	f.r.FlushInterval = 5 * time.Millisecond
	// The old target is gone for good; the new one connects.
	f.opener.setState(stateFor(f.opener, "info-old", fabrics.TargetConnected))
	w := f.startWorker(t)

	var oldAdds atomic.Int32
	newAttaching := make(chan struct{})
	oldReturned := make(chan struct{})
	var newOnce, oldOnce sync.Once
	f.opener.attachFn = func(_ *sharedSource, info string) (*fabrics.TargetInfo, error) {
		switch info {
		case "info-old":
			if oldAdds.Add(1) == 2 {
				defer oldOnce.Do(func() { close(oldReturned) })
				publishTargetInfo(t, f, "m1", "info-new")
				w.enqueue("m1")
				select {
				case <-newAttaching:
				case <-time.After(300 * time.Millisecond):
				}
			}
		case "info-new":
			newOnce.Do(func() { close(newAttaching) })
			select {
			case <-oldReturned:
			case <-time.After(300 * time.Millisecond):
			}
			time.Sleep(20 * time.Millisecond)
		}
		return &fabrics.TargetInfo{}, nil
	}
	w.enqueue("m1")

	onNew := func() bool {
		e := f.entry("m1")
		return e != nil && e.infoStr == "info-new" && e.connected.Load()
	}
	require.Eventually(t, onNew, 3*time.Second, 5*time.Millisecond,
		"the mirror did not end on the restarted target's TargetInfo")
	time.Sleep(100 * time.Millisecond)
	assert.True(t, onNew(), "the mirror left the restarted target's TargetInfo")
}

// Origin rotations and reader rebuilds still reconcile mirrors outside
// the workqueue, so two passes over one mirror can attach at once. When
// the pass that read the older TargetInfo registers first, the pass
// holding the newer one finds it and backs out; asking for nothing more
// left the mirror on the old TargetInfo, which the flusher publishes
// nothing for. The losing pass has to ask for another look.
func TestSourceReconcile_LostAttachRaceOnANewerTargetInfo_Requeues(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-old", mxlv1alpha1.ProviderTCP),
	)
	// The old pass is inside AddTarget when the new TargetInfo is
	// published, and stays there until the new pass is in its own; the
	// new pass's AddTarget returns only once the old pass has finished.
	oldAttaching := make(chan struct{})
	newAttaching := make(chan struct{})
	oldDone := make(chan struct{})
	var racing atomic.Bool
	f.opener.attachFn = func(_ *sharedSource, info string) (*fabrics.TargetInfo, error) {
		switch {
		case info == "info-old":
			publishTargetInfo(t, f, "m1", "info-new")
			close(oldAttaching)
			<-newAttaching
		case !racing.Swap(true):
			close(newAttaching)
			<-oldDone
		}
		return &fabrics.TargetInfo{}, nil
	}
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	go func() {
		defer close(oldDone)
		_, _ = f.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	}()
	<-oldAttaching
	res, err := f.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Equal(t, "info-old", f.entry("m1").infoStr, "the race did not go the way under test")
	assert.True(t, res.Requeue || res.RequeueAfter > 0,
		"a pass that lost the attach to an older TargetInfo asked for no second look")

	_, err = f.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, "info-new", f.entry("m1").infoStr)
}

// publishTargetInfo writes info as the mirror's TargetInfo, as the
// target side does when it is set up again.
func publishTargetInfo(t *testing.T, f *fanoutFixture, name, info string) {
	t.Helper()
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.c.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: name}, &m))
	m.Status.TargetInfo = info
	require.NoError(t, f.c.Status().Update(context.Background(), &m))
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
			f.startWorker(t).enqueue("m1")
			require.Eventually(t, func() bool { return f.entry("m1") != nil }, time.Second, time.Millisecond)

			time.Sleep(backoffFor(1) + 100*time.Millisecond)
			assert.Equal(t, int32(1), f.opener.attaches.Load())
			assert.Equal(t, int32(0), f.opener.detaches.Load())
			assert.Equal(t, state == fabrics.TargetConnected, f.entry("m1").connected.Load())
		})
	}
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

// A mirror whose source moves to another node leaves this node's
// sources, and its run of re-adds has to go with it: kept, it would make
// the mirror wait out a backoff earned here if its source came back.
func TestSourceReconcile_SourceNodeMove_ClearsRedials(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	_, attempt := f.r.takeRedial(key)
	require.Equal(t, uint32(1), attempt)

	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.c.Get(context.Background(), key, &m))
	m.Spec.SourceNode = "node-z"
	require.NoError(t, f.c.Update(context.Background(), &m))
	f.reconcile(t, "m1")

	assert.Nil(t, f.entry("m1"))
	f.r.mu.Lock()
	_, kept := f.r.redials[key]
	f.r.mu.Unlock()
	assert.False(t, kept, "the re-add run outlived the source's move off this node")
}
