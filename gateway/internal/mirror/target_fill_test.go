package mirror

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// fills counts what targets tell the placeholder manager.
type fills struct {
	mu               sync.Mutex
	filled, unfilled int
}

func (f *fills) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.filled, f.unfilled
}

// filledTarget is a reconciler on node-a whose one live target, for
// mirror ns1/m1, filled a flow the way openTarget does on an mxlfs
// domain.
func filledTarget(t *testing.T, objs ...client.Object) (*TargetReconciler, *targetEntry, *fills) {
	t.Helper()
	f := &fills{}
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	entry := &targetEntry{}
	done := make(chan struct{})
	close(done)
	entry.done = done
	r := &TargetReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(newSourceTestScheme(t)).
			WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
			WithObjects(objs...).
			Build(),
		Scheme:   newSourceTestScheme(t),
		NodeName: "node-a",
		targets:  map[types.NamespacedName]*targetEntry{key: entry},
		attempts: attemptTable[targetOpenInputs]{},
		Filled:   func(string, string) { f.mu.Lock(); f.filled++; f.mu.Unlock() },
		Unfilled: func(string, string) { f.mu.Lock(); f.unfilled++; f.mu.Unlock() },
	}
	entry.ringRelease = r.fill("/domain", "flow-1", func() {})
	return r, entry, f
}

// A fill lasts as long as the target writing the flow, not as long as
// its mirror: a fill no Unfilled ends keeps the placeholder manager
// from holding one for the flow on this node until the gateway
// restarts, and keeps a held placeholder unmarked while nothing feeds
// it, which the agent publishes as a local producer's flow.
func TestTargetFillEndsWithTheEntry(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	ready := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{
		Phase: mxlv1alpha1.MxlFlowMirrorReady,
	})

	t.Run("mirror deleted", func(t *testing.T) {
		r, _, f := filledTarget(t, deletingMirror("node-x", "node-a"))
		_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		filled, unfilled := f.counts()
		require.Equal(t, 1, filled)
		require.Equal(t, 1, unfilled)
	})

	t.Run("fabric rebuild fails", func(t *testing.T) {
		r, _, f := filledTarget(t, ready)
		r.openFabricSideFn = func(*mxl.Writer, fabrics.Provider, string) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
			return nil, nil, "", errors.New("fabrics rebuild refused")
		}
		r.recoverFromFatalError(key)
		_, unfilled := f.counts()
		require.Equal(t, 1, unfilled, "the rebuild dropped the entry")
	})

	t.Run("recovery cap reached", func(t *testing.T) {
		r, entry, f := filledTarget(t, ready)
		entry.recoveryAttempts.Store(maxStuckRebuilds)
		r.recoverFromFatalError(key)
		_, unfilled := f.counts()
		require.Equal(t, 1, unfilled, "the cap dropped the entry")
	})

	// A progress loop parked inside libmxl-fabrics keeps the writer it
	// may be using, and the entry is given up without it. The fill is
	// not the loop's, and goes with the entry.
	t.Run("dropped with a wedged progress loop", func(t *testing.T) {
		r, entry, f := filledTarget(t, ready)
		entry.done = make(chan struct{})
		entry.cancel = func() {}
		r.TeardownGrace = time.Millisecond
		entry.recoveryAttempts.Store(maxStuckRebuilds)
		r.recoverFromFatalError(key)
		_, unfilled := f.counts()
		require.Equal(t, 1, unfilled)
	})

	t.Run("flusher cap with a wedged progress loop", func(t *testing.T) {
		w := newTargetWatchdogFixture(t, 1)
		f := &fills{}
		w.r.Filled = func(string, string) { f.mu.Lock(); f.filled++; f.mu.Unlock() }
		w.r.Unfilled = func(string, string) { f.mu.Lock(); f.unfilled++; f.mu.Unlock() }
		w.r.TeardownGrace = time.Millisecond
		w.entry.ringRelease = w.r.fill("/domain", "flow-1", func() {})
		w.entry.done = make(chan struct{})
		w.entry.recoveryAttempts.Store(maxStuckRebuilds)
		w.stampSourceActivity(t, time.Now())
		w.startFlusher(t)
		select {
		case <-w.done:
		case <-time.After(5 * time.Second):
			t.Fatal("the flusher's cap branch never ran")
		}
		w.cancel = nil
		_, unfilled := f.counts()
		require.Equal(t, 1, unfilled)
	})

	t.Run("closed for another open", func(t *testing.T) {
		r, entry, f := filledTarget(t, ready)
		require.True(t, r.closeEntry(key, keepFlow))
		require.True(t, closeTargetHandles(entry, keepFlow, 0))
		require.Nil(t, entry.abandon())
		_, unfilled := f.counts()
		require.Equal(t, 1, unfilled, "one fill, ended once")
	})
}

// A mirror deleted with no live target here filled nothing, so it ends
// no fill: the placeholder manager counts the fills of every mirror of
// a flow into this node, and an Unfilled of none would end another's.
func TestDeletedMirrorWithoutATargetEndsNoFill(t *testing.T) {
	r, _, f := filledTarget(t, deletingMirror("node-x", "node-a"))
	r.targets = map[types.NamespacedName]*targetEntry{}
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "m1"}})
	require.NoError(t, err)
	_, unfilled := f.counts()
	require.Zero(t, unfilled)
}
