package mirror

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// repointFixture is a target whose entry went live for a mirror from
// node-x over tcp, and whose mirror has since been rewritten the way
// the operator rewrites it when the flow's origin moves. The mirror is
// Ready with fresh grains, so the Ready fast path is open: the repoint
// is the only thing that can make Reconcile touch the entry.
type repointFixture struct {
	r     *TargetReconciler
	entry *targetEntry
	req   reconcile.Request
	// rebuilds receives the provider of every fabric-side rebuild.
	rebuilds chan fabrics.Provider
}

func newRepointFixture(t *testing.T, sourceNode string, provider mxlv1alpha1.MxlFabricsProvider) *repointFixture {
	t.Helper()
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	fresh := metav1.NewTime(time.Now())
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{
		Phase:       mxlv1alpha1.MxlFlowMirrorReady,
		TargetInfo:  "info-1",
		LastGrainAt: &fresh,
	})
	mirror.Spec.SourceNode = sourceNode
	mirror.Spec.Provider = provider
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	// The progress loop has already exited, so a rebuild does not wait.
	done := make(chan struct{})
	close(done)
	entry := &targetEntry{flowID: "flow-1", peerNode: "node-x", provider: fabrics.ProviderTCP, done: done}

	f := &repointFixture{entry: entry, req: reconcile.Request{NamespacedName: key}, rebuilds: make(chan fabrics.Provider, 4)}
	f.r = &TargetReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		DegradedAfter: time.Hour,
		TeardownGrace: 2 * time.Second,
		targets:       map[types.NamespacedName]*targetEntry{key: entry},
		attempts:      attemptTable[targetOpenInputs]{},
		openFabricSideFn: func(_ *mxl.Writer, p fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
			f.rebuilds <- p
			// Refused, so the rebuild ends without a progress loop to
			// drive: what is under test is the provider it was asked
			// for.
			return nil, nil, "", errors.New("rebuild refused")
		},
	}
	return f
}

// peerNode is the source the entry's counters name, read the way the
// collector reads it.
func (f *repointFixture) peerNode(t *testing.T) string {
	t.Helper()
	got := f.r.Throughput()
	require.Len(t, got, 1)
	return got[0].PeerNode
}

func (f *repointFixture) assertNoRebuild(t *testing.T) {
	t.Helper()
	select {
	case p := <-f.rebuilds:
		t.Fatalf("the fabric side was rebuilt for %s", p)
	case <-time.After(200 * time.Millisecond):
	}
}

// A mirror moved to another source on the same fabric keeps its
// target: the target does not care which node dials it. Its counters
// have to name the new source, or every byte the new origin sends is
// attributed to the node the flow left.
func TestReconcile_RepointedSourceRelabelsTheLiveTarget(t *testing.T) {
	f := newRepointFixture(t, "node-y", mxlv1alpha1.ProviderTCP)

	res, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	assert.Zero(t, res.RequeueAfter, "a relabel has nothing to look at again")

	assert.Equal(t, "node-y", f.peerNode(t),
		"the target still reports its bytes against the source the mirror left")
	f.assertNoRebuild(t)
	assert.Contains(t, f.r.targets, f.req.NamespacedName,
		"a relabel must leave the live target in place")
	assert.False(t, f.entry.recovering.Load(), "a relabel must not park the flusher")
}

// A mirror whose spec says what the entry already carries is left
// exactly as it is, however often it is reconciled. Guards the repoint
// check against firing on every pass of the fast path.
func TestReconcile_UnchangedMirrorLeavesTheLiveTargetAlone(t *testing.T) {
	f := newRepointFixture(t, "node-x", mxlv1alpha1.ProviderTCP)

	for range 3 {
		res, err := f.r.Reconcile(context.Background(), f.req)
		require.NoError(t, err)
		assert.Zero(t, res.RequeueAfter)
	}

	assert.Equal(t, "node-x", f.peerNode(t))
	f.assertNoRebuild(t)
	assert.Contains(t, f.r.targets, f.req.NamespacedName)
	assert.False(t, f.entry.recovering.Load())
}

// A mirror moved onto another fabric cannot be served by the fabric
// side the target opened: the new source dials the provider the spec
// names. The target rebuilds for that provider through the recovery
// path, which keeps the writer and republishes targetInfo.
func TestReconcile_RepointedProviderRebuildsTheLiveTarget(t *testing.T) {
	f := newRepointFixture(t, "node-y", mxlv1alpha1.ProviderVerbs)

	res, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter,
		"the outcome of the rebuild has to be looked at again")

	select {
	case p := <-f.rebuilds:
		assert.Equal(t, fabrics.ProviderVerbs, p,
			"the fabric side was rebuilt for the provider the mirror left")
	case <-time.After(5 * time.Second):
		t.Fatal("a provider change on a live Ready mirror never rebuilt the target")
	}
	f.r.mu.Lock()
	assert.Equal(t, "node-y", f.entry.peerNode, "the relabel does not wait for the rebuild")
	f.r.mu.Unlock()

	// The refused rebuild drops the entry and clears the gate; wait for
	// it so the goroutine does not outlive the test.
	require.Eventually(t, func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		return f.r.targets[f.req.NamespacedName] == nil && !f.entry.recovering.Load()
	}, 5*time.Second, 10*time.Millisecond)
}

// A rebuild already running, the watchdog's or a fatal loop's, may have
// read the provider from before the repoint. Reconcile does not start a
// second one alongside it, and comes back to check what it built.
func TestReconcile_RepointedProviderWaitsOutARebuildInFlight(t *testing.T) {
	f := newRepointFixture(t, "node-y", mxlv1alpha1.ProviderVerbs)
	f.entry.recovering.Store(true)

	res, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter,
		"nothing else wakes the mirror once the running rebuild has published")

	f.assertNoRebuild(t)
	assert.Equal(t, "node-y", f.peerNode(t), "the relabel does not wait for the rebuild")
	assert.True(t, f.entry.recovering.Load(), "the running rebuild owns the gate")
}

// A rebuild the progress loop or the watchdog started after a repoint
// opens the fabric side for the provider the spec names now, not the
// one the entry was opened with.
func TestTarget_RecoveryRebuildsForTheProviderTheSpecNames(t *testing.T) {
	f := newRepointFixture(t, "node-y", mxlv1alpha1.ProviderVerbs)

	f.r.recoverFromFatalError(f.req.NamespacedName)

	select {
	case p := <-f.rebuilds:
		assert.Equal(t, fabrics.ProviderVerbs, p)
	default:
		t.Fatal("the recovery never reached the fabric-side rebuild")
	}
}

// Once the rebuild for a new provider has succeeded, the entry carries
// that provider and the new targetInfo is published, so the next pass
// finds nothing left to follow. An entry left on the old provider
// would read as changed on every pass and rebuild the target, rotating
// targetInfo under the source, every providerRecheckInterval for good.
//
// The zero-value fabric target the rebuild hands back makes the
// progress loop it starts fail at once, and that loop's own recovery
// publishes Materializing, which keeps a full Reconcile off the fast
// path. The follow-up pass is therefore the repoint check itself. That
// recovery's rebuild is held until the end of the test.
func TestReconcile_RepointedProviderSettlesAfterTheRebuild(t *testing.T) {
	f := newRepointFixture(t, "node-y", mxlv1alpha1.ProviderVerbs)
	f.entry.writer = &mxl.Writer{}

	release := make(chan struct{})
	var (
		callsMu sync.Mutex
		calls   int
	)
	f.r.openFabricSideFn = func(_ *mxl.Writer, p fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
		f.rebuilds <- p
		callsMu.Lock()
		calls++
		first := calls == 1
		callsMu.Unlock()
		if first {
			return &fabrics.Target{}, nil, "info-2", nil
		}
		<-release
		return nil, nil, "", errors.New("rebuild refused")
	}
	t.Cleanup(func() {
		close(release)
		require.Eventually(t, func() bool {
			f.r.mu.Lock()
			defer f.r.mu.Unlock()
			return f.r.targets[f.req.NamespacedName] == nil && !f.entry.recovering.Load()
		}, 5*time.Second, 10*time.Millisecond)
	})

	ctx := context.Background()
	_, err := f.r.Reconcile(ctx, f.req)
	require.NoError(t, err)
	select {
	case p := <-f.rebuilds:
		require.Equal(t, fabrics.ProviderVerbs, p)
	case <-time.After(5 * time.Second):
		t.Fatal("a provider change on a live Ready mirror never rebuilt the target")
	}

	var mirror mxlv1alpha1.MxlFlowMirror
	require.Eventually(t, func() bool {
		require.NoError(t, f.r.Get(ctx, f.req.NamespacedName, &mirror))
		return mirror.Status.TargetInfo == "info-2"
	}, 5*time.Second, 10*time.Millisecond, "the rebuilt fabric side's targetInfo was never published")

	f.entry.lifecycle.Lock()
	provider := f.entry.provider
	f.entry.lifecycle.Unlock()
	assert.Equal(t, fabrics.ProviderVerbs, provider,
		"the entry still records the provider it was opened with")

	res, again := f.r.followRepoint(ctx, f.req.NamespacedName, &mirror)
	assert.False(t, again, "a target already rebuilt for the spec's provider was sent to rebuild again")
	assert.Zero(t, res.RequeueAfter)
}
