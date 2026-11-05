package mirror

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// linkRunning reports whether a mirror's transfer loop is still
// running. The fixture's loops are stand-ins that only wait for their
// context, so a test can tell a running loop from a stopped one without
// linking libmxl.
func linkRunning(l *sourceLink) bool {
	select {
	case <-l.done:
		return false
	default:
		return true
	}
}

// fanoutMirror is one MxlFlowMirror of flowID sourced from node-a,
// carrying its own target node, provider and published target info.
func fanoutMirror(name, flowID, targetNode, targetInfo string, provider mxlv1alpha1.MxlFabricsProvider) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "ns1",
			Finalizers: []string{SourceFinalizerName},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: "node-a",
			TargetNode: targetNode,
			Provider:   provider,
		},
		Status: mxlv1alpha1.MxlFlowMirrorStatus{TargetInfo: targetInfo},
	}
}

// fanoutFixture reconciles a set of mirrors of one flow through a
// reconciler whose opener is the inline fake, and hands the test the
// pieces it asserts on.
type fanoutFixture struct {
	r      *SourceReconciler
	opener *fakeOpener
	c      client.Client
	scheme *runtime.Scheme
}

func newFanoutFixture(t *testing.T, mirrors ...*mxlv1alpha1.MxlFlowMirror) *fanoutFixture {
	t.Helper()
	scheme := newSourceTestScheme(t)
	objs := make([]client.Object, 0, len(mirrors))
	for _, m := range mirrors {
		objs = append(objs, m)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(objs...).
		Build()

	opener := &fakeOpener{
		loopFn: func(ctx context.Context, _ string, _ *sourceEntry) { <-ctx.Done() },
	}
	r := &SourceReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		opener:        opener,
		FlushInterval: time.Hour,
		sources:       map[types.NamespacedName]*sourceEntry{},
		shared:        map[sourceKey]*sharedSource{},
		attempts:      attemptTable[sourceAddInputs]{},
		rebuilds:      map[sourceKey]uint32{},
	}
	f := &fanoutFixture{r: r, opener: opener, c: c, scheme: scheme}
	t.Cleanup(f.closeAll)
	return f
}

// closeAll tears every live entry down, so the stand-in transfer
// goroutines do not outlive the test and trip goleak.
func (f *fanoutFixture) closeAll() {
	f.r.mu.Lock()
	keys := make([]types.NamespacedName, 0, len(f.r.sources))
	for k := range f.r.sources {
		keys = append(keys, k)
	}
	f.r.mu.Unlock()
	for _, k := range keys {
		f.r.closeEntry(k)
	}
	f.r.mu.Lock()
	shared := make([]*sharedSource, 0, len(f.r.shared))
	for _, s := range f.r.shared {
		shared = append(shared, s)
	}
	f.r.shared = map[sourceKey]*sharedSource{}
	f.r.mu.Unlock()
	for _, s := range shared {
		closeShared(s)
	}
}

func (f *fanoutFixture) reconcile(t *testing.T, name string) {
	t.Helper()
	key := types.NamespacedName{Namespace: "ns1", Name: name}
	_, err := f.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
}

func (f *fanoutFixture) delete(t *testing.T, name string) {
	t.Helper()
	key := types.NamespacedName{Namespace: "ns1", Name: name}
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.c.Get(context.Background(), key, &m))
	require.NoError(t, f.c.Delete(context.Background(), &m))
	_, err := f.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
}

func (f *fanoutFixture) sharedFor(flowID string, provider fabrics.Provider) *sharedSource {
	f.r.mu.Lock()
	defer f.r.mu.Unlock()
	return f.r.shared[sourceKey{flowID: flowID, provider: provider}]
}

func (f *fanoutFixture) entry(name string) *sourceEntry {
	f.r.mu.Lock()
	defer f.r.mu.Unlock()
	return f.r.sources[types.NamespacedName{Namespace: "ns1", Name: name}]
}

// Mirrors of one flow read one ring, so they share one reader. Each
// gets its own initiator and loop: an initiator posts every transfer to
// all its targets and reports not-ready while any of them has work
// pending (afterProgressResult in RCInitiator.cpp and RDMInitiator.cpp).
func TestSource_MirrorsOfOneFlowShareOneReader(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m3", "flow-1", "node-d", "info-d", mxlv1alpha1.ProviderTCP),
	)
	for _, n := range []string{"m1", "m2", "m3"} {
		f.reconcile(t, n)
	}

	assert.Equal(t, int32(1), f.opener.opens.Load(), "three mirrors of one flow must share one reader")
	assert.Equal(t, int32(3), f.opener.attaches.Load(), "each mirror sets up its own initiator")
	assert.ElementsMatch(t, []string{"info-b", "info-c", "info-d"}, f.opener.addedTargets())

	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)
	assert.Len(t, s.mirrors, 3)
	links := map[*sourceLink]bool{}
	for _, n := range []string{"m1", "m2", "m3"} {
		e := f.entry(n)
		require.NotNil(t, e)
		require.NotNil(t, e.link)
		assert.True(t, linkRunning(e.link), n)
		links[e.link] = true
	}
	assert.Len(t, links, 3, "no two mirrors share a link")
}

func TestSource_MirrorsOfOneFlowOnDifferentProvidersDoNotShare(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderVerbs),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	assert.Equal(t, int32(2), f.opener.opens.Load())
	require.NotNil(t, f.sharedFor("flow-1", fabrics.ProviderTCP))
	require.NotNil(t, f.sharedFor("flow-1", fabrics.ProviderVerbs))
}

// A transfer that never completes holds up its own mirror only. With
// one initiator for every mirror of a flow, MakeProgress reported the
// stalled target as not-ready for all of them.
func TestSource_AStalledMirrorDoesNotHoldAnotherOfTheSameFlow(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-stalled", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-healthy", mxlv1alpha1.ProviderTCP),
	)
	start := time.Now()
	// One reader: both loops probe the same head, a grain per ms.
	probe := func() (uint64, error) { return 1000 + uint64(time.Since(start)/time.Millisecond), nil }
	var stalledPosts atomic.Int32
	f.opener.loopFn = func(ctx context.Context, info string, e *sourceEntry) {
		done := make(chan struct{})
		transfer := func(uint64) (bool, uint64, error) {
			if info == "info-stalled" {
				stalledPosts.Add(1)
			}
			return true, 1, nil
		}
		connected := false
		progress := func() error {
			if info == "info-stalled" && connected {
				time.Sleep(time.Millisecond)
				return fabrics.ErrNotReady
			}
			connected = true
			return nil
		}
		runTransferLoop(ctx, done, "flow-1", probe, transfer, progress, progress, time.Millisecond, time.Millisecond, e)
	}
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	require.Eventually(t, func() bool { return f.entry("m2").progress.Load() >= 20 }, time.Second, time.Millisecond,
		"the healthy mirror keeps completing while the other one's transfer is outstanding")
	assert.Zero(t, f.entry("m1").progress.Load())
	assert.Equal(t, int32(1), stalledPosts.Load(), "the stalled mirror posts nothing while its transfer is outstanding")
}

func TestSource_DeletingOneMirrorLeavesTheOthersTransferring(t *testing.T) {
	// A teardown must not reach past the mirror being deleted and
	// close the reader the other mirrors of the flow read from.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m3", "flow-1", "node-d", "info-d", mxlv1alpha1.ProviderTCP),
	)
	for _, n := range []string{"m1", "m2", "m3"} {
		f.reconcile(t, n)
	}
	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)
	m2Link := f.entry("m2").link

	f.delete(t, "m2")

	assert.Equal(t, []string{"info-c"}, f.opener.removedTargets())
	assert.Equal(t, int32(1), f.opener.opens.Load(), "a detach must not reopen anything")
	assert.Same(t, s, f.sharedFor("flow-1", fabrics.ProviderTCP))
	assert.False(t, linkRunning(m2Link))
	assert.Nil(t, f.entry("m2"))
	assert.True(t, linkRunning(f.entry("m1").link))
	assert.True(t, linkRunning(f.entry("m3").link))
	assert.Len(t, s.mirrors, 2)
}

func TestSource_LastMirrorLeavingClosesTheSharedSource(t *testing.T) {
	// A reader kept open on a flow no mirror transfers is what stops
	// libmxl reclaiming that flow.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")
	require.NotNil(t, f.sharedFor("flow-1", fabrics.ProviderTCP))

	f.delete(t, "m1")
	require.NotNil(t, f.sharedFor("flow-1", fabrics.ProviderTCP))

	f.delete(t, "m2")
	assert.Nil(t, f.sharedFor("flow-1", fabrics.ProviderTCP),
		"the last mirror to leave must release the shared source")
	assert.ElementsMatch(t, []string{"info-b", "info-c"}, f.opener.removedTargets())
}

func TestSource_TargetInfoRotationDoesNotDisturbTheOtherMirrors(t *testing.T) {
	// A target gateway restarting republishes its info on a fresh
	// address. Only that mirror's link is replaced.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")
	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)
	m1Entry := f.entry("m1")
	require.NotNil(t, m1Entry)

	ctx := context.Background()
	key := types.NamespacedName{Namespace: "ns1", Name: "m2"}
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.c.Get(ctx, key, &m))
	m.Status.TargetInfo = "info-c2"
	require.NoError(t, f.c.Status().Update(ctx, &m))
	f.reconcile(t, "m2")

	assert.Equal(t, int32(1), f.opener.opens.Load(),
		"a rotated target info must not rebuild the reader the other mirrors read")
	assert.Equal(t, []string{"info-c"}, f.opener.removedTargets())
	assert.Equal(t, []string{"info-b", "info-c", "info-c2"}, f.opener.addedTargets())
	assert.Same(t, s, f.sharedFor("flow-1", fabrics.ProviderTCP))
	assert.Same(t, m1Entry, f.entry("m1"))
	assert.True(t, linkRunning(m1Entry.link))
	assert.Equal(t, "info-c2", f.entry("m2").infoStr)
}

func TestSource_ReaderRebuildReopensTheSourceForEveryAttachedMirror(t *testing.T) {
	// The stall watchdog tears down the reader, and the reader is
	// shared, so every mirror on it loses its link. Leaving them to a
	// watch would strand them: the events that would wake them --
	// origin lease renewals, MxlFlow status writes -- stop precisely
	// when the producer is in trouble.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")
	first := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, first)
	oldLinks := []*sourceLink{f.entry("m1").link, f.entry("m2").link}

	f.r.rebuildWedgedReader(context.Background(),
		types.NamespacedName{Namespace: "ns1", Name: "m1"})

	assert.Equal(t, int32(2), f.opener.opens.Load(),
		"the wedged reader must be reopened exactly once for the whole flow")
	for _, l := range oldLinks {
		assert.False(t, linkRunning(l), "the loops on the wedged reader must be stopped")
	}
	second := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, second)
	assert.NotSame(t, first, second)
	assert.Len(t, second.mirrors, 2, "both mirrors must be attached to the fresh reader")
	assert.Equal(t, []string{"info-b", "info-c", "info-b", "info-c"}, f.opener.addedTargets())
}

func TestSource_ReaderRebuildBudgetIsSpentPerSharedSource(t *testing.T) {
	// The budget bounds reopens of a reader, and one reader carries
	// every mirror of the flow. Counting it per mirror would hand a
	// flow mirrored to N nodes N times the cap.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	skey := sourceKey{flowID: "flow-1", provider: fabrics.ProviderTCP}
	require.Equal(t, uint32(1), f.r.bumpReaderRebuilds(f.entry("m1").sourceKey()))
	require.Equal(t, uint32(2), f.r.bumpReaderRebuilds(f.entry("m2").sourceKey()),
		"a reopen asked for by either mirror spends the same reader's budget")

	f.r.mu.Lock()
	got := f.r.rebuilds[skey]
	f.r.mu.Unlock()
	assert.Equal(t, uint32(2), got)
}

func TestSource_FailedAddTargetReleasesTheReaderItOpened(t *testing.T) {
	// A source that opened a reader and then could not attach its
	// first mirror holds a flow open that nothing transfers, and libmxl
	// reclaims a flow only once no handle denies it the lock.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
	)
	f.opener.attachFn = func(*sharedSource, string) error {
		return fmt.Errorf("%w: connect refused", errAddTargetFailed)
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	res, err := f.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter)
	assert.Nil(t, f.sharedFor("flow-1", fabrics.ProviderTCP),
		"a source with no mirror must not keep the flow's reader open")
}

func TestSource_FailedAddTargetLeavesAnInUseSourceAlone(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)

	f.opener.attachFn = func(*sharedSource, string) error {
		return fmt.Errorf("%w: connect refused", errAddTargetFailed)
	}
	f.reconcile(t, "m2")

	assert.Same(t, s, f.sharedFor("flow-1", fabrics.ProviderTCP),
		"one mirror's AddTarget failure must not close the reader another is using")
	assert.True(t, linkRunning(f.entry("m1").link))
	assert.Len(t, s.mirrors, 1)
}

// A grain the transfer loop passes over without offering it is a hole on
// the target, and is counted with why. A head more than maxMirrorLag
// ahead, as a producer catching up after a stall puts it, resyncs to
// the head; a grain the producer has already reused ages out and the
// loop jumps to the head.
func TestTransferLoop_CountsTheGrainsItPassesOver(t *testing.T) {
	f := newFanoutFixture(t, fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderVerbs))
	f.reconcile(t, "m1")
	e := f.entry("m1")
	require.NotNil(t, e)

	// 1000 is where the loop tunes in. 1030 is a catch-up burst: the
	// loop resyncs to it and drops 1003..1029. At 1034, within
	// maxMirrorLag, grain 1031 has aged out: the loop jumps to the head
	// and drops 1031..1034.
	h := &headScript{heads: []uint64{1000, 1001, 1002, 1030, 1034}}
	transfer := func(idx uint64) (bool, uint64, error) {
		if idx == 1031 {
			return false, 0, errors.New(readerAgedOutMarker)
		}
		return true, 0, nil
	}
	_, stop := runGrainLoop(h.probe, transfer, func() error { return nil }, time.Second, e)
	require.Eventually(t, func() bool { return e.agedOutGrains.Load() > 0 }, time.Second, time.Millisecond)
	stop()

	col := &ThroughputCollector{NodeName: "n01", Source: f.r}
	require.NoError(t, testutil.CollectAndCompare(col, strings.NewReader(`
# HELP mxl_gateway_mirror_source_unsent_grains_total Grains the source half did not offer to the initiator, by reason: invalid (the producer committed a video grain invalid), resync (the loop fell more than a few grains behind the head and resumed near it) or aged_out (the producer had reused the slot). Each one is a grain no target receives. An invalid data grain is not counted here: its header is offered, and the target commits it invalid.
# TYPE mxl_gateway_mirror_source_unsent_grains_total counter
mxl_gateway_mirror_source_unsent_grains_total{domain="",flow_id="flow-1",node="n01",peer_node="node-b",provider="verbs",reason="aged_out"} 4
mxl_gateway_mirror_source_unsent_grains_total{domain="",flow_id="flow-1",node="n01",peer_node="node-b",provider="verbs",reason="invalid"} 0
mxl_gateway_mirror_source_unsent_grains_total{domain="",flow_id="flow-1",node="n01",peer_node="node-b",provider="verbs",reason="resync"} 27
`), "mxl_gateway_mirror_source_unsent_grains_total"))
}
