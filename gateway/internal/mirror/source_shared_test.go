package mirror

import (
	"context"
	"fmt"
	"strings"
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

// runningSource is a sharedSource whose transfer goroutine is a stand
// -in that only waits for its context, so a test can tell a loop that
// is still running from one that has been stopped without linking
// libmxl.
func runningSource() *sharedSource {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
	}()
	return &sharedSource{cancel: cancel, done: done}
}

func loopRunning(s *sharedSource) bool {
	select {
	case <-s.done:
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
		openFn: func(string, fabrics.Provider) (*sharedSource, error) {
			return runningSource(), nil
		},
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

func TestSource_MirrorsOfOneFlowShareOneReaderAndInitiator(t *testing.T) {
	// libmxl-fabrics enqueues a transfer to every target added to an
	// initiator, so a flow mirrored to N nodes is N AddTarget calls on
	// one initiator. Opening one initiator per mirror instead read the
	// same ring N times, ran N transfer loops, and posted N copies of
	// every grain to the NIC -- work that grew with fan-out for no
	// payload that reached a peer any sooner.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m3", "flow-1", "node-d", "info-d", mxlv1alpha1.ProviderTCP),
	)
	for _, n := range []string{"m1", "m2", "m3"} {
		f.reconcile(t, n)
	}

	assert.Equal(t, int32(1), f.opener.opens.Load(),
		"three mirrors of one flow must share one reader and one initiator")
	assert.Equal(t, int32(3), f.opener.attaches.Load(),
		"each mirror still contributes its own target")
	assert.ElementsMatch(t, []string{"info-b", "info-c", "info-d"}, f.opener.addedTargets())

	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)
	assert.Len(t, s.attached(), 3,
		"every mirror must be in the fanout set the transfer loop accounts to")
}

func TestSource_MirrorsOfOneFlowOnDifferentProvidersDoNotShare(t *testing.T) {
	// An initiator is set up against one interface, chosen per
	// provider, so a tcp mirror and a verbs mirror of the same flow
	// cannot be served by one initiator however alike they look.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderVerbs),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	assert.Equal(t, int32(2), f.opener.opens.Load(),
		"two providers cannot share one initiator's interface")
	require.NotNil(t, f.sharedFor("flow-1", fabrics.ProviderTCP))
	require.NotNil(t, f.sharedFor("flow-1", fabrics.ProviderVerbs))
}

func TestSource_DeletingOneMirrorLeavesTheOthersTransferring(t *testing.T) {
	// The failure this guards against is the worst one the sharing
	// introduces: a teardown that reaches past the mirror being
	// deleted and closes the reader every other mirror of the flow is
	// transferring through.
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

	f.delete(t, "m2")

	assert.Equal(t, []string{"info-c"}, f.opener.removedTargets(),
		"only the deleted mirror's target may be removed from the initiator")
	assert.Equal(t, int32(1), f.opener.opens.Load(),
		"a detach must not rebuild anything")
	assert.Same(t, s, f.sharedFor("flow-1", fabrics.ProviderTCP),
		"the shared source must survive one of its mirrors going away")
	assert.True(t, loopRunning(s),
		"the transfer loop still has two targets to feed")
	assert.Nil(t, f.entry("m2"))
	assert.NotNil(t, f.entry("m1"))
	assert.NotNil(t, f.entry("m3"))
	assert.Len(t, s.attached(), 2)
}

func TestSource_LastMirrorLeavingClosesTheSharedSource(t *testing.T) {
	// The other half of the same rule: a reader kept open on a flow no
	// mirror transfers is what stops libmxl reclaiming that flow.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")
	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)

	f.delete(t, "m1")
	require.True(t, loopRunning(s))

	f.delete(t, "m2")
	assert.Nil(t, f.sharedFor("flow-1", fabrics.ProviderTCP),
		"the last mirror to leave must release the shared source")
	assert.False(t, loopRunning(s),
		"the transfer goroutine must stop once nothing is attached")
	assert.ElementsMatch(t, []string{"info-b", "info-c"}, f.opener.removedTargets())
}

func TestSource_AccountingLandsPerMirrorAcrossTheFanout(t *testing.T) {
	// One TransferGrain call really does enqueue the grain to every
	// target the initiator holds, so every attached mirror really did
	// move those bytes and really is at that index. Folding the
	// counters onto the flow instead would drop the peer_node label
	// the throughput series are told apart by, and leave every mirror
	// but the first with a status that never reports progress.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-a", "n02", "info-b", mxlv1alpha1.ProviderVerbs),
		fanoutMirror("m2", "flow-a", "n03", "info-c", mxlv1alpha1.ProviderVerbs),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	s := f.sharedFor("flow-a", fabrics.ProviderVerbs)
	require.NotNil(t, s)

	sentAt := time.Now()
	s.recordHead(41, sentAt)
	s.recordTransfer(42, sentAt)
	s.addBytes(1000)

	for _, n := range []string{"m1", "m2"} {
		e := f.entry(n)
		require.NotNil(t, e, n)
		assert.Equal(t, uint64(1), e.progress.Load(), n)
		assert.Equal(t, uint64(1000), e.bytes.Load(), n)
		assert.Equal(t, uint64(41), e.lastHead.Load(), n)
		require.NotNil(t, e.lastSentAt.Load(), n)
	}

	col := &ThroughputCollector{NodeName: "n01", Source: f.r}
	require.NoError(t, testutil.CollectAndCompare(col, strings.NewReader(`
# HELP mxl_gateway_mirror_transmitted_bytes_total Payload bytes handed to the fabric for a mirror this node is the source of.
# TYPE mxl_gateway_mirror_transmitted_bytes_total counter
mxl_gateway_mirror_transmitted_bytes_total{flow_id="flow-a",node="n01",peer_node="n02",provider="verbs"} 1000
mxl_gateway_mirror_transmitted_bytes_total{flow_id="flow-a",node="n01",peer_node="n03",provider="verbs"} 1000
`), "mxl_gateway_mirror_transmitted_bytes_total"),
		"the shared transfer must still be attributed per peer")
}

func TestSource_AgedOutSkipReachesEveryAttachedMirror(t *testing.T) {
	// The skip is a property of the reader, and the reader is shared,
	// so a mirror that is not the one whose flusher happens to look
	// first must still see it -- otherwise ReaderAgedOut, and the
	// reopen it asks for, would only ever be published for one of the
	// mirrors on a wedged flow.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	f.reconcile(t, "m2")

	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)
	s.recordAgedOut(time.Now())

	for _, n := range []string{"m1", "m2"} {
		e := f.entry(n)
		require.NotNil(t, e, n)
		require.NotNil(t, e.agedOutAt.Load(), n)
	}
}

func TestSource_TargetInfoRotationDoesNotDisturbTheOtherMirrors(t *testing.T) {
	// A target gateway restarting republishes its info on a fresh
	// address. Rebuilding the whole source for that would drop every
	// other mirror of the flow, so the rotation is a RemoveTarget plus
	// an AddTarget on the initiator they all share.
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
	assert.Equal(t, []string{"info-c"}, f.opener.removedTargets(),
		"only the rotating mirror's stale target may be removed")
	assert.Equal(t, []string{"info-b", "info-c", "info-c2"}, f.opener.addedTargets())
	assert.Same(t, s, f.sharedFor("flow-1", fabrics.ProviderTCP))
	assert.True(t, loopRunning(s))
	assert.Same(t, m1Entry, f.entry("m1"),
		"the untouched mirror must keep the entry it was transferring through")
	assert.Equal(t, "info-c2", f.entry("m2").infoStr)
}

func TestSource_ReaderRebuildReopensTheSourceForEveryAttachedMirror(t *testing.T) {
	// The stall watchdog tears down the reader, and the reader is
	// shared, so every mirror on it loses its target. Leaving them to
	// a watch would strand them: the events that would wake them --
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

	f.r.rebuildWedgedReader(context.Background(),
		types.NamespacedName{Namespace: "ns1", Name: "m1"})

	assert.Equal(t, int32(2), f.opener.opens.Load(),
		"the wedged reader must be reopened exactly once for the whole flow")
	assert.False(t, loopRunning(first), "the wedged transfer loop must be stopped")
	second := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, second)
	assert.NotSame(t, first, second)
	assert.Len(t, second.attached(), 2,
		"both mirrors must be re-added to the fresh initiator")
	assert.Equal(t, []string{"info-b", "info-c", "info-b", "info-c"}, f.opener.addedTargets())
}

func TestSource_ReaderRebuildBudgetIsSpentPerSharedSource(t *testing.T) {
	// The budget bounds reopens of a reader, and one reader now
	// carries every mirror of the flow. Counting it per mirror would
	// hand a flow mirrored to N nodes N times the cap on the one
	// reader they share, which is the cap not applying at all.
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
	// A source that opened a reader and then could not add its first
	// target holds a flow open for a mirror that is transferring
	// nothing, and libmxl reclaims a flow only once no handle denies
	// it the lock.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
	)
	f.opener.attachFn = func(*sharedSource, string) (*fabrics.TargetInfo, error) {
		return nil, fmt.Errorf("%w: connect refused", errAddTargetFailed)
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	res, err := f.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter)
	assert.Nil(t, f.sharedFor("flow-1", fabrics.ProviderTCP),
		"a source with no target must not keep the flow's reader open")
}

func TestSource_FailedAddTargetLeavesAnInUseSourceAlone(t *testing.T) {
	// The same release must not reach a source another mirror is
	// already transferring through.
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
		fanoutMirror("m2", "flow-1", "node-c", "info-c", mxlv1alpha1.ProviderTCP),
	)
	f.reconcile(t, "m1")
	s := f.sharedFor("flow-1", fabrics.ProviderTCP)
	require.NotNil(t, s)

	f.opener.attachFn = func(*sharedSource, string) (*fabrics.TargetInfo, error) {
		return nil, fmt.Errorf("%w: connect refused", errAddTargetFailed)
	}
	f.reconcile(t, "m2")

	assert.Same(t, s, f.sharedFor("flow-1", fabrics.ProviderTCP),
		"one mirror's AddTarget failure must not close the reader another is using")
	assert.True(t, loopRunning(s))
	assert.Len(t, s.attached(), 1)
}
