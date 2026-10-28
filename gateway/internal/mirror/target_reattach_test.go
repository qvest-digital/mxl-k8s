package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// armSourceReattach puts the fixture in the state a source gateway
// restart leaves the target in: grains landed on this fabric side and
// stopped two seconds ago, the source keeps sending into the
// connection it cannot make, and it added the target addedAfterCommit
// after the last grain. The stuck-handshake window is the production
// one, so neither of the older wedge shapes can fire inside a test.
func (f *targetWatchdogFixture) armSourceReattach(t *testing.T, provider fabrics.Provider, addedAfterCommit time.Duration) time.Time {
	t.Helper()
	f.r.StuckHandshakeAfter = defaultStuckHandshakeAfter
	f.entry.provider = provider
	now := time.Now()
	f.entry.commitsAtFabricOpen.Store(0)
	f.entry.commits.Store(1)
	commitAt := now.Add(-2 * time.Second)
	f.entry.lastCommitAt.Store(&commitAt)

	var current mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.c.Get(context.Background(), f.key, &current))
	current.Status.LastSentAt = &metav1.Time{Time: now}
	current.Status.TargetAddedAt = &metav1.Time{Time: commitAt.Add(addedAfterCommit)}
	require.NoError(t, f.c.Status().Update(context.Background(), &current))
	return commitAt
}

func (f *targetWatchdogFixture) requireNoSpawn(t *testing.T, msg string) {
	t.Helper()
	select {
	case <-f.spawnCh:
		t.Fatal(msg)
	case <-time.After(200 * time.Millisecond):
	}
	assert.Zero(t, f.entry.recoveryAttempts.Load())
}

// A connection-based target that has accepted a connection listens
// on a new port once it goes, and the TargetInfo it published names
// the old one. A restarted source gateway dials that and is refused,
// so the target has to set up again; waiting for the stuck-handshake
// window held every mirror out of a restarted source for over 20 s.
func TestRunFlusher_SourceReattached_SetsTheTargetUpAgain(t *testing.T) {
	for _, p := range []fabrics.Provider{fabrics.ProviderVerbs, fabrics.ProviderTCP} {
		t.Run(p.String(), func(t *testing.T) {
			f := newTargetWatchdogFixture(t, 4)
			f.armSourceReattach(t, p, time.Second)
			f.startFlusher(t)
			defer f.stopFlusher(t)

			select {
			case <-f.spawnCh:
			case <-time.After(time.Second):
				t.Fatal("a source that added the target after its last grain did not get the target set up again")
			}
		})
	}
}

// A connectionless target keeps its address, and the restarted source
// reaches it as it is.
func TestRunFlusher_SourceReattached_LeavesAConnectionlessTarget(t *testing.T) {
	f := newTargetWatchdogFixture(t, 4)
	f.armSourceReattach(t, fabrics.ProviderEFA, time.Second)
	f.startFlusher(t)
	defer f.stopFlusher(t)

	f.requireNoSpawn(t, "an efa target was set up again for a source that can reach it")
}

// The source adds the target again after the target set up again, and
// grains land after that add. Nothing is wrong.
func TestRunFlusher_SourceAddedBeforeTheLastGrain_DoesNotTrigger(t *testing.T) {
	f := newTargetWatchdogFixture(t, 4)
	f.armSourceReattach(t, fabrics.ProviderVerbs, -time.Second)
	f.startFlusher(t)
	defer f.stopFlusher(t)

	f.requireNoSpawn(t, "a target whose grains postdate the add was set up again")
}

// On a fabric side no grain has reached yet, the initiator that added
// the target is the one it is waiting for.
func TestRunFlusher_SourceAddedBeforeTheFirstGrain_DoesNotTrigger(t *testing.T) {
	f := newTargetWatchdogFixture(t, 4)
	f.armSourceReattach(t, fabrics.ProviderVerbs, time.Second)
	f.entry.commitsAtFabricOpen.Store(f.entry.commits.Load())
	// Opened inside the stuck-handshake window, so neverHandshook
	// stays out of it.
	opened := time.Now().Add(-time.Second)
	f.entry.fabricOpenedAt.Store(&opened)
	f.startFlusher(t)
	defer f.stopFlusher(t)

	f.requireNoSpawn(t, "a target still waiting for its first connection was set up again")
}

// Grains still landing say the connection is live even when the
// source's clock puts its add after them.
func TestRunFlusher_SourceAddedWhileGrainsLand_DoesNotTrigger(t *testing.T) {
	f := newTargetWatchdogFixture(t, 4)
	f.r.FlushInterval = 20 * time.Millisecond
	f.armSourceReattach(t, fabrics.ProviderVerbs, 3*time.Second)
	landing := make(chan struct{})
	go func() {
		defer close(landing)
		for end := time.Now().Add(300 * time.Millisecond); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			now := time.Now()
			f.entry.lastCommitAt.Store(&now)
		}
	}()
	f.startFlusher(t)
	defer f.stopFlusher(t)

	f.requireNoSpawn(t, "a target with grains landing was set up again")
	<-landing
}

// The target reads the add from status.targetAddedAt, so every source
// publish carries it.
func TestRunFlusher_PublishesTargetAddedAt(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	flowID := "flow-1"
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", flowID, "info-1")
	opened := metav1.NewTime(time.Now().Add(-time.Minute))
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{
				NodeName:     "node-a",
				Phase:        mxlv1alpha1.MxlFlowLocationOrigin,
				LastObserved: &opened,
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, flow).
		Build()
	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		opener: &fakeOpener{openFn: func(string, fabrics.Provider) (*sharedSource, error) {
			return &sharedSource{}, nil
		}},
		FlushInterval: time.Millisecond,
		sources:       map[types.NamespacedName]*sourceEntry{},
		attempts:      attemptTable[sourceAddInputs]{},
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	t.Cleanup(func() { r.closeEntry(key) })
	r.mu.Lock()
	entry := r.sources[key]
	r.mu.Unlock()
	require.NotNil(t, entry)

	var got mxlv1alpha1.MxlFlowMirror
	require.Eventually(t, func() bool {
		return c.Get(context.Background(), key, &got) == nil && got.Status.TargetAddedAt != nil
	}, 5*time.Second, 5*time.Millisecond, "the source never published when it added the target")
	assert.True(t, got.Status.TargetAddedAt.Time.Equal(entry.openedAt.Truncate(time.Second)),
		"published %s for a target added at %s", got.Status.TargetAddedAt.Time, entry.openedAt)
}
