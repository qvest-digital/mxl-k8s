package mirror

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A re-add that holds its worker for seconds has to say where the time
// went, or the next slow restart is diagnosed by guesswork. The line
// reporting the target added carries each stage's duration.
func TestSourceReconcile_TargetAddedReportsStageDurations(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("m1", "flow-1", "node-b", "info-b", mxlv1alpha1.ProviderTCP),
	)
	f.opener.openFn = func(string, fabrics.Provider) (*sharedSource, error) {
		time.Sleep(60 * time.Millisecond)
		return runningSource(), nil
	}
	var mu sync.Mutex
	var added map[string]any
	sink := funcr.NewJSON(func(obj string) {
		var m map[string]any
		if json.Unmarshal([]byte(obj), &m) == nil && m["msg"] == "target added" {
			mu.Lock()
			added = m
			mu.Unlock()
		}
	}, funcr.Options{})
	ctx := log.IntoContext(context.Background(), sink)

	_, err := f.r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "m1"}})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, added, "no target added line")
	for _, stage := range []string{"took", "lookup", "close", "liveCheck", "open", "attach", "openReader", "openSetup"} {
		require.Contains(t, added, stage)
	}
	open, err := time.ParseDuration(added["open"].(string))
	require.NoError(t, err)
	require.GreaterOrEqual(t, open, 60*time.Millisecond,
		"the open stage must carry the time the open took")
}

// After a target gateway restart the target publishes a new TargetInfo
// while the source still holds the entry it added against the old one,
// queued behind other re-adds. The shared source goes on transferring
// to its other targets and stamps lastSentAt on this entry too, and
// libmxl-fabrics accepts those transfers for a target it is not
// connected to. Publishing that lastSentAt would tell the target a
// source is sending into its new fabric side before any source has
// been added to it, and the target would set itself up again before the
// re-add lands.
func TestRunFlusher_EntryOnAReplacedTargetInfo_PublishesNothing(t *testing.T) {
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "info-new")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()
	r := &SourceReconciler{
		Client:   c,
		Scheme:   scheme,
		NodeName: "node-a",
		sources:  map[types.NamespacedName]*sourceEntry{},
		attempts: attemptTable[sourceAddInputs]{},
	}
	entry := &sourceEntry{key: key, shared: testShared("flow-1", fabrics.ProviderTCP), infoStr: "info-old"}
	entry.progress.Store(1)
	sent := time.Now()
	entry.lastSentAt.Store(&sent)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go r.runFlusher(ctx, done, entry, 2*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &got))
	require.Nil(t, got.Status.LastSentAt,
		"an entry added against a replaced TargetInfo published lastSentAt")
}
