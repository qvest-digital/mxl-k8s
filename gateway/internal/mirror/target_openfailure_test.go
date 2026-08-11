package mirror

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const openFailureFlowID = "11111111-2222-3333-4444-555555555555"

// openFailureFixture wires a target reconciler whose open always fails
// with openErr, against a mirror and a published MxlFlow whose local
// location carries locPhase.
type openFailureFixture struct {
	r      *TargetReconciler
	key    types.NamespacedName
	req    reconcile.Request
	domain string
	opens  int

	// advance moves the reconciler's clock. Consecutive open attempts
	// are separated by the open backoff now that it is enforced rather
	// than advised, so a test driving several has to let it elapse the
	// way the requeue does in production.
	advance func(time.Duration)
}

// advancePastBackoff releases the open gate however long the last
// failure earned, without a test having to track the attempt count.
func (f *openFailureFixture) advancePastBackoff() {
	f.advance(backoffFor(maxTargetOpenAttempts) + time.Second)
}

func newOpenFailureFixture(t *testing.T, openErr error, locPhase mxlv1alpha1.MxlFlowLocationPhase) *openFailureFixture {
	t.Helper()
	scheme := newSourceTestScheme(t)
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", openFailureFlowID,
		mxlv1alpha1.MxlFlowMirrorStatus{})
	mirror.Spec.Provider = mxlv1alpha1.ProviderTCP
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: openFailureFlowID},
		Spec: mxlv1alpha1.MxlFlowSpec{
			ID:         openFailureFlowID,
			Definition: runtime.RawExtension{Raw: []byte(`{"id":"` + openFailureFlowID + `"}`)},
		},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{NodeName: "node-a", Phase: locPhase}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, flow).
		Build()

	clock, advance := fixedClock(time.Now())
	f := &openFailureFixture{
		key:     key,
		req:     reconcile.Request{NamespacedName: key},
		domain:  t.TempDir(),
		advance: advance,
	}
	f.r = &TargetReconciler{
		nowFn:      clock,
		Client:     c,
		Scheme:     scheme,
		NodeName:   "node-a",
		DomainPath: f.domain,
		targets:    map[types.NamespacedName]*targetEntry{},
		attempts:   attemptTable[targetOpenInputs]{},
		openTargetFn: func(types.NamespacedName, string, fabrics.Provider) (*targetEntry, error) {
			f.opens++
			return nil, openErr
		},
	}
	return f
}

// writeTornFlowDir lays down a flow directory shaped like the one a
// gateway restart leaves behind: flow_def.json and a grains/ directory
// holding fewer segments than the ring needs.
func writeTornFlowDir(t *testing.T, domain, flowID string, grains int) string {
	t.Helper()
	dir := filepath.Join(domain, flowID+flowDirSuffix)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, grainDirName), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flow_def.json"), []byte(`{}`), 0o644))
	for i := range grains {
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, grainDirName, fmt.Sprintf("grain.%d", i)), []byte("x"), 0o644))
	}
	return dir
}

func (f *openFailureFixture) mirror(t *testing.T) mxlv1alpha1.MxlFlowMirror {
	t.Helper()
	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, f.r.Get(context.Background(), f.key, &got))
	return got
}

func TestTarget_OpenFailureEscalatesOutOfMaterializing(t *testing.T) {
	// A target that cannot be opened used to sit at Materializing with
	// no counter for as long as the failure lasted, which reads exactly
	// like a mirror a second away from Ready: nothing to alert on and
	// nothing to threshold. Consecutive failures must be counted and
	// must escalate the phase once they rule out a transient fault.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)

	for attempt := uint32(1); attempt < maxTargetOpenAttempts; attempt++ {
		res, err := f.r.Reconcile(context.Background(), f.req)
		require.NoError(t, err)
		assert.Positive(t, res.RequeueAfter,
			"a failed open must come back as a bounded-backoff requeue")
		f.advancePastBackoff()

		got := f.mirror(t)
		assert.Equal(t, mxlv1alpha1.MxlFlowMirrorMaterializing, got.Status.Phase,
			"the first failures are still indistinguishable from a slow "+
				"handshake, so the mirror stays Materializing")
		assert.Equal(t, int32(attempt), got.Status.TargetAttemptCount,
			"every failed open must advance the counter, otherwise a wedged "+
				"mirror is indistinguishable from one that just started")
	}

	_, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)

	got := f.mirror(t)
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorDegraded, got.Status.Phase,
		"past maxTargetOpenAttempts the mirror must leave Materializing; "+
			"a consumer waits through Materializing forever otherwise")
	assert.Equal(t, int32(maxTargetOpenAttempts), got.Status.TargetAttemptCount)
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, mxlv1alpha1.ReasonOpenTargetFailed, got.Status.Conditions[0].Reason)
	assert.Equal(t, metav1.ConditionFalse, got.Status.Conditions[0].Status)
}

func TestTarget_OpenAttemptCountSeededFromStatus(t *testing.T) {
	// The counter lives in memory, so a gateway restart would otherwise
	// hand a mirror that has been unopenable across the bounce a fresh
	// budget and drop it back to Materializing.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)

	m := f.mirror(t)
	m.Status.TargetAttemptCount = int32(maxTargetOpenAttempts) - 1
	require.NoError(t, f.r.Status().Update(context.Background(), &m))

	_, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)

	got := f.mirror(t)
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorDegraded, got.Status.Phase,
		"the persisted count must be restored before the escalation "+
			"threshold is applied, or a restart resets the escalation")
	assert.Equal(t, int32(maxTargetOpenAttempts), got.Status.TargetAttemptCount)
}

func TestTarget_WriterFailureReclaimsFlowDirAtThreshold(t *testing.T) {
	// A gateway restart part-way through materialising a target leaves
	// a flow directory whose grain ring is short of what its header
	// declares. Every later open of that path fails, so retrying
	// against the same directory can only keep failing: the directory
	// this gateway wrote as a mirror copy has to go so the next pass
	// materialises a complete one.
	f := newOpenFailureFixture(t,
		fmt.Errorf("%w: NewWriter: mxl: unknown error", errOpenWriterFailed),
		mxlv1alpha1.MxlFlowLocationReady)
	dir := writeTornFlowDir(t, f.domain, openFailureFlowID, 46)

	for attempt := uint32(1); attempt < maxTargetOpenAttempts; attempt++ {
		_, err := f.r.Reconcile(context.Background(), f.req)
		require.NoError(t, err)
		assert.DirExists(t, dir,
			"a failure that has not yet repeated must not cost a directory")
		f.advancePastBackoff()
	}

	res, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	assert.NoDirExists(t, dir,
		"at the threshold the unopenable directory must be removed; "+
			"leaving it makes every later NewWriter fail the same way")
	assert.Positive(t, res.RequeueAfter)
	assert.Less(t, res.RequeueAfter, backoffFor(maxTargetOpenAttempts),
		"the retry that materialises a fresh directory must not sit out "+
			"the accumulated backoff first")
}

func TestTarget_WriterFailureKeepsOriginFlowDir(t *testing.T) {
	// A flow this node is Origin for belongs to a local producer that
	// took the directory over from the mirror. Removing it would take
	// the producer's flow with it, so the open keeps failing rather
	// than the gateway destroying something it does not own.
	f := newOpenFailureFixture(t,
		fmt.Errorf("%w: NewWriter: mxl: unknown error", errOpenWriterFailed),
		mxlv1alpha1.MxlFlowLocationOrigin)
	dir := writeTornFlowDir(t, f.domain, openFailureFlowID, 46)

	for range maxTargetOpenAttempts + 1 {
		_, err := f.r.Reconcile(context.Background(), f.req)
		require.NoError(t, err)
		f.advancePastBackoff()
	}

	assert.DirExists(t, dir,
		"an Origin location on this node makes the directory the "+
			"producer's, not the mirror's")
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorDegraded, f.mirror(t).Status.Phase,
		"the failure still has to escalate; only the reclaim is withheld")
}

func TestTarget_FabricFailureKeepsFlowDir(t *testing.T) {
	// Only a writer that will not open says anything about the
	// directory. A libmxl-fabrics setup failure is about the endpoint,
	// and removing the flow file over it would invalidate every
	// consumer FlowReader on the node for nothing.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)
	dir := writeTornFlowDir(t, f.domain, openFailureFlowID, 59)

	for range maxTargetOpenAttempts + 1 {
		_, err := f.r.Reconcile(context.Background(), f.req)
		require.NoError(t, err)
		f.advancePastBackoff()
	}

	assert.DirExists(t, dir,
		"a fabric-side failure must not be answered by deleting the "+
			"local flow directory")
}

func TestTarget_OpenSuccessClearsAttemptCount(t *testing.T) {
	// The counter is the length of the current failure run, so a target
	// that opens must publish zero: a stale count would keep an alert
	// on a mirror that has recovered.
	f := newOpenFailureFixture(t, errors.New("Target.Setup: mxl: unknown error"),
		mxlv1alpha1.MxlFlowLocationReady)

	_, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	require.Equal(t, int32(1), f.mirror(t).Status.TargetAttemptCount)

	info := "info-1"
	f.r.openTargetFn = func(types.NamespacedName, string, fabrics.Provider) (*targetEntry, error) {
		return &targetEntry{infoStr: info}, nil
	}
	// The open that succeeds only runs once the failure's backoff has
	// elapsed; inside it the reconciler does not attempt at all.
	f.advancePastBackoff()
	// A successful Reconcile starts the per-mirror flusher; drop the
	// entry so the goroutine is joined before goleak inspects.
	t.Cleanup(func() { f.r.closeEntry(f.key, keepFlow) })
	_, err = f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)

	got := f.mirror(t)
	assert.Equal(t, mxlv1alpha1.MxlFlowMirrorReady, got.Status.Phase)
	assert.Zero(t, got.Status.TargetAttemptCount,
		"an open target must reset the count, otherwise the mirror keeps "+
			"reporting a failure run that has ended")
}

func TestReclaimUnusableFlowDir_MissingDirectory(t *testing.T) {
	// Nothing on disk means nothing to reclaim, and the caller must not
	// treat that as a fresh start it can retry immediately.
	scheme := newSourceTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &TargetReconciler{
		Client:     c,
		Scheme:     scheme,
		NodeName:   "node-a",
		DomainPath: t.TempDir(),
	}
	assert.False(t, r.reclaimUnusableFlowDir(context.Background(), openFailureFlowID))
}

func TestReclaimUnusableFlowDir_UnsetDomainPath(t *testing.T) {
	// Without a domain path there is no directory the gateway can name,
	// so the reclaim must decline rather than resolve a relative one.
	scheme := newSourceTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &TargetReconciler{Client: c, Scheme: scheme, NodeName: "node-a"}
	assert.False(t, r.reclaimUnusableFlowDir(context.Background(), openFailureFlowID))
}
