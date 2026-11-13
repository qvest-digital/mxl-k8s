package mirror

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A live target bound to one interface whose mirror the operator placed
// on another rebuilds its fabric side on the placed address. Placement
// changes only when the bound interface went away or the placement was
// cleared, so staying would leave the mirror where nothing plans it.
func TestReconcile_PlacementMoveRebuildsOnThePlacedInterface(t *testing.T) {
	f := newRepointFixture(t, "node-x", mxlv1alpha1.ProviderTCP)
	f.entry.boundInterface = "192.0.2.1"
	wants := make(chan string, 4)
	f.r.openFabricSideFn = func(_ *mxl.Writer, p fabrics.Provider, want string) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
		wants <- want
		return nil, nil, "", errors.New("rebuild refused")
	}
	place(t, f.r.Client, f.req.NamespacedName, "192.0.2.1")

	_, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	select {
	case w := <-wants:
		t.Fatalf("a placement naming the bound interface rebuilt the target on %s", w)
	case <-time.After(200 * time.Millisecond):
	}

	place(t, f.r.Client, f.req.NamespacedName, "192.0.2.129")
	res, err := f.r.Reconcile(context.Background(), f.req)
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter)
	select {
	case w := <-wants:
		assert.Equal(t, "192.0.2.129", w)
	case <-time.After(5 * time.Second):
		t.Fatal("a moved placement never rebuilt the target")
	}
	require.Eventually(t, func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		return f.r.targets[f.req.NamespacedName] == nil && !f.entry.recovering.Load()
	}, 5*time.Second, 10*time.Millisecond)
}

func place(t *testing.T, c client.Client, key types.NamespacedName, target string) {
	t.Helper()
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), key, &m))
	m.Spec.Placement = &mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "192.0.2.2", TargetInterface: target}
	require.NoError(t, c.Update(context.Background(), &m))
}

// awaitFixture is an unplaced mirror on a node advertising n tcp
// interfaces, created at created.
func awaitFixture(t *testing.T, n int, created time.Time, conds ...metav1.Condition) (*TargetReconciler, *mxlv1alpha1.MxlFlowMirror) {
	t.Helper()
	scheme := newSourceTestScheme(t)
	m := mirrorWithTargetFinalizer("m1", "ns1", "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{Conditions: conds})
	m.Spec.Provider = mxlv1alpha1.ProviderTCP
	m.CreationTimestamp = metav1.NewTime(created)
	caps := &mxlv1alpha1.MxlNodeCapabilities{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	var ifs []mxlv1alpha1.MxlFabricInterface
	for i := range n {
		ifs = append(ifs, mxlv1alpha1.MxlFabricInterface{Address: []string{"192.0.2.1", "192.0.2.129", "192.0.2.65"}[i]})
	}
	caps.Status.Providers = []mxlv1alpha1.MxlFabricsProviderCapability{{Name: mxlv1alpha1.ProviderTCP, Interfaces: ifs}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlNodeCapabilities{}).
		WithObjects(m, caps).Build()
	now := time.Unix(1_000_002, 0)
	return &TargetReconciler{Client: c, NodeName: "node-a", PlacementWait: 10 * time.Second,
		nowFn: func() time.Time { return now }}, m
}

func TestAwaitPlacement(t *testing.T) {
	ctx := context.Background()
	created := time.Unix(1_000_000, 0)

	r, m := awaitFixture(t, 2, created)
	res, wait := r.awaitPlacement(ctx, m, fabrics.ProviderTCP)
	assert.True(t, wait, "two interfaces and no placement: wait for the operator")
	assert.Equal(t, 8*time.Second, res.RequeueAfter, "until the wait since creation runs out")

	r, m = awaitFixture(t, 1, created)
	_, wait = r.awaitPlacement(ctx, m, fabrics.ProviderTCP)
	assert.False(t, wait, "one interface has nothing to choose")

	r, m = awaitFixture(t, 2, created.Add(-time.Minute))
	_, wait = r.awaitPlacement(ctx, m, fabrics.ProviderTCP)
	assert.False(t, wait, "an operator that has not placed within the wait is not waited for")

	r, m = awaitFixture(t, 2, created)
	m.Spec.Placement = &mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: "192.0.2.2", TargetInterface: "192.0.2.129"}
	_, wait = r.awaitPlacement(ctx, m, fabrics.ProviderTCP)
	assert.False(t, wait)

	r, m = awaitFixture(t, 2, created, metav1.Condition{Type: mxlv1alpha1.ConditionTypePlaced, Status: metav1.ConditionFalse,
		Reason: mxlv1alpha1.ReasonNoInterface})
	_, wait = r.awaitPlacement(ctx, m, fabrics.ProviderTCP)
	assert.False(t, wait, "the operator found no pair; nothing is coming")

	r, m = awaitFixture(t, 2, created)
	r.PlacementWait = 0
	_, wait = r.awaitPlacement(ctx, m, fabrics.ProviderTCP)
	assert.False(t, wait, "zero turns the wait off")

	r, m = awaitFixture(t, 2, created.Add(-time.Hour), metav1.Condition{Type: mxlv1alpha1.ConditionTypePlaced,
		Status: metav1.ConditionFalse, Reason: mxlv1alpha1.ReasonHeld, Message: "No interface pair has room"})
	res, wait = r.awaitPlacement(ctx, m, fabrics.ProviderTCP)
	assert.True(t, wait, "a held mirror is never set up")
	assert.Zero(t, res.RequeueAfter, "the condition changing is what wakes it")
	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(m), &got))
	c := meta.FindStatusCondition(got.Status.Conditions, mxlv1alpha1.ConditionTypeTargetProgress)
	require.NotNil(t, c)
	assert.Equal(t, mxlv1alpha1.ReasonHeld, c.Reason)
}
