package receiver_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/receiver"
	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

// flowIDFor derives a deterministic UUID-shaped string from the
// current test's name so the cluster-scoped MxlFlow does not collide
// across the suite's tests. SHA-256 keeps the result reproducible
// across runs; t.Cleanup deletes the flow when the test exits.
func flowIDFor(t *testing.T) string {
	t.Helper()
	h := sha256.Sum256([]byte(t.Name()))
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(h[0:4]),
		binary.BigEndian.Uint16(h[4:6]),
		binary.BigEndian.Uint16(h[6:8]),
		binary.BigEndian.Uint16(h[8:10]),
		h[10:16])
}

// newFlow creates a cluster-scoped MxlFlow with a per-test UUID,
// optionally pins its Origin location, and registers cleanup so the
// flow does not leak into the next test.
func newFlow(t *testing.T, originNode string) *mxlv1alpha1.MxlFlow {
	t.Helper()
	id := flowIDFor(t)
	flow := testutil.NewFlow(testutil.WithFlowID(id))
	require.NoError(t, env.Client.Create(context.Background(), flow))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = env.Client.Delete(ctx, &mxlv1alpha1.MxlFlow{
			ObjectMeta: metav1.ObjectMeta{Name: id},
		})
	})
	if originNode != "" {
		require.NoError(t, withFlowOriginStatus(env.Client, flow, originNode))
	}
	return flow
}

// All envtest scenarios share the package-level env spun up by
// TestMain (see suite_test.go). Each Test* takes its own namespace so
// the assertions are isolated from sibling tests.

func reconcile(t *testing.T, r *receiver.Reconciler, ns, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	require.NoError(t, err)
	return res
}

func mustGetReceiver(t *testing.T, c client.Client, ns, name string) *mxlv1alpha1.MxlReceiver {
	t.Helper()
	var out mxlv1alpha1.MxlReceiver
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: name}, &out))
	return &out
}

func TestReconcile_NoMatchingPods_MarksPendingAndRequeues(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	rec := testutil.NewReceiver(ns, "r")
	require.NoError(t, env.Client.Create(context.Background(), rec))

	res := reconcile(t, r, ns, "r")
	assert.Equal(t, 10*time.Second, res.RequeueAfter,
		"the requeue interval is the contract every consumer expects; "+
			"shrinking it would burn the operator's leader-election lease, "+
			"growing it would slow first-grain visibility on a cold cluster")

	got := mustGetReceiver(t, env.Client, ns, "r")
	assert.Equal(t, mxlv1alpha1.MxlReceiverPending, got.Status.Phase)
	assert.Nil(t, got.Status.BoundMirror)

	// And no mirror was created.
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &mirrors, client.InNamespace(ns)))
	assert.Empty(t, mirrors.Items)
}

func TestReconcile_PodsButNoFlow_MarksPendingAndRequeues(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(context.Background(), testutil.NewPod(ns, "consumer-a", "worker-1")))
	require.NoError(t, env.Client.Create(context.Background(), testutil.NewReceiver(ns, "r")))

	res := reconcile(t, r, ns, "r")
	assert.Equal(t, 10*time.Second, res.RequeueAfter)
	assert.Equal(t, mxlv1alpha1.MxlReceiverPending, mustGetReceiver(t, env.Client, ns, "r").Status.Phase)

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &mirrors, client.InNamespace(ns)))
	assert.Empty(t, mirrors.Items,
		"a missing flow must not produce a half-formed mirror; the receiver "+
			"reconciler is the only writer of MxlFlowMirror and an early "+
			"create would deadlock the gateway lifecycle")
}

func TestReconcile_DistinctSourceAndTarget_CreatesOneMirror(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	// Source pod sits on worker-source; target pod on worker-target.
	// The flow advertises worker-source as Origin.
	require.NoError(t, env.Client.Create(context.Background(), testutil.NewPod(ns, "consumer-a", "worker-target")))

	flow := newFlow(t, "worker-source")
	require.NoError(t, env.Client.Create(context.Background(),
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))

	res := reconcile(t, r, ns, "r")
	assert.Equal(t, ctrl.Result{}, res, "Bound state should not requeue")

	rec := mustGetReceiver(t, env.Client, ns, "r")
	assert.Equal(t, mxlv1alpha1.MxlReceiverBound, rec.Status.Phase)
	require.NotNil(t, rec.Status.BoundMirror, "Bound state must carry a mirror reference")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	mirror := mirrors.Items[0]
	assert.Equal(t, "worker-source", mirror.Spec.SourceNode)
	assert.Equal(t, "worker-target", mirror.Spec.TargetNode)
	assert.Equal(t, flow.Spec.ID, mirror.Spec.FlowID)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, mirror.Spec.Provider,
		"the receiver's Provider must flow through to the mirror; the "+
			"gateway selects libfabric provider from this field")
}

func TestReconcile_SameSourceAndTarget_SkipsMirrorCreation(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	// Pod on the same node as the flow's origin: no mirror needed.
	require.NoError(t, env.Client.Create(context.Background(), testutil.NewPod(ns, "consumer-a", "worker-shared")))

	flow := newFlow(t, "worker-shared")
	require.NoError(t, env.Client.Create(context.Background(),
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))

	res := reconcile(t, r, ns, "r")
	assert.Equal(t, ctrl.Result{}, res)

	rec := mustGetReceiver(t, env.Client, ns, "r")
	assert.Equal(t, mxlv1alpha1.MxlReceiverBound, rec.Status.Phase)
	assert.Nil(t, rec.Status.BoundMirror,
		"same-node skip leaves the receiver Bound but without a mirror ref; "+
			"the consumer reads the local flow directly without going through libfabric")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &mirrors, client.InNamespace(ns)))
	assert.Empty(t, mirrors.Items)
}

func TestReconcile_MultipleTargetNodes_CreatesOneMirrorPerNode(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(context.Background(), testutil.NewPod(ns, "a", "n-b")))
	require.NoError(t, env.Client.Create(context.Background(), testutil.NewPod(ns, "b", "n-c")))
	// A duplicate consumer on n-b must not produce a second mirror.
	require.NoError(t, env.Client.Create(context.Background(), testutil.NewPod(ns, "c", "n-b")))

	flow := newFlow(t, "n-source")
	require.NoError(t, env.Client.Create(context.Background(),
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))

	res := reconcile(t, r, ns, "r")
	assert.Equal(t, ctrl.Result{}, res)

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 2)

	gotNodes := map[string]struct{}{}
	for _, m := range mirrors.Items {
		gotNodes[m.Spec.TargetNode] = struct{}{}
	}
	assert.Equal(t,
		map[string]struct{}{"n-b": {}, "n-c": {}},
		gotNodes,
		"two distinct target nodes -> two mirrors; the duplicate pod on n-b must collapse")
}

func TestReconcile_IsIdempotent_DoesNotRecreateExistingMirror(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(context.Background(), testutil.NewPod(ns, "consumer-a", "worker-target")))
	flow := newFlow(t, "worker-source")
	require.NoError(t, env.Client.Create(context.Background(),
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))

	// First reconcile creates the mirror.
	reconcile(t, r, ns, "r")
	var before mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &before, client.InNamespace(ns)))
	require.Len(t, before.Items, 1)
	firstUID := before.Items[0].UID

	// Second reconcile must reuse the same mirror (same UID), not
	// produce a new one. The reconciler picks names deterministically;
	// any drift would either error on AlreadyExists or replace the
	// running mirror, both of which would interrupt the data plane.
	reconcile(t, r, ns, "r")
	var after mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &after, client.InNamespace(ns)))
	require.Len(t, after.Items, 1)
	assert.Equal(t, firstUID, after.Items[0].UID)
}

func TestReconcile_DeletionTimestamp_NoOps(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	// Create a receiver carrying a finalizer so it lingers under
	// deletion, then issue Delete; envtest will set the deletion
	// timestamp and keep the object around for the reconciler to see.
	rec := testutil.NewReceiver(ns, "r")
	rec.Finalizers = []string{"test.mxl.qvest-digital.com/keepalive"}
	require.NoError(t, env.Client.Create(context.Background(), rec))
	require.NoError(t, env.Client.Delete(context.Background(), rec))

	res := reconcile(t, r, ns, "r")
	assert.Equal(t, ctrl.Result{}, res,
		"a receiver mid-deletion must not be requeued or mutated; the "+
			"reconciler's only safe action here is to walk away")

	// And no mirror should have been created.
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(context.Background(), &mirrors, client.InNamespace(ns)))
	assert.Empty(t, mirrors.Items)
}

func TestReconcile_NotFound_ReturnsClean(t *testing.T) {
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: "does-not-exist"},
	})
	require.NoError(t, err,
		"a Reconcile call for a deleted CR must not bubble up the 404; "+
			"controller-runtime requeues errors aggressively and would tight-loop")
	assert.Equal(t, ctrl.Result{}, res)
}

// withFlowOriginStatus updates the given flow's status subresource so
// it advertises the given node as Origin. The status subresource is
// separate from the spec; the test client must Update through .Status().
func withFlowOriginStatus(c client.Client, flow *mxlv1alpha1.MxlFlow, node string) error {
	var live mxlv1alpha1.MxlFlow
	if err := c.Get(context.Background(), types.NamespacedName{Name: flow.Name}, &live); err != nil {
		return err
	}
	live.Status.Locations = []mxlv1alpha1.MxlFlowLocation{
		{NodeName: node, Phase: mxlv1alpha1.MxlFlowLocationOrigin},
	}
	return c.Status().Update(context.Background(), &live)
}

func TestReceiver_ReconvergesAfterProducerReschedule(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "worker-target")))

	flow := newFlow(t, "worker-source-1")
	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))

	// First pass: mirror points at the original source node.
	reconcile(t, r, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	assert.Equal(t, "worker-source-1", mirrors.Items[0].Spec.SourceNode)
	originalUID := mirrors.Items[0].UID

	// Producer reschedules to a new node: flow rewrites its Origin
	// location. The receiver must update the existing mirror in
	// place (same UID) so the gateway can re-open the initiator
	// against the new source.
	require.NoError(t, withFlowOriginStatus(env.Client, flow, "worker-source-2"))
	reconcile(t, r, ns, "r")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1,
		"the producer reschedule must rewrite the existing mirror, not "+
			"create a second one; a delete+create would interrupt the data plane")
	assert.Equal(t, originalUID, mirrors.Items[0].UID,
		"merge-patch must keep the same object UID; a destroy+create cycle "+
			"would tear down the running fabrics initiator on the gateway side")
	assert.Equal(t, "worker-source-2", mirrors.Items[0].Spec.SourceNode)
}

func TestReceiver_GCDeletesOrphanMirrorsAfterPodMove(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	pod := testutil.NewPod(ns, "consumer-a", "node-a")
	require.NoError(t, env.Client.Create(ctx, pod))

	flow := newFlow(t, "node-src")
	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))

	reconcile(t, r, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	assert.Equal(t, "node-a", mirrors.Items[0].Spec.TargetNode)

	// Pod moves to a new node. The orphan mirror on node-a must
	// disappear and a fresh mirror for node-b must appear; otherwise
	// the gateway on node-a would keep holding the libfabric target
	// open for a consumer that no longer reads it. Force grace=0
	// because envtest has no kubelet to finalize the pod removal;
	// without it the pod would linger with DeletionTimestamp set and
	// the receiver would see both pods at once.
	zero := int64(0)
	require.NoError(t, env.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero}))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-b")))
	reconcile(t, r, ns, "r")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	gotTargets := map[string]struct{}{}
	for _, m := range mirrors.Items {
		gotTargets[m.Spec.TargetNode] = struct{}{}
	}
	assert.Equal(t, map[string]struct{}{"node-b": {}}, gotTargets,
		"the orphan mirror for node-a must be GC'd and a new mirror for "+
			"node-b created; the receiver label is what scopes the GC to "+
			"this receiver's own mirrors")
}

func TestReceiver_FinalizerCascadesDeleteToLabeledMirrors(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-a")))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-b")))

	flow := newFlow(t, "node-src")
	rec := testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))
	require.NoError(t, env.Client.Create(ctx, rec))

	reconcile(t, r, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 2,
		"setup precondition: two distinct nodes must produce two mirrors")

	// In production the gateway source and target reconcilers each
	// stamp their own finalizer on the mirror so the libfabric
	// initiator and target descriptors get torn down before the API
	// object disappears. Simulate that here so the test exercises
	// the requeue-until-clean branch of handleDeletion as well as
	// the eventual final remove.
	const fakeGatewayFinalizer = "test.mxl.qvest-digital.com/keepalive"
	for i := range mirrors.Items {
		mirrors.Items[i].Finalizers = append(mirrors.Items[i].Finalizers, fakeGatewayFinalizer)
		require.NoError(t, env.Client.Update(ctx, &mirrors.Items[i]))
	}

	got := mustGetReceiver(t, env.Client, ns, "r")
	require.Contains(t, got.Finalizers, receiver.MxlReceiverFinalizer,
		"the reconciler stamps its finalizer on first contact so the "+
			"cascade delete below has somewhere to hook in")

	require.NoError(t, env.Client.Delete(ctx, got))

	res := reconcile(t, r, ns, "r")
	assert.NotZero(t, res.RequeueAfter,
		"with mirrors still terminating the receiver reconcile must requeue, "+
			"not remove its finalizer prematurely")

	var pending mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &pending, client.InNamespace(ns)))
	require.Len(t, pending.Items, 2,
		"the cascade Delete request hits the API server but the mirror "+
			"finalizer keeps the objects around until the gateway side "+
			"clears it")
	for i := range pending.Items {
		assert.False(t, pending.Items[i].DeletionTimestamp.IsZero(),
			"every labeled mirror must carry a DeletionTimestamp after the "+
				"receiver delete kicks off the cascade")
		// Now let the "gateway" finish its teardown so the mirror can
		// finalize and the next receiver reconcile can remove its
		// own finalizer.
		require.NoError(t, clearGatewayFinalizer(env.Client, &pending.Items[i], fakeGatewayFinalizer))
	}

	reconcile(t, r, ns, "r")

	var after mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &after, client.InNamespace(ns)))
	assert.Empty(t, after.Items, "labeled mirrors must be gone")

	// And the receiver itself must now be deletable: the finalizer
	// is removed, so envtest finishes its delete.
	err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "r"}, &mxlv1alpha1.MxlReceiver{})
	assert.True(t, apierrors.IsNotFound(err),
		"the cascade must end with the receiver actually gone; got %v", err)
}

// clearGatewayFinalizer removes the named finalizer from the mirror,
// simulating the source-side / target-side gateway finishing its
// teardown. Used by the cascade test where envtest has no real
// gateway running.
func clearGatewayFinalizer(c client.Client, m *mxlv1alpha1.MxlFlowMirror, finalizer string) error {
	var live mxlv1alpha1.MxlFlowMirror
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: m.Namespace, Name: m.Name}, &live); err != nil {
		return err
	}
	kept := live.Finalizers[:0]
	for _, f := range live.Finalizers {
		if f != finalizer {
			kept = append(kept, f)
		}
	}
	live.Finalizers = kept
	return c.Update(context.Background(), &live)
}

// Defensive: ensure a stray apierrors import does not break under
// future refactor; the helpers above use IsNotFound semantics through
// the controller-runtime client.
var _ = apierrors.IsNotFound
var _ = corev1.Pod{}
