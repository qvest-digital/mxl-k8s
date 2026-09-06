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

	rec := mustGetReceiver(t, env.Client, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	assert.Equal(t, "node-a", mirrors.Items[0].Spec.TargetNode)
	require.True(t, hasOwnerUID(&mirrors.Items[0], rec.UID),
		"the receiver must own the mirror it created same-namespace; without "+
			"the OwnerReference apiserver GC has nothing to act on")

	// Pod moves to a new node. The receiver drops its reference from
	// the orphan mirror on node-a and stops there; deleting the now
	// ownerless object is the mirror controller's, which collects it
	// once it has been unclaimed for a grace period. Force grace=0
	// because envtest has no kubelet to finalize the pod removal.
	zero := int64(0)
	require.NoError(t, env.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero}))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-b")))
	reconcile(t, r, ns, "r")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	gotOwners := map[string]int{}
	for _, m := range mirrors.Items {
		gotOwners[m.Spec.TargetNode] = len(m.OwnerReferences)
	}
	assert.Equal(t, map[string]int{"node-a": 0, "node-b": 1}, gotOwners,
		"the receiver must release the mirror its pod has left and own the "+
			"one on the node the pod moved to; the released mirror is then "+
			"unclaimed, which is what the mirror controller collects on")
}

// The finalizer exists for the cross-namespace mirrors, which carry no
// owner reference and would otherwise leak. A receiver that owns none
// must not be held up by it.
func TestReceiver_FinalizerCompletesWithNoCrossNsMirrors(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, APIReader: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-a")))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-b")))
	flow := newFlow(t, "node-src")
	rec := testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))
	require.NoError(t, env.Client.Create(ctx, rec))
	reconcile(t, r, ns, "r")

	live := mustGetReceiver(t, env.Client, ns, "r")
	require.Contains(t, live.Finalizers, receiver.MxlReceiverFinalizer)
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 2, "setup precondition: two nodes, two mirrors")

	require.NoError(t, env.Client.Delete(ctx, live))
	res := reconcile(t, r, ns, "r")
	assert.Zero(t, res.RequeueAfter)

	var recv mxlv1alpha1.MxlReceiver
	err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "r"}, &recv)
	assert.True(t, apierrors.IsNotFound(err),
		"deletion completes as soon as the finalizer comes off; the mirrors "+
			"are reaped by the apiserver cascade the dangling references "+
			"trigger, which this receiver must not block on")
}

// hasOwnerUID reports whether the mirror lists the given UID in its
// OwnerReferences. Used by the same-namespace ownership assertions
// so the test text stays readable.
func hasOwnerUID(m *mxlv1alpha1.MxlFlowMirror, uid types.UID) bool {
	for _, or := range m.OwnerReferences {
		if or.UID == uid {
			return true
		}
	}
	return false
}

// newSidecarNamespace creates a second envtest namespace whose name
// is derived from t.Name() plus a suffix. env.NewNamespace keys the
// name on t.Name() alone, so a test that needs more than one
// namespace would otherwise collide. Cleanup runs on t.Cleanup so
// the suffixed namespace disappears with the test.
func newSidecarNamespace(t *testing.T, suffix string) string {
	t.Helper()
	name := t.Name() + "-" + suffix
	dns := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z':
			dns = append(dns, c+('a'-'A'))
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			dns = append(dns, c)
		default:
			dns = append(dns, '-')
		}
	}
	if len(dns) > 63 {
		dns = dns[:63]
	}
	for len(dns) > 0 && dns[0] == '-' {
		dns = dns[1:]
	}
	for len(dns) > 0 && dns[len(dns)-1] == '-' {
		dns = dns[:len(dns)-1]
	}
	ns := string(dns)
	require.NoError(t, env.Client.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = env.Client.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})
	})
	return ns
}

func TestReconcile_TwoReceiversSameFlowSameTarget_ShareOneMirror_TwoOwners(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-target",
		testutil.WithPodLabels(map[string]string{"app": "consumer-a"}))))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-target",
		testutil.WithPodLabels(map[string]string{"app": "consumer-b"}))))

	flow := newFlow(t, "node-source")
	require.NoError(t, env.Client.Create(ctx, testutil.NewReceiver(ns, "ra",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverSelector(map[string]string{"app": "consumer-a"}))))
	require.NoError(t, env.Client.Create(ctx, testutil.NewReceiver(ns, "rb",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverSelector(map[string]string{"app": "consumer-b"}))))

	reconcile(t, r, ns, "ra")
	reconcile(t, r, ns, "rb")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1,
		"co-resident same-flow receivers must share one mirror; the cluster "+
			"only needs one libmxl-fabrics target per (flow, node)")

	mirror := mirrors.Items[0]
	require.Len(t, mirror.OwnerReferences, 2,
		"both receivers must appear as owners; that is the apiserver-GC "+
			"refcount the operator relies on")

	ra := mustGetReceiver(t, env.Client, ns, "ra")
	rb := mustGetReceiver(t, env.Client, ns, "rb")
	uids := map[types.UID]struct{}{}
	for _, or := range mirror.OwnerReferences {
		uids[or.UID] = struct{}{}
		require.NotNil(t, or.Controller)
		assert.False(t, *or.Controller,
			"Controller must be false; multiple non-controller owners is the "+
				"whole point of refcounting via OwnerReferences")
		require.NotNil(t, or.BlockOwnerDeletion)
		assert.False(t, *or.BlockOwnerDeletion,
			"BlockOwnerDeletion must be false; receiver deletion must not "+
				"wait on the co-owned mirror finalising")
	}
	assert.Contains(t, uids, ra.UID)
	assert.Contains(t, uids, rb.UID)
}

// Deleting a receiver leaves its reference dangling rather than
// removing it, which is the state apiserver garbage collection acts
// on -- and only once every owner is in it, so a mirror a sibling
// receiver still owns survives.
func TestHandleDeletion_LeavesSameNsRefsForTheApiserverCascade(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, APIReader: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-a")))
	flow := newFlow(t, "node-src")
	a := testutil.NewReceiver(ns, "ra", testutil.WithReceiverFlowID(flow.Spec.ID))
	b := testutil.NewReceiver(ns, "rb", testutil.WithReceiverFlowID(flow.Spec.ID))
	require.NoError(t, env.Client.Create(ctx, a))
	require.NoError(t, env.Client.Create(ctx, b))
	reconcile(t, r, ns, "ra")
	reconcile(t, r, ns, "rb")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1, "two receivers on one target share one mirror")
	require.Len(t, mirrors.Items[0].OwnerReferences, 2)
	name := mirrors.Items[0].Name

	live := mustGetReceiver(t, env.Client, ns, "ra")
	require.NoError(t, env.Client.Delete(ctx, live))
	res := reconcile(t, r, ns, "ra")
	assert.Zero(t, res.RequeueAfter,
		"a receiver owning only same-namespace mirrors has nothing to wait "+
			"on: its finalizer exists for the cross-namespace ones")

	var recv mxlv1alpha1.MxlReceiver
	err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "ra"}, &recv)
	assert.True(t, apierrors.IsNotFound(err), "the finalizer must come off")

	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &m))
	assert.Len(t, m.OwnerReferences, 2,
		"the surviving receiver's claim is untouched, and the departed "+
			"receiver's reference is what the apiserver cascade keys on")
}

func TestHandleDeletion_DeletesCrossNsMirror(t *testing.T) {
	ctx := context.Background()
	recvNs := env.NewNamespace(t)
	podNs := newSidecarNamespace(t, "pod")
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(podNs, "consumer", "node-target")))

	flow := newFlow(t, "node-source")
	rec := testutil.NewReceiver(recvNs, "r",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverPodRef("consumer"))
	rec.Spec.PodRef.Namespace = podNs
	require.NoError(t, env.Client.Create(ctx, rec))

	reconcile(t, r, recvNs, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNs)))
	require.Len(t, mirrors.Items, 1)
	const fakeGatewayFinalizer = "test.mxl.qvest-digital.com/keepalive"
	mirrors.Items[0].Finalizers = append(mirrors.Items[0].Finalizers, fakeGatewayFinalizer)
	require.NoError(t, env.Client.Update(ctx, &mirrors.Items[0]))

	live := mustGetReceiver(t, env.Client, recvNs, "r")
	require.NoError(t, env.Client.Delete(ctx, live))

	res := reconcile(t, r, recvNs, "r")
	assert.NotZero(t, res.RequeueAfter,
		"a cross-namespace mirror lingering under a gateway finalizer must "+
			"keep the receiver in requeue until the mirror is gone")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNs)))
	require.Len(t, mirrors.Items, 1)
	assert.False(t, mirrors.Items[0].DeletionTimestamp.IsZero(),
		"the cross-ns mirror must carry a DeletionTimestamp after the receiver "+
			"delete kicks off the cascade")

	require.NoError(t, clearGatewayFinalizer(env.Client, &mirrors.Items[0], fakeGatewayFinalizer))

	reconcile(t, r, recvNs, "r")

	err := env.Client.Get(ctx, types.NamespacedName{Namespace: recvNs, Name: "r"}, &mxlv1alpha1.MxlReceiver{})
	assert.True(t, apierrors.IsNotFound(err),
		"the cascade must end with the receiver actually gone once the "+
			"cross-ns mirror finalises; got %v", err)
}

func TestGcOrphanMirrors_DeletesMirror_CrossNs(t *testing.T) {
	ctx := context.Background()
	recvNs := env.NewNamespace(t)
	podNsA := newSidecarNamespace(t, "poda")
	podNsB := newSidecarNamespace(t, "podb")
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(podNsA, "consumer", "node-a")))

	flow := newFlow(t, "node-src")
	rec := testutil.NewReceiver(recvNs, "r",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverPodRef("consumer"))
	rec.Spec.PodRef.Namespace = podNsA
	require.NoError(t, env.Client.Create(ctx, rec))

	reconcile(t, r, recvNs, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNsA)))
	require.Len(t, mirrors.Items, 1)

	// Re-point the receiver at a pod in a different namespace. The
	// original cross-ns mirror in podNsA must be deleted; a new
	// cross-ns mirror appears in podNsB.
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(podNsB, "consumer", "node-b")))
	live := mustGetReceiver(t, env.Client, recvNs, "r")
	live.Spec.PodRef.Namespace = podNsB
	require.NoError(t, env.Client.Update(ctx, live))

	reconcile(t, r, recvNs, "r")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNsA)))
	if len(mirrors.Items) > 0 {
		assert.False(t, mirrors.Items[0].DeletionTimestamp.IsZero(),
			"the obsolete cross-ns mirror must carry a DeletionTimestamp; "+
				"envtest may not have finalised the delete yet but the "+
				"reconciler's r.Delete must have run")
	}
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNsB)))
	require.Len(t, mirrors.Items, 1,
		"a fresh cross-ns mirror must appear in the new pod's namespace")
}

func TestReconcile_NoLabelPingpong(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-target",
		testutil.WithPodLabels(map[string]string{"app": "consumer-a"}))))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-target",
		testutil.WithPodLabels(map[string]string{"app": "consumer-b"}))))

	flow := newFlow(t, "node-source")
	require.NoError(t, env.Client.Create(ctx, testutil.NewReceiver(ns, "ra",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverSelector(map[string]string{"app": "consumer-a"}))))
	require.NoError(t, env.Client.Create(ctx, testutil.NewReceiver(ns, "rb",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverSelector(map[string]string{"app": "consumer-b"}))))

	// ra creates the mirror and stamps its label.
	reconcile(t, r, ns, "ra")
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	mirror := mirrors.Items[0]
	originalLabel := mirror.Labels[mxlv1alpha1.LabelCreatedByReceiver]
	originalRV := mirror.ResourceVersion

	// rb reconciles against the same mirror; under the pre-refcount
	// design it would rewrite the receiver label on every pass.
	// The current contract: label stays at its first-creator value
	// and never gets re-patched on subsequent reconciles.
	for i := 0; i < 3; i++ {
		reconcile(t, r, ns, "rb")
		reconcile(t, r, ns, "ra")
	}

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: mirror.Namespace, Name: mirror.Name}, &live))
	assert.Equal(t, originalLabel, live.Labels[mxlv1alpha1.LabelCreatedByReceiver],
		"the label is a first-creator diagnostic tag; rewriting it on every "+
			"reconcile would re-introduce the pingpong PR #79 left in place")

	// OwnerReferences may grow to 2 (one ensure per receiver) and
	// then stay stable; nothing else on the object should churn.
	// Spec must remain stable, and the only resourceVersion bumps
	// come from the owner-ref appends -- bounded by the number of
	// distinct receivers, not by the number of reconciles.
	require.Len(t, live.OwnerReferences, 2,
		"both receivers must appear as owners; the second ensure must add "+
			"its ref, not rewrite the existing label")

	// Record the post-stable resourceVersion, then run the
	// reconcile pair again and assert no further bump.
	stableRV := live.ResourceVersion
	require.NotEqual(t, originalRV, stableRV,
		"the second receiver's owner-ref append legitimately bumps RV once")
	reconcile(t, r, ns, "ra")
	reconcile(t, r, ns, "rb")
	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: mirror.Namespace, Name: mirror.Name}, &live))
	assert.Equal(t, stableRV, live.ResourceVersion,
		"once both receivers are co-owners, reconciling either must be a "+
			"no-op on the mirror -- no label rewrite, no spec patch, no "+
			"owner-ref re-append; resourceVersion is the only signal of "+
			"a write actually hitting the apiserver")
}

func TestReconcile_CrossNs_NoDeleteLoop(t *testing.T) {
	ctx := context.Background()
	recvNs := env.NewNamespace(t)
	podNs := newSidecarNamespace(t, "pod")
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(podNs, "consumer", "node-target")))

	flow := newFlow(t, "node-source")
	rec := testutil.NewReceiver(recvNs, "r",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverPodRef("consumer"))
	rec.Spec.PodRef.Namespace = podNs
	require.NoError(t, env.Client.Create(ctx, rec))

	reconcile(t, r, recvNs, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNs)))
	require.Len(t, mirrors.Items, 1,
		"the first reconcile must create the cross-ns mirror")
	firstUID := mirrors.Items[0].UID

	// A second reconcile must NOT see the mirror as orphan: the
	// desired-map name must match what ensureMirror produced.
	// Without the per-receiver suffix on both sides the cross-ns
	// mirror would Delete-loop every reconcile.
	reconcile(t, r, recvNs, "r")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNs)))
	require.Len(t, mirrors.Items, 1,
		"the cross-ns mirror must survive a second reconcile; if the desired "+
			"map keyed by bare mirrorName misses the suffixed name ensureMirror "+
			"produced, gcOrphanMirrors would delete it on every pass")
	assert.Equal(t, firstUID, mirrors.Items[0].UID,
		"the mirror must be the same object; a Delete-then-Create would "+
			"interrupt the data plane and bump the UID")
	assert.True(t, mirrors.Items[0].DeletionTimestamp.IsZero(),
		"the second reconcile must not have Deleted the mirror")
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

// A receiver that stops wanting a mirror drops its own reference and
// stops there. Deleting the object that leaves behind is the mirror
// controller's, which collects it once it has been unclaimed for a
// grace period -- and a pod that returns to the node inside that
// window gets the mirror back rather than paying to rebuild it.
func TestGcOrphanMirrors_ReleasesTheReferenceAndLeavesTheObject(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, APIReader: env.Client, Scheme: env.Scheme}

	pod := testutil.NewPod(ns, "consumer-a", "node-a")
	require.NoError(t, env.Client.Create(ctx, pod))
	flow := newFlow(t, "node-src")
	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))
	reconcile(t, r, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	name := mirrors.Items[0].Name

	zero := int64(0)
	require.NoError(t, env.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero}))
	reconcile(t, r, ns, "r")

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: ns, Name: name}, &live))
	assert.Empty(t, live.OwnerReferences,
		"the receiver's obligation is to stop claiming the mirror; what "+
			"becomes of an unclaimed one is decided in one place for every "+
			"mirror, whichever path created it")
}

// An owner reference to a receiver that no longer exists is not a
// claim, and the mirror controller is where that is decided -- it
// evaluates every reference against a live MxlReceiver before counting
// it. Scrubbing the reference here as well would be a second opinion
// on the same question, and the apiserver's own collector already
// deletes a dependent whose owners have all gone.
func TestReconcile_GhostOwnerRef_IsLeftForTheApiserverCollector(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, APIReader: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-a")))
	flow := newFlow(t, "node-src")
	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))
	reconcile(t, r, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	m := mirrors.Items[0].DeepCopy()

	// Graft on a reference to a receiver that never existed.
	m.OwnerReferences = append(m.OwnerReferences, metav1.OwnerReference{
		APIVersion: mxlv1alpha1.GroupVersion.String(),
		Kind:       "MxlReceiver",
		Name:       "ghost",
		UID:        "ghost-uid",
	})
	require.NoError(t, env.Client.Update(ctx, m))

	reconcile(t, r, ns, "r")

	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: ns, Name: m.Name}, m))
	assert.Len(t, m.OwnerReferences, 2,
		"the receiver polices its own reference and nothing else; a stale "+
			"one costs nothing because neither collector counts it as a claim")
}

func TestReconcile_LegacyLabelOnlyMirror_AdoptsOwnerRef(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-target",
		testutil.WithPodLabels(map[string]string{"app": "consumer-a"}))))

	flow := newFlow(t, "node-source")

	// Hand-craft a mirror that mimics the pre-PR-86 shape: stamped
	// only with the receiver label, zero owner references. ensureMirror
	// must adopt it on the bind path instead of trying to Create a
	// second mirror under the same deterministic name.
	const targetNode = "node-target"
	// mirrorName composes "<lowercased flowID>--<lowercased target>";
	// flowIDFor only emits lowercase hex and dashes, and targetNode is
	// already DNS-safe lowercase, so the join below matches what the
	// package-internal mirrorName helper produces. Reconstructing it
	// here keeps the test in the receiver_test package without leaking
	// an export-for-test of an internal helper.
	legacyName := flow.Spec.ID + "--" + targetNode
	legacy := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      legacyName,
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByReceiver: "ra",
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flow.Spec.ID,
			SourceNode: "node-source",
			TargetNode: targetNode,
			Provider:   mxlv1alpha1.ProviderTCP,
		},
	}
	require.NoError(t, env.Client.Create(ctx, legacy))
	legacyUID := legacy.UID

	require.NoError(t, env.Client.Create(ctx, testutil.NewReceiver(ns, "ra",
		testutil.WithReceiverFlowID(flow.Spec.ID),
		testutil.WithReceiverSelector(map[string]string{"app": "consumer-a"}))))

	reconcile(t, r, ns, "ra")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1,
		"the legacy label-only mirror must be adopted in place; a parallel "+
			"Create under the same deterministic name would either error or "+
			"split ownership across two MxlFlowMirror objects for one (flow, node)")
	assert.Equal(t, legacyUID, mirrors.Items[0].UID,
		"adoption must keep the existing object UID; a destroy+recreate "+
			"would interrupt any gateway already initiated against the legacy mirror")

	ra := mustGetReceiver(t, env.Client, ns, "ra")
	assert.True(t, hasOwnerUID(&mirrors.Items[0], ra.UID),
		"the HIGH-3 self-healing migration must stamp the receiver's owner "+
			"ref onto the legacy mirror so apiserver-GC refcounting takes "+
			"over from the label-only contract on the very next reconcile")
}

// Defensive: ensure a stray apierrors import does not break under
// future refactor; the helpers above use IsNotFound semantics through
// the controller-runtime client.
var _ = apierrors.IsNotFound
var _ = corev1.Pod{}

// A mirror pinned open by a gateway finalizer stays Terminating until
// that gateway releases it. Mirror names are derived from (flow,
// target node), so the receiver cannot create a replacement while the
// name is taken: it reports Pending and retries rather than binding
// to an object on its way out.
func TestReconcile_TerminatingMirror_MarksPendingUntilNameFrees(t *testing.T) {
	const keepalive = "test.mxl.qvest-digital.com/keepalive"
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "worker-target")))
	flow := newFlow(t, "worker-source")
	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))

	reconcile(t, r, ns, "r")
	require.Equal(t, mxlv1alpha1.MxlReceiverBound,
		mustGetReceiver(t, env.Client, ns, "r").Status.Phase)

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	mirror := mirrors.Items[0]
	originalUID := mirror.UID

	pinned := mirror.DeepCopy()
	pinned.Finalizers = append(pinned.Finalizers, keepalive)
	require.NoError(t, env.Client.Update(ctx, pinned))
	require.NoError(t, env.Client.Delete(ctx, pinned))

	var terminating mxlv1alpha1.MxlFlowMirror
	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: ns, Name: mirror.Name}, &terminating))
	require.False(t, terminating.DeletionTimestamp.IsZero(),
		"fixture must leave the mirror terminating")

	res := reconcile(t, r, ns, "r")
	assert.Positive(t, res.RequeueAfter,
		"the receiver has to come back once the name frees up")
	assert.Equal(t, mxlv1alpha1.MxlReceiverPending,
		mustGetReceiver(t, env.Client, ns, "r").Status.Phase,
		"a target whose mirror is mid-deletion has no usable mirror, so the "+
			"receiver is not Bound")

	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: ns, Name: mirror.Name}, &terminating))
	assert.Equal(t, "worker-source", terminating.Spec.SourceNode,
		"the reconcile must not patch spec onto a terminating mirror")

	require.NoError(t, clearGatewayFinalizer(env.Client, &mirror, keepalive))
	reconcile(t, r, ns, "r")

	assert.Equal(t, mxlv1alpha1.MxlReceiverBound,
		mustGetReceiver(t, env.Client, ns, "r").Status.Phase,
		"once the name frees up the receiver creates a replacement and binds")
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	assert.NotEqual(t, originalUID, mirrors.Items[0].UID,
		"the replacement is a new object, not the resurrected tombstone")
	assert.True(t, mirrors.Items[0].DeletionTimestamp.IsZero())
}

// An origin the receiver cannot resolve is not a statement that its
// mirrors are unwanted. The desired set is the targets minus the
// source node, and the source node is exactly what is unknown, so
// reaping on it released every mirror in the cluster whenever a
// source-side agent went a renewal window without renewing -- while
// the producer and both gateways carried on delivering.
func TestReconcile_UnresolvableOrigin_LeavesExistingMirrorsAlone(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	lease := &stubLease{fresh: true}
	r := &receiver.Reconciler{
		Client: env.Client, APIReader: env.Client, Scheme: env.Scheme, Lease: lease,
	}

	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-a", "node-a")))
	flow := newFlow(t, "node-src")
	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))))
	reconcile(t, r, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1)
	name := mirrors.Items[0].Name
	require.Len(t, mirrors.Items[0].OwnerReferences, 1)

	// The source node's agent stops renewing. The flow still names the
	// Origin; only the liveness signal has lapsed.
	lease.fresh = false
	reconcile(t, r, ns, "r")

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: ns, Name: name}, &live))
	assert.Len(t, live.OwnerReferences, 1,
		"the receiver still wants this mirror; it simply cannot say where "+
			"the flow lives right now, and releasing on that basis makes an "+
			"agent restart cost every mirror sourced from that node")

	recv := mustGetReceiver(t, env.Client, ns, "r")
	assert.Equal(t, mxlv1alpha1.MxlReceiverPending, recv.Status.Phase)
}

// A cross-namespace mirror carries no owner reference, so
// spec.requestor is its only claim. Stamped on Create and never
// refreshed, it named a pod that was gone the first time the consumer
// was replaced under the same name -- and a mirror this receiver still
// wants would then read unclaimed and be collected.
func TestReconcile_CrossNs_RequestorFollowsAReplacedConsumer(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	podNs := newSidecarNamespace(t, "pod")
	r := &receiver.Reconciler{Client: env.Client, APIReader: env.Client, Scheme: env.Scheme}

	pod := testutil.NewPod(podNs, "consumer", "node-a")
	require.NoError(t, env.Client.Create(ctx, pod))
	flow := newFlow(t, "node-src")
	rec := testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flow.Spec.ID))
	rec.Spec.PodSelector = nil
	rec.Spec.PodRef = &mxlv1alpha1.PodRef{Namespace: podNs, Name: "consumer"}
	require.NoError(t, env.Client.Create(ctx, rec))
	reconcile(t, r, ns, "r")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(podNs)))
	require.Len(t, mirrors.Items, 1)
	name := mirrors.Items[0].Name
	require.NotNil(t, mirrors.Items[0].Spec.Requestor)
	assert.Equal(t, string(pod.UID), mirrors.Items[0].Spec.Requestor.UID)

	// The consumer is replaced under the same name, as a StatefulSet
	// pod is. The mirror's name is derived from the receiver, so the
	// same object is reused.
	zero := int64(0)
	require.NoError(t, env.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero}))
	replacement := testutil.NewPod(podNs, "consumer", "node-a")
	require.NoError(t, env.Client.Create(ctx, replacement))
	reconcile(t, r, ns, "r")

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: podNs, Name: name}, &live))
	require.NotNil(t, live.Spec.Requestor)
	assert.Equal(t, string(replacement.UID), live.Spec.Requestor.UID,
		"a stale requestor names a dead pod, and the mirror collector "+
			"reads it as nothing asking for the mirror")
}

// stubLease drives the receiver's origin resolution without a
// coordination.k8s.io fixture. fresh is flipped mid-test to model an
// agent that stops renewing while its producer keeps writing.
type stubLease struct{ fresh bool }

func (s *stubLease) IsFresh(_ context.Context, _, _ string) (bool, time.Time, error) {
	if !s.fresh {
		return false, time.Time{}, nil
	}
	return true, time.Now().Add(30 * time.Second), nil
}
