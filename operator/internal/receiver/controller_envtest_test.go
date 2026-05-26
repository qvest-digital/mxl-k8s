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

	// Pod moves to a new node. The orphan mirror on node-a must
	// lose this receiver's OwnerReference -- apiserver GC then
	// removes it out-of-band once the owner list empties. envtest
	// does not run the GC controller, so the assertion is on owner
	// list state, not on object existence. Force grace=0 because
	// envtest has no kubelet to finalize the pod removal.
	zero := int64(0)
	require.NoError(t, env.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero}))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-b")))
	reconcile(t, r, ns, "r")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	gotTargets := map[string]struct{}{}
	gotOwners := map[string]int{}
	for _, m := range mirrors.Items {
		gotTargets[m.Spec.TargetNode] = struct{}{}
		gotOwners[m.Spec.TargetNode] = len(m.OwnerReferences)
	}
	assert.Equal(t, map[string]struct{}{"node-a": {}, "node-b": {}}, gotTargets,
		"the receiver does not Delete same-namespace mirrors directly; "+
			"removing its OwnerReference is the only action and apiserver GC "+
			"finishes the job out-of-band")
	assert.Equal(t, 0, gotOwners["node-a"],
		"the orphan mirror for node-a must have its receiver OwnerReference "+
			"removed; apiserver GC will then delete the object once its owner "+
			"list is empty")
	assert.Equal(t, 1, gotOwners["node-b"],
		"the freshly-created mirror for node-b must carry the receiver as an owner")
}

func TestReceiver_FinalizerCompletesWhenOwnerRefsCleared(t *testing.T) {
	ctx := context.Background()
	ns := env.NewNamespace(t)
	r := &receiver.Reconciler{Client: env.Client, Scheme: env.Scheme}

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
	require.Len(t, mirrors.Items, 2,
		"setup precondition: two distinct nodes must produce two mirrors")
	for i := range mirrors.Items {
		require.True(t, hasOwnerUID(&mirrors.Items[i], live.UID))
	}

	require.NoError(t, env.Client.Delete(ctx, live))

	// One reconcile pass on a same-namespace receiver releases the
	// owner refs and removes the finalizer. envtest does not run
	// the apiserver-GC controller, so the mirrors stay around with
	// an empty OwnerReferences list; in a real cluster GC removes
	// them once no owner remains. The contract verified here is
	// that the receiver itself unblocks immediately after dropping
	// the refs -- it must not wait on the mirror object actually
	// disappearing.
	res := reconcile(t, r, ns, "r")
	assert.Zero(t, res.RequeueAfter,
		"same-namespace ownership is by OwnerReferences; receiver deletion "+
			"completes the moment the refs are gone and does not block on "+
			"apiserver GC")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	for _, m := range mirrors.Items {
		assert.False(t, hasOwnerUID(&m, live.UID),
			"every mirror must have lost the receiver OwnerReference; "+
				"otherwise apiserver GC would never remove it")
	}

	err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "r"}, &mxlv1alpha1.MxlReceiver{})
	assert.True(t, apierrors.IsNotFound(err),
		"the receiver finalizer must be gone so envtest finishes the delete; "+
			"got %v", err)
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

func TestHandleDeletion_RemovesOnlyMyOwnerRef(t *testing.T) {
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

	rb := mustGetReceiver(t, env.Client, ns, "rb")
	ra := mustGetReceiver(t, env.Client, ns, "ra")
	require.NoError(t, env.Client.Delete(ctx, ra))

	reconcile(t, r, ns, "ra")

	var mirrors mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	require.Len(t, mirrors.Items, 1,
		"a sibling receiver still co-owns the mirror; the operator must not "+
			"delete it out from under the surviving consumer")
	mirror := mirrors.Items[0]
	require.Len(t, mirror.OwnerReferences, 1,
		"only one owner ref must remain after recv-a's deletion")
	assert.Equal(t, rb.UID, mirror.OwnerReferences[0].UID,
		"the surviving owner must be recv-b")

	err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "ra"}, &mxlv1alpha1.MxlReceiver{})
	assert.True(t, apierrors.IsNotFound(err),
		"recv-a's finalizer must be gone so envtest finishes its delete; got %v", err)
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

func TestGcOrphanMirrors_RemovesOwnerRef_SameNs(t *testing.T) {
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
	require.True(t, hasOwnerUID(&mirrors.Items[0], rec.UID))

	zero := int64(0)
	require.NoError(t, env.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero}))
	require.NoError(t, env.Client.Create(ctx, testutil.NewPod(ns, "consumer-b", "node-b")))
	reconcile(t, r, ns, "r")

	require.NoError(t, env.Client.List(ctx, &mirrors, client.InNamespace(ns)))
	gotByTarget := map[string]mxlv1alpha1.MxlFlowMirror{}
	for _, m := range mirrors.Items {
		gotByTarget[m.Spec.TargetNode] = m
	}
	require.Contains(t, gotByTarget, "node-a")
	require.Contains(t, gotByTarget, "node-b")
	assert.False(t, hasOwnerUID(&mirrors.Items[0], rec.UID) && hasOwnerUID(&mirrors.Items[1], rec.UID),
		"the orphan must lose this receiver's owner ref while the desired "+
			"mirror keeps it; otherwise apiserver GC has nothing to act on")
	mA := gotByTarget["node-a"]
	mB := gotByTarget["node-b"]
	assert.False(t, hasOwnerUID(&mA, rec.UID),
		"the obsolete-target mirror must have the receiver owner ref removed")
	assert.True(t, hasOwnerUID(&mB, rec.UID),
		"the new-target mirror must carry the receiver as an owner")
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

// Defensive: ensure a stray apierrors import does not break under
// future refactor; the helpers above use IsNotFound semantics through
// the controller-runtime client.
var _ = apierrors.IsNotFound
var _ = corev1.Pod{}
