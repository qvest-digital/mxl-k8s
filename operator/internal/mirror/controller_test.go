package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	utilptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	flowID  = "11111111-2222-3333-4444-555555555555"
	srcNode = "n-src"
	tgtNode = "n-target"
	testNS  = "ns"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// flowOriginAt is the flow as the agent on origin publishes it.
func flowOriginAt(origin string) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: origin, Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
}

type mirrorOpt func(*mxlv1alpha1.MxlFlowMirror)

func newMirror(opts ...mirrorOpt) *mxlv1alpha1.MxlFlowMirror {
	m := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "m1"},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: srcNode,
			TargetNode: tgtNode,
			Provider:   mxlv1alpha1.ProviderTCP,
		},
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// withRequestor is the on-demand path's claim: the agent records the
// pod whose ENOENT triggered the materialization.
func withRequestor(name, uid string) mirrorOpt {
	return func(m *mxlv1alpha1.MxlFlowMirror) {
		m.Spec.Requestor = &mxlv1alpha1.PodRef{
			Namespace: testNS, Name: name, UID: uid,
		}
	}
}

// withReceiverOwner is the declarative path's claim.
func withReceiverOwner(name string, uid types.UID) mirrorOpt {
	return func(m *mxlv1alpha1.MxlFlowMirror) {
		m.OwnerReferences = append(m.OwnerReferences, metav1.OwnerReference{
			APIVersion:         mxlv1alpha1.GroupVersion.String(),
			Kind:               "MxlReceiver",
			Name:               name,
			UID:                uid,
			Controller:         utilptr.To(false),
			BlockOwnerDeletion: utilptr.To(false),
		})
	}
}

// withIntentLabel stamps the creator label. It is diagnostic only:
// nothing in the lifecycle may read it, which is what these pin.
func withIntentLabel() mirrorOpt {
	return func(m *mxlv1alpha1.MxlFlowMirror) {
		if m.Labels == nil {
			m.Labels = map[string]string{}
		}
		m.Labels[mxlv1alpha1.LabelCreatedByIntent] = tgtNode
	}
}

func pod(name, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS, Name: name, UID: types.UID(uid),
		},
		Spec: corev1.PodSpec{
			NodeName:   tgtNode,
			Containers: []corev1.Container{{Name: "c", Image: "pause:3.10"}},
		},
	}
}

func receiver(name string, uid types.UID) *mxlv1alpha1.MxlReceiver {
	return &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name, UID: uid},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			FlowID:      flowID,
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "c"}},
		},
	}
}

type fakeLease struct {
	fresh    map[string]bool
	deadline time.Time
}

func (f *fakeLease) IsFresh(_ context.Context, flow, nodeName string) (bool, time.Time, error) {
	if f.fresh == nil {
		return true, f.deadline, nil
	}
	return f.fresh[flow+"/"+nodeName], f.deadline, nil
}

// harness builds the reconciler with both nodes present and a lease
// checker that considers every origin fresh unless told otherwise.
func harness(t *testing.T, grace time.Duration, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(node(srcNode), node(tgtNode)).
		WithObjects(objs...).
		Build()
	return &Reconciler{
		Client:      c,
		Recorder:    record.NewFakeRecorder(32),
		Lease:       &fakeLease{},
		GracePeriod: grace,
	}, c
}

func reconcileOnce(t *testing.T, r *Reconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
	require.NoError(t, err)
	return res
}

func getMirror(t *testing.T, c client.Client, name string) *mxlv1alpha1.MxlFlowMirror {
	t.Helper()
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, &m))
	return &m
}

func mirrorGone(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var m mxlv1alpha1.MxlFlowMirror
	err := c.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: name}, &m)
	return apierrors.IsNotFound(err)
}

func condition(t *testing.T, m *mxlv1alpha1.MxlFlowMirror, typ string) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(m.Status.Conditions, typ)
}

// backdate moves both operator-owned conditions back so a test can
// reach the far side of the grace period without waiting it out. The
// clock living on the object rather than in the operator's memory is
// the point: it used to reset on every mirror at once whenever the
// operator restarted.
func backdate(t *testing.T, c client.Client, name string, by time.Duration) {
	t.Helper()
	m := getMirror(t, c, name)
	for i := range m.Status.Conditions {
		switch m.Status.Conditions[i].Type {
		case mxlv1alpha1.ConditionTypeClaimed, mxlv1alpha1.ConditionTypeSourceable:
			m.Status.Conditions[i].LastTransitionTime = metav1.NewTime(time.Now().Add(-by))
		}
	}
	require.NoError(t, c.Status().Update(context.Background(), m))
}

// --- claim -----------------------------------------------------------

// A claim names its claimant, so the claimant being gone is not the
// ambiguous signal a missing source is: a producer can come back to
// the node it left, a named pod cannot. Waiting would pull a whole
// flow across the fabric for the grace period with nothing reading it.
func TestClaim_RequestorPodGone_CollectsAtOnce(t *testing.T) {
	m := newMirror(withIntentLabel(), withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Hour, m, flowOriginAt(srcNode))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

func TestClaim_RequestorPodAlive_IsKept(t *testing.T) {
	m := newMirror(withIntentLabel(), withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode), pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	assert.False(t, mirrorGone(t, c, m.Name))

	claimed := condition(t, getMirror(t, c, m.Name), mxlv1alpha1.ConditionTypeClaimed)
	require.NotNil(t, claimed)
	assert.Equal(t, mxlv1alpha1.ReasonRequestorLive, claimed.Reason)
}

// A pod recreated under the same name is a different consumer. The
// UID is what says so.
func TestClaim_RequestorPodReplaced_IsUnclaimed(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode), pod("consumer", "uid-2"))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

func TestClaim_LiveReceiverOwner_IsKept(t *testing.T) {
	m := newMirror(withReceiverOwner("recv", "recv-uid"))
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode), receiver("recv", "recv-uid"))

	reconcileOnce(t, r, m.Name)
	assert.False(t, mirrorGone(t, c, m.Name))

	claimed := condition(t, getMirror(t, c, m.Name), mxlv1alpha1.ConditionTypeClaimed)
	require.NotNil(t, claimed)
	assert.Equal(t, mxlv1alpha1.ReasonReceiverOwned, claimed.Reason)
}

// The trap the old collectors left open. A receiver that stops wanting
// a mirror removes its owner reference rather than being deleted, and
// apiserver garbage collection fires when an owner is deleted, not
// when the list is emptied by an update. Nothing collected the result.
func TestClaim_OwnerlessMirror_IsCollected(t *testing.T) {
	m := newMirror()
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// An owner reference to a receiver that no longer exists is not a
// claim. The name may even have been reused by a different object,
// which the UID catches.
func TestClaim_DanglingOwnerRef_IsNotAClaim(t *testing.T) {
	m := newMirror(withReceiverOwner("recv", "old-uid"))
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode),
		receiver("recv", "new-uid"))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// The mirror that survived a day and a half on the showcase cluster.
// Its creator labels had been edited off, so the label-keyed collector
// skipped it and even stripped its own finalizer; the owner-ref-keyed
// one never looked at it either. Its requestor pod had been gone since
// the day before, which is the fact that should have decided it.
func TestClaim_NoCreatorLabels_IsStillJudgedOnItsRequestor(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Labels = nil
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name),
		"a claim is what the requesting side wrote into spec and "+
			"ownerReferences; a label that can be edited off must not be "+
			"what decides whether an object can ever be collected")
}

// --- source ----------------------------------------------------------

// The dev-cluster case: the producer's node was reclaimed, the flow
// kept the mirror target's Ready location and nothing else, and the
// mirror addressed the departed node forever. Its requestor pod was
// alive the whole time, so the claim alone would have kept it.
func TestSource_FlowHasNoOrigin_CollectsAfterGraceDespiteALiveClaim(t *testing.T) {
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: tgtNode, Phase: mxlv1alpha1.MxlFlowLocationReady},
			},
		},
	}
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Hour, m, flow, pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	live := getMirror(t, c, m.Name)
	assert.Equal(t, metav1.ConditionTrue,
		condition(t, live, mxlv1alpha1.ConditionTypeClaimed).Status)
	sourceable := condition(t, live, mxlv1alpha1.ConditionTypeSourceable)
	require.NotNil(t, sourceable)
	assert.Equal(t, metav1.ConditionFalse, sourceable.Status)
	assert.Equal(t, mxlv1alpha1.ReasonOriginUnresolved, sourceable.Reason,
		"no node claims to hold the flow, which is what a reclaimed "+
			"producer node leaves once its location has been pruned; "+
			"nothing is going to take that back")

	backdate(t, c, m.Name, 2*time.Hour)
	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name),
		"a consumer that happens to still be running is not a reason to "+
			"keep a mirror of a flow that has no producer left")
}

func TestSource_FlowGone_IsUnsourceable(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Nanosecond, m, pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// An expired lease says a node that still claims to hold the flow has
// no agent renewing for it. That is a control-plane failure, and the
// producer and both gateways may be fine and still delivering: an
// agent stuck in CrashLoopBackOff produces exactly this. Collecting on
// it would tear down a live transfer on the evidence of a component
// that is not carrying it, so the condition reports the fault and the
// mirror stays.
func TestSource_EveryOriginLeaseExpired_IsReportedButNotCollected(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode), pod("consumer", "uid-1"))
	r.Lease = &fakeLease{fresh: map[string]bool{}}

	reconcileOnce(t, r, m.Name)
	sourceable := condition(t, getMirror(t, c, m.Name), mxlv1alpha1.ConditionTypeSourceable)
	require.NotNil(t, sourceable)
	assert.Equal(t, metav1.ConditionFalse, sourceable.Status)
	assert.Equal(t, mxlv1alpha1.ReasonLeaseExpired, sourceable.Reason)

	backdate(t, c, m.Name, 2*time.Hour)
	reconcileOnce(t, r, m.Name)
	assert.False(t, mirrorGone(t, c, m.Name),
		"the grace period is not what decides this: no amount of waiting "+
			"makes a lapsed lease evidence that the flow has no producer")
}

// A mirror that is both unclaimed and merely lease-stale is still
// collected: nothing asks for it, whatever its source is doing.
func TestSource_LeaseExpiredAndUnclaimed_IsStillCollected(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Hour, m, flowOriginAt(srcNode))
	r.Lease = &fakeLease{fresh: map[string]bool{}}

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// No gateway will ever open the writer: the DaemonSet pod that would
// have done it died with the node. A departed node is the one signal
// the platform already treats as terminal -- it is what separates a
// departed node from a drained one -- so this does not wait.
func TestSource_TargetNodeGone_CollectsAtOnce(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Spec.TargetNode = "reclaimed"
	r, c := harness(t, time.Hour, m, flowOriginAt(srcNode), pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// The same at the other end. Nothing will publish on a node that has
// left, so a mirror still sourcing from it is reporting a producer
// that cannot exist, and waiting buys nothing while the fabric carries
// a stream nobody can read.
func TestSource_SourceNodeGone_CollectsAtOnce(t *testing.T) {
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
	}
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Spec.SourceNode = "reclaimed"
	r, c := harness(t, time.Hour, m, flow, pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// A mirror that is wanted and sourceable is left alone however badly
// it is doing. Degraded says grains are not moving, which is a
// data-plane fault the gateway may still recover from -- collecting on
// it would tear down a stream that was about to come back.
func TestCollect_DegradedButJustified_IsLeftAlone(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Status.Phase = mxlv1alpha1.MxlFlowMirrorDegraded
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode), pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	assert.False(t, mirrorGone(t, c, m.Name))
}

// --- repoint ---------------------------------------------------------

// Mirror names do not encode the source node, so a mirror created
// before the producer moved addresses the node it left for life. The
// source gateway there opens a reader on a copy nothing writes to, and
// reopening it -- the only recovery the data plane has -- yields
// another reader on the same dead copy.
func TestRepoint_OriginMoved_SpecFollowsIt(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Hour, m, flowOriginAt("n-moved"),
		pod("consumer", "uid-1"), node("n-moved"))

	reconcileOnce(t, r, m.Name)

	got := getMirror(t, c, m.Name)
	assert.Equal(t, "n-moved", got.Spec.SourceNode)
	assert.Equal(t, srcNode, got.Status.PreviousSourceNode)
	assert.NotNil(t, got.Status.SourceRetargetedAt,
		"a mirror briefly Degraded after its producer moves is converging "+
			"and one Degraded with no recent retarget is not; without the "+
			"timestamp the two look alike on the object")
	assert.Equal(t, metav1.ConditionTrue,
		condition(t, got, mxlv1alpha1.ConditionTypeSourceable).Status)
}

// Both creation paths land on the same object, so repointing has to
// cover both. It used to live in the agent, which only looked at
// mirrors targeting its own node and carrying no owner reference.
func TestRepoint_CoversReceiverOwnedMirrorsToo(t *testing.T) {
	m := newMirror(withReceiverOwner("recv", "recv-uid"))
	r, c := harness(t, time.Hour, m, flowOriginAt("n-moved"),
		receiver("recv", "recv-uid"), node("n-moved"))

	reconcileOnce(t, r, m.Name)
	assert.Equal(t, "n-moved", getMirror(t, c, m.Name).Spec.SourceNode)
}

// The origin landing on the mirror's own target node makes the mirror
// a transfer from a node to itself; the consumer reads the local copy
// directly.
func TestRepoint_OriginLandsOnTheTarget_MirrorIsDeleted(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Hour, m, flowOriginAt(tgtNode), pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// --- finalizer -------------------------------------------------------

// The finalizer an earlier collector added did nothing on deletion but
// remove itself, so all it bought was a round trip and one more way
// for a mirror to sit in Terminating while the operator was down.
func TestLegacyIntentFinalizer_IsStripped(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Finalizers = []string{legacyIntentFinalizer}
	r, c := harness(t, time.Hour, m, flowOriginAt(srcNode), pod("consumer", "uid-1"))

	reconcileOnce(t, r, m.Name)
	assert.NotContains(t, getMirror(t, c, m.Name).Finalizers, legacyIntentFinalizer)
}

func TestReconcile_DeletingMirror_IsLeftToTheGateway(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Finalizers = []string{"gateway.mxl.qvest-digital.com/target-side"}
	now := metav1.Now()
	m.DeletionTimestamp = &now
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode))

	res := reconcileOnce(t, r, m.Name)
	assert.Equal(t, ctrl.Result{}, res)
	assert.False(t, mirrorGone(t, c, m.Name),
		"the gateway finalizers own the teardown; nothing this controller "+
			"decides applies to an object already on its way out")
}

func TestReconcile_MissingMirror_NoError(t *testing.T) {
	r, _ := harness(t, time.Hour)
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "absent"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
}

// --- watches ---------------------------------------------------------

func TestWatches_EnqueueTheMirrorsEachInputAffects(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"),
		withReceiverOwner("recv", "recv-uid"))
	r, _ := harness(t, time.Hour, m, flowOriginAt(srcNode))
	ctx := context.Background()

	assert.Len(t, r.podToMirrors(ctx, pod("consumer", "uid-1")), 1)
	assert.Empty(t, r.podToMirrors(ctx, pod("other", "uid-9")))

	assert.Len(t, r.receiverToMirrors(ctx, receiver("recv", "recv-uid")), 1)
	assert.Empty(t, r.receiverToMirrors(ctx, receiver("recv", "different-uid")))

	assert.Len(t, r.flowToMirrors(ctx, flowOriginAt(srcNode)), 1)

	lease := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
		Namespace: mxlv1alpha1.LeaseNamespace,
		Name:      mxlv1alpha1.LeaseName(flowID, srcNode),
	}}
	assert.Len(t, r.leaseToMirrors(ctx, lease), 1)
	lease.Name = "kube-controller-manager"
	assert.Empty(t, r.leaseToMirrors(ctx, lease))

	assert.Len(t, r.nodeToMirrors(ctx, node(tgtNode)), 1)
	assert.Len(t, r.nodeToMirrors(ctx, node(srcNode)), 1)
	assert.Empty(t, r.nodeToMirrors(ctx, node("unrelated")))
}

func TestPodPredicate_DenyKubeSystem(t *testing.T) {
	p := podLifecyclePredicate()
	sys := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "p"}}
	app := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "p"}}

	assert.False(t, p.Create(event.CreateEvent{Object: sys}))
	assert.True(t, p.Create(event.CreateEvent{Object: app}))
	assert.False(t, p.Delete(event.DeleteEvent{Object: sys}))
	assert.True(t, p.Delete(event.DeleteEvent{Object: app}))
	assert.False(t, p.Generic(event.GenericEvent{Object: app}))

	same := app.DeepCopy()
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: app, ObjectNew: same}),
		"a status tick is not a change of claim")
	replaced := app.DeepCopy()
	replaced.UID = "different"
	assert.True(t, p.Update(event.UpdateEvent{ObjectOld: app, ObjectNew: replaced}))
}

func TestNodeDeletedOnly_AcceptsDeletesOnly(t *testing.T) {
	p := nodeDeletedOnly()
	n := node(tgtNode)
	assert.False(t, p.Create(event.CreateEvent{Object: n}))
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: n, ObjectNew: n}))
	assert.False(t, p.Generic(event.GenericEvent{Object: n}))
	assert.True(t, p.Delete(event.DeleteEvent{Object: n}))
}

// The grace measures from the Sourceable condition alone; the claim
// half does not wait at all.
func TestUnsourceableSince_ReadsTheSourceableCondition(t *testing.T) {
	when := metav1.NewTime(time.Now().Add(-time.Hour))
	m := newMirror()
	m.Status.Conditions = []metav1.Condition{
		{Type: mxlv1alpha1.ConditionTypeClaimed, LastTransitionTime: metav1.Now()},
		{Type: mxlv1alpha1.ConditionTypeSourceable, LastTransitionTime: when},
	}
	assert.Equal(t, when.Time, unsourceableSince(m))
}

// A mirror this operator has not written conditions onto yet gets a
// full grace period rather than being collected on sight, so an
// upgrade does not reap every collectable mirror at once.
func TestUnsourceableSince_NoConditionYetCountsAsJustTurned(t *testing.T) {
	assert.WithinDuration(t, time.Now(), unsourceableSince(newMirror()), time.Second)
}

// --- review-driven regressions ---------------------------------------

// The delete is preconditioned on the version the judgement was taken
// against, and the condition write that precedes it moves that version
// on. Returning the pre-apply one made every precondition fail as a
// conflict, which is silently tolerated, so nothing was ever
// collected -- the whole change set, disabled by one stale field.
func TestCollect_DeleteUsesTheVersionTheConditionWriteProduced(t *testing.T) {
	m := newMirror()
	r, c := harness(t, time.Nanosecond, m, flowOriginAt(srcNode))

	reconcileOnce(t, r, m.Name)
	assert.True(t, mirrorGone(t, c, m.Name))
}

// A write that lands between the judgement and the delete makes the
// precondition fail, and the requeue judges the mirror again against
// what it now says.
func TestCollect_StaleReadDoesNotDeleteOnAMovedResourceVersion(t *testing.T) {
	// A flow with no Origin while both nodes are still in the cluster:
	// a producer that may be restarting, so the mirror waits and can
	// be read back.
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
	}
	m := newMirror(withRequestor("consumer", "uid-1"))
	r, c := harness(t, time.Hour, m, flow, pod("consumer", "uid-1"))
	reconcileOnce(t, r, m.Name)
	backdate(t, c, m.Name, 2*time.Hour)

	stale := getMirror(t, c, m.Name)

	// Somebody writes, moving the resourceVersion on.
	live := getMirror(t, c, m.Name)
	live.Status.Phase = mxlv1alpha1.MxlFlowMirrorReady
	require.NoError(t, c.Status().Update(context.Background(), live))

	require.NoError(t, r.delete(context.Background(), stale,
		ReasonMirrorCollected, "Collected: %s", "test"))
	assert.False(t, mirrorGone(t, c, m.Name),
		"deleting on a stale read would drop a mirror whose judgement had "+
			"already moved on")
}

// Whoever created the mirror may have pinned the provider -- the
// agent's --provider flag forces one cluster-wide, a receiver's
// spec.provider per consumer -- and neither is recorded anywhere this
// controller can read. Re-resolving on every move would silently
// overrule that on the first origin change.
func TestRepoint_KeepsAProviderBothNodesStillSpeak(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Spec.Provider = mxlv1alpha1.ProviderTCP
	r, c := harness(t, time.Hour, m, flowOriginAt("n-moved"),
		pod("consumer", "uid-1"), node("n-moved"),
		capsFor("n-moved", mxlv1alpha1.ProviderTCP, mxlv1alpha1.ProviderVerbs),
		capsFor(tgtNode, mxlv1alpha1.ProviderTCP, mxlv1alpha1.ProviderVerbs))

	reconcileOnce(t, r, m.Name)

	got := getMirror(t, c, m.Name)
	assert.Equal(t, "n-moved", got.Spec.SourceNode)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, got.Spec.Provider,
		"both nodes speak tcp, so the recorded choice still works and "+
			"nothing here knows better than whoever made it")
}

// A move onto a node that does not speak it is the case where
// overruling is the only option: a mirror asking for a provider one of
// its ends cannot build never comes up.
func TestRepoint_ReresolvesAProviderTheNewSourceCannotSpeak(t *testing.T) {
	m := newMirror(withRequestor("consumer", "uid-1"))
	m.Spec.Provider = mxlv1alpha1.ProviderVerbs
	r, c := harness(t, time.Hour, m, flowOriginAt("n-moved"),
		pod("consumer", "uid-1"), node("n-moved"),
		capsFor("n-moved", mxlv1alpha1.ProviderTCP),
		capsFor(tgtNode, mxlv1alpha1.ProviderTCP, mxlv1alpha1.ProviderVerbs))

	reconcileOnce(t, r, m.Name)
	assert.Equal(t, mxlv1alpha1.ProviderTCP, getMirror(t, c, m.Name).Spec.Provider)
}

// capsFor is the MxlNodeCapabilities a probed gateway publishes for a
// node that found a device for each named provider.
func capsFor(node string, providers ...mxlv1alpha1.MxlFabricsProvider) *mxlv1alpha1.MxlNodeCapabilities {
	caps := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Status: mxlv1alpha1.MxlNodeCapabilitiesStatus{
			Conditions: []metav1.Condition{{
				Type:               mxlv1alpha1.ConditionTypeProbed,
				Status:             metav1.ConditionTrue,
				Reason:             mxlv1alpha1.ReasonProbeComplete,
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	for _, p := range providers {
		caps.Status.Providers = append(caps.Status.Providers,
			mxlv1alpha1.MxlFabricsProviderCapability{Name: p, DeviceCount: 1})
	}
	return caps
}
