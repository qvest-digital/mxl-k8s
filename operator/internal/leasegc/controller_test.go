package leasegc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	utilptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	flowID = "11111111-2222-3333-4444-555555555555"
	holder = "n1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

// lease is the Origin Lease the agent on node writes for flow, last
// renewed renewedAgo ago.
func lease(flow, node string, renewedAgo time.Duration) *coordinationv1.Lease {
	renew := metav1.NewMicroTime(time.Now().Add(-renewedAgo))
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mxlv1alpha1.LeaseNamespace,
			Name:      mxlv1alpha1.LeaseName(flow, node),
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       utilptr.To(node),
			LeaseDurationSeconds: utilptr.To(int32(30)),
			RenewTime:            &renew,
		},
	}
}

func flow(id string) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: id},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: id},
	}
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func harness(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...).
		Build()
	return &Reconciler{Client: c}, c
}

func reconcileLease(t *testing.T, r *Reconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: mxlv1alpha1.LeaseNamespace, Name: name,
		},
	})
	require.NoError(t, err)
	return res
}

func leaseGone(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var l coordinationv1.Lease
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: mxlv1alpha1.LeaseNamespace, Name: name,
	}, &l)
	return apierrors.IsNotFound(err)
}

// The state a showcase cluster sat in for a week: eight Leases naming
// a node that had been removed, last renewed the day it went. The
// agent that wrote them is the only thing that deletes one, and it
// died with the node.
func TestCollect_HolderNodeGone_IsDeleted(t *testing.T) {
	l := lease(flowID, "reclaimed", time.Hour)
	r, c := harness(t, l, flow(flowID))

	reconcileLease(t, r, l.Name)
	assert.True(t, leaseGone(t, c, l.Name))
}

// A flow the collector removed leaves its Leases behind on any node
// whose agent was down at the time.
func TestCollect_FlowGone_IsDeleted(t *testing.T) {
	l := lease(flowID, holder, time.Hour)
	r, c := harness(t, l, node(holder))

	reconcileLease(t, r, l.Name)
	assert.True(t, leaseGone(t, c, l.Name))
}

// Both halves are required and neither is a heuristic. An expired
// Lease whose node is still there belongs to an agent that will renew
// it on its next pass or release it on its next rescan; deleting it
// would race a live producer.
func TestCollect_ExpiredButNodeAndFlowPresent_IsKept(t *testing.T) {
	l := lease(flowID, holder, time.Hour)
	r, c := harness(t, l, node(holder), flow(flowID))

	reconcileLease(t, r, l.Name)
	assert.False(t, leaseGone(t, c, l.Name))
}

// A fresh Lease is one an agent is actively renewing, so the flow it
// names is on disk and about to be republished even if the object is
// momentarily absent.
func TestCollect_FreshLeaseIsNeverDeleted(t *testing.T) {
	l := lease(flowID, "reclaimed", time.Second)
	r, c := harness(t, l)

	res := reconcileLease(t, r, l.Name)
	assert.False(t, leaseGone(t, c, l.Name))
	assert.Positive(t, res.RequeueAfter,
		"time passing raises no event, so the only way an unrenewed Lease "+
			"is looked at again is a wake-up booked here")
	assert.Less(t, res.RequeueAfter, 31*time.Second)
}

// A Lease that was never renewed has no window to be inside, so it is
// judged on the orphan test alone rather than being parked forever.
func TestCollect_NeverRenewedOrphan_IsDeleted(t *testing.T) {
	l := lease(flowID, "reclaimed", 0)
	l.Spec.RenewTime = nil
	r, c := harness(t, l, flow(flowID))

	reconcileLease(t, r, l.Name)
	assert.True(t, leaseGone(t, c, l.Name))
}

// The namespace is the agent's but nothing stops another component
// from putting a Lease in it, and leader-election Leases are exactly
// what would land there.
func TestCollect_ForeignLeaseIsLeftAlone(t *testing.T) {
	l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace: mxlv1alpha1.LeaseNamespace,
		Name:      "mxl-operator.mxl.qvest-digital.com",
	}}
	r, c := harness(t, l)

	reconcileLease(t, r, l.Name)
	assert.False(t, leaseGone(t, c, l.Name))
}

func TestCollect_MissingLease_NoError(t *testing.T) {
	r, _ := harness(t)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: mxlv1alpha1.LeaseNamespace, Name: "absent",
		},
	})
	require.NoError(t, err)
}

func TestWatches_EnqueueTheLeasesEachDepartureOrphans(t *testing.T) {
	mine := lease(flowID, "ip-10-66-1-235", time.Hour)
	other := lease("22222222-2222-3333-4444-555555555555", "n2", time.Hour)
	foreign := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace: mxlv1alpha1.LeaseNamespace, Name: "kube-controller-manager",
	}}
	r, _ := harness(t, mine, other, foreign)
	ctx := context.Background()

	reqs := r.nodeToLeases(ctx, node("ip-10-66-1-235"))
	require.Len(t, reqs, 1,
		"an EC2 node name is full of dashes; a Lease name split at the "+
			"last one would match no node here at all")
	assert.Equal(t, mine.Name, reqs[0].Name)

	reqs = r.flowToLeases(ctx, flow(flowID))
	require.Len(t, reqs, 1)
	assert.Equal(t, mine.Name, reqs[0].Name)

	assert.Empty(t, r.nodeToLeases(ctx, node("unrelated")))
	assert.Empty(t, r.flowToLeases(ctx, node("not-a-flow")))
}

func TestPredicates(t *testing.T) {
	l := lease(flowID, holder, 0)
	assert.True(t, inMxlSystem().Create(event.CreateEvent{Object: l}))
	elsewhere := l.DeepCopy()
	elsewhere.Namespace = "kube-node-lease"
	assert.False(t, inMxlSystem().Create(event.CreateEvent{Object: elsewhere}),
		"kube-node-lease alone holds one Lease per node renewed every few "+
			"seconds")

	// A renewal can only make a Lease less collectable, so the one
	// thing that has to be noticed is time passing.
	p := createOnly()
	assert.True(t, p.Create(event.CreateEvent{Object: l}))
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: l, ObjectNew: l}))
	assert.False(t, p.Delete(event.DeleteEvent{Object: l}))
	assert.False(t, p.Generic(event.GenericEvent{Object: l}))

	d := deletedOnly()
	assert.True(t, d.Delete(event.DeleteEvent{Object: node(holder)}))
	assert.False(t, d.Create(event.CreateEvent{Object: node(holder)}))
	assert.False(t, d.Update(event.UpdateEvent{ObjectOld: node(holder), ObjectNew: node(holder)}))
	assert.False(t, d.Generic(event.GenericEvent{Object: node(holder)}))
}

func TestExpiry_UsesTheDeclaredWindow(t *testing.T) {
	l := lease(flowID, holder, 0)
	assert.WithinDuration(t, time.Now().Add(30*time.Second), expiry(l), time.Second)

	l.Spec.LeaseDurationSeconds = utilptr.To(int32(120))
	assert.WithinDuration(t, time.Now().Add(2*time.Minute), expiry(l), time.Second)

	l.Spec.LeaseDurationSeconds = nil
	assert.WithinDuration(t, time.Now().Add(mxlv1alpha1.DefaultLeaseDuration), expiry(l), time.Second)

	l.Spec.RenewTime = nil
	assert.True(t, expiry(l).IsZero())
}
