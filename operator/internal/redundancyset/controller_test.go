package redundancyset

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	memberA = "11111111-1111-4111-8111-111111111111"
	memberB = "22222222-2222-4222-8222-222222222222"
	output  = "33333333-3333-4333-8333-333333333333"
	ns      = "mxl-system"

	video1080 = `{"format":"urn:x-nmos:format:video","media_type":"video/v210",` +
		`"grain_rate":{"numerator":50,"denominator":1},"frame_width":1920,"frame_height":1080}`
	video720 = `{"format":"urn:x-nmos:format:video","media_type":"video/v210",` +
		`"grain_rate":{"numerator":50,"denominator":1},"frame_width":1280,"frame_height":720}`
)

func flow(id, def, origin string) *mxlv1alpha1.MxlFlow {
	f := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: id},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: id, Definition: runtime.RawExtension{Raw: []byte(def)}},
	}
	if origin != "" {
		f.Status.Locations = []mxlv1alpha1.MxlFlowLocation{{NodeName: origin, Phase: mxlv1alpha1.MxlFlowLocationOrigin}}
	}
	return f
}

func newSet(nodes ...string) *mxlv1alpha1.MxlFlowRedundancySet {
	return &mxlv1alpha1.MxlFlowRedundancySet{
		ObjectMeta: metav1.ObjectMeta{Name: "programme", UID: "set-uid", Generation: 1},
		Spec: mxlv1alpha1.MxlFlowRedundancySetSpec{
			Members: []mxlv1alpha1.RedundancyFlowRef{{FlowID: memberA}, {FlowID: memberB}},
			Output:  mxlv1alpha1.RedundancyFlowRef{FlowID: output},
		},
		Status: mxlv1alpha1.MxlFlowRedundancySetStatus{Nodes: nodes},
	}
}

func harness(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowRedundancySet{}).
		WithObjects(objs...).Build()
	return &Reconciler{Client: c, Namespace: ns}, c
}

func reconcile(t *testing.T, r *Reconciler) (ctrl.Result, *metav1.Condition) {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "programme"}})
	require.NoError(t, err)
	var got mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "programme"}, &got))
	return res, meta.FindStatusCondition(got.Status.Conditions, ConditionValid)
}

func mirrors(t *testing.T, c client.Client) map[string]mxlv1alpha1.MxlFlowMirror {
	t.Helper()
	var list mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(context.Background(), &list))
	out := map[string]mxlv1alpha1.MxlFlowMirror{}
	for _, m := range list.Items {
		out[m.Spec.FlowID+"->"+m.Spec.TargetNode] = m
	}
	return out
}

// Each member is mirrored to the reading node from its own origin, and
// the set owns the mirror, which is what keeps it claimed.
func TestMirrorsEveryMemberToTheReadingNode(t *testing.T) {
	r, c := harness(t, newSet("reader"),
		flow(memberA, video1080, "node-a"), flow(memberB, video1080, "node-b"))

	_, cond := reconcile(t, r)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)

	got := mirrors(t, c)
	require.Len(t, got, 2)
	a := got[memberA+"->reader"]
	assert.Equal(t, "node-a", a.Spec.SourceNode)
	assert.Equal(t, ns, a.Namespace)
	require.Len(t, a.OwnerReferences, 1)
	assert.Equal(t, "MxlFlowRedundancySet", a.OwnerReferences[0].Kind)
	assert.Equal(t, types.UID("set-uid"), a.OwnerReferences[0].UID)
	assert.Equal(t, "node-b", got[memberB+"->reader"].Spec.SourceNode)
}

// A member produced on the reading node is read there in place; a
// mirror onto its own origin would name the same node twice.
func TestNoMirrorForAMemberProducedOnTheReadingNode(t *testing.T) {
	r, c := harness(t, newSet("node-a"),
		flow(memberA, video1080, "node-a"), flow(memberB, video1080, "node-b"))

	reconcile(t, r)

	got := mirrors(t, c)
	require.Len(t, got, 1)
	assert.Contains(t, got, memberB+"->node-a")
}

// A mirror follows its member's origin when that moves.
func TestMirrorFollowsAMovedOrigin(t *testing.T) {
	r, c := harness(t, newSet("reader"),
		flow(memberA, video1080, "node-a"), flow(memberB, video1080, "node-b"))
	reconcile(t, r)

	var f mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: memberA}, &f))
	f.Status.Locations[0].NodeName = "node-c"
	require.NoError(t, c.Update(context.Background(), &f))
	reconcile(t, r)

	assert.Equal(t, "node-c", mirrors(t, c)[memberA+"->reader"].Spec.SourceNode)
}

// Slots are selected one for one, so members that disagree in geometry
// would show one member's picture cut to the other's layout.
func TestRefusesMisalignedMembers(t *testing.T) {
	r, c := harness(t, newSet("reader"),
		flow(memberA, video1080, "node-a"), flow(memberB, video720, "node-b"))

	_, cond := reconcile(t, r)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, ReasonMisaligned, cond.Reason)
	assert.Contains(t, cond.Message, "frame_width")
	assert.Empty(t, mirrors(t, c))
}

// A view selects among pages of one superblock, so a member in another
// domain cannot be served through the output.
func TestRefusesAMemberInAnotherDomain(t *testing.T) {
	s := newSet("reader")
	s.Spec.Members[1].Domain = "studio"
	r, c := harness(t, s, flow(memberA, video1080, "node-a"))

	_, cond := reconcile(t, r)
	assert.Equal(t, ReasonOtherDomain, cond.Reason)
	assert.Empty(t, mirrors(t, c))
}

// The output is written by the merge on each reading node; a producer
// of the same id would be a second writer of the same ring.
func TestRefusesAnOutputThatIsAFlow(t *testing.T) {
	r, c := harness(t, newSet("reader"),
		flow(memberA, video1080, "node-a"), flow(memberB, video1080, "node-b"), flow(output, video1080, "node-c"))

	_, cond := reconcile(t, r)
	assert.Equal(t, ReasonOutputTaken, cond.Reason)
	assert.Empty(t, mirrors(t, c))
}

// A member not yet published is the normal start-up order, not an
// error: the set waits for it.
func TestWaitsForAMissingMember(t *testing.T) {
	r, c := harness(t, newSet("reader"), flow(memberA, video1080, "node-a"))

	res, cond := reconcile(t, r)
	assert.Equal(t, ReasonMemberMissing, cond.Reason)
	assert.NotZero(t, res.RequeueAfter)
	assert.Empty(t, mirrors(t, c))
}

// A member whose producer is down is mirrored once it comes back; the
// other member is mirrored meanwhile, so the output runs on one leg.
func TestMirrorsTheMembersThatHaveAnOrigin(t *testing.T) {
	r, c := harness(t, newSet("reader"),
		flow(memberA, video1080, "node-a"), flow(memberB, video1080, ""))

	res, _ := reconcile(t, r)
	assert.NotZero(t, res.RequeueAfter)
	got := mirrors(t, c)
	require.Len(t, got, 1)
	assert.Contains(t, got, memberA+"->reader")
}

// Nothing is mirrored before a reader wants the output somewhere.
func TestNoMirrorsWithoutAReadingNode(t *testing.T) {
	r, c := harness(t, newSet(),
		flow(memberA, video1080, "node-a"), flow(memberB, video1080, "node-b"))

	_, cond := reconcile(t, r)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Empty(t, mirrors(t, c))
}
