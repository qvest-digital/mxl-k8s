package redundancyset

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// gateway is the gateway pod on node, Ready since readyFor ago; a
// negative readyFor makes it not Ready.
func gateway(node string, readyFor time.Duration) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "gateway-" + node,
			Labels: map[string]string{"app.kubernetes.io/component": "gateway"}},
		Spec: corev1.PodSpec{NodeName: node},
	}
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-readyFor))}
	if readyFor < 0 {
		ready.Status, ready.LastTransitionTime = corev1.ConditionFalse, metav1.Now()
	}
	p.Status.Conditions = []corev1.PodCondition{ready}
	return p
}

func outputReadyOf(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	var got mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "programme"}, &got))
	cond := meta.FindStatusCondition(got.Status.Conditions, ConditionOutputReady)
	require.NotNil(t, cond, "no OutputReady condition")
	return cond
}

func readyMembers() []client.Object {
	return []client.Object{flow(memberA, video1080, "node-a"), flow(memberB, video1080, "node-b")}
}

// Every member is produced and advancing, so MembersReady holds, but
// the gateway that merges the output on the reading node is gone, and
// with it the output there. The set has to say so.
func TestReportsTheOutputNotServedWhereItsGatewayIsGone(t *testing.T) {
	objs := append(readyMembers(), newSet("reader", "node-c"), gateway("node-c", time.Minute))
	r, c := harness(t, objs...)

	res, _ := reconcile(t, r)

	assert.Equal(t, metav1.ConditionTrue, membersReadyOf(t, c).Status, "the members are fine")
	cond := outputReadyOf(t, c)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, ReasonGatewayNotReady, cond.Reason)
	assert.Contains(t, cond.Message, "not served on reader:")
	assert.Zero(t, res.RequeueAfter, "the gateway's return is a pod event; nothing to wait for")
}

// A gateway being deleted has stopped merging, Ready or not, and so has
// one whose container is not Ready.
func TestATerminatingOrUnreadyGatewayDoesNotServe(t *testing.T) {
	terminating := gateway("reader", time.Minute)
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	terminating.Finalizers = []string{"test/keep"}
	for name, pod := range map[string]*corev1.Pod{"terminating": terminating, "unready": gateway("reader", -1)} {
		t.Run(name, func(t *testing.T) {
			r, c := harness(t, append(readyMembers(), newSet("reader"), pod)...)
			reconcile(t, r)
			assert.Equal(t, ReasonGatewayNotReady, outputReadyOf(t, c).Reason)
		})
	}
}

// A replacement gateway reports Ready before it has built its engines,
// so the output counts as served again only once it has been Ready for
// gatewaySettle; the set is checked again when that is over.
func TestAReturningGatewaySettlesFirst(t *testing.T) {
	r, c := harness(t, append(readyMembers(), newSet("reader"), gateway("reader", time.Second))...)

	res, _ := reconcile(t, r)
	assert.Equal(t, metav1.ConditionFalse, outputReadyOf(t, c).Status, "served the moment the gateway was Ready")
	assert.Greater(t, res.RequeueAfter, time.Duration(0))
	assert.LessOrEqual(t, res.RequeueAfter, gatewaySettle-time.Second+100*time.Millisecond)

	// The same gateway, gatewaySettle later.
	require.NoError(t, c.Delete(context.Background(), gateway("reader", 0)))
	require.NoError(t, c.Create(context.Background(), gateway("reader", gatewaySettle+time.Second)))
	reconcile(t, r)
	cond := outputReadyOf(t, c)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, ReasonGatewaysReady, cond.Reason)
}

// Only gateway pods in the gateways' namespace count: a pod of another
// component on the node is no gateway.
func TestOnlyAGatewayServesTheOutput(t *testing.T) {
	other := gateway("reader", time.Minute)
	other.Labels["app.kubernetes.io/component"] = "agent"
	elsewhere := gateway("reader", time.Minute)
	elsewhere.Namespace = "other"
	r, c := harness(t, append(readyMembers(), newSet("reader"), other, elsewhere)...)

	reconcile(t, r)

	assert.Equal(t, metav1.ConditionFalse, outputReadyOf(t, c).Status)
}

func TestNoNodeWantsTheOutput(t *testing.T) {
	r, c := harness(t, append(readyMembers(), newSet())...)
	reconcile(t, r)
	cond := outputReadyOf(t, c)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, ReasonNoNodes, cond.Reason)
}

// A gateway pod changing brings back the sets wanted on its node.
func TestAGatewayPodChecksTheSetsOnItsNode(t *testing.T) {
	r, _ := harness(t, newSet("reader"))
	assert.Len(t, r.setsOn(context.Background(), "reader"), 1)
	assert.Empty(t, r.setsOn(context.Background(), "node-c"))
	assert.Empty(t, r.setsOn(context.Background(), ""), "an unscheduled pod is on no node")
}
