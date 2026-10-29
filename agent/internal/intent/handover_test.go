package intent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/qvest-digital/mxl-k8s/agent/internal/observed"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const handoverNode = "n-target"

func handoverPod(ns, name, uid string, age time.Duration) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, UID: types.UID(uid),
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age).Truncate(time.Second)),
		},
		Spec: corev1.PodSpec{NodeName: handoverNode},
	}
}

// onDemandMirror is a mirror as ensureMirror creates it for requestor.
func onDemandMirror(requestor *corev1.Pod) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: requestor.Namespace,
			Name:      mxlv1alpha1.MirrorName(flowID, handoverNode),
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByIntent: handoverNode,
				mxlv1alpha1.LabelRequestorPodUID: string(requestor.UID),
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID: flowID, SourceNode: "n-src", TargetNode: handoverNode,
			Requestor: &mxlv1alpha1.PodRef{Namespace: requestor.Namespace, Name: requestor.Name, UID: string(requestor.UID)},
		},
	}
}

func handoverDispatcher(t *testing.T, objs ...client.Object) (*Dispatcher, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		Build()
	return &Dispatcher{
		Client:   c,
		NodeName: handoverNode,
		Own:      func(p *corev1.Pod) bool { return p.Namespace == "mxl-system" },
	}, c
}

// reads is a scan in which each pod named maps the flow as a reader.
func reads(uids ...string) func() (map[string][]observed.Use, error) {
	out := map[string][]observed.Use{}
	for _, uid := range uids {
		out[uid] = []observed.Use{{FlowID: flowID, Direction: observed.Input}}
	}
	return func() (map[string][]observed.Use, error) { return out, nil }
}

func requestorOf(t *testing.T, c client.Client, m *mxlv1alpha1.MxlFlowMirror) (mxlv1alpha1.PodRef, string) {
	t.Helper()
	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(m), &got))
	require.NotNil(t, got.Spec.Requestor)
	return *got.Spec.Requestor, got.Labels[mxlv1alpha1.LabelRequestorPodUID]
}

// A reader replaced on the same node opens the mirrored copy that is
// already there, so it never asks for the mirror and the mirror stays
// claimed by the pod it replaced. Once that pod is gone the operator
// collects the mirror and the new reader is left reading a ring nothing
// writes. The pod the kernel says still maps the flow is the claimant.
func TestHandOver_RequestorGone_NamesThePodStillReading(t *testing.T) {
	old := handoverPod("p", "engine-old", "uid-old", time.Hour)
	repl := handoverPod("p", "engine-new", "uid-new", time.Minute)
	m := onDemandMirror(old)
	d, c := handoverDispatcher(t, m, repl)

	require.NoError(t, d.HandOver(context.Background(), reads("uid-new")))

	ref, label := requestorOf(t, c, m)
	assert.Equal(t, mxlv1alpha1.PodRef{Namespace: "p", Name: "engine-new", UID: "uid-new"}, ref)
	assert.Equal(t, "uid-new", label, "the per-pod limit counts mirrors by this label, so it has to move with the claim")
}

// While the requestor still maps the flow nothing is written: a sweep
// every ten seconds must not churn the spec of every mirror on the node.
func TestHandOver_RequestorStillReading_IsLeftAlone(t *testing.T) {
	req := handoverPod("p", "engine", "uid-1", time.Hour)
	other := handoverPod("p", "preview", "uid-2", 2*time.Hour)
	m := onDemandMirror(req)
	d, c := handoverDispatcher(t, m, req, other)
	before := resourceVersion(t, c, m)

	require.NoError(t, d.HandOver(context.Background(), reads("uid-1", "uid-2")))

	assert.Equal(t, before, resourceVersion(t, c, m))
}

// The gateway writing the copy and the exporter reading it map the flow
// too. Naming either would keep a mirror alive that no consumer reads.
func TestHandOver_OnlyMxlK8sOwnPodsMapIt_IsLeftAlone(t *testing.T) {
	old := handoverPod("p", "engine-old", "uid-old", time.Hour)
	exporter := handoverPod("mxl-system", "exporter", "uid-exp", time.Hour)
	m := onDemandMirror(old)
	d, c := handoverDispatcher(t, m, exporter)

	require.NoError(t, d.HandOver(context.Background(), reads("uid-exp")))

	ref, _ := requestorOf(t, c, m)
	assert.Equal(t, "uid-old", ref.UID)
}

// A pod that is shutting down is about to go the same way as the one it
// would replace; handing it the claim buys one more collection.
func TestHandOver_PrefersARunningReaderOverOneTerminating(t *testing.T) {
	old := handoverPod("p", "engine-old", "uid-old", time.Hour)
	leaving := handoverPod("p", "engine-leaving", "uid-leaving", 2*time.Hour)
	leaving.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	leaving.Finalizers = []string{"test/keep"}
	staying := handoverPod("p", "engine-new", "uid-new", time.Minute)
	m := onDemandMirror(old)
	d, c := handoverDispatcher(t, m, leaving, staying)

	require.NoError(t, d.HandOver(context.Background(), reads("uid-leaving", "uid-new")))

	ref, _ := requestorOf(t, c, m)
	assert.Equal(t, "uid-new", ref.UID)
}

// The mirror lives in its requestor's namespace and goes with it, so a
// reader there is named before one elsewhere, however much older that
// one is.
func TestHandOver_PrefersAReaderInTheMirrorsNamespace(t *testing.T) {
	old := handoverPod("p", "engine-old", "uid-old", time.Hour)
	elsewhere := handoverPod("q", "monitor", "uid-q", 24*time.Hour)
	same := handoverPod("p", "engine-new", "uid-new", time.Minute)
	m := onDemandMirror(old)
	d, c := handoverDispatcher(t, m, elsewhere, same)

	require.NoError(t, d.HandOver(context.Background(), reads("uid-q", "uid-new")))

	ref, _ := requestorOf(t, c, m)
	assert.Equal(t, "uid-new", ref.UID)
}

// Nobody reads the flow any more: the mirror keeps the requestor it has
// and the operator collects it once the handover window closes. A pod
// writing the flow here is no reader of the copy; it makes this node
// the origin, which the operator answers by deleting the mirror.
func TestHandOver_NoReader_IsLeftAlone(t *testing.T) {
	old := handoverPod("p", "engine-old", "uid-old", time.Hour)
	writer := handoverPod("p", "writer", "uid-writer", time.Minute)
	m := onDemandMirror(old)
	d, c := handoverDispatcher(t, m, writer)
	before := resourceVersion(t, c, m)

	require.NoError(t, d.HandOver(context.Background(), func() (map[string][]observed.Use, error) {
		return map[string][]observed.Use{
			"uid-writer": {{FlowID: flowID, Direction: observed.Output}},
		}, nil
	}))

	assert.Equal(t, before, resourceVersion(t, c, m))
}

// The sweep runs every ten seconds on every node. Walking /proc there
// buys nothing when the node has no mirror created on demand to hand
// over, so a node without one is not scanned.
func TestHandOver_NoOnDemandMirror_DoesNotScan(t *testing.T) {
	receiverOwned := onDemandMirror(handoverPod("p", "engine", "uid-1", time.Hour))
	receiverOwned.Labels = nil
	receiverOwned.Spec.Requestor = nil
	d, _ := handoverDispatcher(t, receiverOwned)

	require.NoError(t, d.HandOver(context.Background(), func() (map[string][]observed.Use, error) {
		t.Error("scanned /proc on a node with no mirror created on demand")
		return nil, nil
	}))
}

// The handover is decided on the mirror as it was read. Anything that
// wrote it since -- the operator deleting it, another sweep handing it
// over -- has a newer answer, and the patch must not land over it.
func TestHandOver_MirrorWrittenSinceItWasRead_IsLeftAsWritten(t *testing.T) {
	old := handoverPod("p", "engine-old", "uid-old", time.Hour)
	repl := handoverPod("p", "engine-new", "uid-new", time.Minute)
	m := onDemandMirror(old)
	base := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(m, repl).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			var cur mxlv1alpha1.MxlFlowMirror
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(obj), &cur))
			cur.Annotations = map[string]string{"written": "since the sweep read it"}
			require.NoError(t, cl.Update(ctx, &cur))
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	d := &Dispatcher{Client: c, NodeName: handoverNode}

	require.NoError(t, d.HandOver(context.Background(), reads("uid-new")))

	ref, label := requestorOf(t, base, m)
	assert.Equal(t, "uid-old", ref.UID, "a handover read before the last write landed on top of it")
	assert.Equal(t, "uid-old", label)
}

func resourceVersion(t *testing.T, c client.Client, m *mxlv1alpha1.MxlFlowMirror) string {
	t.Helper()
	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(m), &got))
	return got.ResourceVersion
}
