package observed

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func claim(ns, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(ClaimGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	return u
}

func pod(ns, name, uid, node, release string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid),
			Labels: map[string]string{"app.kubernetes.io/instance": release}},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func newWriter(t *testing.T, uses map[string][]Use, objs ...client.Object) (*ClaimWriter, client.Client) {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	s.AddKnownTypeWithName(ClaimGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(ClaimGVK.GroupVersion().WithKind("MediaFunctionClaimList"), &unstructured.UnstructuredList{})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
		WithStatusSubresource(claim("", "")).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).Build()
	scan := &fakeScan{uses: uses}
	return &ClaimWriter{Client: c, NodeName: "node-a", Label: "app.kubernetes.io/instance",
		Scanner: &Scanner{}, Log: logr.Discard(), scan: scan.Scan}, c
}

type fakeScan struct{ uses map[string][]Use }

func (f *fakeScan) Scan() (map[string][]Use, error) { return f.uses, nil }

func statusIO(t *testing.T, c client.Client, ns, name string) []any {
	t.Helper()
	u := claim(ns, name)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, u))
	io, _, _ := unstructured.NestedSlice(u.Object, "status", "io")
	return io
}

func TestSyncWritesWhatTheClaimsPodsUse(t *testing.T) {
	w, c := newWriter(t, map[string][]Use{
		"uid-writer": {{FlowID: flowA, Direction: Output}},
		"uid-reader": {{FlowID: flowA, Direction: Input}, {Domain: "studio", FlowID: flowB, Direction: Input}},
		"uid-other":  {{FlowID: flowB, Direction: Output}},
	},
		claim("prod", "srt-ingest"), claim("prod", "compositor"),
		pod("prod", "srt-ingest-0", "uid-writer", "node-a", "srt-ingest"),
		pod("prod", "compositor-0", "uid-reader", "node-a", "compositor"),
		pod("prod", "elsewhere-0", "uid-other", "node-b", "elsewhere"),
	)

	require.NoError(t, w.Sync(context.Background()))

	require.Equal(t, []any{map[string]any{"direction": "Output", "type": "MxlFlow", "id": flowA}},
		statusIO(t, c, "prod", "srt-ingest"))
	require.ElementsMatch(t, []any{
		map[string]any{"direction": "Input", "type": "MxlFlow", "id": flowA},
		map[string]any{"direction": "Input", "type": "MxlFlow", "id": flowB, "domain": "studio"},
	}, statusIO(t, c, "prod", "compositor"))
}

func TestSyncSkipsPodsWithoutAClaim(t *testing.T) {
	w, c := newWriter(t, map[string][]Use{"uid-1": {{FlowID: flowA, Direction: Input}}},
		pod("prod", "tool-0", "uid-1", "node-a", "not-a-claim"))

	require.NoError(t, w.Sync(context.Background()))

	var list unstructured.UnstructuredList
	list.SetGroupVersionKind(ClaimGVK.GroupVersion().WithKind("MediaFunctionClaimList"))
	require.NoError(t, c.List(context.Background(), &list))
	require.Empty(t, list.Items)
}
