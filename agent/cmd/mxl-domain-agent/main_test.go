package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/qvest-digital/mxl-k8s/agent/internal/config"
	"github.com/qvest-digital/mxl-k8s/agent/internal/mxlfsmount"
	"github.com/qvest-digital/mxl-k8s/agent/internal/observed"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Without mxlfs the publisher asks about no redundancy set: each flow
// that appears would otherwise cost an uncached List of a kind the agent
// then has no access to.
func TestNewFlowPublisherConsultsSetsOnlyWithMxlfs(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	fp, sets := newFlowPublisher(&config.Config{DomainPath: "/run/mxl/domain", NodeName: "n1"}, c, nil)
	assert.Nil(t, fp.IsOutput)
	assert.Nil(t, sets)

	fp, sets = newFlowPublisher(&config.Config{DomainPath: "/run/mxl/domain", NodeName: "n1", Mxlfs: true}, c, nil)
	assert.NotNil(t, fp.IsOutput)
	assert.NotNil(t, sets)
}

func TestRequireKindNamesTheMissingCRD(t *testing.T) {
	err := requireKind(meta.NewDefaultRESTMapper(nil), "MxlFlowRedundancySet")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MxlFlowRedundancySet.mxl.qvest-digital.com is not served")

	m := meta.NewDefaultRESTMapper(nil)
	m.Add(mxlv1alpha1.GroupVersion.WithKind("MxlFlowRedundancySet"), meta.RESTScopeRoot)
	require.NoError(t, requireKind(m, "MxlFlowRedundancySet"))
}

// Without mxlfs a CSI publish binds at once, as a plain directory needs
// nothing first; with it, only once the agent has decided the domain.
func TestCSIReadyWaitsOnlyWithMxlfs(t *testing.T) {
	assert.Nil(t, csiReady(nil))

	m := &mxlfsmount.Mounter{IsMxlfs: func(string) bool { return true }}
	ready := csiReady(m)
	dir := t.TempDir()
	assert.False(t, ready(dir))
	require.NoError(t, m.Ensure(dir))
	assert.True(t, ready(dir))
}

type podsByPID map[int32]*corev1.Pod

func (p podsByPID) PodForPID(_ context.Context, pid int32) (*corev1.Pod, error) {
	if pod, ok := p[pid]; ok {
		return pod, nil
	}
	return nil, errors.New("no pod")
}

// Only the components that open placeholders themselves are skipped. A
// consumer deployed into the agent's namespace still gets its mirror,
// and a pid whose pod lookup fails is not silently dropped as ours.
func TestOursIsOnlyMxlK8sComponents(t *testing.T) {
	pod := func(ns, component string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns,
			Labels: map[string]string{"app.kubernetes.io/component": component}}}
	}
	o := ours(podsByPID{
		1: pod("mxl-system", "gateway"),
		2: pod("mxl-system", "exporter"),
		3: pod("mxl-system", "agent"),
		4: pod("mxl-system", ""),
		5: pod("media", "gateway"),
	}, "mxl-system")
	ctx := context.Background()
	assert.True(t, o(ctx, 1), "gateway")
	assert.True(t, o(ctx, 2), "exporter")
	assert.True(t, o(ctx, 3), "agent")
	assert.False(t, o(ctx, 4), "a consumer in the agent's namespace")
	assert.False(t, o(ctx, 5), "a gateway-labelled pod elsewhere")
	assert.False(t, o(ctx, 6), "a pid with no resolvable pod")
}

// The gateway maps the output it merges and the exporter reads every
// flow; counted as readers they would keep every output in use forever.
func TestFlowsInUseLeavesOutMxlK8sComponents(t *testing.T) {
	pod := func(name, ns, uid, node, component string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid),
			Labels: map[string]string{"app.kubernetes.io/component": component}}, Spec: corev1.PodSpec{NodeName: node}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pod("gw", "mxl-system", "gw", "n1", "gateway"), pod("exp", "mxl-system", "exp", "n1", "exporter"),
			pod("viewer", "media", "viewer", "n1", "")).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).Build()
	const merged, other = "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444"
	scan := func() (map[string][]observed.Use, error) {
		return map[string][]observed.Use{
			"gw":     {{FlowID: merged, Direction: observed.Output}},
			"exp":    {{FlowID: merged, Direction: observed.Input}},
			"viewer": {{Domain: "studio", FlowID: other, Direction: observed.Input}},
		}, nil
	}

	got, err := flowsInUse(context.Background(), c, "n1", "mxl-system", scan)
	require.NoError(t, err)
	assert.Equal(t, map[mxlv1alpha1.FlowRef]bool{{Domain: "studio", ID: other}: true}, got)
}
