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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/qvest-digital/mxl-k8s/agent/internal/config"
	"github.com/qvest-digital/mxl-k8s/agent/internal/mxlfsmount"
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
