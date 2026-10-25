package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
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
