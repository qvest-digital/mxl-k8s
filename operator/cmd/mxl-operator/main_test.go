package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func reconcilerNames(t *testing.T, mxlfs bool) []string {
	t.Helper()
	// Nothing is set up or started, so the manager never reaches the
	// address.
	mgr, err := ctrl.NewManager(&rest.Config{Host: "https://127.0.0.1:1"}, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	require.NoError(t, err)
	var names []string
	for _, rc := range reconcilers(mgr, time.Minute, mxlfs, "ns") {
		names = append(names, rc.name)
	}
	return names
}

// Without mxlfs the operator neither watches redundancy sets nor holds
// RBAC for them, and a cluster may not have their CRD at all.
func TestReconcilers_RedundancySetsOnlyWithMxlfs(t *testing.T) {
	assert.NotContains(t, reconcilerNames(t, false), "MxlFlowRedundancySet")
	assert.Contains(t, reconcilerNames(t, true), "MxlFlowRedundancySet")
	assert.Contains(t, reconcilerNames(t, false), "MxlFlowMirror")
}

func TestRequireKind_NamesTheMissingCRD(t *testing.T) {
	err := requireKind(meta.NewDefaultRESTMapper(nil), "MxlFlowRedundancySet")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MxlFlowRedundancySet.mxl.qvest-digital.com is not served")

	m := meta.NewDefaultRESTMapper(nil)
	m.Add(mxlv1alpha1.GroupVersion.WithKind("MxlFlowRedundancySet"), meta.RESTScopeRoot)
	require.NoError(t, requireKind(m, "MxlFlowRedundancySet"))
}
