package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/mirror"
)

func TestProviderNames(t *testing.T) {
	cases := []struct {
		name string
		in   []fabrics.Provider
		want []string
	}{
		{"empty", nil, []string{}},
		{"single", []fabrics.Provider{fabrics.ProviderTCP}, []string{"tcp"}},
		{
			"multiple preserves order",
			[]fabrics.Provider{fabrics.ProviderTCP, fabrics.ProviderVerbs, fabrics.ProviderSHM},
			[]string{"tcp", "verbs", "shm"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := providerNames(tc.in)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Without mxlfs the gateway runs what it ran before mxlfs existed: no
// placeholder or redundancy set controller, and no hook on the target
// reconciler that would call into them.
func TestMxlfsComponentsOnlyWithMxlfs(t *testing.T) {
	target := &mirror.TargetReconciler{}
	assert.Empty(t, mxlfsComponents(false, nil, "n1", &instance.Set{}, target))
	assert.Nil(t, target.Filled)
	assert.Nil(t, target.Unfilled)
	assert.Nil(t, target.Retire, "without mxlfs a target never replaces a directory of another geometry")
	assert.Nil(t, target.SamplesCommitted)

	var names []string
	for _, c := range mxlfsComponents(true, nil, "n1", &instance.Set{}, target) {
		names = append(names, c.name)
	}
	assert.ElementsMatch(t, []string{"stale placeholder markers", "redundancy set reconciler", "placeholder reconciler"}, names)
	assert.NotNil(t, target.Filled)
	assert.NotNil(t, target.Unfilled)
	assert.NotNil(t, target.Retire)
	assert.NotNil(t, target.SamplesCommitted)
}

func TestRequireKindNamesTheMissingCRD(t *testing.T) {
	err := requireKind(meta.NewDefaultRESTMapper(nil), "MxlFlowRedundancySet")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MxlFlowRedundancySet.mxl.qvest-digital.com is not served")

	m := meta.NewDefaultRESTMapper(nil)
	m.Add(mxlv1alpha1.GroupVersion.WithKind("MxlFlowRedundancySet"), meta.RESTScopeRoot)
	require.NoError(t, requireKind(m, "MxlFlowRedundancySet"))
}

// A gateway that exits leaves its placeholders and their markers. The
// next one opening a domain, the primary one at start or any other as
// it is first opened, keeps those markers: until it holds a
// placeholder again, its directory without a marker reads as a local
// copy that no mirror fills. The placeholder manager's first pass
// clears the ones it does not hold.
func TestPlaceholderMarkersOutliveADomainOpen(t *testing.T) {
	marker := func(domain string) string {
		p := mxlv1alpha1.PlaceholderMarker(domain, "5fbec3b1-1b0f-417d-9059-8b94a47197ed")
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, nil, 0o644))
		return p
	}
	root := t.TempDir()
	primary := filepath.Join(root, "domain")
	studio := filepath.Join(root, mxlv1alpha1.DomainsDir, "studio")
	left, later := marker(primary), marker(studio)
	h, err := instance.Open(primary, true)
	require.NoError(t, err)
	domains := &instance.Set{Primary: h, Directory: func(context.Context, string) (string, error) {
		return filepath.Join(mxlv1alpha1.DomainsDir, "studio"), nil
	}}
	t.Cleanup(func() { _ = domains.Close() })

	for _, c := range mxlfsComponents(true, nil, "n1", domains, &mirror.TargetReconciler{}) {
		if c.name == "stale placeholder markers" {
			require.NoError(t, c.setup(nil))
		}
	}
	assert.FileExists(t, left, "the primary domain's marker")

	_, err = domains.For(context.Background(), "studio")
	require.NoError(t, err)
	assert.FileExists(t, later, "a domain opened later")
}
