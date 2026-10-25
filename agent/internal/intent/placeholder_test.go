package intent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A placeholder has a flow_def.json like any copy, but nothing feeds it:
// a consumer opening it needs a mirror, so it must not count as local.
func TestPlaceholderIsNotALocalFlow(t *testing.T) {
	domain := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	flowDir := filepath.Join(domain, id+".mxl-flow")
	require.NoError(t, os.Mkdir(flowDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flowDir, "flow_def.json"), []byte(`{}`), 0o644))
	d := &Dispatcher{DomainPath: domain}
	ref := mxlv1alpha1.FlowRef{ID: id}

	require.True(t, d.flowExistsLocally(ref))

	require.NoError(t, os.MkdirAll(filepath.Join(domain, mxlv1alpha1.PlaceholderMarkerDir), 0o755))
	require.NoError(t, os.WriteFile(mxlv1alpha1.PlaceholderMarker(domain, id), nil, 0o644))
	require.False(t, d.flowExistsLocally(ref))
}
