package domain

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A placeholder only lists a flow the node has no copy of. Reporting it
// would show a flow present here whose ring nothing writes.
func TestScanSkipsPlaceholders(t *testing.T) {
	d := newObservedFlow(t)
	require.Len(t, d.Observe(), 1)

	require.NoError(t, os.MkdirAll(filepath.Join(d.Path(), mxlv1alpha1.PlaceholderMarkerDir), 0o755))
	require.NoError(t, os.WriteFile(mxlv1alpha1.PlaceholderMarker(d.Path(), activeTestFlowID), nil, 0o644))
	require.NoError(t, d.Scan())

	for _, o := range d.Observe() {
		require.False(t, o.Present, "a placeholder reported as a present flow")
	}
}
