package domain

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// runtimeRoot lays out a runtime root the way a node does: the primary
// domain at domain/, and every other domain at domains/<id>/.
func runtimeRoot(t *testing.T) (primary, others string) {
	t.Helper()
	root := t.TempDir()
	primary = filepath.Join(root, "domain")
	others = filepath.Join(root, mxlv1alpha1.DomainsDir)
	require.NoError(t, os.MkdirAll(primary, 0o755))
	return primary, others
}

// writeFlow creates the test flow in the domain at dir, with one grain
// written, and keeps its writer open for the test.
func writeFlow(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	inst, err := mxl.NewInstance(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	w, _, err := inst.NewWriter(activeTestFlowDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	ga, err := w.OpenGrain(mxl.CurrentIndex(w.Config().Common.GrainRate))
	require.NoError(t, err)
	require.NoError(t, ga.Commit(ga.TotalSlices, 0))
}

func paths(ds []*Domain) []string {
	var out []string
	for _, d := range ds {
		out = append(out, d.Path())
	}
	return out
}

// A flow in a domain other than the primary is reported from the node
// that has it. Without that, the only series a node had for it was
// mxl_flow_location_info, which is what a flow the control plane places
// on a node and the node does not hold looks like.
func TestSetReportsFlowsInEveryDomain(t *testing.T) {
	primary, others := runtimeRoot(t)
	studio := filepath.Join(others, "5d0c4a9e-0000-4000-8000-000000000001")
	writeFlow(t, studio)

	s, err := OpenSet(primary, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.Scan())

	ds := s.Domains()
	require.Equal(t, []string{primary, studio}, paths(ds), "the primary first, then each other domain")
	require.Empty(t, ds[0].Observe())
	obs := ds[1].Observe()
	require.Len(t, obs, 1)
	require.Equal(t, activeTestFlowID, obs[0].ID)
	require.True(t, obs[0].Present)
}

// A domain appearing after start is picked up by the next scan, and one
// that is removed stops being reported rather than leaving its flows'
// series behind for good.
func TestSetFollowsDomainsAppearingAndLeaving(t *testing.T) {
	primary, others := runtimeRoot(t)
	s, err := OpenSet(primary, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.Scan(), "no domains/ yet is a node with only the primary domain")
	require.Equal(t, []string{primary}, paths(s.Domains()))

	studio := filepath.Join(others, "5d0c4a9e-0000-4000-8000-000000000002")
	require.NoError(t, os.MkdirAll(studio, 0o755))
	require.NoError(t, s.Scan())
	require.Equal(t, []string{primary, studio}, paths(s.Domains()))

	require.NoError(t, os.RemoveAll(studio))
	require.NoError(t, s.Scan())
	require.Equal(t, []string{primary}, paths(s.Domains()))
}

// A placeholder in another domain is skipped the same way as in the
// primary: it is a flow the node has no copy of.
func TestSetSkipsPlaceholdersInOtherDomains(t *testing.T) {
	primary, others := runtimeRoot(t)
	studio := filepath.Join(others, "5d0c4a9e-0000-4000-8000-000000000003")
	writeFlow(t, studio)
	require.NoError(t, os.MkdirAll(filepath.Join(studio, mxlv1alpha1.PlaceholderMarkerDir), 0o755))
	require.NoError(t, os.WriteFile(mxlv1alpha1.PlaceholderMarker(studio, activeTestFlowID), nil, 0o644))

	s, err := OpenSet(primary, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.Scan())

	ds := s.Domains()
	require.Len(t, ds, 2)
	require.Empty(t, ds[1].Observe())
}
