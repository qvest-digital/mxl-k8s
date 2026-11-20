package domain

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
)

// On mxlfs a lookup by name of a flow directory that is not there waits
// for it to appear. A scan lists the domain, walks every flow's size and
// then opens readers with the domain's lock held, which leaves time for
// a listed flow to go; the reader is opened only for a flow the domain
// lists again.
func TestScanOpensReadersOnlyForListedFlows(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	var asked []string
	saved := newReader
	newReader = func(inst *mxl.Instance, id string) (*mxl.Reader, error) {
		asked = append(asked, id)
		return saved(inst, id)
	}
	t.Cleanup(func() { newReader = saved })

	d.openReader(&entry{id: activeTestFlowID})
	require.Empty(t, asked, "a flow the listing did not return was looked up by name")

	inst, err := mxl.NewInstance(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	w, _, err := inst.NewWriter(activeTestFlowDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, d.Scan())
	require.Equal(t, []string{activeTestFlowID}, asked)
	require.True(t, d.Observe()[0].HaveInfo)
}

// A flow's size is walked through the directories the walk opened, not
// by path: on mxlfs every lookup through a flow directory that went
// mid-walk waits for it to come back. A domain moved away once its
// directory is open still yields every flow's size.
func TestDirSizeWalksTheOpenDirectory(t *testing.T) {
	dom := t.TempDir()
	flow := filepath.Join(dom, activeTestFlowID+flowDirSuffix)
	require.NoError(t, os.MkdirAll(filepath.Join(flow, "grains"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flow, "data"), make([]byte, 100), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(flow, "grains", "data.0"), make([]byte, 20), 0o644))
	require.NoError(t, os.Symlink("data", filepath.Join(flow, "link")))
	dir, err := os.Open(dom)
	require.NoError(t, err)
	t.Cleanup(func() { dir.Close() })

	require.NoError(t, os.Rename(dom, dom+".gone"))
	t.Cleanup(func() { _ = os.Rename(dom+".gone", dom) })

	require.EqualValues(t, 120, dirSize(dir, activeTestFlowID+flowDirSuffix))
}

// A symlink in place of a directory is not walked: a flow directory
// swapped for a link between the type check and the open would
// otherwise have the walk count whatever the link points at.
func TestDirSizeDoesNotFollowASymlink(t *testing.T) {
	dom := t.TempDir()
	other := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(other, "data"), make([]byte, 100), 0o644))
	require.NoError(t, os.Symlink(other, filepath.Join(dom, activeTestFlowID+flowDirSuffix)))
	dir, err := os.Open(dom)
	require.NoError(t, err)
	t.Cleanup(func() { dir.Close() })

	require.Zero(t, dirSize(dir, activeTestFlowID+flowDirSuffix))
}
