//go:build linux

package mxlfs

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs/uapitest"
)

// On a filesystem that is not an mxlfs, every call is an unknown ioctl.
func TestCallsOnOtherFilesystem(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "f"))
	require.NoError(t, err)
	defer f.Close()
	_, err = NodePages(f)
	require.True(t, Unsupported(err), "%v", err)
	_, err = MoveTo(f, 0, MoveDryRun)
	require.True(t, Unsupported(err), "%v", err)
	_, err = MountNodes(f)
	require.True(t, Unsupported(err), "%v", err)
	require.True(t, Unsupported(Hold(f, true)))
	require.True(t, Unsupported(SetNode(f, 0)))
	_, err = ReadStats(f)
	require.True(t, Unsupported(err), "%v", err)
	_, err = IsView(f)
	require.True(t, Unsupported(err), "%v", err)
}

// On a real mxlfs (MXLFS_TEST_DIR): IsView tells a view from the file it
// shows, and STATS at the size this package asks with succeeds and reads
// the mount's allocation. It does not check the counters a module older
// than this package's struct leaves zero.
func TestOnMxlfs(t *testing.T) {
	dir := os.Getenv("MXLFS_TEST_DIR")
	if dir == "" {
		t.Skip("MXLFS_TEST_DIR not set")
	}
	h, err := uapitest.Parse()
	require.NoError(t, err)
	tmp, err := os.MkdirTemp(dir, "api-mxlfs-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmp) })

	src, err := os.Create(filepath.Join(tmp, "source"))
	require.NoError(t, err)
	defer src.Close()
	_, err = src.WriteAt(make([]byte, 4096), 0)
	require.NoError(t, err)
	view, err := os.Create(filepath.Join(tmp, "view"))
	require.NoError(t, err)
	defer view.Close()
	require.NoError(t, view.Truncate(4096))
	sources := struct {
		Count, Flags uint32
		FDs          [8]int32
	}{Count: 1}
	sources.FDs[0] = int32(src.Fd())
	require.NoError(t, ioctl(view, "MXLFS_IOC_VIEW_SET_SOURCES", h.Ioctls["MXLFS_IOC_VIEW_SET_SOURCES"], unsafe.Pointer(&sources)))

	ok, err := IsView(view)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = IsView(src)
	require.NoError(t, err)
	require.False(t, ok)

	d, err := os.Open(tmp)
	require.NoError(t, err)
	defer d.Close()
	s, err := ReadStats(d)
	require.NoError(t, err)
	require.NotZero(t, s.BytesAllocated, "the source's page")
}
