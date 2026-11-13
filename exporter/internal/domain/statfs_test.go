package domain

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// driftBytes bounds how far the filesystem may move between the
// reference statfs and the one under test.
const driftBytes = 16 << 20

// pathWithReservedBlocks returns a directory on a filesystem where
// blocks available to unprivileged users differ from free blocks, so
// that reading the wrong counter is visible. Falls back to a temp dir
// when no such filesystem is mounted.
func pathWithReservedBlocks(t *testing.T) string {
	t.Helper()
	for _, p := range []string{"/var/tmp", "/home", "/", os.TempDir()} {
		var s unix.Statfs_t
		if unix.Statfs(p, &s) == nil && s.Bavail != s.Bfree {
			return p
		}
	}
	t.Log("no filesystem with reserved blocks found; Bavail vs Bfree is not distinguished")
	return t.TempDir()
}

func TestStat_ReportsTheFilesystemOfTheDomain(t *testing.T) {
	path := pathWithReservedBlocks(t)
	var want unix.Statfs_t
	require.NoError(t, unix.Statfs(path, &want))
	bsize := uint64(want.Bsize)

	d, err := Open(path, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	st, err := d.Stat()
	require.NoError(t, err)

	assert.Equal(t, want.Blocks*bsize, st.TotalBytes, "total is Blocks*Bsize")
	// Available is what the next flow can allocate (Bavail); used is
	// Blocks-Bfree, so reserved blocks are neither used nor available.
	assert.InDelta(t, float64(want.Bavail*bsize), float64(st.AvailableBytes), driftBytes)
	assert.InDelta(t, float64((want.Blocks-want.Bfree)*bsize), float64(st.UsedBytes), driftBytes)
	assert.Positive(t, st.UsedBytes)
}

func TestStat_RemovedDomainFailsNamingIt(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	require.NoError(t, os.RemoveAll(dir))

	_, err = d.Stat()
	require.ErrorIs(t, err, syscall.ENOENT)
	assert.Contains(t, err.Error(), dir)
}
