//go:build linux

package statfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

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

func TestStats_ReportsTheFilesystemOfThePath(t *testing.T) {
	path := pathWithReservedBlocks(t)
	var want unix.Statfs_t
	require.NoError(t, unix.Statfs(path, &want))
	bsize := int64(want.Bsize)

	capacity, free, err := Stats(path)
	require.NoError(t, err)

	assert.Equal(t, int64(want.Blocks)*bsize, capacity, "capacity is Blocks*Bsize")
	// Free is what an unprivileged writer can use (Bavail), not Bfree.
	assert.InDelta(t, float64(int64(want.Bavail)*bsize), float64(free), driftBytes)
	assert.Positive(t, free)
}

func TestStats_MissingPathFailsNamingIt(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	_, _, err := Stats(missing)
	require.ErrorIs(t, err, syscall.ENOENT)
	assert.Contains(t, err.Error(), missing)
}
