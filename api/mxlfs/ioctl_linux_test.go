//go:build linux

package mxlfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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
}
