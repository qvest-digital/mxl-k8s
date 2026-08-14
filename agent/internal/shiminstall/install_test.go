package shiminstall

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The installed file is loaded by LD_PRELOAD in pods the agent never
// sees, so the tests pin what those pods depend on: the bytes are
// the ones the agent carries, the mode lets an arbitrary uid read
// them, and the replacement is a rename rather than a rewrite, which
// is what keeps a consumer that has already mapped the file running
// across an agent upgrade.

func writeFile(t *testing.T, path string, content string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), mode))
}

func TestInstall_PlacesReadableCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "image", "libmxl-intent.so")
	require.NoError(t, os.MkdirAll(filepath.Dir(src), 0o755))
	writeFile(t, src, "shim-bytes", 0o755)

	dst := filepath.Join(dir, "run", "libmxl-intent.so")
	require.NoError(t, Install(src, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, "shim-bytes", string(got))

	info, err := os.Stat(dst)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm(),
		"a consumer container runs as its own uid and only needs to "+
			"read the .so; the loader neither needs nor should get the "+
			"execute bit")
}

func TestInstall_ReplacesPreviousVersionByRename(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "libmxl-intent.so.new")
	writeFile(t, src, "new-shim", 0o644)

	dst := filepath.Join(dir, "libmxl-intent.so")
	writeFile(t, dst, "old-shim", 0o600)

	// A consumer that preloaded the previous version holds the file
	// open across the upgrade.
	mapped, err := os.Open(dst)
	require.NoError(t, err)
	defer mapped.Close()

	require.NoError(t, Install(src, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, "new-shim", string(got),
		"an upgraded agent must not leave the previous version's .so "+
			"on the node")

	info, err := os.Stat(dst)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm(),
		"the mode must come from the install, not from whatever the "+
			"file it replaced happened to carry")

	held := make([]byte, len("old-shim"))
	n, err := mapped.ReadAt(held, 0)
	require.NoError(t, err)
	assert.Equal(t, "old-shim", string(held[:n]),
		"the replacement must be a rename: overwriting in place would "+
			"change the bytes under a consumer that already mapped them")
}

func TestInstall_CreatesTargetDirectory(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "libmxl-intent.so.new")
	writeFile(t, src, "shim-bytes", 0o644)

	dst := filepath.Join(dir, "run", "mxl", "libmxl-intent.so")
	require.NoError(t, Install(src, dst),
		"the agent installs the shim before it binds the socket, so "+
			"nothing else has created the directory yet")

	_, err := os.Stat(dst)
	require.NoError(t, err)
}

func TestInstall_LeavesNoPartialFileOnCopyFailure(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "run", "libmxl-intent.so")

	// Reading a directory as a file fails after the temp file has
	// been created, which is the window a leftover could survive.
	require.Error(t, Install(dir, dst))

	entries, err := os.ReadDir(filepath.Dir(dst))
	require.NoError(t, err)
	assert.Empty(t, entries,
		"a failed install must not leave a truncated .so behind for a "+
			"consumer to preload")
}

func TestInstall_MissingSourceIsReported(t *testing.T) {
	dir := t.TempDir()
	err := Install(filepath.Join(dir, "absent.so"), filepath.Join(dir, "libmxl-intent.so"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "absent.so")
}
