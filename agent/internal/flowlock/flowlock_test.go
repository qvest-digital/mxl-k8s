package flowlock

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const flowID = "11111111-2222-3333-4444-555555555555"

// makeFlowDir builds the directory libmxl would have renamed into
// place, with the entries a flow of the given shape carries. Only the
// data file matters to the probe; the rest are there so the fixture
// cannot accidentally pass by looking at the wrong name.
func makeFlowDir(t *testing.T, domain, id string, extra ...string) string {
	t.Helper()
	dir := filepath.Join(domain, id+flowDirSuffix)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flow_def.json"),
		[]byte(`{"id":"`+id+`"}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, dataFileName), nil, 0o644))
	for _, e := range extra {
		require.NoError(t, os.WriteFile(filepath.Join(dir, e), nil, 0o644))
	}
	return dir
}

// holdShared takes the lock libmxl's writer holds for as long as it is
// attached, and releases it when the test ends. flock is owned by the
// open file description rather than by the process, so the exclusive
// probe conflicts with it even from the same test binary.
func holdShared(t *testing.T, dir string) {
	t.Helper()
	fd, err := syscall.Open(filepath.Join(dir, dataFileName),
		syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	require.NoError(t, err)
	require.NoError(t, syscall.Flock(fd, syscall.LOCK_SH|syscall.LOCK_NB))
	t.Cleanup(func() { _ = syscall.Close(fd) })
}

func TestWriterAttached_SharedLockHeld(t *testing.T) {
	domain := t.TempDir()
	dir := makeFlowDir(t, domain, flowID)
	holdShared(t, dir)

	attached, err := WriterAttached(domain, flowID)
	require.NoError(t, err)
	assert.True(t, attached,
		"a writer holds a shared lock on the data file for as long as it "+
			"is attached; that is libmxl's own definition of a live flow")
}

func TestWriterAttached_NoLockHeld(t *testing.T) {
	domain := t.TempDir()
	makeFlowDir(t, domain, flowID)

	attached, err := WriterAttached(domain, flowID)
	require.NoError(t, err)
	assert.False(t, attached,
		"the directory outlives the writer by up to one domain sweep, so "+
			"an unlocked data file is a copy nothing feeds")
}

// The three media types differ in the entries beside the data file --
// grains and access for discrete video and ANC, channels for
// continuous audio -- and not in the file the lock is taken on. A
// probe that keyed on any of the others would answer for one type and
// silently misreport the other two.
func TestWriterAttached_SameAnswerForEveryMediaType(t *testing.T) {
	cases := map[string][]string{
		"video": {"access", "grains"},
		"audio": {"channels"},
		"data":  {"access", "grains"},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			domain := t.TempDir()
			dir := makeFlowDir(t, domain, flowID, extra...)
			holdShared(t, dir)

			attached, err := WriterAttached(domain, flowID)
			require.NoError(t, err)
			assert.True(t, attached)
		})
	}
}

func TestWriterAttached_MissingDataFile(t *testing.T) {
	domain := t.TempDir()
	dir := filepath.Join(domain, flowID+flowDirSuffix)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	attached, err := WriterAttached(domain, flowID)
	require.NoError(t, err)
	assert.False(t, attached,
		"libmxl creates the data file before it allocates anything, so its "+
			"absence means nothing ever got as far as attaching")
}

func TestWriterAttached_MissingFlowDir(t *testing.T) {
	attached, err := WriterAttached(t.TempDir(), flowID)
	require.NoError(t, err)
	assert.False(t, attached)
}

// A probe that cannot open the file has to report a writer. Reading
// "cannot tell" as "abandoned" demotes a live producer's location and
// costs every consumer of that flow its source; reading it the other
// way costs one more pass.
func TestWriterAttached_UnreadableDataFileReportsAttached(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses the mode bits this case turns on")
	}
	domain := t.TempDir()
	dir := makeFlowDir(t, domain, flowID)
	require.NoError(t, os.Chmod(filepath.Join(dir, dataFileName), 0o000))

	attached, err := WriterAttached(domain, flowID)
	assert.Error(t, err)
	assert.True(t, attached)
}
