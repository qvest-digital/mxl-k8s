//go:build linux

package statfs

import (
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStats_ReportsTheFilesystemOfThePath(t *testing.T) {
	capacity, free, err := Stats(t.TempDir())
	require.NoError(t, err)
	assert.Positive(t, capacity)
	assert.GreaterOrEqual(t, free, int64(0))
	assert.LessOrEqual(t, free, capacity, "free space is a part of the capacity")
}

func TestStats_MissingPathFailsNamingIt(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	_, _, err := Stats(missing)
	require.ErrorIs(t, err, syscall.ENOENT)
	assert.Contains(t, err.Error(), missing)
}
