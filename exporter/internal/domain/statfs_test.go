package domain

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStat_ReportsTheFilesystemOfTheDomain(t *testing.T) {
	d, err := Open(t.TempDir(), time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	st, err := d.Stat()
	require.NoError(t, err)
	assert.Positive(t, st.TotalBytes)
	assert.LessOrEqual(t, st.UsedBytes, st.TotalBytes)
	assert.LessOrEqual(t, st.AvailableBytes, st.TotalBytes)
	// Blocks reserved for root count as neither used nor available.
	assert.LessOrEqual(t, st.UsedBytes+st.AvailableBytes, st.TotalBytes)
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
