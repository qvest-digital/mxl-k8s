package instance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMxlfs makes every directory look like an mxlfs and records the
// lazy allocation Open turns on.
func fakeMxlfs(t *testing.T) (stats, lazy *int) {
	t.Helper()
	stats, lazy = new(int), new(int)
	prevIs, prevLazy := isMxlfs, setLazy
	isMxlfs = func(string) (bool, error) { *stats++; return true, nil }
	setLazy = func(string, bool) error { *lazy++; return nil }
	t.Cleanup(func() { isMxlfs, setLazy = prevIs, prevLazy })
	return stats, lazy
}

func open(t *testing.T, withMxlfs bool) *Handles {
	t.Helper()
	h, err := Open(t.TempDir(), withMxlfs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// Without mxlfs a domain is a plain directory whatever it is mounted
// as: no statfs, no lazy allocation, and nothing downstream takes the
// mxlfs paths.
func TestOpenWithoutMxlfsNeverTreatsTheDomainAsOne(t *testing.T) {
	stats, lazy := fakeMxlfs(t)
	h := open(t, false)
	assert.False(t, h.OnMxlfs())
	assert.Zero(t, *stats)
	assert.Zero(t, *lazy)
}

func TestOpenWithMxlfsAllocatesLazilyOnOne(t *testing.T) {
	_, lazy := fakeMxlfs(t)
	h := open(t, true)
	assert.True(t, h.OnMxlfs())
	assert.Equal(t, 1, *lazy)
}
