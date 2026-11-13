//go:build !linux

package mxlfs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Off Linux there is no mxlfs: every call says so.
func TestCallsAreUnsupportedOffLinux(t *testing.T) {
	_, err := NodePages(nil)
	require.ErrorIs(t, err, errors.ErrUnsupported)
	_, err = MoveTo(nil, 0, 0)
	require.ErrorIs(t, err, errors.ErrUnsupported)
	_, err = MountNodes(nil)
	require.ErrorIs(t, err, errors.ErrUnsupported)
	_, err = ReadStats(nil)
	require.ErrorIs(t, err, errors.ErrUnsupported)
	require.ErrorIs(t, Hold(nil, true), errors.ErrUnsupported)
	require.ErrorIs(t, SetNode(nil, 0), errors.ErrUnsupported)
	require.True(t, Unsupported(SetNode(nil, 0)))
}
