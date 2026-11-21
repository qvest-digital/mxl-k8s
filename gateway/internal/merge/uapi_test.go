package merge

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs/uapitest"
)

// The view structs and ioctl numbers must match mxlfs_uapi.h, or a view
// call reads its argument from the wrong bytes or lands on another
// command.
func TestViewCallsMatchHeader(t *testing.T) {
	h, err := uapitest.Parse()
	require.NoError(t, err)
	require.NoError(t, h.CheckStruct("mxlfs_view_sources", reflect.TypeFor[viewSources]()))
	require.NoError(t, h.CheckStruct("mxlfs_view_range", reflect.TypeFor[viewRange]()))
	require.NoError(t, h.CheckStruct("mxlfs_view_replace", reflect.TypeFor[viewReplace]()))
	for name, nr := range map[string]uintptr{
		"MXLFS_IOC_VIEW_SET_SOURCES":    iocViewSetSources,
		"MXLFS_IOC_VIEW_SELECT":         iocViewSelect,
		"MXLFS_IOC_VIEW_SELECT_RANGE":   iocViewSelectRange,
		"MXLFS_IOC_ALLOCATE":            iocAllocate,
		"MXLFS_IOC_VIEW_REPLACE_SOURCE": iocViewReplaceSource,
	} {
		require.Equal(t, h.Ioctls[name], nr, "%s", name)
	}
	require.EqualValues(t, h.Consts["MXLFS_VIEW_MAX_SOURCES"], maxSources)
}
