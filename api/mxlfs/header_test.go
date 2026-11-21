package mxlfs

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs/uapitest"
)

// Every struct, ioctl and constant of the header is mirrored here and
// matches, or is named as mirrored elsewhere or not used, so a header
// that grows or changes fails this test until each mirror follows.
func TestMirrorMatchesHeader(t *testing.T) {
	h, err := uapitest.Parse()
	require.NoError(t, err)

	structs := map[string]reflect.Type{
		"mxlfs_stats":       reflect.TypeFor[Stats](),
		"mxlfs_node_pref":   reflect.TypeFor[nodePref](),
		"mxlfs_node_pages":  reflect.TypeFor[nodePages](),
		"mxlfs_move":        reflect.TypeFor[Move](),
		"mxlfs_node_stat":   reflect.TypeFor[NodeStat](),
		"mxlfs_mount_nodes": reflect.TypeFor[mountNodes](),
	}
	// Mirrored in another module, whose tests check them against the
	// same header, or not used.
	elsewhere := []string{
		"mxlfs_view_sources", "mxlfs_view_range", "mxlfs_view_replace", // gateway/internal/merge
		"mxlfs_wanted_req", // agent/internal/mxlfswatch
		"mxlfs_wanted_fail",
	}
	for name := range h.Structs {
		if typ, ok := structs[name]; ok {
			require.NoError(t, h.CheckStruct(name, typ))
			continue
		}
		require.Contains(t, elsewhere, name, "struct %s is not mirrored", name)
	}
	for name := range structs {
		require.Contains(t, h.Structs, name, "the header has no struct %s", name)
	}

	ioctls := map[string]uintptr{
		"MXLFS_IOC_VIEW_GET_SELECTED": iocGetSelected,
		"MXLFS_IOC_STATS":             iocStats,
		"MXLFS_IOC_SET_NODE":          iocSetNode,
		"MXLFS_IOC_NODE_PAGES":        iocNodePages,
		"MXLFS_IOC_MOVE":              iocMove,
		"MXLFS_IOC_HOLD":              iocHold,
		"MXLFS_IOC_MOUNT_NODES":       iocMountNodes,
	}
	ioctlsElsewhere := []string{
		"MXLFS_IOC_VIEW_SET_SOURCES", "MXLFS_IOC_VIEW_SELECT", "MXLFS_IOC_VIEW_SELECT_RANGE",
		"MXLFS_IOC_VIEW_REPLACE_SOURCE", "MXLFS_IOC_ALLOCATE", // gateway/internal/merge
		"MXLFS_IOC_LAZY_ALLOC",  // gateway/internal/mxlfs, and ALLOCATE
		"MXLFS_IOC_WANTED_NEXT", // agent/internal/mxlfswatch
		"MXLFS_IOC_WANTED_FAIL",
	}
	for name, nr := range h.Ioctls {
		if want, ok := ioctls[name]; ok {
			require.Equal(t, nr, want, "%s", name)
			continue
		}
		require.Contains(t, ioctlsElsewhere, name, "ioctl %s is not mirrored", name)
	}
	for name := range ioctls {
		require.Contains(t, h.Ioctls, name)
	}

	consts := map[string]int64{
		"MXLFS_NODE_NONE":          NodeNone,
		"MXLFS_MOVE_DRY_RUN":       MoveDryRun,
		"MXLFS_MOVE_STRICT":        MoveStrict,
		"MXLFS_NODE_PAGES_RECOUNT": NodePagesRecount,
		"MXLFS_MOVED_ALL":          int64(MovedAll),
		"MXLFS_MOVED_PARTIAL":      int64(MovedPartial),
		"MXLFS_MOVED_NONE":         int64(MovedNone),
		"MXLFS_REFUSE_PINNED":      int64(RefusePinned),
		"MXLFS_REFUSE_VIEW":        int64(RefuseView),
		"MXLFS_REFUSE_BUSY":        int64(RefuseBusy),
		"MXLFS_REFUSE_NOMEM":       int64(RefuseNomem),
		"MXLFS_REFUSE_HELD":        int64(RefuseHeld),
		"MXLFS_REFUSE_SIGNAL":      int64(RefuseSignal),
		"MXLFS_REFUSE_POISONED":    int64(RefusePoisoned),
		"MXLFS_REFUSE_MAX":         RefuseMax,
	}
	constsElsewhere := []string{
		"MXLFS_MAGIC",            // every module's statfs check
		"MXLFS_VIEW_MAX_SOURCES", // gateway/internal/merge
		"MXLFS_VIEW_MAX_SIZE", "MXLFS_STATS_SIZE_VER0",
	}
	for name, v := range h.Consts {
		if want, ok := consts[name]; ok {
			require.Equal(t, v, want, "%s", name)
			continue
		}
		require.Contains(t, constsElsewhere, name, "constant %s is not mirrored", name)
	}
	for name := range consts {
		require.Contains(t, h.Consts, name)
	}
}

// Every reason the header names has a label of its own, so none is
// exported as error.
func TestEveryRefuseReasonHasALabel(t *testing.T) {
	h, err := uapitest.Parse()
	require.NoError(t, err)
	seen := map[string]string{}
	for name, v := range h.Consts {
		if !strings.HasPrefix(name, "MXLFS_REFUSE_") || name == "MXLFS_REFUSE_MAX" {
			continue
		}
		label := Reason(v).String()
		require.NotEqual(t, "error", label, "%s", name)
		require.NotContains(t, seen, label, "%s and %s", name, seen[label])
		seen[label] = name
	}
	require.NotEmpty(t, seen)
}
