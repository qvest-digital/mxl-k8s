package uapitest

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// Layouts and numbers the C compiler gives the header, from gcc on
// x86_64.
func TestParseLaysOutTheHeader(t *testing.T) {
	h, err := Parse()
	require.NoError(t, err)
	require.EqualValues(t, 288, h.Sizes["mxlfs_stats"])
	require.EqualValues(t, 4096, h.Sizes["mxlfs_wanted_req"])
	require.Equal(t, Field{Name: "flags", Type: "__u32", Offset: 4092, Size: 4}, h.Structs["mxlfs_wanted_req"][4])
	require.Equal(t, Field{Name: "fds", Type: "__s32", Len: 8, Offset: 8, Size: 32}, h.Structs["mxlfs_view_sources"][2])
	require.EqualValues(t, 0x81207804, h.Ioctls["MXLFS_IOC_STATS"])
	require.EqualValues(t, 0xd0007805, h.Ioctls["MXLFS_IOC_WANTED_NEXT"])
	require.EqualValues(t, 0x7808, h.Ioctls["MXLFS_IOC_ALLOCATE"])
	require.EqualValues(t, 0x80047803, h.Ioctls["MXLFS_IOC_VIEW_GET_SELECTED"])
	require.EqualValues(t, -1, h.Consts["MXLFS_NODE_NONE"])
	require.EqualValues(t, 4<<30, h.Consts["MXLFS_VIEW_MAX_SIZE"])
	require.EqualValues(t, 2, h.Consts["MXLFS_MOVE_STRICT"])
	require.EqualValues(t, 7, h.Consts["MXLFS_REFUSE_POISONED"])
	require.EqualValues(t, 8, h.Consts["MXLFS_REFUSE_MAX"])
	require.EqualValues(t, 3, h.Consts["MXLFS_MOVED_NONE"])
}

// A struct padded to its alignment, and a member the parser cannot read
// is an error rather than a wrong layout.
func TestParsePadsAndRefuses(t *testing.T) {
	h, err := parse("struct a {\n\t__u64 x;\n\t__u32 y;\n};\n#define IOC_A _IOWR('x', 1, struct a)\n")
	require.NoError(t, err)
	require.EqualValues(t, 16, h.Sizes["a"])
	require.EqualValues(t, uintptr(3<<30|16<<16|'x'<<8|1), h.Ioctls["IOC_A"])
	_, err = parse("struct b {\n\t__u16 x;\n};\n")
	require.ErrorContains(t, err, "unknown member")
	_, err = parse("#define IOC_B _IOW('y', 1, __u32)\n")
	require.ErrorContains(t, err, "unknown ioctl form")
}

func TestCheckStruct(t *testing.T) {
	h, err := parse("struct a {\n\t__s32 x;\n\t__u32 y;\n\tchar p[4];\n};\n")
	require.NoError(t, err)
	type good struct {
		X int32
		Y uint32
		P [4]byte
	}
	require.NoError(t, h.CheckStruct("a", reflect.TypeFor[good]()))
	type signed struct {
		X uint32
		Y uint32
		P [4]byte
	}
	require.ErrorContains(t, h.CheckStruct("a", reflect.TypeFor[signed]()), "field 0")
	type short struct {
		X int32
		Y uint32
		P [3]byte
	}
	require.ErrorContains(t, h.CheckStruct("a", reflect.TypeFor[short]()), "field 2")
	type moved struct {
		X int32
		P [4]byte
		Y uint32
	}
	require.ErrorContains(t, h.CheckStruct("a", reflect.TypeFor[moved]()), "field 1")
	require.ErrorContains(t, h.CheckStruct("b", reflect.TypeFor[good]()), "no struct b")
}
