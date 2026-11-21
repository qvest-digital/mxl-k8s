package mxlfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// The numbers must be the kernel's encoding of mxlfs_uapi.h, with the
// sizes of the structs as Go lays them out, or an ioctl lands on another
// command or none.
func TestIoctlNumbers(t *testing.T) {
	const w, r, rw = 1, 2, 3
	ioc := func(dir, nr, size uintptr) uintptr { return dir<<30 | size<<16 | uintptr('x')<<8 | nr }
	require.Equal(t, ioc(r, 3, 4), uintptr(iocGetSelected), "_IOR('x', 3, __u32)")
	require.Equal(t, ioc(r, 4, unsafe.Sizeof(Stats{})), uintptr(iocStats), "_IOR('x', 4, struct mxlfs_stats)")
	require.Equal(t, ioc(w, 11, unsafe.Sizeof(nodePref{})), uintptr(iocSetNode), "_IOW('x', 11, struct mxlfs_node_pref)")
	require.Equal(t, ioc(rw, 12, unsafe.Sizeof(nodePages{})), uintptr(iocNodePages), "_IOWR('x', 12, struct mxlfs_node_pages)")
	require.Equal(t, ioc(rw, 13, unsafe.Sizeof(Move{})), uintptr(iocMove), "_IOWR('x', 13, struct mxlfs_move)")
	require.Equal(t, ioc(w, 14, 4), uintptr(iocHold), "_IOW('x', 14, __u32)")
	require.Equal(t, ioc(rw, 15, unsafe.Sizeof(mountNodes{})), uintptr(iocMountNodes), "_IOWR('x', 15, struct mxlfs_mount_nodes)")
}

// The sizes the header gives: every field naturally aligned, no padding.
func TestStructSizes(t *testing.T) {
	require.EqualValues(t, 8, unsafe.Sizeof(nodePref{}))
	require.EqualValues(t, 32, unsafe.Sizeof(nodePages{}))
	require.EqualValues(t, 16+8*8, unsafe.Sizeof(Move{}))
	require.EqualValues(t, 5*8, unsafe.Sizeof(NodeStat{}))
	require.EqualValues(t, 16, unsafe.Sizeof(mountNodes{}))
	// The five fields before the NUMA ones, then 8 counters, 8 refusal
	// reasons, window sum and max, 9 buckets, holds and dry runs, and the
	// two counters of waits for missing flows.
	require.EqualValues(t, (5+8+RefuseMax+2+9+2+2)*8, unsafe.Sizeof(Stats{}))
	require.Len(t, WindowBuckets, len(Stats{}.MoveWindowHist)-1)
}

func TestReasonLabels(t *testing.T) {
	for r, want := range map[Reason]string{0: "none", RefusePinned: "pinned", RefuseView: "view",
		RefuseBusy: "busy", RefuseNomem: "nomem", RefuseHeld: "held", RefuseSignal: "signal", RefusePoisoned: "poisoned", 8: "error"} {
		require.Equal(t, want, r.String())
	}
}

func TestUnsupported(t *testing.T) {
	require.True(t, Unsupported(fmt.Errorf("x: %w", syscall.ENOTTY)))
	require.True(t, Unsupported(errors.ErrUnsupported))
	require.False(t, Unsupported(syscall.EBUSY))
	require.False(t, Unsupported(nil))
}

func TestPagesOff(t *testing.T) {
	p := Pages{PerNode: []uint64{3, 5, 7}}
	require.EqualValues(t, 12, p.Off(0))
	require.EqualValues(t, 15, p.Off(NodeNone))
}

func TestRingFiles(t *testing.T) {
	dir := t.TempDir()
	_, err := RingFiles(dir)
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "channels"), nil, 0o644))
	got, err := RingFiles(dir)
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(dir, "channels")}, got)

	require.NoError(t, os.Mkdir(filepath.Join(dir, "grains"), 0o755))
	for _, n := range []string{"data.0", "data.1"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "grains", n), nil, 0o644))
	}
	got, err = RingFiles(dir)
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(dir, "grains", "data.0"), filepath.Join(dir, "grains", "data.1")}, got)
}

// Listed answers from the directory's listing alone. A directory that
// can be read but not searched lists its entries while every lookup of
// a name in it fails, so only a listing finds the entry there.
func TestListedReadsTheListingAlone(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses the mode bits this test turns on")
	}
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "a.mxl-flow"), 0o755))
	require.NoError(t, os.Chmod(dir, 0o400))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	ok, err := Listed(dir, "a.mxl-flow")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = Listed(dir, "b.mxl-flow")
	require.NoError(t, err)
	require.False(t, ok)
}

// More entries than one read returns are all looked at.
func TestListedFindsAnEntryPastTheFirstRead(t *testing.T) {
	dir := t.TempDir()
	for i := range 600 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d", i)), nil, 0o644))
	}
	for i := range 600 {
		ok, err := Listed(dir, fmt.Sprintf("f%03d", i))
		require.NoError(t, err)
		require.True(t, ok, "f%03d", i)
	}
}

// A directory that is not there is an error, not an empty listing: the
// caller decides whether a missing domain lists no flow.
func TestListedFailsOnAMissingDirectory(t *testing.T) {
	_, err := Listed(filepath.Join(t.TempDir(), "absent"), "a.mxl-flow")
	require.ErrorIs(t, err, os.ErrNotExist)
}
