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
	// reasons, window sum and max, 9 buckets, holds and dry runs.
	require.EqualValues(t, (5+8+RefuseMax+2+9+2)*8, unsafe.Sizeof(Stats{}))
	require.Len(t, WindowBuckets, len(Stats{}.MoveWindowHist)-1)
}

func TestReasonLabels(t *testing.T) {
	for r, want := range map[Reason]string{0: "none", RefusePinned: "pinned", RefuseView: "view",
		RefuseBusy: "busy", RefuseNomem: "nomem", RefuseHeld: "held", RefuseSignal: "signal", 7: "error"} {
		require.Equal(t, want, r.String())
	}
}

func TestUnsupported(t *testing.T) {
	require.True(t, Unsupported(fmt.Errorf("x: %w", syscall.ENOTTY)))
	require.True(t, Unsupported(errors.ErrUnsupported))
	require.False(t, Unsupported(syscall.EBUSY))
	require.False(t, Unsupported(nil))
}

// On a filesystem that is not an mxlfs, every call is an unknown ioctl.
func TestCallsOnOtherFilesystem(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "f"))
	require.NoError(t, err)
	defer f.Close()
	_, err = NodePages(f)
	require.True(t, Unsupported(err), "%v", err)
	_, err = MoveTo(f, 0, MoveDryRun)
	require.True(t, Unsupported(err), "%v", err)
	_, err = MountNodes(f)
	require.True(t, Unsupported(err), "%v", err)
	require.True(t, Unsupported(Hold(f, true)))
	require.True(t, Unsupported(SetNode(f, 0)))
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

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	}
}

func TestHostNICNode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		online  string // "" for no list
		devices map[string]string
		want    int32
		wantErr string
	}{
		{name: "no NUMA", want: 0},
		{name: "one node", online: "1", devices: map[string]string{"mlx5_0": "-1"}, want: 1},
		{name: "two nodes, devices agree", online: "0-1", devices: map[string]string{"mlx5_0": "1", "mlx5_1": "1"}, want: 1},
		{name: "two nodes, devices disagree", online: "0-1", devices: map[string]string{"mlx5_0": "0", "mlx5_1": "1"},
			wantErr: "NUMA nodes 0 and 1"},
		{name: "two nodes, device without node", online: "0,1", devices: map[string]string{"mlx5_0": "-1"},
			wantErr: "reports NUMA node -1"},
		{name: "two nodes, no device", online: "0-1", wantErr: "reports a NUMA node"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			files := map[string]string{}
			for dev, n := range tc.devices {
				files[filepath.Join("class", dev, "device", "numa_node")] = n + "\n"
			}
			// A software device has no PCI device and is passed over.
			files[filepath.Join("class", "rxe0", "ports")] = ""
			if tc.online != "" {
				files["online"] = tc.online + "\n"
			}
			writeFiles(t, root, files)
			got, err := HostNICNode(filepath.Join(root, "class"), filepath.Join(root, "online"))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.EqualValues(t, NodeNone, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
