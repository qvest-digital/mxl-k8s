package mxlfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The numbers must match the kernel's encoding of the mxlfs_uapi.h
// definitions, or the ioctls land on another command or none.
func TestIoctlNumbers(t *testing.T) {
	ioc := func(dir, nr, size uintptr) uintptr {
		return dir<<30 | size<<16 | uintptr('x')<<8 | nr
	}
	require.Equal(t, ioc(1, 7, 4), uintptr(iocLazyAlloc), "_IOW('x', 7, __u32)")
	require.Equal(t, ioc(0, 8, 0), uintptr(iocAllocate), "_IO('x', 8)")
}

func TestIsMxlfsOnOtherFilesystem(t *testing.T) {
	ok, err := IsMxlfs(t.TempDir())
	require.NoError(t, err)
	require.False(t, ok)
}

func TestFlowDir(t *testing.T) {
	id := [16]byte{0x5f, 0xbe, 0xc3, 0xb1, 0x1b, 0x0f, 0x41, 0x7d, 0x90, 0x59, 0x8b, 0x94, 0xa4, 0x71, 0x97, 0xed}
	require.Equal(t, "/d/5fbec3b1-1b0f-417d-9059-8b94a47197ed.mxl-flow", FlowDir("/d", id))
}

func TestAllocateFlowWithoutRing(t *testing.T) {
	err := AllocateFlow(t.TempDir())
	require.ErrorContains(t, err, "neither grains nor channels")
}

const flowDef = `{"description":"mxlfs test","id":"5fbec3b1-1b0f-417d-9059-8b94a47197ed",
"tags":{"urn:x-nmos:tag:grouphint/v1.0":["mxlfs-test:Video"]},"format":"urn:x-nmos:format:video",
"label":"mxlfs test","parents":[],"media_type":"video/v210","grain_rate":{"numerator":50,"denominator":1},
"frame_width":1920,"frame_height":1080,"interlace_mode":"progressive","colorspace":"BT709",
"components":[{"name":"Y","width":1920,"height":1080,"bit_depth":10},
{"name":"Cb","width":960,"height":1080,"bit_depth":10},{"name":"Cr","width":960,"height":1080,"bit_depth":10}]}`

func allocated(t *testing.T, dir string) uint64 {
	t.Helper()
	var st unix.Statfs_t
	require.NoError(t, unix.Statfs(dir, &st))
	return (st.Blocks - st.Bfree) * uint64(st.Bsize)
}

// TestLazyWriterThenAllocate needs a writable mxlfs in MXLFS_TEST_DIR; it
// is skipped otherwise. A lazily allocating process
// creates a flow through libmxl at the cost of its headers, and
// AllocateFlow then allocates the whole ring.
func TestLazyWriterThenAllocate(t *testing.T) {
	dir := os.Getenv("MXLFS_TEST_DIR")
	if dir == "" {
		t.Skip("MXLFS_TEST_DIR not set")
	}
	ok, err := IsMxlfs(dir)
	require.NoError(t, err)
	require.True(t, ok, "%s is not an mxlfs", dir)

	domain, err := os.MkdirTemp(dir, "domain-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(domain) })
	require.NoError(t, os.WriteFile(filepath.Join(domain, "options.json"),
		[]byte(`{"urn:x-mxl:option:history_duration/v1.0": 200000000}`), 0o644))

	require.NoError(t, SetLazy(domain, true))
	t.Cleanup(func() { _ = SetLazy(domain, false) })

	inst, err := mxl.NewInstance(domain, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })

	before := allocated(t, domain)
	w, created, err := inst.NewWriter(flowDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.True(t, created)

	flowDir := FlowDir(domain, w.Config().Common.ID)
	grains, err := filepath.Glob(filepath.Join(flowDir, "grains", "data.*"))
	require.NoError(t, err)
	require.NotEmpty(t, grains)
	st, err := os.Stat(grains[0])
	require.NoError(t, err)
	ring := uint64(len(grains)) * uint64(st.Size())

	lazy := allocated(t, domain) - before
	require.Less(t, lazy, ring/10, "a lazily created flow allocated %d of its %d-byte ring", lazy, ring)

	require.NoError(t, AllocateFlow(flowDir))
	full := allocated(t, domain) - before
	require.GreaterOrEqual(t, full, ring, "AllocateFlow left the ring at %d of %d bytes", full, ring)
	require.True(t, strings.HasSuffix(flowDir, "5fbec3b1-1b0f-417d-9059-8b94a47197ed.mxl-flow"))
}
