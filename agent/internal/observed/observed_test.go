package observed

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	flowA = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
	flowB = "11111111-2222-3333-4444-555555555555"
	podW  = "aaaaaaaa-1111-2222-3333-444444444444"
	podR  = "bbbbbbbb-1111-2222-3333-444444444444"
)

// proc writes a fake /proc with the files Scan reads.
func proc(t *testing.T, locks string, pids map[int][2]string) string {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "locks"), []byte(locks), 0o644))
	for pid, cm := range pids {
		dir := filepath.Join(root, itoa(pid))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup"), []byte(cm[0]), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "maps"), []byte(cm[1]), 0o644))
	}
	return root
}

func itoa(i int) string { return strconv.Itoa(i) }

func TestScanTellsWritersFromReaders(t *testing.T) {
	maps := func(flow string) string {
		return "7f00-7f01 rw-s 00000000 00:2a 101 /run/mxl/domain/" + flow + ".mxl-flow/data\n" +
			"7f02-7f03 r--s 00000000 00:2a 102 /run/mxl/domain/" + flow + ".mxl-flow/grains/data.0\n" +
			"7f04-7f05 r-xp 00000000 08:01 999 /usr/lib/libmxl.so\n"
	}
	root := proc(t,
		// The writer pid 11 holds the flock on flowA's data file.
		"1: FLOCK  ADVISORY  READ 11 00:2a:101 0 EOF\n",
		map[int][2]string{
			11: {"0::/kubepods.slice/kubepods-pod" + podW[:8] + "_1111_2222_3333_444444444444.slice/cri-1\n", maps(flowA)},
			22: {"0::/kubepods/besteffort/pod" + podR + "/cri-2\n", maps(flowA) +
				"7f06-7f07 r--s 00000000 00:2b 201 /run/mxl/domains/studio/" + flowB + ".mxl-flow/channels\n"},
			33: {"0::/system.slice/containerd.service\n", maps(flowA)},
		})
	s := &Scanner{ProcRoot: root, Domains: func() map[string]string {
		return map[string]string{"00:2a": "", "00:2b": "studio"}
	}}

	got, err := s.Scan()
	require.NoError(t, err)
	require.Equal(t, map[string][]Use{
		podW: {{Domain: "", FlowID: flowA, Direction: Output}},
		podR: {{Domain: "", FlowID: flowA, Direction: Input}, {Domain: "studio", FlowID: flowB, Direction: Input}},
	}, got, "process 33 is in no pod and is not reported")
}

func TestDeviceMatchesProcFormat(t *testing.T) {
	d, err := Device("/proc")
	require.NoError(t, err)
	require.Regexp(t, `^[0-9a-f]{2,}:[0-9a-f]{2,}$`, d)
}

// A flow file removed while a reader still maps it is in use by that
// reader until it unmaps it.
func TestScanCountsMappingsOfDeletedFiles(t *testing.T) {
	root := proc(t, "", map[int][2]string{
		22: {"0::/kubepods/besteffort/pod" + podR + "/cri-2\n",
			"7f02-7f03 r--s 00000000 00:2a 102 /run/mxl/domain/" + flowA + ".mxl-flow/grains/data.0 (deleted)\n"},
	})
	s := &Scanner{ProcRoot: root, Domains: func() map[string]string { return map[string]string{"00:2a": ""} }}

	got, err := s.Scan()
	require.NoError(t, err)
	require.Equal(t, map[string][]Use{podR: {{FlowID: flowA, Direction: Input}}}, got)
}
