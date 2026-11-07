package numarelease

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcNode(t *testing.T) {
	h := newHost(t)
	topo, err := readTopology(h.nodeRoot)
	require.NoError(t, err)

	h.pid(10, "1", "4-5", "6", "7")
	h.pid(20, "0-1", "3-4")
	h.pid(30, "0", "4-7")
	h.pid(40, "0-1", "0-3")
	h.pid(50, "1") // no task directory: the process's own list, 0-7

	for pid, want := range map[int]int32{10: 1, 20: -1, 30: -1, 40: 0, 50: -1} {
		n, ok := procNode(h.proc, pid, topo)
		if want < 0 {
			require.False(t, ok, "pid %d", pid)
			continue
		}
		require.True(t, ok, "pid %d", pid)
		require.Equal(t, want, n, "pid %d", pid)
	}
	_, ok := procNode(h.proc, 99, topo)
	require.False(t, ok, "a process that is gone has no node")
}

func TestParseList(t *testing.T) {
	got, err := parseList("0-2,5,7-8\n")
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 2, 5, 7, 8}, got)
	_, err = parseList("3-1")
	require.Error(t, err)
	_, err = parseList("x")
	require.Error(t, err)
}
