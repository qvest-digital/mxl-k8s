package flowlocks

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCountParsesProcLocks(t *testing.T) {
	const locks = `1: FLOCK  ADVISORY  READ 514830 00:2a:1234 0 EOF
2: FLOCK  ADVISORY  READ 42 00:2a:1234 0 EOF
3: FLOCK  ADVISORY  READ 7 00:2a:9999 0 EOF
4: POSIX  ADVISORY  READ 9 00:2a:1234 0 EOF
5: FLOCK  ADVISORY  READ 0 00:2a:1234 0 EOF
5: -> FLOCK  ADVISORY  READ 11 00:2a:1234 0 EOF
6: FLOCK  ADVISORY  WRITE 13 00:2a:1234 0 EOF
6: -> FLOCK  ADVISORY  WRITE 17 00:2a:1234 0 EOF
`
	// 514830, 0 (a holder outside our pid namespace) and the waiter 11
	// hold or want a writer's lock on 00:2a:1234; 42 is self; 7 is
	// another file; 9 is POSIX. 13 and 17 hold or want an exclusive
	// lock, which is libmxl probing whether the flow is active: a
	// reader can do that, and counting it took a placeholder from the
	// gateway for a producer that does not exist.
	n, err := count(strings.NewReader(locks), 0, 0x2a, 1234, 42)
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

func TestForeignWritersMissingFile(t *testing.T) {
	n, err := ForeignWriters(filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	require.Zero(t, n)
}

// A shared flock this process holds is its own; one another process
// holds is foreign. This is how a libmxl writer in a producer shows up.
func TestForeignWritersCountsOtherProcesses(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(data, nil, 0o644))

	own, err := os.Open(data)
	require.NoError(t, err)
	defer own.Close()
	require.NoError(t, unix.Flock(int(own.Fd()), unix.LOCK_SH))

	n, err := ForeignWriters(data)
	require.NoError(t, err)
	require.Zero(t, n, "our own flock counted as foreign")

	other := exec.Command("flock", "--shared", data, "sleep", "30")
	if err := other.Start(); err != nil {
		t.Skipf("flock(1) not available: %v", err)
	}
	defer func() { _ = other.Process.Kill(); _ = other.Wait() }()
	require.Eventually(t, func() bool {
		n, err := ForeignWriters(data)
		return err == nil && n == 1
	}, 5*time.Second, 20*time.Millisecond)
}

// Writers counts this process's own writers too: a directory one of the
// gateway's writers holds is not a leftover to remove.
func TestWritersCountsOwnFlocks(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(data, nil, 0o644))
	n, err := Writers(data)
	require.NoError(t, err)
	require.Zero(t, n)

	own, err := os.Open(data)
	require.NoError(t, err)
	defer own.Close()
	require.NoError(t, unix.Flock(int(own.Fd()), unix.LOCK_SH))
	n, err = Writers(data)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}
