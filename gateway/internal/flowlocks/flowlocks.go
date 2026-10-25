// Package flowlocks tells whether a process other than this one writes a
// local flow.
//
// Every libmxl writer holds a shared flock on its flow's data file for as
// long as it is attached, and readers hold none. The gateway's own writers
// share one open file per flow, so any other flock on that file is another
// process's writer: a local producer.
package flowlocks

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// ProcLocks is where the kernel lists every file lock.
var ProcLocks = "/proc/locks"

// ForeignWriters counts the flocks other processes hold on the file at
// dataPath. A missing file has none.
func ForeignWriters(dataPath string) (int, error) { return writers(dataPath, os.Getpid()) }

// Writers counts every flock on the file at dataPath, this process's
// own included. A missing file has none.
func Writers(dataPath string) (int, error) { return writers(dataPath, -1) }

func writers(dataPath string, self int) (int, error) {
	var st unix.Stat_t
	if err := unix.Stat(dataPath, &st); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("stat %s: %w", dataPath, err)
	}
	f, err := os.Open(ProcLocks)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return count(f, unix.Major(st.Dev), unix.Minor(st.Dev), st.Ino, self)
}

// count reads /proc/locks lines such as
//
//	1: FLOCK  ADVISORY  READ 514830 08:01:11011342 0 EOF
//
// and counts FLOCK entries on major:minor:ino held by a pid other than
// self. The kernel prints the device as hex and the inode as decimal. It
// leaves out holders the reader's pid namespace cannot see, so a reader
// outside the host's sees none of another container's locks.
func count(r io.Reader, major, minor uint32, ino uint64, self int) (int, error) {
	want := fmt.Sprintf("%02x:%02x:%d", major, minor, ino)
	n := 0
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		// "->" marks a waiter blocked behind a holder, one field more.
		if len(fields) > 1 && fields[1] == "->" {
			fields = append(fields[:1], fields[2:]...)
		}
		if len(fields) < 6 || fields[1] != "FLOCK" || fields[5] != want {
			continue
		}
		if pid, err := strconv.Atoi(fields[4]); err == nil && pid == self {
			continue
		}
		n++
	}
	return n, sc.Err()
}
