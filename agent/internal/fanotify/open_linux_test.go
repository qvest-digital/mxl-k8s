//go:build linux

package fanotify

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// record builds one fanotify_event_metadata as the kernel lays it out.
func record(fd, pid int32) []byte {
	b := make([]byte, 24)
	binary.NativeEndian.PutUint32(b[0:4], 24)
	b[4] = unix.FANOTIFY_METADATA_VERSION
	binary.NativeEndian.PutUint16(b[6:8], 24)
	binary.NativeEndian.PutUint64(b[8:16], unix.FAN_OPEN)
	binary.NativeEndian.PutUint32(b[16:20], uint32(fd))
	binary.NativeEndian.PutUint32(b[20:24], uint32(pid))
	return b
}

// Each record's fd resolves to the opened file's path and is closed;
// a record without an fd (a queue overflow) is skipped.
func TestDispatchOpensResolvesAndClosesFDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.0")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(path, unix.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := append(record(int32(fd), 4242), record(-1, 1)...)

	out := make(chan Open, 4)
	if err := dispatchOpens(context.Background(), buf, out); err != nil {
		t.Fatal(err)
	}
	close(out)
	var got []Open
	for o := range out {
		got = append(got, o)
	}
	if len(got) != 1 || got[0].PID != 4242 || got[0].Path != path {
		t.Fatalf("got %+v, want one open of %s by 4242", got, path)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
		t.Fatal("the event's fd was left open")
	}
}
