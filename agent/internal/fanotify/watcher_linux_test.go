//go:build linux

package fanotify

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A per-domain tracker is stopped by cancelling its watcher while nothing
// happens in the domain. The stop waits for Run, and the tracker's lock is
// held meanwhile, so a Run that sleeps on in read until the next event
// stalls every later domain sync on the node. A pipe stands in for the
// fanotify descriptor, which needs CAP_SYS_ADMIN.
func TestReadUntilDoneReturnsOnCancelWithNothingToRead(t *testing.T) {
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(p[0])
	defer unix.Close(p[1])

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- readUntilDone(ctx, p[0], func([]byte) error { return nil }) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("still reading after cancel")
	}
}

func TestReadUntilDoneHandsOverWhatArrives(t *testing.T) {
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(p[0])
	defer unix.Close(p[1])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, 1)
	go func() {
		_ = readUntilDone(ctx, p[0], func(b []byte) error { got <- string(b); cancel(); return nil })
	}()
	if _, err := unix.Write(p[1], []byte("ev")); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "ev" {
			t.Fatalf("got %q", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("nothing handed over")
	}
}
