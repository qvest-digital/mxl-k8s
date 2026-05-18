package mirror

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
)

// TestMain in source_test.go runs goleak.VerifyTestMain for the
// whole package; the recovery paths exercised below all rely on
// progress goroutines exiting cleanly when ctx is cancelled or when
// the loop hits a fatal error.

func TestRunTargetProgressLoop_CommitsArrivedGrains(t *testing.T) {
	// Sequence: idx=10 arrives, idx=11 arrives, then ErrNotReady,
	// then cancel. The loop must commit 10 and 11 in order; the
	// idle sleep must not eat the cancel signal.
	var seq atomic.Int32
	read := func() (uint64, error) {
		switch seq.Add(1) {
		case 1:
			return 10, nil
		case 2:
			return 11, nil
		default:
			return 0, fabrics.ErrNotReady
		}
	}

	var (
		mu        sync.Mutex
		committed []uint64
	)
	commit := func(idx uint64) error {
		mu.Lock()
		committed = append(committed, idx)
		mu.Unlock()
		return nil
	}
	committedSnap := func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		out := make([]uint64, len(committed))
		copy(out, committed)
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	fatal := atomic.Int32{}
	onFatal := func() { fatal.Add(1) }

	go runTargetProgressLoop(ctx, done, read, commit, onFatal)

	require.Eventually(t, func() bool { return len(committedSnap()) == 2 },
		time.Second, time.Millisecond, "expected 2 commits")

	cancel()
	<-done

	assert.Equal(t, []uint64{10, 11}, committedSnap())
	assert.Zero(t, fatal.Load(),
		"a clean ctx cancel must not look like a fatal target error; "+
			"that would trigger spurious fabric rebuilds on every restart")
}

func TestRunTargetProgressLoop_FatalErrorExitsAndCallsOnFatalOnce(t *testing.T) {
	// ReadGrain returns a non-ErrNotReady error once. The loop must
	// exit, close done, and invoke onFatal exactly once. A second
	// call would double-rebuild the fabric side.
	read := func() (uint64, error) {
		return 0, errors.New("EFI_RXMSG dropped, target dead")
	}
	commit := func(uint64) error {
		t.Fatal("commit must not be called on a fatal read error")
		return nil
	}

	fatalCalls := atomic.Int32{}
	onFatal := func() { fatalCalls.Add(1) }

	done := make(chan struct{})
	go runTargetProgressLoop(context.Background(), done, read, commit, onFatal)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on fatal error")
	}
	assert.Equal(t, int32(1), fatalCalls.Load(),
		"onFatal must fire exactly once; a duplicate call would race two "+
			"concurrent recoverFromFatalError invocations against the same writer")
}

func TestRunTargetProgressLoop_CommitErrorIsLoggedButLoopContinues(t *testing.T) {
	// A commit-side error (e.g. OpenGrain returning busy under load)
	// must not exit the loop. The next ReadGrain is allowed to
	// surface the next index.
	var seq atomic.Int32
	read := func() (uint64, error) {
		switch seq.Add(1) {
		case 1:
			return 1, nil
		case 2:
			return 2, nil
		default:
			return 0, fabrics.ErrNotReady
		}
	}
	commitCalls := atomic.Int32{}
	commit := func(uint64) error {
		commitCalls.Add(1)
		return errors.New("transient")
	}
	onFatal := func() { t.Fatal("commit failure must not be reported as fatal") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, commit, onFatal)

	require.Eventually(t, func() bool { return commitCalls.Load() >= 2 },
		time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestRunTargetProgressLoop_CtxCancelExitsImmediately(t *testing.T) {
	read := func() (uint64, error) { return 0, fabrics.ErrNotReady }
	commit := func(uint64) error { return nil }
	onFatal := func() { t.Fatal("ctx cancel must not look fatal") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, commit, onFatal)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not honour ctx cancel during idle sleep")
	}
}

func TestRunTargetProgressLoop_NilOnFatalDoesNotPanic(t *testing.T) {
	// A nil onFatal would be a programming mistake from the caller,
	// but the loop must defend against it (a panic in the goroutine
	// would crash the gateway). Catches a regression where someone
	// forgot the if onFatal != nil guard.
	read := func() (uint64, error) { return 0, errors.New("fatal") }
	commit := func(uint64) error { return nil }

	done := make(chan struct{})
	go runTargetProgressLoop(context.Background(), done, read, commit, nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on fatal read with nil onFatal")
	}
}
