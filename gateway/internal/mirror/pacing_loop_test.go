package mirror

import (
	"context"
	"errors"

	"github.com/qvest-digital/go-mxl/fabrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTransferLoop_PacesOnlyTheLiveHead(t *testing.T) {
	// Pacing a backlog would keep a mirror behind for good: every grain
	// it is already late on would be stretched over a further grain
	// interval. Only the grain with nothing queued behind it may be
	// paced, so a loop that wakes to find several grains outstanding
	// clears all but the last at full speed.
	//
	// First probe reports head 10 (where the mirror tunes in), the next
	// reports 14. That leaves 11, 12, 13, 14 outstanding on one tick.
	var calls atomic.Int32
	probe := func() (uint64, error) {
		if calls.Add(1) == 1 {
			return 10, nil
		}
		return 14, nil
	}

	var mu sync.Mutex
	paced := map[uint64]bool{}
	transfer := func(idx uint64, p bool) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		if _, seen := paced[idx]; !seen {
			paced[idx] = p
		}
		return false, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, time.Millisecond, nil)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(paced) >= 4
	}, time.Second, time.Millisecond)

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	assert.False(t, paced[11], "11 has a backlog behind it")
	assert.False(t, paced[12], "12 has a backlog behind it")
	assert.False(t, paced[13], "13 has a backlog behind it")
	assert.True(t, paced[14], "14 is the live head with nothing queued behind it")
}

func TestRunTransferLoop_PacesEachSteadyStateGrain(t *testing.T) {
	// In steady state the head advances one grain per tick, so every
	// grain is the live head and every grain is paced. A rule that only
	// paced the first would leave the shaping off in normal operation.
	//
	// The producer advances on its own clock, independently of whether
	// the mirror transferred anything: the first probe is where the
	// mirror tunes in, and each later one finds exactly one new grain.
	var probes atomic.Uint64
	probe := func() (uint64, error) { return 100 + probes.Add(1) - 1, nil }

	var mu sync.Mutex
	var pacedFlags []bool
	transfer := func(_ uint64, p bool) (bool, error) {
		mu.Lock()
		pacedFlags = append(pacedFlags, p)
		mu.Unlock()
		return false, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTransferLoop(ctx, done, "f", probe, transfer, func() error { return nil }, time.Millisecond, nil)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pacedFlags) >= 5
	}, time.Second, time.Millisecond)

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	for i, p := range pacedFlags {
		assert.Truef(t, p, "steady-state grain %d must be paced", i)
	}
}

func TestRunTargetProgressLoop_CommitsOnlyOnceAGrainIsComplete(t *testing.T) {
	// A paced source transfers one grain as several slice ranges, and
	// each range raises a separate arrival at the target carrying a
	// growing valid-slice count. commitArrivedGrain marks every slice
	// valid regardless of what landed, so committing on an early
	// arrival would publish a grain with most of its lines unwritten.
	arrivals := make(chan uint64, 8)
	for i := 0; i < 4; i++ {
		arrivals <- 7
	}
	close(arrivals)

	read := func() (uint64, error) {
		idx, ok := <-arrivals
		if !ok {
			return 0, fabrics.ErrNotReady
		}
		return idx, nil
	}

	var completeCalls atomic.Int32
	complete := func(uint64) (bool, error) {
		// Only the fourth arrival finishes the grain.
		return completeCalls.Add(1) == 4, nil
	}

	var mu sync.Mutex
	var committed []uint64
	commit := func(idx uint64) error {
		mu.Lock()
		committed = append(committed, idx)
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, complete, commit, func() {}, nil)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(committed) == 1
	}, time.Second, time.Millisecond)

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint64{7}, committed,
		"a grain arriving in four ranges is committed exactly once, on the range that completes it")
}

func TestRunTargetProgressLoop_CompletenessErrorDoesNotCommitOrKillTheLoop(t *testing.T) {
	// GrainInfo failing is not a reason to publish a possibly-partial
	// grain, nor to tear down a fabric that is still delivering.
	var reads atomic.Int32
	read := func() (uint64, error) {
		if reads.Add(1) > 3 {
			return 0, fabrics.ErrNotReady
		}
		return 1, nil
	}
	complete := func(uint64) (bool, error) { return false, errors.New("grain info failed") }

	var commits atomic.Int32
	commit := func(uint64) error {
		commits.Add(1)
		return nil
	}
	var fatal atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, complete, commit, func() { fatal.Store(true) }, nil)

	require.Eventually(t, func() bool { return reads.Load() > 3 }, time.Second, time.Millisecond)
	cancel()
	<-done

	assert.Zero(t, commits.Load(), "an unknown completeness must not commit")
	assert.False(t, fatal.Load(), "a GrainInfo failure is not a fabric failure")
}

func TestRunTargetProgressLoop_NilGateCommitsEveryArrival(t *testing.T) {
	// The sample path and the existing tests pass no gate; that has to
	// keep meaning "commit what arrives".
	var reads atomic.Int32
	read := func() (uint64, error) {
		if reads.Add(1) > 3 {
			return 0, fabrics.ErrNotReady
		}
		return uint64(reads.Load()), nil
	}
	var commits atomic.Int32
	commit := func(uint64) error {
		commits.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go runTargetProgressLoop(ctx, done, read, nil, commit, func() {}, nil)

	require.Eventually(t, func() bool { return commits.Load() == 3 }, time.Second, time.Millisecond)
	cancel()
	<-done
}
