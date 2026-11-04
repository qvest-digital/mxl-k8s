package mirror

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTargetProgressLoop_CommitsOnlyOnceAGrainIsComplete(t *testing.T) {
	// An arrival whose header reports fewer valid slices than the grain
	// holds is not committed: commitArrivedGrain marks every slice
	// valid regardless of what landed.
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
