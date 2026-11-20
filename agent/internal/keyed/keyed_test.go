package keyed

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// Run starts a goroutine per key it handles, so a path that returns
// without waiting for them would leak one per flow event.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type event struct {
	key string
	seq int
}

func keyOf(e event) string { return e.key }

// start runs Run in the background and returns what waits for it.
func start(ctx context.Context, in <-chan event, limit int, handle func(event)) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, in, limit, keyOf, handle)
	}()
	return done
}

// send fails the test when Run does not read e in time.
func send(t *testing.T, in chan<- event, e event) {
	t.Helper()
	select {
	case in <- e:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not read %+v", e)
	}
}

// One flow whose publish hangs on the API must not hold another flow's
// event: serially, a vanished flow waited for every slow one before it,
// and the kernel queue overflowed behind them. Every event of the slow
// flow is read past too, so its own backlog does not stop the reader.
func TestASlowKeyDoesNotHoldAnother(t *testing.T) {
	in := make(chan event)
	release := make(chan struct{})
	gotB := make(chan struct{})
	done := start(context.Background(), in, 2, func(e event) {
		switch e.key {
		case "a":
			<-release
		case "b":
			close(gotB)
		}
	})

	for i := range 5 {
		send(t, in, event{key: "a", seq: i})
	}
	send(t, in, event{key: "b"})
	select {
	case <-gotB:
	case <-time.After(5 * time.Second):
		t.Fatal("b was not handled while a was blocked")
	}

	close(release)
	close(in)
	<-done
}

// An appear and a vanish of one flow must reach the API in the order
// they happened, or a gone flow is published as present. Two handlers
// of one key at once would race the same way.
func TestOneKeyRunsInOrderAndAlone(t *testing.T) {
	const keys, perKey = 6, 200
	var (
		mu       sync.Mutex
		seen     = map[string][]int{}
		inFlight = map[string]*atomic.Int32{}
	)
	for k := range keys {
		inFlight[fmt.Sprint(k)] = &atomic.Int32{}
	}
	var overlapped atomic.Bool
	in := make(chan event)
	done := start(context.Background(), in, 4, func(e event) {
		n := inFlight[e.key]
		if n.Add(1) != 1 {
			overlapped.Store(true)
		}
		time.Sleep(time.Duration(e.seq%3) * 10 * time.Microsecond)
		mu.Lock()
		seen[e.key] = append(seen[e.key], e.seq)
		mu.Unlock()
		n.Add(-1)
	})

	for i := range perKey {
		for k := range keys {
			send(t, in, event{key: fmt.Sprint(k), seq: i})
		}
	}
	close(in)
	<-done

	assert.False(t, overlapped.Load(), "two handlers of one key ran at once")
	want := make([]int, perKey)
	for i := range want {
		want[i] = i
	}
	for k := range keys {
		assert.Equal(t, want, seen[fmt.Sprint(k)], "key %d", k)
	}
}

// The limit bounds how many API calls the agent has in flight at once,
// however many flows change together.
func TestAtMostLimitHandlersRun(t *testing.T) {
	const limit, keys = 3, 10
	var running, peak atomic.Int32
	release := make(chan struct{})
	entered := make(chan struct{}, keys)
	in := make(chan event)
	done := start(context.Background(), in, limit, func(event) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		running.Add(-1)
	})

	go func() {
		for k := range keys {
			in <- event{key: fmt.Sprint(k)}
		}
		close(in)
	}()
	for range limit {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("fewer than %d handlers ran at once", limit)
		}
	}
	// Give an unbounded Run the time to start more.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(limit), peak.Load())

	close(release)
	<-done
	assert.Equal(t, int32(limit), peak.Load())
}

// A closed input is handled to the end before Run returns, so the agent
// does not stop with a change read but never published.
func TestClosedInputIsDrained(t *testing.T) {
	in := make(chan event, 20)
	for i := range 20 {
		in <- event{key: fmt.Sprint(i % 4), seq: i}
	}
	close(in)
	var handled atomic.Int32
	Run(context.Background(), in, 2, keyOf, func(event) {
		time.Sleep(time.Millisecond)
		handled.Add(1)
	})
	assert.Equal(t, int32(20), handled.Load())
}

// Once ctx ends, Run waits for the handlers that are running and starts
// no other, so a stopping tracker neither leaks a goroutine nor calls
// the API after it has released its leases.
func TestCancelWaitsForRunningAndDropsQueued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan event)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var handled atomic.Int32
	done := start(ctx, in, 2, func(e event) {
		handled.Add(1)
		entered <- struct{}{}
		<-release
	})

	send(t, in, event{key: "a", seq: 0})
	send(t, in, event{key: "a", seq: 1})
	send(t, in, event{key: "b", seq: 0})
	<-entered
	<-entered
	cancel()

	select {
	case <-done:
		t.Fatal("Run returned before its running handlers")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx ended")
	}
	require.Equal(t, int32(2), handled.Load(), "a queued item ran after ctx ended")
}
