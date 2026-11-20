package mxlfswatch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/agent/internal/intent"
)

// wantedFlow is the i-th of many distinct flows, as the kernel names it.
func wantedFlow(i int) string {
	return fmt.Sprintf("/domain/%08x-0000-4000-8000-000000000000.mxl-flow", i)
}

func (q *fakeQueue) push(paths ...string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queued = append(q.queued, paths...)
}

func (q *fakeQueue) left() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queued)
}

// runWanted runs w until the test ends and waits for it to return.
func runWanted(t *testing.T, w *Wanted, d Materializer, claim func(context.Context, string) bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx, d, claim, nil, logr.Discard())
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// held counts the Materialize calls in flight; every call for a flow
// slow names waits for release.
type held struct {
	mu                   sync.Mutex
	running, most, calls int
	slow                 func(path string) bool
	release              chan struct{}
	once                 sync.Once
}

// free releases every call, once.
func (h *held) free() { h.once.Do(func() { close(h.release) }) }

func (h *held) Materialize(_ context.Context, _ int32, path string) error {
	h.mu.Lock()
	h.running++
	h.calls++
	h.most = max(h.most, h.running)
	h.mu.Unlock()
	if h.slow(path) {
		<-h.release
	}
	h.mu.Lock()
	h.running--
	h.mu.Unlock()
	return nil
}

func (h *held) read() (running, most, calls int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running, h.most, h.calls
}

// Every want costs API calls on the agent's one client, which publishing
// and mirror setup share. A goroutine per want let a reader looking up
// many missing names run as many lookups at once as it had names.
func TestWantedRunBoundsTheFlowsHandledAtOnce(t *testing.T) {
	q := &fakeQueue{}
	for i := range 1000 {
		q.push(wantedFlow(i))
	}
	h := &held{slow: func(string) bool { return true }, release: make(chan struct{})}
	runWanted(t, &Wanted{mount: Mount{Root: "/", MountPoint: "/run/mxl"}, next: q.next}, h, nil)
	t.Cleanup(h.free)

	require.Eventually(t, func() bool { r, _, _ := h.read(); return r >= wantedHandlers },
		2*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, most, _ := h.read()
	require.Equal(t, wantedHandlers, most, "more flows handled at once than the bound")
	require.Positive(t, q.left(), "the rest wait in the kernel queue, not in goroutines")

	h.free()
	require.Eventually(t, func() bool { _, _, c := h.read(); return c == 1000 },
		5*time.Second, time.Millisecond)
	_, most, _ = h.read()
	require.Equal(t, wantedHandlers, most)
}

// One flow whose request hangs, with its own wants piling up behind it,
// must not hold the wants for every other flow.
func TestOneSlowWantDoesNotHoldTheQueue(t *testing.T) {
	slow := wantedFlow(0)
	q := &fakeQueue{}
	for i := 1; i <= 100; i++ {
		q.push(slow, wantedFlow(i))
	}
	h := &held{slow: func(p string) bool { return p == "/run/mxl"+slow+"/data" }, release: make(chan struct{})}
	runWanted(t, &Wanted{mount: Mount{Root: "/", MountPoint: "/run/mxl"}, next: q.next}, h, nil)
	t.Cleanup(h.free)

	require.Eventually(t, func() bool { _, _, c := h.read(); return c == 101 },
		2*time.Second, time.Millisecond, "the other flows' wants were held behind the slow one")
	r, _, _ := h.read()
	require.Equal(t, 1, r, "a flow's wants are handled one at a time")
}

// answering answers every request with ErrFlowNotKnown until known is
// set, after lag, and counts the requests and the claims offered.
type answering struct {
	lag           time.Duration
	mu            sync.Mutex
	known         bool
	calls, claims int
	granted       chan struct{}
}

func (a *answering) Materialize(context.Context, int32, string) error {
	time.Sleep(a.lag)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if !a.known {
		return intent.ErrFlowNotKnown
	}
	a.granted <- struct{}{}
	return nil
}

func (a *answering) claim(context.Context, string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.claims++
	return false
}

func (a *answering) counts() (calls, claims int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls, a.claims
}

// clock is a settable time.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// drained waits until the queue is read and gives the wants read a
// moment to be handled; a dropped want leaves nothing else to wait for.
func drained(t *testing.T, q *fakeQueue) {
	t.Helper()
	require.Eventually(t, func() bool { return q.left() == 0 }, 2*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond)
}

// A crashlooping reader routed to a flow nobody publishes walks into it
// again and again, and every walk queues a want. Each cost a redundancy
// set List, a pod List and an MxlFlow Get for the same answer. The wants
// that arrive while the first is still being answered are spared too.
func TestRepeatedWantsForAnUnknownFlowLookItUpOnce(t *testing.T) {
	q := &fakeQueue{}
	for range 50 {
		q.push(wantedFlow(0))
	}
	a := &answering{lag: 20 * time.Millisecond, granted: make(chan struct{}, 1)}
	c := &clock{t: time.Unix(1000, 0)}
	runWanted(t, &Wanted{mount: Mount{Root: "/", MountPoint: "/run/mxl"}, next: q.next, now: c.now}, a, a.claim)

	drained(t, q)
	calls, claims := a.counts()
	require.Equal(t, 1, calls, "one lookup for the window, not one per want")
	require.Equal(t, 1, claims)
}

// The window only spares repeat lookups: once it has passed, the next
// want for a flow that has become known gets its mirror. Inside the
// window the wanted path does not see the flow appear; the gateway's
// placeholder ends the walks waiting on it, and the open trigger asks
// for the mirror.
func TestAFlowKnownAfterTheWindowGetsItsMirror(t *testing.T) {
	q := &fakeQueue{}
	a := &answering{granted: make(chan struct{}, 1)}
	c := &clock{t: time.Unix(1000, 0)}
	runWanted(t, &Wanted{mount: Mount{Root: "/", MountPoint: "/run/mxl"}, next: q.next, now: c.now}, a, nil)

	q.push(wantedFlow(0))
	drained(t, q)
	calls, _ := a.counts()
	require.Equal(t, 1, calls)

	a.mu.Lock()
	a.known = true
	a.mu.Unlock()
	c.add(unknownFor - time.Millisecond)
	q.push(wantedFlow(0))
	drained(t, q)
	calls, _ = a.counts()
	require.Equal(t, 1, calls, "a want inside the window is not looked up again")

	c.add(time.Millisecond)
	q.push(wantedFlow(0))
	select {
	case <-a.granted:
	case <-time.After(2 * time.Second):
		t.Fatal("no mirror for a flow known after the window")
	}
}
