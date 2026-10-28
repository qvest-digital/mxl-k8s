package mxlfswatch

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

type limited struct{}

func (limited) Error() string             { return "pod had 8 mirrors created on demand in the last 10s" }
func (limited) RetryAfter() time.Duration { return 10 * time.Millisecond }

// scripted answers each request with the next error of its script, nil
// once the script is spent.
type scripted struct {
	mu     sync.Mutex
	errs   []error
	calls  int
	before func() // runs ahead of every request after the first
}

func (s *scripted) Materialize(context.Context, int32, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls > 1 && s.before != nil {
		s.before()
	}
	if len(s.errs) == 0 {
		return nil
	}
	err := s.errs[0]
	s.errs = s.errs[1:]
	return err
}

func (s *scripted) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// openRing has pid open the placeholder's ring and waits for every
// request that open started.
func openRing(t *testing.T, tr *Trigger, d string, pid int32) {
	t.Helper()
	require.True(t, tr.Handle(context.Background(),
		fanotify.Open{PID: pid, Path: filepath.Join(d, id+".mxl-flow", "grains", "data.0")}))
	done := make(chan struct{})
	go func() { tr.requests.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the request did not end")
	}
}

func retryTrigger(t *testing.T, d string, m Materializer, start func(int32) (string, bool)) *Trigger {
	t.Helper()
	saved := minRetry
	minRetry = time.Millisecond
	t.Cleanup(func() { minRetry = saved })
	return &Trigger{Domain: d, Dispatcher: m, Window: time.Minute, Log: logr.Discard(), ProcessStart: start}
}

func alive(int32) (string, bool) { return "4242", true }

// The open of a placeholder has succeeded before the agent hears of it,
// so a refusal by a limit reaches no reader: the process maps a ring
// nothing fills and never opens it again. The request is made again
// while that process runs, until it passes.
func TestARefusedPlaceholderRequestIsMadeAgainWhileTheProcessRuns(t *testing.T) {
	d := domainWithPlaceholder(t)
	m := &scripted{errs: []error{limited{}, limited{}}}
	openRing(t, retryTrigger(t, d, m, alive), d, 77)
	require.Equal(t, 3, m.count(), "a refused request was not made again until it passed")
}

// A process that is gone, or whose pid now names another process, no
// longer reads the ring.
func TestARefusedRequestEndsWithItsProcess(t *testing.T) {
	for name, start := range map[string]func(int32) (string, bool){
		"exited": func(int32) (string, bool) { return "", false },
		"replaced": func() func(int32) (string, bool) {
			n := 0
			return func(int32) (string, bool) { n++; return string(rune('0' + n)), true }
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			d := domainWithPlaceholder(t)
			m := &scripted{errs: []error{limited{}, limited{}}}
			openRing(t, retryTrigger(t, d, m, start), d, 77)
			require.Equal(t, 1, m.count())
		})
	}
}

// A flow that is no placeholder here any more, filled by a mirror another
// request brought, needs nothing.
func TestARefusedRequestEndsOnceThePlaceholderIsFilled(t *testing.T) {
	d := domainWithPlaceholder(t)
	m := &scripted{errs: []error{limited{}, limited{}, limited{}}}
	tr := retryTrigger(t, d, m, alive)
	m.before = func() { _ = os.Remove(mxlv1alpha1.PlaceholderMarker(d, id)) }
	openRing(t, tr, d, 77)
	require.Equal(t, 2, m.count(), "requests went on for a flow no longer a placeholder")
}

type capped struct{}

func (capped) Error() string             { return "pod already has 64 mirrors created on demand, its limit" }
func (capped) RetryAfter() time.Duration { return 0 }

// A refusal waiting cannot lift, by a count of live mirrors or by
// anything but a limit, is not made again: each request costs API calls
// on the agent's one client, and a pod at its count stays there.
func TestARefusalWaitingCannotLiftIsFinal(t *testing.T) {
	for name, err := range map[string]error{
		"count of live mirrors": capped{},
		"no limit":              errors.New("MxlFlow not yet known cluster-wide"),
	} {
		t.Run(name, func(t *testing.T) {
			d := domainWithPlaceholder(t)
			m := &scripted{errs: []error{err}}
			openRing(t, retryTrigger(t, d, m, alive), d, 77)
			require.Equal(t, 1, m.count())
		})
	}
}

// A process opening the flow again after the dedup window, while its
// first request still waits to ask again, starts no second request.
func TestAReopenWhileARequestWaitsStartsNoOther(t *testing.T) {
	d := domainWithPlaceholder(t)
	release := make(chan struct{})
	m := &scripted{errs: []error{limited{}}}
	tr := retryTrigger(t, d, m, alive)
	tr.Window = time.Nanosecond
	m.before = func() { <-release }
	open := fanotify.Open{PID: 77, Path: filepath.Join(d, id+".mxl-flow", "grains", "data.0")}
	require.True(t, tr.Handle(context.Background(), open))
	require.Eventually(t, func() bool { return m.count() >= 1 }, 5*time.Second, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	require.False(t, tr.Handle(context.Background(), open), "a second request for the same process and flow")
	close(release)
	tr.requests.Wait()
	require.Equal(t, 2, m.count(), "one refused request and its one retry")
}

// The start time tells a process from a later one under the same pid,
// so a child started after this test process has read a later one.
func TestProcessStartReadsTheKernelsStartTime(t *testing.T) {
	mine, ok := processStart(int32(os.Getpid()))
	require.True(t, ok)
	again, _ := processStart(int32(os.Getpid()))
	require.Equal(t, mine, again)

	time.Sleep(50 * time.Millisecond) // past one clock tick
	child := exec.Command("sleep", "5")
	require.NoError(t, child.Start())
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	theirs, ok := processStart(int32(child.Process.Pid))
	require.True(t, ok)
	a, err := strconv.ParseUint(mine, 10, 64)
	require.NoError(t, err)
	b, err := strconv.ParseUint(theirs, 10, 64)
	require.NoError(t, err)
	require.Greater(t, b, a, "a later process read no later start time")

	_, ok = processStart(1 << 30)
	require.False(t, ok)
}

// Once a request has ended, the same process opening the flow again after
// the dedup window starts a fresh one: the entry that blocks a second
// concurrent request is released.
func TestAReopenAfterARequestEndedStartsANewOne(t *testing.T) {
	d := domainWithPlaceholder(t)
	m := &scripted{}
	tr := retryTrigger(t, d, m, alive)
	tr.Window = time.Nanosecond
	open := fanotify.Open{PID: 77, Path: filepath.Join(d, id+".mxl-flow", "grains", "data.0")}
	require.True(t, tr.Handle(context.Background(), open))
	tr.requests.Wait()
	time.Sleep(time.Millisecond)
	require.True(t, tr.Handle(context.Background(), open), "a finished request still blocked the next one")
	tr.requests.Wait()
	require.Equal(t, 2, m.count())
}
