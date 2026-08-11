package domaingc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCollector counts sweeps and can fail them.
type fakeCollector struct {
	calls atomic.Int32
	err   error
}

func (f *fakeCollector) GarbageCollect() error {
	f.calls.Add(1)
	return f.err
}

func newSweeper(c Collector, interval, grace time.Duration) *Sweeper {
	return &Sweeper{
		Collector: c,
		Interval:  interval,
		Grace:     grace,
		Log:       logr.Discard(),
	}
}

func TestSweeperHoldsBackTheFirstSweep(t *testing.T) {
	// A gateway restart leaves every mirror target it was writing
	// without an attached writer, and libmxl's collector only tests for
	// a writer - a consumer pod holding a FlowReader on that copy does
	// not protect it. Sweeping before the target reconciler has
	// re-established its writers therefore deletes flows that consumers
	// are still reading and that were about to come back.
	c := &fakeCollector{}
	s := newSweeper(c, 10*time.Millisecond, 300*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx) }()

	time.Sleep(100 * time.Millisecond)
	require.Zero(t, c.calls.Load(),
		"no sweep may run inside the grace; this window is exactly when a "+
			"restarted gateway's own mirror copies look unheld")

	require.Eventually(t, func() bool { return c.calls.Load() > 0 },
		2*time.Second, 10*time.Millisecond,
		"the sweep must start once the grace has passed")
}

func TestSweeperRepeatsOnTheInterval(t *testing.T) {
	c := &fakeCollector{}
	s := newSweeper(c, 20*time.Millisecond, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx) }()

	require.Eventually(t, func() bool { return c.calls.Load() >= 3 },
		2*time.Second, 10*time.Millisecond,
		"an abandoned directory that appears after the first sweep still "+
			"has to be reclaimed")
}

func TestSweeperKeepsGoingAfterAFailedSweep(t *testing.T) {
	// A domain that cannot be swept now is no worse than one never
	// swept; giving up would strand every later sweep on the node.
	c := &fakeCollector{err: errors.New("domain busy")}
	s := newSweeper(c, 10*time.Millisecond, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx) }()

	require.Eventually(t, func() bool { return c.calls.Load() >= 3 },
		2*time.Second, 10*time.Millisecond,
		"a failing collect must not end the loop")
}

func TestSweeperDisabled(t *testing.T) {
	// Both the interval and the instance are ways to turn the sweep
	// off, and neither may leave a goroutine running.
	for name, s := range map[string]*Sweeper{
		"zero interval": newSweeper(&fakeCollector{}, 0, 0),
		"no instance":   newSweeper(nil, time.Millisecond, 0),
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- s.Start(context.Background()) }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("Start must return immediately when disabled")
			}
		})
	}
}

func TestSweeperStopsOnContextCancel(t *testing.T) {
	s := newSweeper(&fakeCollector{}, time.Hour, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("a cancelled context must end the sweeper, including inside the grace")
	}
}

func TestCountFlowsOnlyCountsFlowDirectories(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "abc"+flowDirSuffix), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "def"+flowDirSuffix), 0o755))
	// libmxl stages a flow under a temp name before publishing it, and
	// the domain also holds files that are not flows at all.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".mxl-tmp-XXXX"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "options.json"), []byte("{}"), 0o644))

	s := &Sweeper{DomainPath: dir, Log: logr.Discard()}
	n, ok := s.countFlows()
	require.True(t, ok)
	assert.Equal(t, 2, n,
		"only published flow directories count; a staging directory is not "+
			"a flow and must not read as one reclaimed")
}

func TestCountFlowsReportsAnUnreadableDomain(t *testing.T) {
	// A failed listing has to be distinguishable from an empty domain:
	// treating it as zero would log a sweep that reclaimed every flow.
	s := &Sweeper{DomainPath: filepath.Join(t.TempDir(), "absent"), Log: logr.Discard()}
	_, ok := s.countFlows()
	assert.False(t, ok)

	s = &Sweeper{Log: logr.Discard()}
	_, ok = s.countFlows()
	assert.False(t, ok, "an unset domain path cannot produce a count")
}
