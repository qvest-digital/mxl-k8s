package tracker

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeStarter struct {
	mu      sync.Mutex
	started map[string]string
	stopped []string
	claimed []string
	fail    map[string]bool
}

func (f *fakeStarter) start(_ context.Context, name, dir string) (Tracker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[name] {
		return Tracker{}, errors.New("no such directory")
	}
	f.started[name] = dir
	return Tracker{
		Stop: func() {
			f.mu.Lock()
			f.stopped = append(f.stopped, name)
			f.mu.Unlock()
		},
		ClaimOrigin: func(_ context.Context, flowID string) error {
			f.mu.Lock()
			f.claimed = append(f.claimed, name+"/"+flowID)
			f.mu.Unlock()
			return nil
		},
	}, nil
}

// Every domain written on the node except the primary gets a tracker, a
// domain that goes away loses it, and one already running is not restarted --
// a restart would miss the flows that appear while it is down.
func TestManagerFollowsTheDomains(t *testing.T) {
	f := &fakeStarter{started: map[string]string{}, fail: map[string]bool{}}
	m := &Manager{PrimaryDir: "domain", Start: f.start}
	ctx := context.Background()

	m.Sync(ctx, map[string]string{"default": "domain", "studio-b": "studio-b"})
	assert.Equal(t, map[string]string{"studio-b": "studio-b"}, f.started, "the primary is tracked by the agent itself")
	running, ready := m.Tracked("studio-b")
	assert.True(t, running)
	assert.True(t, ready)

	f.started = map[string]string{}
	m.Sync(ctx, map[string]string{"default": "domain", "studio-b": "studio-b"})
	assert.Empty(t, f.started, "not restarted")

	m.Sync(ctx, map[string]string{"default": "domain"})
	assert.Equal(t, []string{"studio-b"}, f.stopped)
	running, _ = m.Tracked("studio-b")
	assert.False(t, running)
}

// A tracker that cannot start is retried on the next sync and reported as not
// running meanwhile, so the domain is not claimed mirrored.
func TestManagerRetriesATrackerThatFailedToStart(t *testing.T) {
	f := &fakeStarter{started: map[string]string{}, fail: map[string]bool{"studio-b": true}}
	m := &Manager{PrimaryDir: "domain", Start: f.start}
	ctx := context.Background()

	m.Sync(ctx, map[string]string{"studio-b": "studio-b"})
	running, _ := m.Tracked("studio-b")
	assert.False(t, running)

	f.fail["studio-b"] = false
	m.Sync(ctx, map[string]string{"studio-b": "studio-b"})
	running, _ = m.Tracked("studio-b")
	assert.True(t, running)
}

// A directory moved to another domain name keeps no tracker under the old
// name, and stopping everything on shutdown stops each once.
func TestManagerStopsEverythingOnce(t *testing.T) {
	f := &fakeStarter{started: map[string]string{}, fail: map[string]bool{}}
	m := &Manager{PrimaryDir: "domain", Start: f.start}
	m.Sync(context.Background(), map[string]string{"a": "a", "b": "b"})
	m.StopAll()
	m.StopAll()
	assert.ElementsMatch(t, []string{"a", "b"}, f.stopped)
}

// A producer's claim reaches the tracker of its flow's domain, and a claim
// in a domain not tracked here fails rather than landing anywhere else.
func TestClaimOriginRoutesToTheDomainsTracker(t *testing.T) {
	f := &fakeStarter{started: map[string]string{}}
	m := &Manager{PrimaryDir: "domain", Start: f.start}
	m.Sync(context.Background(), map[string]string{"a": "domains/a", "b": "domains/b"})

	if err := m.ClaimOrigin(context.Background(), "b", "id1"); err != nil {
		t.Fatal(err)
	}
	if err := m.ClaimOrigin(context.Background(), "c", "id1"); err == nil {
		t.Fatal("claim in an untracked domain succeeded")
	}
	if len(f.claimed) != 1 || f.claimed[0] != "b/id1" {
		t.Fatalf("claimed %v, want [b/id1]", f.claimed)
	}
}
