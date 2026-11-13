// Package tracker runs flow tracking for every MXL domain on the node other
// than the primary one, which the agent tracks itself.
package tracker

import (
	"context"
	"fmt"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Tracker is one running domain tracker: what stops it, and what records
// this node as the origin of one of its flows.
type Tracker struct {
	Stop        func()
	ClaimOrigin func(ctx context.Context, flowID string) error
}

// StartFunc starts tracking the flows of one domain directory. It returns
// once tracking is live, or with the reason it could not start.
type StartFunc func(ctx context.Context, name, dir string) (Tracker, error)

// Manager keeps one tracker per domain written on the node, starting and
// stopping them as domains come and go.
type Manager struct {
	// PrimaryDir is the directory the agent tracks itself.
	PrimaryDir string
	Start      StartFunc

	mu      sync.Mutex
	running map[string]entry
}

type entry struct {
	dir string
	Tracker
}

// Sync makes the running trackers match the domains written on the node,
// name to directory. A tracker already running for a domain in the same
// directory is left alone: restarting it would miss the flows that appear
// while it is down.
func (m *Manager) Sync(ctx context.Context, domains map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running == nil {
		m.running = map[string]entry{}
	}
	for name, e := range m.running {
		if dir, ok := domains[name]; !ok || dir != e.dir || dir == m.PrimaryDir {
			e.Stop()
			delete(m.running, name)
		}
	}
	for name, dir := range domains {
		if dir == m.PrimaryDir {
			continue
		}
		if _, ok := m.running[name]; ok {
			continue
		}
		t, err := m.Start(ctx, name, dir)
		if err != nil {
			// Retried on the next sync; meanwhile the domain is reported
			// as not mirrored.
			log.FromContext(ctx).Info("cannot track domain yet", "domain", name,
				"directory", dir, "reason", err.Error())
			continue
		}
		m.running[name] = entry{dir: dir, Tracker: t}
	}
}

// Tracked reports whether a domain's flows are tracked on this node. A
// running tracker is live: Start returns only once it is.
func (m *Manager) Tracked(name string) (running, ready bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.running[name]
	return ok, ok
}

// ClaimOrigin records this node as the origin of a flow of a tracked
// domain.
func (m *Manager) ClaimOrigin(ctx context.Context, domain, flowID string) error {
	m.mu.Lock()
	e, ok := m.running[domain]
	m.mu.Unlock()
	if !ok || e.ClaimOrigin == nil {
		return fmt.Errorf("domain %s is not tracked on this node", domain)
	}
	return e.ClaimOrigin(ctx, flowID)
}

// StopAll stops every tracker.
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, e := range m.running {
		e.Stop()
		delete(m.running, name)
	}
}
