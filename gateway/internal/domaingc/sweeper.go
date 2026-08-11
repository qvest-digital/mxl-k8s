// Package domaingc reclaims abandoned flow directories from the
// node's MXL domain.
//
// libmxl already decides what "abandoned" means and mxl-k8s never
// asked. mxlGarbageCollectFlows walks the domain and removes each
// flow directory it can take an exclusive lock on, which is the
// library's own definition of a flow nothing holds: writers take a
// shared lock for as long as they are attached, so a flow with a live
// producer or a live mirror writer is never a candidate, however long
// it has been since the last grain. Nothing time-based is involved
// and no threshold has to be guessed.
//
// Readers take no lock at all. A directory whose writer has gone is
// therefore collectable even while consumer pods still hold
// FlowReaders on it, which is what makes the start-up grace below
// load-bearing rather than decorative.
package domaingc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
)

// flowDirSuffix is the directory-name suffix libmxl gives a per-flow
// directory under a domain. Only used to count what a sweep removed;
// the removal itself is libmxl's.
const flowDirSuffix = ".mxl-flow"

// DefaultInterval is how often the domain is swept when no interval is
// configured. An abandoned directory costs only tmpfs until it is
// collected, so the sweep is meant to be unnoticeable rather than
// prompt.
const DefaultInterval = 5 * time.Minute

// DefaultGrace is how long after start the first sweep is held back.
//
// A gateway restart leaves every mirror target it was writing without
// an attached writer, and a reader holds no lock, so a sweep in that
// window would delete flow copies that consumer pods are still reading
// and that the target reconciler is about to re-establish. The
// reconciler needs an informer sync plus one pass per mirror to get
// its writers back; the grace has to cover that comfortably, because
// being slow to reclaim costs disk and being early costs a consumer
// its flow.
const DefaultGrace = 2 * time.Minute

// Collector is the libmxl instance surface a sweep needs. Narrowed to
// one method so tests drive the sweeper without a real domain.
type Collector interface {
	GarbageCollect() error
}

// Sweeper periodically asks libmxl to reclaim unheld flow directories.
type Sweeper struct {
	// Collector is the open MXL instance for this node's domain. A nil
	// Collector disables the sweep.
	Collector Collector

	// DomainPath is the directory the flow count is read from. Only
	// used for logging what a sweep reclaimed; an unreadable path
	// costs the count, not the sweep.
	DomainPath string

	// Interval is the gap between sweeps. Zero or negative disables
	// the sweeper entirely.
	Interval time.Duration

	// Grace is how long after Start the first sweep waits. See
	// DefaultGrace for why this is not optional.
	Grace time.Duration

	Log logr.Logger
}

// Start runs until ctx is cancelled. Signature matches
// manager.RunnableFunc so the gateway can add it to the Manager,
// which starts it only once the caches have synced.
func (s *Sweeper) Start(ctx context.Context) error {
	if s.Collector == nil || s.Interval <= 0 {
		s.Log.Info("domain garbage collection disabled",
			"interval", s.Interval, "haveInstance", s.Collector != nil)
		return nil
	}

	grace := s.Grace
	if grace < 0 {
		grace = 0
	}
	s.Log.Info("domain garbage collection scheduled",
		"interval", s.Interval, "grace", grace)

	select {
	case <-ctx.Done():
		return nil
	case <-time.After(grace):
	}

	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		s.sweep(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// sweep runs one collection. Failures are logged and the loop
// continues: a domain that cannot be swept now is no worse than one
// that was never swept, and giving up would strand every later sweep.
func (s *Sweeper) sweep(ctx context.Context) {
	before, haveBefore := s.countFlows()
	if err := s.Collector.GarbageCollect(); err != nil {
		s.Log.Error(err, "garbage collect domain")
		return
	}
	after, haveAfter := s.countFlows()
	if haveBefore && haveAfter && before > after {
		s.Log.Info("reclaimed abandoned flow directories",
			"removed", before-after, "remaining", after)
	}
	_ = ctx
}

// countFlows reports how many flow directories the domain holds. The
// second result is false when the domain could not be read, which
// makes the count unusable rather than zero - reporting a sweep that
// removed every flow because the directory listing failed would be
// worse than reporting nothing.
func (s *Sweeper) countFlows() (int, bool) {
	if s.DomainPath == "" {
		return 0, false
	}
	entries, err := os.ReadDir(s.DomainPath)
	if err != nil {
		return 0, false
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() && strings.HasSuffix(e.Name(), flowDirSuffix) {
			n++
		}
	}
	return n, true
}

// FlowDirPath is the directory libmxl gives the named flow under a
// domain.
func FlowDirPath(domain, flowID string) string {
	return filepath.Join(domain, fmt.Sprintf("%s%s", flowID, flowDirSuffix))
}
