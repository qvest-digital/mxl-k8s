// Package replmon records the gateway's replication through
// libmxl-monitoring (github.com/marquies/libmxl-monitoring).
//
// Every mirror has one handle per role it plays on this node: the
// replication source where it reads the flow, the replication target
// where it writes it. A handle is reported under the mirror's
// "<namespace>/<name>", the same on both nodes, and points at the flow
// it reads or writes. The library counts reads, writes, content issues,
// skipped units and latency against the grains' media timestamps.
package replmon

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	mm "github.com/marquies/libmxl-monitoring/go"
	"github.com/qvest-digital/go-mxl/mxl"
	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/mirror"
)

// PollInterval is how often Run polls the library, which turns counts
// into the delivering state, stalls and conditions. The library wants it
// shorter than its one-second recover delay.
const PollInterval = 500 * time.Millisecond

// Monitoring implements mirror.Recorders.
type Monitoring struct {
	m   *mm.Monitoring
	log logr.Logger

	mu      sync.Mutex
	sources map[types.NamespacedName]*source
	targets map[types.NamespacedName]*target
}

// New creates the library context.
func New(log logr.Logger) (*Monitoring, error) {
	m, err := mm.New()
	if err != nil {
		return nil, err
	}
	return &Monitoring{m: m, log: log,
		sources: map[types.NamespacedName]*source{}, targets: map[types.NamespacedName]*target{}}, nil
}

// Run polls the library until ctx is done.
func (m *Monitoring) Run(ctx context.Context) error {
	t := time.NewTicker(PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := m.m.Poll(); err != nil {
				m.log.Error(err, "poll libmxl-monitoring")
			}
		}
	}
}

// handle is one library flow handle. Record calls hold it shared;
// release takes it exclusively, so a loop that outlives its mirror's
// teardown never records into a removed handle.
type handle struct {
	mu       sync.RWMutex
	flow     *mm.Flow
	released bool
}

func (h *handle) record(fn func(*mm.Flow)) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.released {
		fn(h.flow)
	}
}

type source struct{ handle }

func (s *source) Read(err error, index, count uint64) {
	s.record(func(f *mm.Flow) { f.RecordRead(err, index, count) })
}

func (s *source) ReadContent(issue mirror.ContentIssue, index, count uint64) {
	s.record(func(f *mm.Flow) { f.RecordReadContent(content(issue), index, count) })
}

func (s *source) Skipped(count uint64) {
	s.record(func(f *mm.Flow) { f.RecordSkipped(count) })
}

type target struct{ handle }

func (t *target) Write(err error, index, count uint64) {
	t.record(func(f *mm.Flow) { f.RecordWrite(err, index, count) })
}

func (t *target) WriteContent(issue mirror.ContentIssue, index, count uint64) {
	t.record(func(f *mm.Flow) { f.RecordWriteContent(content(issue), index, count) })
}

func content(issue mirror.ContentIssue) mm.Content {
	switch issue {
	case mirror.ContentIncomplete:
		return mm.ContentIncomplete
	case mirror.ContentMissing:
		return mm.ContentMissing
	}
	return mm.ContentInvalid
}

// id is the name a mirror's handles are reported under.
func id(key types.NamespacedName) string { return key.Namespace + "/" + key.Name }

// format says what the flow is, so the library can place indices on the
// media clock.
func format(f *mm.Flow, cfg mxl.FlowConfig) error {
	unit := mm.UnitGrain
	if cfg.Common.Format == mxl.FormatAudio {
		unit = mm.UnitSample
	}
	return f.SetFormat(cfg.Common.Format, unit, cfg.Common.GrainRate, cfg.Common.Format.String())
}

// Timing is the library's reader rule (DESIGN.md "Counting rules"): a
// unit may land one read span after its timestamp, plus 50 ms for
// scheduling, and a stall is four spans without data, at least 100 ms.
func Timing(span time.Duration) (grace, stall time.Duration) {
	return span + 50*time.Millisecond, max(4*span, 100*time.Millisecond)
}

// Source implements mirror.Recorders. A mirror keeps its handle across
// the rebuilds of its fabric side; each one counts as an open.
func (m *Monitoring) Source(key types.NamespacedName, flowID string, cfg mxl.FlowConfig, span time.Duration) mirror.SourceRecorder {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sources[key]
	if s == nil {
		f, err := m.m.AddFlow(id(key), mm.RoleReplicationSource)
		if err != nil {
			m.log.Error(err, "add replication source", "mirror", key)
			return nil
		}
		s = &source{handle: handle{flow: f}}
		m.sources[key] = s
	}
	f := s.flow
	if err := f.SetTarget(flowID); err != nil {
		m.log.Error(err, "set replication source flow", "mirror", key)
	}
	if err := format(f, cfg); err != nil {
		m.log.Error(err, "set replication source format", "mirror", key)
	}
	if span > 0 {
		grace, stall := Timing(span)
		if err := f.SetTiming(grace, stall); err != nil {
			m.log.Error(err, "set replication source timing", "mirror", key)
		}
	}
	f.RecordAccess(nil)
	return s
}

// Target implements mirror.Recorders.
func (m *Monitoring) Target(key types.NamespacedName, flowID string, cfg mxl.FlowConfig) mirror.TargetRecorder {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.targets[key]
	if t == nil {
		f, err := m.m.AddFlow(id(key), mm.RoleReplicationTarget)
		if err != nil {
			m.log.Error(err, "add replication target", "mirror", key)
			return nil
		}
		t = &target{handle: handle{flow: f}}
		m.targets[key] = t
	}
	f := t.flow
	if err := f.SetTarget(flowID); err != nil {
		m.log.Error(err, "set replication target flow", "mirror", key)
	}
	if err := format(f, cfg); err != nil {
		m.log.Error(err, "set replication target format", "mirror", key)
	}
	f.RecordAccess(nil)
	return t
}

// ReleaseSource implements mirror.Recorders.
func (m *Monitoring) ReleaseSource(key types.NamespacedName) {
	m.mu.Lock()
	s := m.sources[key]
	delete(m.sources, key)
	m.mu.Unlock()
	if s != nil {
		m.release(&s.handle)
	}
}

// ReleaseTarget implements mirror.Recorders.
func (m *Monitoring) ReleaseTarget(key types.NamespacedName) {
	m.mu.Lock()
	t := m.targets[key]
	delete(m.targets, key)
	m.mu.Unlock()
	if t != nil {
		m.release(&t.handle)
	}
}

func (m *Monitoring) release(h *handle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.released = true
	if err := m.m.RemoveFlow(h.flow); err != nil {
		m.log.Error(err, "remove replication handle")
	}
}

// Close releases the library context.
func (m *Monitoring) Close() error { return m.m.Close() }
