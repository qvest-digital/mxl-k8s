// Package domain reads an MXL domain directory and reports what each
// flow in it is doing. Every value comes from go-mxl, so the
// shared-memory layout stays libmxl's to define.
package domain

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
)

// flowDirSuffix is what libmxl names a flow's directory inside a
// domain: "<flow-id>.mxl-flow".
const flowDirSuffix = ".mxl-flow"

// Observation is one flow's state at the moment it was sampled.
type Observation struct {
	// ID is the flow UUID, taken from its directory name.
	ID string

	// Present reports whether the flow's directory is still in the
	// domain. A departed flow keeps being observed, with Present
	// false, until its lifetime expires.
	Present bool

	// Active reports whether the flow was written within its own
	// activity window, measured on the MXL clock that stamps the write
	// itself. A flow can be present and inactive, which is what a
	// stalled writer looks like from outside.
	Active bool

	// HaveInfo reports whether Info and Latency could be read. A flow
	// whose directory exists but whose reader cannot be opened yet
	// (mid-creation, or already torn down) has none.
	HaveInfo bool

	// Info is the flow's config and runtime state.
	Info mxl.FlowInfo

	// LatencyGrains is how far the head index sits behind where the
	// MXL clock says it should be at this grain rate. Negative means
	// the writer is running ahead of the clock.
	LatencyGrains int64

	// WriteAge is the MXL-clock time since the last write. Derived
	// from the same clock that stamps LastWriteTime rather than from
	// wall time, so the two are comparable.
	WriteAge time.Duration

	// Def is the parsed flow_def.json, nil when it could not be read
	// or parsed.
	Def *FlowDef
}

// FSStat describes the filesystem backing a domain.
type FSStat struct {
	TotalBytes     uint64
	AvailableBytes uint64
	UsedBytes      uint64
}

// entry tracks one flow across observations.
type entry struct {
	id     string
	reader *mxl.Reader
	def    *FlowDef

	removedAt   time.Time
	sizeBytes   uint64
	openAttempt bool
}

// Domain is an open MXL domain directory.
type Domain struct {
	path     string
	lifetime time.Duration

	inst *mxl.Instance

	mu        sync.Mutex
	flows     map[string]*entry
	sizeBytes uint64
}

// Open opens the domain at path. lifetime is how long a departed flow
// keeps being reported before it is forgotten.
func Open(path string, lifetime time.Duration) (*Domain, error) {
	inst, err := mxl.NewInstance(path, "")
	if err != nil {
		return nil, fmt.Errorf("open mxl domain %q: %w", path, err)
	}
	return &Domain{
		path:     path,
		lifetime: lifetime,
		inst:     inst,
		flows:    map[string]*entry{},
	}, nil
}

// Path returns the domain directory this Domain reports on.
func (d *Domain) Path() string { return d.path }

// Close releases every open reader and the underlying instance.
func (d *Domain) Close() error {
	d.mu.Lock()
	for _, e := range d.flows {
		e.close()
	}
	d.flows = map[string]*entry{}
	d.mu.Unlock()
	return d.inst.Close()
}

// Scan re-lists the domain directory, opening readers for flows that
// appeared, closing those for flows that left, and dropping entries
// whose lifetime has expired. It also refreshes the cached on-disk
// size, which keeps the directory walk off the scrape path.
func (d *Domain) Scan() error {
	ids, sizes, err := d.listFlows()
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	var total uint64
	for _, size := range sizes {
		total += size
	}
	d.sizeBytes = total

	for id := range ids {
		e, ok := d.flows[id]
		if !ok {
			e = &entry{id: id}
			d.flows[id] = e
		}
		e.removedAt = time.Time{}
		e.sizeBytes = sizes[id]
		d.openReader(e)
	}

	now := time.Now()
	for id, e := range d.flows {
		if _, still := ids[id]; still {
			continue
		}
		if e.removedAt.IsZero() {
			// The flow is gone; drop the libmxl handle now rather than
			// at expiry, so a held reader never keeps a torn-down flow
			// from being collected.
			e.removedAt = now
			e.close()
			continue
		}
		if d.lifetime >= 0 && now.Sub(e.removedAt) > d.lifetime {
			delete(d.flows, id)
		}
	}
	return nil
}

// openReader opens a reader for e if it does not have one. A flow
// caught mid-creation fails here and is retried on the next scan.
func (d *Domain) openReader(e *entry) {
	if e.reader != nil {
		return
	}
	r, err := d.inst.NewReader(e.id)
	if err != nil {
		e.openAttempt = false
		return
	}
	e.reader = r
	e.openAttempt = true
	if raw, err := d.inst.FlowDef(e.id); err == nil {
		if def, err := ParseFlowDef(raw); err == nil {
			e.def = def
		}
	}
}

// minActivityWindow floors the window derived from a flow's cadence.
// A 48 kHz audio flow commits every 10 ms and a 60 fps video flow
// every 17 ms; three of either is shorter than the jitter between two
// scrapes, so without a floor a healthy fast flow would flap.
const minActivityWindow = 100 * time.Millisecond

// activeGrains is how many writes a flow may miss before it reads as
// stalled.
const activeGrains = 3

// activityWindow returns how long after its last write a flow still
// counts as active, derived from the flow's own cadence.
//
// libmxl does not define this. Its own notion of an active flow is
// whether any process holds the data file locked, which is what
// mxlGarbageCollectFlows tests; it maintains lastWriteTime but attaches
// no threshold to it. The threshold is this exporter's policy, so it
// has to be relative to the flow rather than a constant: a fixed window
// that means three grains at 25 fps means a fifth of a grain at 1 fps,
// where it would report a healthy flow stalled between every frame.
//
// On a continuous flow the grain rate is the sample rate and writes
// land one commit batch apart, not one sample apart.
func activityWindow(cfg mxl.CommonFlowConfig) time.Duration {
	rate := cfg.GrainRate
	if rate.Num <= 0 || rate.Den <= 0 {
		return minActivityWindow
	}
	perWrite := time.Duration(float64(rate.Den) / float64(rate.Num) * float64(time.Second))
	if !cfg.Format.IsDiscrete() {
		if batch := cfg.MaxCommitBatchSizeHint; batch > 0 {
			perWrite *= time.Duration(batch)
		}
	}
	if w := activeGrains * perWrite; w > minActivityWindow {
		return w
	}
	return minActivityWindow
}

// Observe samples every tracked flow. It holds no state across calls:
// every value is read from the flow header, so calling it twice in a
// row answers the same thing twice.
//
// Activity used to be a delta against the previous call, which made it
// a property of the caller rather than of the flow. Two collectors are
// registered and Prometheus calls both in one scrape, so whichever ran
// first consumed the head advancement and the other reported the flow
// idle - the same scrape could carry an advancing head index and
// active=0. Anything else sampling the endpoint, a probe or a second
// Prometheus, corrupted the answer the same way.
func (d *Domain) Observe() []Observation {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := mxl.Now()
	out := make([]Observation, 0, len(d.flows))
	for _, e := range d.flows {
		obs := Observation{
			ID:      e.id,
			Present: e.removedAt.IsZero(),
			Def:     e.def,
		}
		active := false
		if e.reader != nil {
			if info, err := e.reader.Info(); err == nil {
				obs.HaveInfo = true
				obs.Info = info

				rate := info.Config.Common.GrainRate
				if rate.Den != 0 && rate.Num != 0 {
					obs.LatencyGrains = int64(mxl.TimestampToIndex(rate, now)) - int64(info.Runtime.HeadIndex)
				}
				if lw := info.Runtime.LastWriteTime; lw != 0 && now > lw {
					obs.WriteAge = time.Duration(now-lw) * time.Nanosecond
					active = obs.WriteAge < activityWindow(info.Config.Common)
				}
			}
		}
		// A departed flow is never active, however recent its last
		// write was.
		obs.Active = active && obs.Present
		out = append(out, obs)
	}
	return out
}

// SizeBytes is the combined on-disk size of the domain's flows as of
// the last Scan.
func (d *Domain) SizeBytes() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sizeBytes
}

// listFlows returns the flow ids present in the domain and the on-disk
// size of each.
func (d *Domain) listFlows() (map[string]struct{}, map[string]uint64, error) {
	dirents, err := os.ReadDir(d.path)
	if err != nil {
		return nil, nil, fmt.Errorf("read domain %q: %w", d.path, err)
	}
	ids := map[string]struct{}{}
	sizes := map[string]uint64{}
	for _, de := range dirents {
		if !de.IsDir() || !strings.HasSuffix(de.Name(), flowDirSuffix) {
			continue
		}
		id := strings.TrimSuffix(de.Name(), flowDirSuffix)
		if id == "" {
			continue
		}
		ids[id] = struct{}{}
		sizes[id] = dirSize(filepath.Join(d.path, de.Name()))
	}
	return ids, sizes, nil
}

// dirSize sums the apparent size of every regular file under root. A
// file that vanishes mid-walk is skipped rather than failing the walk:
// flows are created and torn down while this runs.
func dirSize(root string) uint64 {
	var total uint64
	_ = filepath.WalkDir(root, func(_ string, de fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if de.IsDir() {
			return nil
		}
		info, err := de.Info()
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

func (e *entry) close() {
	if e.reader != nil {
		_ = e.reader.Close()
		e.reader = nil
	}
}
