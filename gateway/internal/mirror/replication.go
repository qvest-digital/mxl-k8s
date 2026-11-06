package mirror

import (
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
	"k8s.io/apimachinery/pkg/types"
)

// ContentIssue is what a slot of the source ring, or an arrival on the
// target, held instead of a complete unit.
type ContentIssue int

const (
	// ContentInvalid is a grain flagged MXL_GRAIN_FLAG_INVALID: libmxl's
	// writer sets it on the grains it jumps over or leaves incomplete.
	ContentInvalid ContentIssue = iota
	// ContentIncomplete is a grain with fewer valid slices than it has.
	ContentIncomplete
	// ContentMissing is a slot holding another index's grain.
	ContentMissing
)

// SourceRecorder receives what one mirror's source half observes. Its
// calls come from the mirror's transfer loop only. Units are grains for
// a discrete flow and samples for a continuous one.
type SourceRecorder interface {
	// Read is one read of the source ring: nil with the units an OK read
	// covers, or the libmxl error of a failed one.
	Read(err error, index, count uint64)
	// ReadContent counts units read that were not complete.
	ReadContent(issue ContentIssue, index, count uint64)
	// Skipped counts units the loop jumped over to reach the head again
	// after a read came back too late: the ring had overtaken them.
	Skipped(count uint64)
}

// TargetRecorder receives what one mirror's target half observes, from
// its progress loop only.
type TargetRecorder interface {
	// Write is one commit to the local flow: nil with the units it
	// completed, or the error of a failed one.
	Write(err error, index, count uint64)
	// WriteContent counts arrivals committed invalid.
	WriteContent(issue ContentIssue, index, count uint64)
}

// Recorders hands out the recorders of each mirror. A mirror keeps its
// recorders across rebuilds of its fabric side, until Release.
type Recorders interface {
	// Source returns mirror's source recorder for a flow configured as
	// cfg, whose transfer loop reads one span at a time.
	Source(mirror types.NamespacedName, flowID string, cfg mxl.FlowConfig, span time.Duration) SourceRecorder
	// Target returns mirror's target recorder.
	Target(mirror types.NamespacedName, flowID string, cfg mxl.FlowConfig) TargetRecorder
	// ReleaseSource and ReleaseTarget drop a mirror's recorder.
	ReleaseSource(mirror types.NamespacedName)
	ReleaseTarget(mirror types.NamespacedName)
}

type noopSource struct{}

func (noopSource) Read(error, uint64, uint64)               {}
func (noopSource) ReadContent(ContentIssue, uint64, uint64) {}
func (noopSource) Skipped(uint64)                           {}

type noopTarget struct{}

func (noopTarget) Write(error, uint64, uint64)               {}
func (noopTarget) WriteContent(ContentIssue, uint64, uint64) {}
