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

// TransferResult is what became of a unit the source held complete.
type TransferResult string

const (
	// TransferDelivered completed within one grain or batch interval.
	TransferDelivered TransferResult = "delivered"
	// TransferLate completed after it.
	TransferLate TransferResult = "late"
	// TransferDropped was passed over by a resync while complete.
	TransferDropped TransferResult = "dropped"
	// TransferFailed was outstanding when the target was lost or its
	// post was refused, or fell between the last unit accounted for and
	// the first one a rebuilt transfer loop sends.
	TransferFailed TransferResult = "failed"
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
	// Skipped counts units jumped over after the ring overtook the loop.
	Skipped(count uint64)
	// Transfer counts units the source held complete, by what became of
	// them.
	Transfer(result TransferResult, index, count uint64)
}

// TargetRecorder receives what one mirror's target half observes, from
// its progress loop only.
type TargetRecorder interface {
	// Write is one commit to the local flow: nil with the units it
	// completed, or the error of a failed one.
	Write(err error, index, count uint64)
	// WriteContent counts arrivals committed invalid.
	WriteContent(issue ContentIssue, index, count uint64)
	// Gap counts units no arrival was committed for, below the one just
	// committed: libmxl's writer marks them invalid.
	Gap(index, count uint64)
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
func (noopSource) Transfer(TransferResult, uint64, uint64)  {}

type noopTarget struct{}

func (noopTarget) Write(error, uint64, uint64)               {}
func (noopTarget) WriteContent(ContentIssue, uint64, uint64) {}
func (noopTarget) Gap(uint64, uint64)                        {}

// slotState is what a passed-over slot of the source ring holds.
type slotState int

const (
	slotComplete slotState = iota
	slotInvalid
	slotIncomplete
	slotMissing
	slotTooLate
)

// slotOf classifies slot idx from a non-blocking read of it.
func slotOf(g mxl.Grain, err error, idx uint64) slotState {
	switch {
	case err != nil:
		// Below the head a slot is readable unless it has been reused.
		return slotTooLate
	case g.Index != idx:
		return slotMissing
	case g.Invalid():
		return slotInvalid
	case g.ValidSlices < g.TotalSlices:
		return slotIncomplete
	}
	return slotComplete
}

// maxInspected bounds the slots one pass-over reads one by one; older
// ones are counted as aged out without a read.
const maxInspected = 4096

// accountPassedOver records what became of the units [from, to) of a
// discrete flow the loop did not send: a complete one as result, one
// that was never complete at the source as content, one already
// reused as skipped.
func accountPassedOver(rec SourceRecorder, slot func(uint64) slotState, from, to uint64, result TransferResult) {
	if to <= from {
		return
	}
	var counts [slotTooLate + 1]uint64
	if to-from > maxInspected {
		counts[slotTooLate] = to - from - maxInspected
		from = to - maxInspected
	}
	for idx := from; idx < to; idx++ {
		counts[slot(idx)]++
	}
	last := to - 1
	if n := counts[slotComplete]; n > 0 {
		rec.Transfer(result, last, n)
	}
	if n := counts[slotInvalid]; n > 0 {
		rec.ReadContent(ContentInvalid, last, n)
	}
	if n := counts[slotIncomplete]; n > 0 {
		rec.ReadContent(ContentIncomplete, last, n)
	}
	if n := counts[slotMissing]; n > 0 {
		rec.ReadContent(ContentMissing, last, n)
	}
	if n := counts[slotTooLate]; n > 0 {
		rec.Read(mxl.ErrOutOfRangeLate, from, 1)
		rec.Skipped(n)
	}
}
