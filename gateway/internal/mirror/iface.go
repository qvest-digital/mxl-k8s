package mirror

import "github.com/qvest-digital/go-mxl/fabrics"

// The two per-mirror progress loops below sit at the heart of every
// data-plane bug the gateway has shipped. They are also the only
// pieces of this package whose state machine can be exercised
// without libmxl-fabrics: the cgo-dependent work happens behind a
// handful of function calls the loops make on every iteration.
//
// The refactor that introduced these types narrowed the loop
// signatures to accept those calls as injected functions. Production
// binds them to thin closures over *mxl.Reader / *mxl.Writer /
// *fabrics.Initiator / *fabrics.Target; the tests bind them to
// closures that record every call and return whatever the scenario
// needs. No interface gymnastics, no mockery boilerplate; the loops
// stay where they were, and only their parameter list changed.

// initiatorOpener is the source reconciler's seam onto the
// cgo-dependent libmxl-fabrics Initiator setup path. Production
// binds it to libmxlOpener, which is a thin struct wrapping the
// existing FlowReader + Regions + Initiator + AddTarget sequence.
// Tests bind it to an inline fake whose open method returns canned
// sourceEntry values without touching libmxl or libmxl-fabrics.
//
// The interface keeps the production binary free of a swappable
// function pointer that a malicious or buggy caller could redirect
// at runtime: the field on SourceReconciler is an interface value
// the constructor sets once and never reassigns.
type initiatorOpener interface {
	open(flowID, targetInfoStr string, provider fabrics.Provider) (*sourceEntry, error)
}

// RuntimeProbe asks the source-side flow reader for the current head
// index. Production reads it from mxl.Reader.Runtime(); tests return
// a value the scenario controls.
type RuntimeProbe func() (head uint64, err error)

// TransferFunc transfers one grain from the source flow to the
// target. The bool return signals "grain was skipped" - production
// returns it true when grain.TotalSlices == 0 (continuous flows have
// no slice subdivision and v0 does not transfer them).
//
// A non-nil error breaks the per-tick loop; the next tick re-reads
// head and re-tries from lastSent+1.
type TransferFunc func(idx uint64) (skipped bool, err error)

// ProgressFunc drives libmxl-fabrics's event/completion queues.
// Production calls Initiator.MakeProgressNonBlocking; the loop
// tolerates fabrics.ErrNotReady silently.
type ProgressFunc func() error

// ReadGrainFunc polls the target side for an arrived grain. Returns
// fabrics.ErrNotReady when nothing landed since the previous call so
// the loop can sleep. Any other error is fatal: it tells the loop
// to exit and the recovery callback to fire.
type ReadGrainFunc func() (idx uint64, err error)

// CommitFunc finishes one arrived grain on the local writer
// (OpenGrain + Commit) so consumer FlowReaders see it. Production
// passes a closure over the per-mirror *mxl.Writer.
type CommitFunc func(idx uint64) error
