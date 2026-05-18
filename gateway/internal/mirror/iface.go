package mirror

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
