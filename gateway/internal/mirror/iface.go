package mirror

// The transfer loops on both sides take their cgo calls as injected
// functions, so their state machines run in tests without libmxl.

// initiatorOpener is the source reconciler's seam onto libmxl and
// libmxl-fabrics.
type initiatorOpener interface {
	// open reads the configuration of the flow key names, which every
	// mirror of it on that provider shares.
	open(key sourceKey) (*sharedSource, error)

	// attach opens a reader and an initiator for one mirror, adds the
	// mirror's target and starts the transfer loop reporting to e. An
	// AddTarget failure is wrapped in errAddTargetFailed so the
	// reconciler can back off without opening the source again.
	attach(s *sharedSource, targetInfoStr string, e *sourceEntry) (*sourceLink, error)

	// detach stops the loop, removes the target and closes the
	// initiator and the mirror's reader.
	detach(link *sourceLink)
}

// RuntimeProbe asks the source-side flow reader for the current head
// index. Production reads it from mxl.Reader.Runtime(); tests return
// a value the scenario controls.
type RuntimeProbe func() (head uint64, err error)

// TransferFunc posts the slices of grain idx of a discrete flow from
// start on to a mirror's target. It returns the slice the grain's next
// batch starts at, zero once the grain is done, and the payload posted.
// A failed post is wrapped in errPost; any other error is a grain the
// reader cannot hand out.
type TransferFunc func(idx uint64, start uint16) (next uint16, bytes uint64, err error)

// ProgressFunc is one MakeProgress call on a mirror's initiator.
type ProgressFunc func() error

// ReadGrainFunc waits for the target side to report an arrived grain.
// Returns fabrics.ErrNotReady when nothing landed before its own
// timeout. Any other error is fatal: it tells the loop to exit and the
// recovery callback to fire.
type ReadGrainFunc func() (idx uint64, err error)

// CommitFunc finishes one arrived grain on the local writer
// (OpenGrain + Commit) so consumer FlowReaders see it. Production
// passes a closure over the per-mirror *mxl.Writer.
type CommitFunc func(idx uint64) error

// SampleTransferFunc posts the run of count samples ending at headIndex
// of a continuous flow to a mirror's target and returns the payload
// posted.
type SampleTransferFunc func(headIndex uint64, count int) (bytes uint64, err error)

// ReadSamplesFunc polls the target side for an arrived run of samples,
// returning the ending head index and the sample count. Returns
// fabrics.ErrNotReady when nothing landed since the previous call so
// the loop can sleep. Any other error is fatal: it tells the loop to
// exit and the recovery callback to fire.
type ReadSamplesFunc func() (headIndex uint64, count int, err error)

// CommitSamplesFunc finishes one arrived run of samples on the local
// writer (OpenSamples + Commit) so consumer FlowReaders see it.
// Production passes a closure over the per-mirror *mxl.Writer.
type CommitSamplesFunc func(headIndex uint64, count int) error
