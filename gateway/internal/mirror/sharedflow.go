package mirror

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

// siblingCommitFreshness bounds how long ago the flow's last commit may
// lie for an arrival that finds its grain or range already committed to
// count as progress. A sibling mirror that is delivering commits every
// grain, so it is far inside this; a flow nobody has advanced for longer
// is one whose arrivals are stale, and those must not keep a wedged
// mirror reading as healthy.
const siblingCommitFreshness = 2 * time.Second

// sharedFlow is what every target entry writing one flow in one libmxl
// instance shares. libmxl hands every writer of a flow in an instance
// the same underlying writer, so two mirrors of a flow into this node --
// one per consumer namespace -- drive one writer whose state is not safe
// for concurrent use, and go-mxl's lock is per Writer. Commits into the
// flow therefore go through here.
type sharedFlow struct {
	mu sync.Mutex
	// lastCommitAt is when any entry last committed to the flow.
	lastCommitAt time.Time
	// lastHead is the head index of the last sample run committed, the
	// value libmxl rejects a run against; haveHead is false until one is.
	lastHead uint64
	haveHead bool
}

// sharedFlowKey names one flow in one libmxl instance: the scope libmxl
// shares a writer across.
type sharedFlowKey struct {
	handles *instance.Handles
	flowID  [16]byte
}

// sharedFlowFor returns the state every target entry writing flowID
// through handles shares. Entries are never removed, so there is one
// per flow this node has received since the gateway started.
func (r *TargetReconciler) sharedFlowFor(handles *instance.Handles, flowID [16]byte) *sharedFlow {
	f, _ := r.sharedFlows.LoadOrStore(sharedFlowKey{handles: handles, flowID: flowID}, &sharedFlow{})
	return f.(*sharedFlow)
}

// grainInfo reads a grain's header under the flow's lock.
func (f *sharedFlow) grainInfo(writer *mxl.Writer, idx uint64) (mxl.Grain, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return writer.GrainInfo(idx)
}

// commitGrain commits an arrived grain. An arrival whose grain is
// already committed returns errGrainAlreadyCommitted and is recorded on
// tracker against the flow's last commit, when that is fresh: the grain
// arrived, and a mirror that always loses the commit to its sibling
// would otherwise read as receiving nothing and be rebuilt.
func (f *sharedFlow) commitGrain(writer *mxl.Writer, idx uint64, tracker commitTracker) (uint64, error) {
	f.mu.Lock()
	n, err := commitArrivedGrain(writer, idx)
	now := time.Now()
	if err == nil {
		f.lastCommitAt = now
	}
	last := f.lastCommitAt
	f.mu.Unlock()
	if errors.Is(err, errGrainAlreadyCommitted) {
		f.recordSibling(tracker, idx, last, now)
	}
	return n, err
}

// commitSamples commits an arrived sample run. A run libmxl rejects
// while it does not advance past the last committed head -- the source
// retransmitted or overlapped it, or a sibling committed it first --
// returns errSamplesAlreadyCommitted and is recorded as commitGrain
// records a grain. libmxl stays the authority on rejection: the tracked
// head only labels one, because it outlives the writer, and a writer
// reopened after the source rewound accepts what it would refuse.
func (f *sharedFlow) commitSamples(writer *mxl.Writer, head uint64, count int, tracker commitTracker) (uint64, error) {
	f.mu.Lock()
	n, err := commitArrivedSamples(writer, head, count)
	now := time.Now()
	if err == nil {
		f.lastCommitAt = now
		f.lastHead = head
		f.haveHead = true
		f.mu.Unlock()
		return n, nil
	}
	behind := f.haveHead && (head <= f.lastHead || head-uint64(count) < f.lastHead)
	last := f.lastCommitAt
	f.mu.Unlock()
	if !errors.Is(err, mxl.ErrInvalidArg) || !behind {
		return 0, err
	}
	f.recordSibling(tracker, head, last, now)
	return 0, fmt.Errorf("%w: %w", errSamplesAlreadyCommitted, err)
}

func (f *sharedFlow) recordSibling(tracker commitTracker, idx uint64, last, now time.Time) {
	if tracker == nil || last.IsZero() || now.Sub(last) > siblingCommitFreshness {
		return
	}
	tracker.recordCommit(idx, last)
}
