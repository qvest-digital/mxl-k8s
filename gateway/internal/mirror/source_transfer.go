package mirror

import (
	"context"
	"errors"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// grainSource is the part of mxl.Reader the grain transfer reads.
type grainSource interface {
	GetGrainNonBlocking(index uint64) (mxl.Grain, error)
	GetGrainSliceNonBlocking(index uint64, minValidSlices uint16) (mxl.Grain, error)
}

// grainSender is the part of fabrics.Initiator the grain transfer
// writes to.
type grainSender interface {
	TransferGrain(grainIdx uint64, startSlice, endSlice uint16) error
}

// progressDriver is the part of fabrics.Initiator that drives its
// event and completion queues.
type progressDriver interface {
	MakeProgress(timeout time.Duration) error
	MakeProgressNonBlocking() error
}

// grainInitiator is the part of fabrics.Initiator a discrete flow's
// transfer drives.
type grainInitiator interface {
	grainSender
	progressDriver
}

// offered folds the answer of one TransferGrain call into whether the
// grain counts as handed over.
//
// libmxl-fabrics offers a grain to every target of the initiator and
// skips it, counted, for a target whose window or send queue is full.
// A not-ready answer therefore says some target skipped the grain, not
// that none got it. Treating it as "retry this index" sent the same
// grain again to every target that had already taken it, once per tick,
// for as long as one target stayed backpressured.
func offered(err error) error {
	if errors.Is(err, fabrics.ErrNotReady) {
		return nil
	}
	return err
}

// newGrainTransfer is the TransferFunc of a discrete (video or data)
// flow: read grain idx, then hand it to the initiator whole, or in the
// pacer's chunks when it is the live head and pacing is on.
func newGrainTransfer(ctx context.Context, src grainSource, dst grainSender, p pacer, data bool, betweenChunks func()) TransferFunc {
	slices := func(idx uint64, start, end uint16) error {
		return offered(dst.TransferGrain(idx, start, end))
	}
	return func(idx uint64, paced bool) (bool, error) {
		grain, err := readForTransfer(src, idx, data)
		if err != nil {
			// Not yet committed or already aged out; the loop breaks and
			// tries again on the next tick.
			return false, err
		}
		if grain.Invalid() {
			// The producer committed it invalid, and libmxl left an
			// earlier lap's bytes in the slot. A video target writer
			// flags the index invalid once the next grain commits past
			// it, so none of it is sent. A data writer never completes
			// a grain, so it flags no index it skips: the header goes
			// over, which the target commits invalid, and the grain is
			// offered like any other.
			if data {
				return false, slices(idx, 0, 0)
			}
			return true, nil
		}
		if data {
			// A data grain is never complete: its valid slices are its
			// length. Only what is valid goes over, so the target
			// commits the same length.
			return false, slices(idx, 0, grain.ValidSlices)
		}
		if grain.TotalSlices == 0 {
			// A discrete grain with no slice subdivision; continuous
			// (audio) flows take the sample path and never reach here.
			return false, nil
		}
		if !paced || !p.enabled() {
			return false, slices(idx, 0, grain.TotalSlices)
		}
		if err := transferPaced(ctx, realClock{}, p.plan(grain.TotalSlices), idx, slices, betweenChunks); err != nil {
			return false, fmt.Errorf("paced transfer of grain %d: %w", idx, err)
		}
		return false, nil
	}
}

// newInitiatorGrainTransfer is newGrainTransfer over a flow's
// initiator, reaping its queues between the chunks of a paced grain.
//
// The reap does not block, whatever the provider. Only a grain's last
// chunk reports a completion, so between its chunks the completion
// queue is empty and a blocking call parks for its whole timeout: 10 ms
// a chunk, a grain paced over 10 ms taking 150 ms in 16 chunks. The
// transfer loop's own call after each tick still blocks.
func newInitiatorGrainTransfer(ctx context.Context, src grainSource, in grainInitiator, p pacer, data bool, flowID string) TransferFunc {
	l := ctrl.Log.WithName("transfer").WithValues("flowID", flowID)
	progress := reapFuncFor(in)
	// Its own throttle rather than the transfer loop's: this call
	// site fires once per chunk instead of once per tick, so it
	// needs its own failure history. Both only ever run on the
	// transfer goroutine, so neither needs a lock.
	var chunkProgress progressThrottle
	betweenChunks := func() {
		chunkProgress.runProgress(progress, func(err error) { logProgressFailure(l, err) })
	}
	return newGrainTransfer(ctx, src, in, p, data, betweenChunks)
}

// defaultGrainsInFlight is how many grains one target may have posted
// and not yet completed. A grain offered to a target that already has
// this many in flight is skipped for that target alone.
const defaultGrainsInFlight = 8

// initiatorOptions is the libmxl-fabrics setup options string of a
// flow's initiator.
//
// The window is capped at a quarter of the source ring. That bounds
// how many grains are in flight, not how long one takes: the provider
// reads a grain's payload while it transmits, retransmissions included,
// so a grain still retrying when the producer comes round to its slot
// again goes out with the newer content. libmxl-fabrics counts such a
// grain as torn, which the gateway publishes. Pacing splits a video grain into chunks, and
// the initiator has to know that to signal and count the grain once;
// a data grain is sent as one range whatever the pacer does.
func initiatorOptions(grainsInFlight int, ringGrains uint64, p pacer, video bool) string {
	window := grainsInFlight
	if window <= 0 {
		window = defaultGrainsInFlight
	}
	if limit := int(ringGrains / 4); ringGrains > 0 && window > limit {
		window = max(limit, 1)
	}
	window = min(window, fabrics.MaxTransfersInFlight)
	chunks := 1
	if video && p.enabled() {
		chunks = p.chunks
	}
	return fmt.Sprintf(`{"grainsInFlight": %d, "chunksPerGrain": %d}`, window, chunks)
}
