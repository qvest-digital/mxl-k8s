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

// A mirror's transfer loop posts one transfer and calls MakeProgress
// until it stops returning not-ready, as libmxl-fabrics requires before
// the transfer counts as complete. Nothing else is posted meanwhile:
// grains and samples the producer commits while a transfer is
// outstanding wait in the ring, and once one transfer missed its
// deadline the loop skips them and resumes at the live head.

// progressTimeout is how long one blocking MakeProgress call waits.
const progressTimeout = 10 * time.Millisecond

// abandonAfter is how long a transfer may stay outstanding before the
// target is treated as lost and added again.
const abandonAfter = 2 * time.Second

// droppedAfterInterrupts is how many consecutive interrupted
// MakeProgress answers mean the initiator holds no target any more.
// libmxl-fabrics answers interrupted for a signal and, on a
// connection-oriented initiator, for having dropped its last target;
// only the second repeats.
const droppedAfterInterrupts = 3

// errTargetLost reports a transfer whose target cannot be reached
// through this initiator any more.
var errTargetLost = errors.New("target lost")

// errPost wraps a failed TransferGrain or TransferSamples call, so the
// loop tells it apart from a grain the reader cannot hand out yet.
var errPost = errors.New("post transfer")

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

// newGrainTransfer is the TransferFunc of a discrete (video or data)
// flow. sliceBytes is the payload of one slice across all planes.
func newGrainTransfer(src grainSource, dst grainSender, data bool, sliceBytes uint64) TransferFunc {
	post := func(idx uint64, start, end uint16) (bool, uint64, error) {
		if err := dst.TransferGrain(idx, start, end); err != nil {
			return false, 0, fmt.Errorf("%w: %w", errPost, err)
		}
		return true, uint64(end-start) * sliceBytes, nil
	}
	return func(idx uint64) (bool, uint64, error) {
		grain, err := readForTransfer(src, idx, data)
		if err != nil {
			return false, 0, err
		}
		if grain.Invalid() {
			// libmxl left an earlier lap's bytes in the slot. A video
			// target writer flags the index invalid once the next grain
			// commits past it, so nothing is sent. A data writer never
			// completes a grain and flags no index it skips, so the
			// header goes over and the target commits it invalid.
			if data {
				return post(idx, 0, 0)
			}
			return false, 0, nil
		}
		if data {
			// A data grain's valid slices are its length.
			return post(idx, 0, grain.ValidSlices)
		}
		return post(idx, 0, grain.TotalSlices)
	}
}

// readForTransfer reads grain idx as the transfer loop sends it. A data
// grain is read once committed at all, with no valid slice too: a writer
// commits an empty one like any other, and a read that waited for a
// slice would retry it until the loop resynced past it, dropping the
// grains behind it as well. A range of no slices still carries the
// grain's header, which is what the target commits from.
func readForTransfer(r grainSource, idx uint64, data bool) (mxl.Grain, error) {
	if data {
		return r.GetGrainSliceNonBlocking(idx, 0)
	}
	return r.GetGrainNonBlocking(idx)
}

// sliceBytes is the payload of one slice of a discrete flow, summed
// over its planes.
func sliceBytes(cfg mxl.FlowConfig) uint64 {
	var n uint64
	for _, s := range cfg.Discrete.SliceSizes {
		n += uint64(s)
	}
	return n
}

// grainInterval is one grain of rate, or zero for an unusable rate.
func grainInterval(rate mxl.Rational) time.Duration {
	if rate.Num <= 0 || rate.Den <= 0 {
		return 0
	}
	return time.Duration(int64(time.Second) * rate.Den / rate.Num)
}

// initiatorOptions sizes the completion queue of a mirror's initiator
// for the writes one transfer takes. A sample batch is a scatter list
// of up to two entries per channel, where it wraps the ring, plus its
// header, and a provider may post a write per entry. A grain is a
// write per plane, which the default depth holds.
func initiatorOptions(cfg mxl.FlowConfig) string {
	if cfg.Common.Format != mxl.FormatAudio {
		return ""
	}
	return fmt.Sprintf(`{"cqDepth": %d}`, 2*cfg.Continuous.ChannelCount+1)
}

// progressDriver is the part of fabrics.Initiator that drives its
// queues.
type progressDriver interface {
	MakeProgress(timeout time.Duration) error
	MakeProgressNonBlocking() error
}

// progressFuncFor drives the initiator the way its interface allows:
// blocking where it reports blocking operations, polling elsewhere.
func progressFuncFor(in progressDriver, caps fabrics.InterfaceCapFlags) ProgressFunc {
	if caps&fabrics.InterfaceCapBlockingOperations == 0 {
		return in.MakeProgressNonBlocking
	}
	return func() error { return in.MakeProgress(progressTimeout) }
}

// awaitTransfer calls progress until the outstanding transfer
// completes, and reports whether that took longer than deadline. It
// returns errTargetLost when the target dropped, the initiator failed
// or the transfer stayed outstanding past abandonAfter, and ctx.Err()
// once ctx is done.
func awaitTransfer(ctx context.Context, progress ProgressFunc, posted time.Time, deadline time.Duration) (late bool, err error) {
	interrupts := 0
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		perr := progress()
		elapsed := time.Since(posted)
		switch kind := classifyFabricError(perr); {
		case perr == nil:
			return deadline > 0 && elapsed > deadline, nil
		case kind == fabricIdle:
			interrupts = 0
			if elapsed > abandonAfter {
				return true, fmt.Errorf("%w: transfer outstanding for %s", errTargetLost, elapsed.Round(time.Millisecond))
			}
		case kind == fabricTransient:
			interrupts++
			if interrupts >= droppedAfterInterrupts {
				return true, fmt.Errorf("%w: %w", errTargetLost, perr)
			}
		default:
			return true, fmt.Errorf("%w: %w", errTargetLost, perr)
		}
	}
}

// wait blocks for d or until ctx is done, and reports whether ctx is
// still live.
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// retryablePost reports whether a failed post leaves the target usable:
// the target not ready for it yet, or the call disturbed.
func retryablePost(err error) bool {
	switch classifyFabricError(err) {
	case fabricIdle, fabricTransient:
		return true
	}
	return false
}

// runTransferLoop transfers the grains of a discrete flow to one
// mirror's target until ctx is canceled or the target is lost. Closes
// done on exit.
//
// poll is how long the loop waits for the producer when it has nothing
// new, interval one grain of the flow and the deadline of each
// transfer.
func runTransferLoop(
	ctx context.Context,
	done chan struct{},
	flowID string,
	probeRuntime RuntimeProbe,
	transferGrain TransferFunc,
	makeProgress ProgressFunc,
	poll, interval time.Duration,
	tracker progressTracker,
) {
	defer close(done)

	l := ctrl.Log.WithName("transfer").WithValues("flowID", flowID)

	// Tail the live flow rather than replay grains the producer may
	// already have aged out of the ring.
	head0, err := probeRuntime()
	if err != nil {
		l.Error(err, "initial Runtime")
		return
	}
	lastSent := int64(head0)
	tracker.recordHead(head0, time.Now())

	resync := false
	for ctx.Err() == nil {
		head, err := probeRuntime()
		if err != nil {
			l.Error(err, "Runtime")
			wait(ctx, poll)
			continue
		}
		tracker.recordHead(head, time.Now())
		h := int64(head)
		if h <= lastSent {
			wait(ctx, poll)
			continue
		}

		next := lastSent + 1
		switch {
		case resync:
			// The last transfer missed its deadline: what the producer
			// committed meanwhile is skipped, not queued.
			tracker.recordPassedOver(unsentResync, uint64(h-next))
			next = h
		case h > maxMirrorLag && lastSent < h-maxMirrorLag:
			// The first probe after attach can report head 0, and a
			// loop held up long enough trails grains the producer has
			// rewritten. Either way only the live head is still sure
			// to be readable.
			if lastSent > 0 {
				l.V(1).Info("mirror fell behind the producer, resyncing to head",
					"lastSent", lastSent, "head", h)
				tracker.recordAgedOut(time.Now())
				tracker.recordPassedOver(unsentResync, uint64(h-next))
			}
			next = h
		}
		resync = false

		sent, bytes, err := transferGrain(uint64(next))
		switch {
		case err == nil:
		case isReaderAgedOut(err):
			l.Info("reader aged out, skipping grains", "fromIndex", next, "toIndex", h)
			tracker.recordAgedOut(time.Now())
			tracker.recordPassedOver(unsentAgedOut, uint64(h-next+1))
			lastSent = h
			continue
		case errors.Is(err, errPost) && retryablePost(err):
			// Nothing was posted. Progress is what moves a connecting
			// target on; the grain is offered again.
			_ = makeProgress()
			wait(ctx, poll)
			continue
		case errors.Is(err, errPost):
			l.Error(err, "TransferGrain", "class", classifyFabricError(err).String(), "index", next)
			tracker.recordDropped(false)
			return
		default:
			// Not committed yet; it is read again once the head moves.
			l.V(2).Info("grain not transferable yet", "index", next, "head", h, "error", err.Error())
			wait(ctx, poll)
			continue
		}
		if !sent {
			tracker.recordInvalid(time.Now())
			lastSent = next
			continue
		}

		posted := time.Now()
		tracker.recordOffered(posted)
		late, err := awaitTransfer(ctx, makeProgress, posted, interval)
		if err != nil {
			if ctx.Err() == nil {
				l.Info("lost the mirror target", "index", next, "error", err.Error())
				tracker.recordDropped(true)
			}
			return
		}
		tracker.recordCompleted(uint64(next), bytes, late, time.Now())
		lastSent = next
		resync = late
	}
}

// runSampleTransferLoop transfers the samples of a continuous (audio)
// flow to one mirror's target until ctx is canceled or the target is
// lost. Closes done on exit.
//
// One transfer carries at most xferBatch samples, which caps it below
// the target's bounce-buffer entry; interval is the time one batch
// spans and the deadline of each transfer. Samples are abandoned only
// once they have left the readable window, maxReadable, or after a
// transfer missed its deadline. A skipped range is not a gap the target
// can see: the next transfer commits at its own index, so the target's
// head moves over the range and republishes stale ring content there.
func runSampleTransferLoop(
	ctx context.Context,
	done chan struct{},
	flowID string,
	probeRuntime RuntimeProbe,
	transferSamples SampleTransferFunc,
	makeProgress ProgressFunc,
	xferBatch uint64,
	maxReadable uint64,
	poll, interval time.Duration,
	tracker progressTracker,
) {
	defer close(done)

	l := ctrl.Log.WithName("sample-transfer").WithValues("flowID", flowID)

	agedOut := maxReadable
	if agedOut > xferBatch {
		agedOut -= xferBatch
	}

	head0, err := probeRuntime()
	if err != nil {
		l.Error(err, "initial Runtime")
		return
	}
	lastSent := head0
	tracker.recordHead(head0, time.Now())

	resync := false
	for ctx.Err() == nil {
		head, err := probeRuntime()
		if err != nil {
			l.Error(err, "Runtime")
			wait(ctx, poll)
			continue
		}
		tracker.recordHead(head, time.Now())
		if head <= lastSent {
			wait(ctx, poll)
			continue
		}

		switch {
		case resync && head-lastSent > xferBatch:
			tracker.recordSkipped(head - xferBatch - lastSent)
			lastSent = head - xferBatch
		case agedOut > 0 && head-lastSent > agedOut:
			l.Info("reader fell behind writer, skipping samples",
				"fromIndex", lastSent+1, "toIndex", head)
			tracker.recordAgedOut(time.Now())
			tracker.recordSkipped(head - agedOut - lastSent)
			lastSent = head - agedOut
		}
		resync = false

		count := min(head-lastSent, xferBatch)
		end := lastSent + count
		bytes, err := transferSamples(end, int(count))
		switch {
		case err == nil:
		case retryablePost(err):
			_ = makeProgress()
			wait(ctx, poll)
			continue
		default:
			l.Error(err, "TransferSamples", "class", classifyFabricError(err).String(),
				"headIndex", end, "count", count)
			tracker.recordDropped(false)
			return
		}

		posted := time.Now()
		tracker.recordOffered(posted)
		late, err := awaitTransfer(ctx, makeProgress, posted, interval)
		if err != nil {
			if ctx.Err() == nil {
				l.Info("lost the mirror target", "headIndex", end, "error", err.Error())
				tracker.recordDropped(true)
			}
			return
		}
		tracker.recordCompleted(end, bytes, late, time.Now())
		lastSent = end
		resync = late
	}
}
