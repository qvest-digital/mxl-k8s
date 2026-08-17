package mirror

import (
	"context"
	"hash/fnv"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
)

// Grain transmission is bursty by construction. TransferGrain hands the
// fabric a whole grain, and libmxl-fabrics turns that into one RMA write
// per plane, so the NIC drains it at line rate however slow the flow
// itself is. A 5.53 MB grain leaves a 25 GbE port in 1.77 ms; at 50 fps
// it then sits idle for the remaining 18 ms of the grain interval. The
// flow's average rate and its instantaneous rate differ by the ratio of
// link speed to flow rate.
//
// That is invisible until several flows converge on one receiver. Their
// bursts overlap, the switch has to absorb the excess, and loss appears
// far below nominal link utilisation - rising with the probability of
// overlap rather than with offered load. Link aggregation cannot help:
// a single flow never splits across members.
//
// Grain indices are derived from the MXL clock (TAI, ST 2059 epoch), so
// flows sharing an edit rate advance their heads on the same instants
// across every producer and every gateway. The bursts are therefore not
// merely likely to overlap, they are phase-aligned by construction.
//
// The pacer spreads one grain's slices across a fraction of the grain
// interval instead. Peak rate falls from line rate to the flow's own
// rate divided by that fraction, which needs no knowledge of the link
// speed - only the flow's edit rate, which the flow itself carries.
// A per-flow phase offset then staggers the chunk boundaries so aligned
// flows do not step in time with one another.
//
// None of that runs unless an operator turns it on. The burst is only
// worth shaping where a receiver loses grains to it, and where nothing
// is lost the added latency and the per-chunk cgo call and RMA writes
// buy nothing.

const (
	// defaultPacingFraction disables pacing, which is what a gateway
	// does unless an operator asks for it. Shaping only pays where a
	// receiver actually loses grains to overlapping bursts, and a
	// fabric that loses none gets the latency and the per-chunk cost
	// for nothing. Half a grain interval is where to start when
	// enabling it: it leaves the rest of the interval as headroom for
	// a late-committing producer and for catch-up, and caps peak rate
	// at twice the flow's own rate.
	defaultPacingFraction = -1

	// defaultPacingChunks is how many slice ranges one grain is split
	// into. Each chunk is a cgo call under the initiator mutex, and a
	// partial range costs one RMA write per plane where a whole grain
	// costs one in total, so this trades a bounded call-count increase
	// for the burst-length reduction. Eight shortens the contiguous
	// burst by 8x, which is the term switch buffer occupancy depends on.
	defaultPacingChunks = 8

	// phaseBuckets quantises the per-flow phase offset. Finer than the
	// number of flows a node carries, coarse enough to stay legible in
	// a log line.
	phaseBuckets = 64
)

// sliceChunk is one slice range of a grain and how long after the start
// of that grain's transmission it is due to be handed to the fabric.
type sliceChunk struct {
	start uint16
	end   uint16
	due   time.Duration
}

// pacer turns a grain's slice count into a release schedule. The zero
// value paces nothing and hands every grain over whole, which is the
// pre-pacing behaviour.
type pacer struct {
	// window is how long one grain's transmission is spread over.
	// Zero disables pacing.
	window time.Duration
	// chunks is how many slice ranges the window is divided into.
	chunks int
	// offset staggers this flow against others sharing its edit rate.
	offset time.Duration
}

// newPacer builds the schedule for a flow from its own edit rate.
//
// fraction <= 0, chunks < 2, or an unusable rate all yield a disabled
// pacer rather than an error: pacing is an optimisation, and a flow
// whose rate libmxl does not report is still worth mirroring unpaced.
// key identifies the flow for the phase offset; mirrors of different
// flows, and of one flow to different targets, must not share it.
func newPacer(rate mxl.Rational, fraction float64, chunks int, key string) pacer {
	if fraction <= 0 || chunks < 2 || rate.Num <= 0 || rate.Den <= 0 {
		return pacer{}
	}
	// Edit rate is grains per second as Num/Den, so one grain interval
	// is Den/Num seconds. Computed in nanoseconds to keep the integer
	// division exact for the rates in use (50, 25, 30000/1001, ...).
	interval := time.Duration(int64(time.Second) * rate.Den / rate.Num)
	if interval <= 0 {
		return pacer{}
	}
	window := time.Duration(float64(interval) * fraction)
	if window <= 0 {
		return pacer{}
	}
	chunkPeriod := window / time.Duration(chunks)
	if chunkPeriod <= 0 {
		return pacer{}
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	offset := chunkPeriod * time.Duration(h.Sum32()%phaseBuckets) / phaseBuckets

	return pacer{window: window, chunks: chunks, offset: offset}
}

// enabled reports whether this pacer spreads anything.
func (p pacer) enabled() bool { return p.window > 0 && p.chunks > 1 }

// plan divides total slices into the ranges to release and when each is
// due, measured from the moment transmission of the grain starts.
//
// A grain with one slice, or a disabled pacer, yields the single whole
// range at due 0 - which is the same call the unpaced path makes, and
// so takes libmxl-fabrics's one-write-per-grain fast path rather than
// its per-plane partial-write path.
func (p pacer) plan(total uint16) []sliceChunk {
	if !p.enabled() || total <= 1 {
		return []sliceChunk{{start: 0, end: total, due: 0}}
	}

	n := p.chunks
	if n > int(total) {
		n = int(total)
	}
	chunkPeriod := p.window / time.Duration(n)

	out := make([]sliceChunk, 0, n)
	// Ceiling division so the ranges cover every slice; the last chunk
	// absorbs the remainder and is the one carrying the completing
	// valid-slice count to the target.
	per := (int(total) + n - 1) / n
	for i, start := 0, 0; start < int(total); i, start = i+1, start+per {
		end := start + per
		if end > int(total) {
			end = int(total)
		}
		out = append(out, sliceChunk{
			start: uint16(start),
			end:   uint16(end),
			due:   p.offset + time.Duration(i)*chunkPeriod,
		})
	}
	return out
}

// pacerClock is the pacer's view of time. Injected so a schedule can be
// asserted without spending it.
type pacerClock interface {
	now() time.Time
	// sleepUntil waits until t, or returns ctx.Err() if the context is
	// canceled first. A t already in the past returns immediately.
	sleepUntil(ctx context.Context, t time.Time) error
}

// realClock is the production pacerClock.
type realClock struct{}

func (realClock) now() time.Time { return time.Now() }

func (realClock) sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// transferPaced hands grain idx to the fabric as the ranges plan
// dictates, waiting out each chunk's due time first.
//
// Deadlines are absolute against a single start instant so a slow chunk
// does not push the ones after it: the schedule is a plan for the whole
// grain, not a chain of relative sleeps whose error accumulates.
//
// betweenChunks is called after every chunk but the last, and is where
// production drives MakeProgress so queued writes drain while the next
// chunk is still waiting. Its failures belong to the caller's log, not
// to the transfer's result.
func transferPaced(
	ctx context.Context,
	clk pacerClock,
	plan []sliceChunk,
	idx uint64,
	transfer TransferSlicesFunc,
	betweenChunks func(),
) error {
	start := clk.now()
	for i, c := range plan {
		if c.due > 0 {
			if err := clk.sleepUntil(ctx, start.Add(c.due)); err != nil {
				return err
			}
		}
		if err := transfer(idx, c.start, c.end); err != nil {
			return err
		}
		if i < len(plan)-1 && betweenChunks != nil {
			betweenChunks()
		}
	}
	return nil
}
