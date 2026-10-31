package mirror

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ringSource is a producer at a fixed grain rate with a ring of
// ringGrains: a grain is readable from the tick it is written until
// the producer comes round to its slot again.
type ringSource struct {
	start      time.Time
	interval   time.Duration
	ringGrains uint64
}

func (r *ringSource) head() uint64 { return 1000 + uint64(time.Since(r.start)/r.interval) }

func (r *ringSource) GetGrainNonBlocking(idx uint64) (mxl.Grain, error) {
	h := r.head()
	switch {
	case idx > h:
		return mxl.Grain{}, errors.New("out of range (too early)")
	case idx+r.ringGrains <= h:
		return mxl.Grain{}, errors.New(readerAgedOutMarker)
	}
	return mxl.Grain{Index: idx, TotalSlices: 1080, GrainSize: 5529600}, nil
}

func (r *ringSource) GetGrainSliceNonBlocking(idx uint64, _ uint16) (mxl.Grain, error) {
	return r.GetGrainNonBlocking(idx)
}

// backpressuredSender answers every TransferGrain the way an initiator
// does when one of its targets has a full window: the grain went to
// the other targets, and the call reports not-ready.
type backpressuredSender struct {
	mu    sync.Mutex
	calls map[uint64]int
}

func (s *backpressuredSender) TransferGrain(idx uint64, _, _ uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[idx]++
	return fabrics.ErrNotReady
}

// A target that refuses a grain must cost that target the grain, not
// every other target a duplicate. The loop used to read not-ready as
// "retry this index" and sent the same grain to every target of the
// initiator on every tick while one of them stayed backpressured.
func TestGrainTransfer_BackpressuredTargetCausesNoResend(t *testing.T) {
	src := &ringSource{start: time.Now(), interval: 20 * time.Millisecond, ringGrains: 100}
	dst := &backpressuredSender{calls: map[uint64]int{}}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	transfer := newGrainTransfer(ctx, src, dst, pacer{}, false, func() {})
	done := make(chan struct{})
	runTransferLoop(ctx, done, "f", func() (uint64, error) { return src.head(), nil }, transfer,
		func() error { return nil }, 2*time.Millisecond, nil)
	<-done

	dst.mu.Lock()
	defer dst.mu.Unlock()
	require.NotEmpty(t, dst.calls)
	for idx, n := range dst.calls {
		assert.Equalf(t, 1, n, "grain %d was handed to the initiator %d times", idx, n)
	}
}

func TestInitiatorOptions(t *testing.T) {
	video := newPacer(mxl.Rational{Num: 50, Den: 1}, 0.5, 8, "k")
	require.True(t, video.enabled())

	assert.JSONEq(t, `{"grainsInFlight": 8, "chunksPerGrain": 1}`, initiatorOptions(0, 100, pacer{}, true),
		"the default window, whole grains when pacing is off")
	assert.JSONEq(t, `{"grainsInFlight": 2, "chunksPerGrain": 1}`, initiatorOptions(8, 8, pacer{}, true),
		"a quarter of a small ring: a grain must complete before its slot is written again")
	assert.JSONEq(t, `{"grainsInFlight": 16, "chunksPerGrain": 1}`, initiatorOptions(30, 1000, pacer{}, true),
		"never more than libmxl-fabrics allows")
	assert.JSONEq(t, `{"grainsInFlight": 8, "chunksPerGrain": 8}`, initiatorOptions(8, 100, video, true),
		"a paced video grain is one transfer of pacingChunks calls")
	assert.JSONEq(t, `{"grainsInFlight": 8, "chunksPerGrain": 1}`, initiatorOptions(8, 100, video, false),
		"a data grain goes as one range whatever the pacer does")
}

// offeredCounter counts what the loop hands over, for the throughput
// case below.
type offeredCounter struct{ n atomic.Int64 }

func (c *offeredCounter) TransferGrain(uint64, uint16, uint16) error {
	c.n.Add(1)
	return nil
}

// Whatever happens per target, every grain the producer wrote is
// offered once while the loop keeps up.
func TestGrainTransfer_OffersEveryGrainOnce(t *testing.T) {
	src := &ringSource{start: time.Now(), interval: 10 * time.Millisecond, ringGrains: 100}
	dst := &offeredCounter{}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	first := src.head()
	transfer := newGrainTransfer(ctx, src, dst, pacer{}, false, func() {})
	done := make(chan struct{})
	runTransferLoop(ctx, done, "f", func() (uint64, error) { return src.head(), nil }, transfer,
		func() error { return nil }, 2*time.Millisecond, nil)
	<-done
	produced := int64(src.head() - first)
	assert.InDelta(t, produced, dst.n.Load(), 2)
}

// flaggedSource holds fixed grains by index, as a producer's ring does.
type flaggedSource map[uint64]mxl.Grain

func (s flaggedSource) GetGrainNonBlocking(idx uint64) (mxl.Grain, error) {
	g, ok := s[idx]
	if !ok {
		return mxl.Grain{}, errors.New("out of range (too early)")
	}
	return g, nil
}

func (s flaggedSource) GetGrainSliceNonBlocking(idx uint64, _ uint16) (mxl.Grain, error) {
	return s.GetGrainNonBlocking(idx)
}

// recordingSender records every slice range handed to the initiator.
type recordingSender struct{ calls [][3]uint64 }

func (s *recordingSender) TransferGrain(idx uint64, start, end uint16) error {
	s.calls = append(s.calls, [3]uint64{idx, uint64(start), uint64(end)})
	return nil
}

// A grain its producer committed as invalid carries no picture: libmxl
// flags the slot of an index the producer skipped and leaves the bytes
// an earlier lap wrote there. The target commits every arrival as a
// whole valid grain - OpenGrain clears the flag and the commit marks
// every slice valid - so a forwarded invalid grain reached every reader
// on every target as a valid one carrying that earlier lap's payload.
//
// None of a video grain is sent: the target writer flags the index
// invalid when the next grain commits past it. A data writer never
// completes a grain, so it flags no index it skips; a data grain goes
// as its header alone, which the target commits invalid.
func TestGrainTransfer_InvalidGrainIsNotForwarded(t *testing.T) {
	for _, tc := range []struct {
		name string
		data bool
		g    mxl.Grain
		want [][3]uint64
	}{
		{"video", false, mxl.Grain{Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 1080, ValidSlices: 1080, GrainSize: 5529600}, nil},
		{"data", true, mxl.Grain{Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 4096, ValidSlices: 64, GrainSize: 4096},
			[][3]uint64{{1001, 0, 0}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &recordingSender{}
			transfer := newGrainTransfer(context.Background(), flaggedSource{1001: tc.g}, dst, pacer{}, tc.data, func() {})

			invalid, err := transfer(1001, false)

			require.NoError(t, err)
			assert.True(t, invalid, "an invalid grain must be reported as such")
			assert.Equal(t, tc.want, dst.calls, "what of an invalid grain was handed to the initiator")
		})
	}
}
