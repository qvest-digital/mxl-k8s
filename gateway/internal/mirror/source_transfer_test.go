package mirror

import (
	"context"
	"errors"
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

// offeredCounter counts what the loop hands over.
type offeredCounter struct{ n atomic.Int64 }

func (c *offeredCounter) TransferGrain(uint64, uint16, uint16) error {
	c.n.Add(1)
	return nil
}

// While transfers complete within their grain interval, every grain
// the producer wrote is sent once.
func TestGrainTransfer_SendsEveryGrainOnceWhileKeepingUp(t *testing.T) {
	src := &ringSource{start: time.Now(), interval: 10 * time.Millisecond, ringGrains: 100}
	dst := &offeredCounter{}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	first := src.head()
	transfer := newGrainTransfer(src, dst, false, 5120)
	done := make(chan struct{})
	runTransferLoop(ctx, done, "f", func() (uint64, error) { return src.head(), nil }, transfer,
		func() error { return nil }, func() error { return nil }, 2*time.Millisecond, src.interval, &recordingTracker{})
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
type recordingSender struct {
	calls [][3]uint64
	err   error
}

func (s *recordingSender) TransferGrain(idx uint64, start, end uint16) error {
	s.calls = append(s.calls, [3]uint64{idx, uint64(start), uint64(end)})
	return s.err
}

// A grain its producer committed as invalid carries no picture: libmxl
// flags the slot of an index the producer skipped and leaves the bytes
// an earlier lap wrote there.
//
// None of a video grain is sent: the target writer flags the index
// invalid when the next grain commits past it. A data writer never
// completes a grain, so it flags no index it skips; a data grain goes as
// its header alone, which the target commits invalid.
func TestGrainTransfer_InvalidGrainIsNotForwarded(t *testing.T) {
	for _, tc := range []struct {
		name string
		data bool
		g    mxl.Grain
		want [][3]uint64
		sent bool
	}{
		{"video", false, mxl.Grain{Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 1080, ValidSlices: 1080, GrainSize: 5529600},
			nil, false},
		{"data", true, mxl.Grain{Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 4096, ValidSlices: 64, GrainSize: 4096},
			[][3]uint64{{1001, 0, 0}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &recordingSender{}
			transfer := newGrainTransfer(flaggedSource{1001: tc.g}, dst, tc.data, 1)

			sent, bytes, err := transfer(1001)

			require.NoError(t, err)
			assert.Equal(t, tc.sent, sent)
			assert.Zero(t, bytes, "an invalid grain carries no payload")
			assert.Equal(t, tc.want, dst.calls)
		})
	}
}

// A valid video grain goes whole, a data grain as its valid slices.
func TestGrainTransfer_SendsTheGrainsSlices(t *testing.T) {
	video := flaggedSource{7: {Index: 7, TotalSlices: 1080, ValidSlices: 1080}}
	dst := &recordingSender{}
	sent, bytes, err := newGrainTransfer(video, dst, false, 5120)(7)
	require.NoError(t, err)
	assert.True(t, sent)
	assert.Equal(t, uint64(1080*5120), bytes)
	assert.Equal(t, [][3]uint64{{7, 0, 1080}}, dst.calls)

	data := flaggedSource{7: {Index: 7, TotalSlices: 4096, ValidSlices: 64}}
	dst = &recordingSender{}
	sent, bytes, err = newGrainTransfer(data, dst, true, 1)(7)
	require.NoError(t, err)
	assert.True(t, sent)
	assert.Equal(t, uint64(64), bytes)
	assert.Equal(t, [][3]uint64{{7, 0, 64}}, dst.calls)
}

// A failed post is told apart from a grain the reader cannot hand out.
func TestGrainTransfer_WrapsAFailedPost(t *testing.T) {
	src := flaggedSource{7: {Index: 7, TotalSlices: 1080, ValidSlices: 1080}}
	_, _, err := newGrainTransfer(src, &recordingSender{err: fabrics.ErrNotReady}, false, 1)(7)
	require.ErrorIs(t, err, errPost)
	require.ErrorIs(t, err, fabrics.ErrNotReady)

	_, _, err = newGrainTransfer(src, &recordingSender{}, false, 1)(8)
	require.Error(t, err)
	assert.NotErrorIs(t, err, errPost)
}

// One sample batch can take a write per channel and wrap, so the
// completion queue is sized for it; a grain's writes fit the default.
func TestInitiatorOptions(t *testing.T) {
	audio := mxl.FlowConfig{Common: mxl.CommonFlowConfig{Format: mxl.FormatAudio},
		Continuous: mxl.ContinuousFlowConfig{ChannelCount: 16}}
	assert.JSONEq(t, `{"cqDepth": 33}`, initiatorOptions(audio))
	assert.Empty(t, initiatorOptions(mxl.FlowConfig{Common: mxl.CommonFlowConfig{Format: mxl.FormatVideo}}))
}

func TestSliceBytes(t *testing.T) {
	cfg := mxl.FlowConfig{Discrete: mxl.DiscreteFlowConfig{SliceSizes: [4]uint32{3840, 1920, 1920, 0}}}
	assert.Equal(t, uint64(7680), sliceBytes(cfg))
}

func TestGrainInterval(t *testing.T) {
	assert.Equal(t, 20*time.Millisecond, grainInterval(mxl.Rational{Num: 50, Den: 1}))
	assert.Equal(t, 33366666*time.Nanosecond, grainInterval(mxl.Rational{Num: 30000, Den: 1001}))
	assert.Zero(t, grainInterval(mxl.Rational{}))
}
