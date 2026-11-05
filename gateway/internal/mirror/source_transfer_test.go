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

// GetGrain waits, as a blocking read does, until the producer writes idx
// or timeout passes.
func (r *ringSource) GetGrain(idx uint64, timeout time.Duration) (mxl.Grain, error) {
	if idx > r.head() {
		written := r.start.Add(time.Duration(idx-1000) * r.interval)
		time.Sleep(min(time.Until(written), timeout))
		if idx > r.head() {
			return mxl.Grain{}, mxl.ErrTimeout
		}
	}
	if idx+r.ringGrains <= r.head() {
		return mxl.Grain{}, errors.New(readerAgedOutMarker)
	}
	return mxl.Grain{Index: idx, TotalSlices: 1080, ValidSlices: 1080, GrainSize: 5529600}, nil
}

func (r *ringSource) GetGrainSlice(idx uint64, _ uint16, timeout time.Duration) (mxl.Grain, error) {
	return r.GetGrain(idx, timeout)
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
	transfer := newGrainTransfer(src, dst, false, 5120, 0)
	done := make(chan struct{})
	runTransferLoop(ctx, done, "f", func() (uint64, error) { return src.head(), nil }, transfer,
		func() error { return nil }, func() error { return nil }, 2*time.Millisecond, src.interval, &recordingTracker{})
	<-done
	produced := int64(src.head() - first)
	assert.InDelta(t, produced, dst.n.Load(), 2)
}

// flaggedSource holds fixed grains by index, as a producer's ring does.
type flaggedSource map[uint64]mxl.Grain

func (s flaggedSource) GetGrain(idx uint64, _ time.Duration) (mxl.Grain, error) {
	g, ok := s[idx]
	if !ok {
		return mxl.Grain{}, mxl.ErrTimeout
	}
	return g, nil
}

func (s flaggedSource) GetGrainSlice(idx uint64, _ uint16, timeout time.Duration) (mxl.Grain, error) {
	return s.GetGrain(idx, timeout)
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

// An invalid grain goes as its header alone, which the target commits
// invalid: docs/FabricsDeveloperGuide.md, "Transferring discrete flows",
// and runDiscrete in tools/mxl-fabrics-demo/demo.cpp transfer slices 0
// to 0 for it.
func TestGrainTransfer_InvalidGrainGoesAsItsHeader(t *testing.T) {
	for _, tc := range []struct {
		name string
		data bool
		g    mxl.Grain
	}{
		{"video", false, mxl.Grain{Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 1080, ValidSlices: 1080, GrainSize: 5529600}},
		{"data", true, mxl.Grain{Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 4096, ValidSlices: 64, GrainSize: 4096}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &recordingSender{}
			transfer := newGrainTransfer(flaggedSource{1001: tc.g}, dst, tc.data, 1, 0)

			next, bytes, err := transfer(1001, 0)

			require.NoError(t, err)
			assert.Zero(t, next, "the header completes the grain")
			assert.Zero(t, bytes, "an invalid grain carries no payload")
			assert.Equal(t, [][3]uint64{{1001, 0, 0}}, dst.calls)
		})
	}
}

// A valid video grain goes whole, a data grain as its valid slices.
func TestGrainTransfer_SendsTheGrainsSlices(t *testing.T) {
	video := flaggedSource{7: {Index: 7, TotalSlices: 1080, ValidSlices: 1080}}
	dst := &recordingSender{}
	next, bytes, err := newGrainTransfer(video, dst, false, 5120, 0)(7, 0)
	require.NoError(t, err)
	assert.Zero(t, next)
	assert.Equal(t, uint64(1080*5120), bytes)
	assert.Equal(t, [][3]uint64{{7, 0, 1080}}, dst.calls)

	data := flaggedSource{7: {Index: 7, TotalSlices: 4096, ValidSlices: 64}}
	dst = &recordingSender{}
	next, bytes, err = newGrainTransfer(data, dst, true, 1, 270)(7, 0)
	require.NoError(t, err)
	assert.Zero(t, next, "a data grain goes in one transfer")
	assert.Equal(t, uint64(64), bytes)
	assert.Equal(t, [][3]uint64{{7, 0, 64}}, dst.calls)
}

// growingSource is a video grain whose producer commits it in slices:
// each read returns valid more slices than the last, and records the
// valid slices it was asked to wait for.
type growingSource struct {
	valid, step, total uint16
	asked              []uint16
}

func (s *growingSource) GetGrainSlice(idx uint64, minValid uint16, _ time.Duration) (mxl.Grain, error) {
	s.asked = append(s.asked, minValid)
	s.valid = min(s.valid+s.step, s.total)
	return mxl.Grain{Index: idx, TotalSlices: s.total, ValidSlices: s.valid}, nil
}

// A video grain goes in batches of the flow's maxSyncBatchSizeHint as
// its slices become valid, each from where the last ended, as
// runDiscrete in tools/mxl-fabrics-demo/demo.cpp reads up to
// startSlice + slicesPerBatch and transfers [startSlice, validSlices).
func TestGrainTransfer_SendsSliceBatchesAsTheyBecomeValid(t *testing.T) {
	src := &growingSource{step: 300, total: 1080}
	dst := &recordingSender{}
	transfer := newGrainTransfer(src, dst, false, 10, 270)

	var start uint16
	var total uint64
	for i := 0; i < 4; i++ {
		next, bytes, err := transfer(7, start)
		require.NoError(t, err)
		total += bytes
		start = next
		if start == 0 {
			break
		}
	}

	assert.Zero(t, start, "the grain is done once every slice went")
	assert.Equal(t, [][3]uint64{{7, 0, 300}, {7, 300, 600}, {7, 600, 900}, {7, 900, 1080}}, dst.calls)
	assert.Equal(t, []uint16{270, 570, 870, 1170}, src.asked)
	assert.Equal(t, uint64(1080*10), total)
}

// A failed post is told apart from a grain the reader cannot hand out.
func TestGrainTransfer_WrapsAFailedPost(t *testing.T) {
	src := flaggedSource{7: {Index: 7, TotalSlices: 1080, ValidSlices: 1080}}
	_, _, err := newGrainTransfer(src, &recordingSender{err: fabrics.ErrNotReady}, false, 1, 0)(7, 0)
	require.ErrorIs(t, err, errPost)
	require.ErrorIs(t, err, fabrics.ErrNotReady)

	_, _, err = newGrainTransfer(src, &recordingSender{}, false, 1, 0)(8, 0)
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
