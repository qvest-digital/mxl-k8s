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

// An invalid video grain is not posted, although
// docs/FabricsDeveloperGuide.md and runDiscrete in
// tools/mxl-fabrics-demo/demo.cpp send its header: back-to-back
// header-only writes outran the one receive a verbs target has posted.
// An invalid data grain goes as its header, which the target commits
// invalid.
func TestGrainTransfer_InvalidVideoGrainIsNotPosted(t *testing.T) {
	video := flaggedSource{1001: {Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 1080, ValidSlices: 1080}}
	dst := &recordingSender{}
	_, bytes, err := newGrainTransfer(video, dst, false, 1, 0)(1001, 0)
	require.ErrorIs(t, err, errInvalidGrain)
	assert.Zero(t, bytes)
	assert.Empty(t, dst.calls, "an invalid video grain was posted")

	data := flaggedSource{1001: {Index: 1001, Flags: mxl.GrainFlagInvalid, TotalSlices: 4096, ValidSlices: 64}}
	dst = &recordingSender{}
	next, bytes, err := newGrainTransfer(data, dst, true, 1, 0)(1001, 0)
	require.NoError(t, err)
	assert.Zero(t, next)
	assert.Zero(t, bytes, "an invalid grain carries no payload")
	assert.Equal(t, [][3]uint64{{1001, 0, 0}}, dst.calls)
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

// A sample batch is the source flow's sync hint, as in runContinuous in
// tools/mxl-fabrics-demo/demo.cpp, but no more than the target's
// bounce-buffer entry holds: one batch of the target flow's sync hint,
// or of libmxl's 10 ms default for a target that publishes none.
// libmxl-fabrics rejects a larger batch for good.
func TestSampleBatch_FitsTheTargetsEntry(t *testing.T) {
	rate := mxl.Rational{Num: 48000, Den: 1}
	assert.Equal(t, uint64(960), sampleBatch(960, 960, rate), "a target created with the source's hints takes its batch")
	assert.Equal(t, uint64(480), sampleBatch(960, 480, rate), "a smaller target entry caps the batch")
	assert.Equal(t, uint64(240), sampleBatch(240, 960, rate), "a source hint below the entry is kept")
	assert.Equal(t, uint64(480), sampleBatch(960, 0, rate), "a target that publishes no hint holds 10 ms")
	assert.Equal(t, uint64(240), sampleBatch(240, 0, rate))
	assert.Equal(t, uint64(480), sampleBatch(0, 0, rate))
	assert.Equal(t, uint64(1), sampleBatch(0, 0, mxl.Rational{}))
}

// A mirror's batch, the time it covers and the head poll follow its own
// target: two mirrors of one flow can have targets of different entries.
func TestSampleTiming_FollowsTheTarget(t *testing.T) {
	s := &sharedSource{
		cfg: mxl.FlowConfig{Common: mxl.CommonFlowConfig{
			Format: mxl.FormatAudio, GrainRate: mxl.Rational{Num: 48000, Den: 1}, MaxSyncBatchSizeHint: 960,
		}},
		interval: grainInterval(mxl.Rational{Num: 48000, Den: 1}),
		maxBatch: 1 << 20,
	}
	batch, interval, poll := s.sampleTiming(960)
	assert.Equal(t, uint64(960), batch)
	assert.Equal(t, 960*s.interval, interval)
	assert.Equal(t, 10*time.Millisecond, poll)

	batch, interval, poll = s.sampleTiming(0)
	assert.Equal(t, uint64(480), batch)
	assert.Equal(t, 480*s.interval, interval)
	assert.Equal(t, 5*time.Millisecond, poll)

	s.maxBatch = 256
	batch, _, _ = s.sampleTiming(960)
	assert.Equal(t, uint64(256), batch, "no batch is longer than the reader hands out")
}
