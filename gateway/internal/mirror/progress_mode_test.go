package mirror

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// progressBlocking partitions the fabric providers by whether their
// completion queue needs a blocking MakeProgress call. EFA drains its
// CQ on the provider's own event queue, so a non-blocking poll is
// correct. The verbs provider does not, and a non-blocking poll returns
// before the NIC finishes the DMA -- so the send queue fills and the
// next transfer call returns EAGAIN. TCP and SHM are loopback, where
// blocking is harmless but buys nothing; the selection keeps them on
// the blocking path because the cost is one short sleep and the
// benefit is a drained CQ on every provider that is not EFA.
func TestProgressBlocking_PartitionsEFAFromEverythingElse(t *testing.T) {
	assert.False(t, progressBlocking(fabrics.ProviderEFA),
		"EFA drains its completion queue on the provider's event queue; a blocking call adds latency without improving throughput")
	for _, p := range []fabrics.Provider{
		fabrics.ProviderVerbs,
		fabrics.ProviderTCP,
		fabrics.ProviderSHM,
		fabrics.ProviderAny,
	} {
		assert.True(t, progressBlocking(p),
			"provider %s needs a blocking MakeProgress so the CQ drains before the next transfer", p)
	}
}

func TestDefaultSampleProgressInterval_48kHz(t *testing.T) {
	// 48000/1 with a 480-sample batch spans 10 ms, and the loop wakes
	// twice within it.
	got := defaultSampleProgressInterval(mxl.Rational{Num: 48000, Den: 1}, 480)
	assert.Equal(t, 5*time.Millisecond, got,
		"48 kHz with a 480-sample batch should yield a 5 ms interval")
}

// A tick period equal to the batch period lets the two clocks drift
// against each other, so one tick finds nothing and the next finds two
// batches: every sample arrives, but in pairs at twice the interval,
// which a reader sees as gaps. Measured across EFA that was 89.7
// advances/s against a producer committing 100/s, with the 90th
// percentile advance carrying two batches rather than one.
func TestDefaultSampleProgressInterval_OversamplesTheCommitCadence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rate  mxl.Rational
		batch uint64
	}{
		{"48kHz/480", mxl.Rational{Num: 48000, Den: 1}, 480},
		{"48kHz/1024", mxl.Rational{Num: 48000, Den: 1}, 1024},
		{"96kHz/480", mxl.Rational{Num: 96000, Den: 1}, 480},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batchSpan := time.Duration(int64(time.Second) *
				int64(tc.rate.Den) * int64(tc.batch) / int64(tc.rate.Num))
			got := defaultSampleProgressInterval(tc.rate, tc.batch)

			assert.Positive(t, got, "a usable rate and batch yield an interval")
			assert.LessOrEqual(t, got*2, batchSpan,
				"the loop must wake at least twice per committed batch, "+
					"or it beats against the producer")
		})
	}
}

// The starvation watchdog counts ticks, so its window is a tick count
// times a tick period. Oversampling shortens the period, and if the
// count does not grow to match, the loop stops riding out a peer
// restart and tears itself down partway through one instead.
func TestSampleStarvationWindow_IsIndependentOfTheTickRate(t *testing.T) {
	rate := mxl.Rational{Num: 48000, Den: 1}
	const batch = 480

	batchSpan := time.Duration(int64(time.Second) * int64(rate.Den) * batch / int64(rate.Num))
	window := time.Duration(maxSampleStarvedTicks) * defaultSampleProgressInterval(rate, batch)

	assert.Equal(t, time.Duration(maxSampleStarvedBatches)*batchSpan, window,
		"the starvation window must stay the same wall-clock span "+
			"however often the loop wakes within a batch")
	assert.Equal(t, 2*time.Second, window,
		"48 kHz in 480-sample batches: 200 batch periods is two seconds")
}

func TestDefaultSampleProgressInterval_ZeroBatchFallsBack(t *testing.T) {
	assert.Zero(t, defaultSampleProgressInterval(mxl.Rational{Num: 48000, Den: 1}, 0),
		"a zero batch leaves the interval to the caller's fallback")
}

func TestDefaultSampleProgressInterval_ZeroRateFallsBack(t *testing.T) {
	assert.Zero(t, defaultSampleProgressInterval(mxl.Rational{}, 480),
		"a zero rate leaves the interval to the caller's fallback")
}
