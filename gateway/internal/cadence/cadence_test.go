package cadence

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rate48k = 48000.0

// synth builds a poll series for a flow whose head advances by batch
// samples every period, sampled every pollEvery, for total.
func synth(total, period, pollEvery time.Duration, batch uint64) []Sample {
	var out []Sample
	var head uint64
	nextAdvance := period
	for t := time.Duration(0); t <= total; t += pollEvery {
		for t >= nextAdvance {
			head += batch
			nextAdvance += period
		}
		out = append(out, Sample{At: t, Head: head})
	}
	return out
}

func TestSmoothStreamPasses(t *testing.T) {
	// 480 samples every 10ms is exactly 48kHz.
	s := synth(2*time.Second, 10*time.Millisecond, 250*time.Microsecond, 480)

	r := Analyse(s, Params{SamplesPerSecond: rate48k})

	assert.Equal(t, VerdictSmooth, r.Verdict)
	assert.InDelta(t, 1.0, r.RealtimeRatio, 0.01)
	assert.Zero(t, r.Stalls)
	assert.Zero(t, r.StarvedWindows)
	assert.Less(t, r.WorstDeficit, 20*time.Millisecond)
}

// The measure this package exists for. A flow that delivers a full
// second of audio once per second averages 100% of real time and is
// unusable. Average throughput cannot tell it apart from a smooth
// stream; the window and deficit measures must.
func TestBurstyStreamAtFullAverageRateIsRejected(t *testing.T) {
	smooth := synth(4*time.Second, 10*time.Millisecond, 250*time.Microsecond, 480)
	bursty := synth(4*time.Second, 1*time.Second, 250*time.Microsecond, 48000)

	rs := Analyse(smooth, Params{SamplesPerSecond: rate48k})
	rb := Analyse(bursty, Params{SamplesPerSecond: rate48k})

	// Identical on the average, which is the whole point.
	assert.InDelta(t, rs.RealtimeRatio, rb.RealtimeRatio, 0.02)

	assert.Equal(t, VerdictSmooth, rs.Verdict)
	assert.Equal(t, VerdictBursty, rb.Verdict)
	assert.Greater(t, rb.StarvedWindows, rs.StarvedWindows)
	assert.Greater(t, rb.WorstDeficit, 500*time.Millisecond)
	assert.Positive(t, rb.Stalls)
}

func TestShortDeliveryIsDistinguishedFromBurst(t *testing.T) {
	// 240 samples every 10ms: evenly paced, but only half the rate.
	s := synth(2*time.Second, 10*time.Millisecond, 250*time.Microsecond, 240)

	r := Analyse(s, Params{SamplesPerSecond: rate48k})

	assert.Equal(t, VerdictShort, r.Verdict)
	assert.InDelta(t, 0.5, r.RealtimeRatio, 0.02)
}

// A mirror that wedges and then catches up delivers every sample it
// owed, so the average clears. What it did to the consumer is the 300ms
// hole, which only the stall and deficit measures see.
func TestStallFollowedByCatchUpIsBurstyNotShort(t *testing.T) {
	s := synth(time.Second, 10*time.Millisecond, 500*time.Microsecond, 480)
	// Freeze the head from 400ms to 700ms; afterwards the original
	// series resumes, so the catch-up arrives as one jump.
	var frozen uint64
	for i := range s {
		if s[i].At == 400*time.Millisecond {
			frozen = s[i].Head
		}
		if s[i].At > 400*time.Millisecond && s[i].At <= 700*time.Millisecond {
			s[i].Head = frozen
		}
	}

	r := Analyse(s, Params{SamplesPerSecond: rate48k})

	assert.Equal(t, VerdictBursty, r.Verdict)
	assert.InDelta(t, 1.0, r.RealtimeRatio, 0.01, "every owed sample did arrive")
	assert.Equal(t, 1, r.Stalls)
	assert.InDelta(t, 300.0, float64(r.StalledFor/time.Millisecond), 15)
	assert.InDelta(t, 300.0, r.GapMillis.Max, 15)
	assert.InDelta(t, 300.0, float64(r.WorstDeficit/time.Millisecond), 20)
	// The catch-up arrives as a single outsized advance.
	assert.Greater(t, r.AdvanceSamples.Max, 480.0*20)
}

// libmxl lets a writer move the head backwards. Delivery must not be
// credited for the jump back, and the regression must not be billed as
// a stall on the next advance.
func TestHeadRegressionIsNotCountedAsDelivery(t *testing.T) {
	s := []Sample{
		{At: 0, Head: 1000},
		{At: 10 * time.Millisecond, Head: 1480},
		{At: 20 * time.Millisecond, Head: 200}, // reset
		{At: 30 * time.Millisecond, Head: 680},
	}

	r := Analyse(s, Params{SamplesPerSecond: rate48k})

	assert.Equal(t, 1, r.Regressions)
	assert.Equal(t, uint64(480+480), r.Delivered)
	assert.Zero(t, r.Stalls)
}

func TestNoDataWhenHeadNeverMoves(t *testing.T) {
	var s []Sample
	for t0 := time.Duration(0); t0 < time.Second; t0 += time.Millisecond {
		s = append(s, Sample{At: t0, Head: 42})
	}

	r := Analyse(s, Params{SamplesPerSecond: rate48k})

	assert.Equal(t, VerdictNoData, r.Verdict)
	assert.Zero(t, r.Delivered)
}

func TestTooFewSamplesReportsNoData(t *testing.T) {
	r := Analyse([]Sample{{At: 0, Head: 1}}, Params{SamplesPerSecond: rate48k})
	assert.Equal(t, VerdictNoData, r.Verdict)
}

func TestQuantilesAndCoV(t *testing.T) {
	q := quantiles([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	require.Equal(t, 10, q.Count)
	assert.Equal(t, 5.0, q.P50)
	assert.Equal(t, 9.0, q.P90)
	assert.Equal(t, 10.0, q.Max)

	// A constant series has no variation; a bursty one does.
	assert.Equal(t, 0.0, coefficientOfVariation([]float64{5, 5, 5, 5}))
	assert.Greater(t, coefficientOfVariation([]float64{0, 0, 0, 20}), 1.0)
}
