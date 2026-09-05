// Package cadence turns a series of head-index observations into a
// verdict about how evenly a flow is being delivered.
//
// Average throughput answers whether the bytes arrived, not whether
// they arrived on time. A mirror that delivers a second of audio in one
// burst every second reads as 100% of real time and is unusable. The
// measures here separate the two: the realtime ratio is the average,
// and the gap, window and deficit measures are the shape.
package cadence

import (
	"math"
	"sort"
	"time"
)

// Sample is one observation of a flow's head index, taken at At after
// the probe started.
type Sample struct {
	At   time.Duration
	Head uint64
}

// Params configures the analysis. SamplesPerSecond is the flow's own
// rate: the sample rate for a continuous flow, the grain rate for a
// discrete one. It is what "real time" means for this flow.
type Params struct {
	SamplesPerSecond float64

	// Window is the width of the fixed windows the delivery is scored
	// over. Zero takes DefaultWindow.
	Window time.Duration

	// StallThreshold is the gap between two head advances above which
	// delivery counts as stalled. Zero takes DefaultStallThreshold.
	StallThreshold time.Duration

	// StarvedFraction is the share of a window's expected samples below
	// which the window counts as starved. Zero takes
	// DefaultStarvedFraction.
	StarvedFraction float64
}

const (
	// DefaultWindow is short enough that a consumer buffering one video
	// frame would already notice a window it missed.
	DefaultWindow = 20 * time.Millisecond

	// DefaultStallThreshold is the point past which a gap in delivery
	// is audible rather than merely uneven.
	DefaultStallThreshold = 50 * time.Millisecond

	// DefaultStarvedFraction counts a window that received less than
	// half of what real time owed it.
	DefaultStarvedFraction = 0.5
)

// Quantiles summarises a distribution. Count is the number of
// observations behind it.
type Quantiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

// Report is the outcome of one probe run.
type Report struct {
	Duration time.Duration `json:"durationNs"`
	Polls    int           `json:"polls"`

	// Delivered is the total head advance over the run, and
	// RealtimeRatio is that against what the flow's own rate owed.
	Delivered     uint64  `json:"delivered"`
	RealtimeRatio float64 `json:"realtimeRatio"`

	// Advances counts the observations where the head moved forward.
	// Regressions counts the ones where it moved back, which libmxl
	// permits and which no delivery measure should be fooled by.
	Advances    int `json:"advances"`
	Regressions int `json:"regressions"`

	// AdvanceSamples is the distribution of how much the head moved per
	// advance; GapMillis the distribution of the interval between two
	// advances. A smooth stream has a tight gap distribution centred on
	// the producer's batch period.
	AdvanceSamples Quantiles `json:"advanceSamples"`
	GapMillis      Quantiles `json:"gapMillis"`

	Stalls          int           `json:"stalls"`
	StalledFor      time.Duration `json:"stalledForNs"`
	StalledFraction float64       `json:"stalledFraction"`

	Windows        int     `json:"windows"`
	StarvedWindows int     `json:"starvedWindows"`
	StarvedRatio   float64 `json:"starvedRatio"`
	WindowCoV      float64 `json:"windowCoV"`

	// WorstDeficit is how far behind real time the cumulative delivery
	// ever fell, in time. It is the number a consumer feels: a deficit
	// of 800ms is 800ms of audio that was not there when it was due.
	WorstDeficit time.Duration `json:"worstDeficitNs"`

	Verdict string `json:"verdict"`
}

// Verdicts. Smooth is the only one that means the flow is usable.
const (
	VerdictSmooth = "SMOOTH"
	VerdictBursty = "BURSTY"
	VerdictShort  = "SHORT"   // delivering under rate: samples are being lost
	VerdictNoData = "NO-DATA" // the head never moved
)

// Analyse reduces a run of samples to a Report. Samples must be in
// observation order; fewer than two leaves nothing to measure.
func Analyse(samples []Sample, p Params) Report {
	if p.Window <= 0 {
		p.Window = DefaultWindow
	}
	if p.StallThreshold <= 0 {
		p.StallThreshold = DefaultStallThreshold
	}
	if p.StarvedFraction <= 0 {
		p.StarvedFraction = DefaultStarvedFraction
	}

	r := Report{Polls: len(samples), Verdict: VerdictNoData}
	if len(samples) < 2 || p.SamplesPerSecond <= 0 {
		return r
	}

	first, last := samples[0], samples[len(samples)-1]
	r.Duration = last.At - first.At
	if r.Duration <= 0 {
		return r
	}

	// Head regressions are real: the writer can reset the index. They
	// contribute nothing to delivery, so the running total only ever
	// takes forward motion.
	var advanceSizes []float64
	var gaps []float64
	prev := first
	lastAdvanceAt := first.At
	for _, s := range samples[1:] {
		switch {
		case s.Head > prev.Head:
			advanceSizes = append(advanceSizes, float64(s.Head-prev.Head))
			gaps = append(gaps, float64(s.At-lastAdvanceAt)/float64(time.Millisecond))
			r.Delivered += s.Head - prev.Head
			if s.At-lastAdvanceAt > p.StallThreshold {
				r.Stalls++
				r.StalledFor += s.At - lastAdvanceAt
			}
			lastAdvanceAt = s.At
			r.Advances++
		case s.Head < prev.Head:
			r.Regressions++
			// Treat the reset as a fresh starting point rather than
			// letting it show up as a stall on the next advance.
			lastAdvanceAt = s.At
		}
		prev = s
	}
	// A run that ends mid-stall would otherwise not count it.
	if tail := last.At - lastAdvanceAt; tail > p.StallThreshold {
		r.Stalls++
		r.StalledFor += tail
	}

	owed := p.SamplesPerSecond * r.Duration.Seconds()
	r.RealtimeRatio = float64(r.Delivered) / owed
	r.StalledFraction = r.StalledFor.Seconds() / r.Duration.Seconds()
	r.AdvanceSamples = quantiles(advanceSizes)
	r.GapMillis = quantiles(gaps)

	r.Windows, r.StarvedWindows, r.WindowCoV = windows(samples, p)
	if r.Windows > 0 {
		r.StarvedRatio = float64(r.StarvedWindows) / float64(r.Windows)
	}
	r.WorstDeficit = worstDeficit(samples, p.SamplesPerSecond)
	r.Verdict = verdict(r, p)
	return r
}

// windows scores delivery over fixed windows: how many received less
// than their share, and how much the per-window totals vary. A stream
// arriving in bursts has the same mean as a smooth one and a far larger
// coefficient of variation.
func windows(samples []Sample, p Params) (total, starved int, cov float64) {
	start := samples[0].At
	end := samples[len(samples)-1].At
	expected := p.SamplesPerSecond * p.Window.Seconds()
	if expected <= 0 {
		return 0, 0, 0
	}

	// headAt walks the samples once, carrying the last observation at
	// or before each window boundary.
	idx := 0
	headAt := func(t time.Duration) uint64 {
		for idx+1 < len(samples) && samples[idx+1].At <= t {
			idx++
		}
		return samples[idx].Head
	}

	var counts []float64
	for w := start; w+p.Window <= end; w += p.Window {
		lo := headAt(w)
		hi := headAt(w + p.Window)
		var got float64
		if hi > lo {
			got = float64(hi - lo)
		}
		counts = append(counts, got)
		total++
		if got < expected*p.StarvedFraction {
			starved++
		}
	}
	return total, starved, coefficientOfVariation(counts)
}

// worstDeficit is the largest amount by which cumulative delivery fell
// behind what the flow's rate owed, converted to time. The baseline is
// the best the run ever did, so a mirror that simply starts late is not
// charged for the head start it never had.
func worstDeficit(samples []Sample, rate float64) time.Duration {
	base := samples[0]
	var best, worst float64
	for _, s := range samples {
		owed := rate * (s.At - base.At).Seconds()
		var got float64
		if s.Head > base.Head {
			got = float64(s.Head - base.Head)
		}
		surplus := got - owed
		if surplus > best {
			best = surplus
		}
		if d := best - surplus; d > worst {
			worst = d
		}
	}
	return time.Duration(worst / rate * float64(time.Second))
}

func verdict(r Report, p Params) string {
	switch {
	case r.Delivered == 0:
		return VerdictNoData
	case r.RealtimeRatio < 0.98:
		return VerdictShort
	case r.StarvedRatio > 0.01, r.Stalls > 0, r.WorstDeficit > 3*p.Window:
		return VerdictBursty
	default:
		return VerdictSmooth
	}
}

func quantiles(v []float64) Quantiles {
	if len(v) == 0 {
		return Quantiles{}
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return Quantiles{
		Count: len(s),
		P50:   pick(s, 0.50),
		P90:   pick(s, 0.90),
		P99:   pick(s, 0.99),
		Max:   s[len(s)-1],
	}
}

func pick(sorted []float64, q float64) float64 {
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func coefficientOfVariation(v []float64) float64 {
	if len(v) < 2 {
		return 0
	}
	var sum float64
	for _, x := range v {
		sum += x
	}
	mean := sum / float64(len(v))
	if mean == 0 {
		return 0
	}
	var sq float64
	for _, x := range v {
		sq += (x - mean) * (x - mean)
	}
	return math.Sqrt(sq/float64(len(v))) / mean
}
