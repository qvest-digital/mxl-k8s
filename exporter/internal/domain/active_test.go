package domain

import (
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
)

const activeTestFlowDef = `{
  "id": "5fbec3b1-1b0f-417d-9059-8b94a47197ed",
  "format": "urn:x-nmos:format:video",
  "tags": { "urn:x-nmos:tag:grouphint/v1.0": ["exporter probe:Video"] },
  "label": "activity probe",
  "media_type": "video/v210",
  "grain_rate": { "numerator": 30000, "denominator": 1001 },
  "frame_width": 16,
  "frame_height": 16,
  "interlace_mode": "progressive",
  "colorspace": "BT709",
  "components": [
    { "name": "Y",  "width": 16, "height": 16, "bit_depth": 10 },
    { "name": "Cb", "width": 8,  "height": 16, "bit_depth": 10 },
    { "name": "Cr", "width": 8,  "height": 16, "bit_depth": 10 }
  ]
}`

const activeTestFlowID = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"

// newObservedFlow opens a domain over a temp dir, writes one grain,
// and returns the Domain with the flow scanned in.
func newObservedFlow(t *testing.T) *Domain {
	t.Helper()
	dir := t.TempDir()
	inst, err := mxl.NewInstance(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })

	w, _, err := inst.NewWriter(activeTestFlowDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	idx := mxl.CurrentIndex(w.Config().Common.GrainRate)
	ga, err := w.OpenGrain(idx)
	require.NoError(t, err)
	require.NoError(t, ga.Commit(ga.TotalSlices, 0))

	d, err := Open(dir, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	require.NoError(t, d.Scan())
	return d
}

func TestObserveIsIdempotentWithinAScrape(t *testing.T) {
	// Two collectors are registered against one Domain and Prometheus
	// calls both inside a single scrape. While activity was a delta
	// against the previous call, whichever collector ran first consumed
	// the head advancement and the other reported the flow idle, so one
	// scrape could carry an advancing head index and active=0. The
	// order the registry walks its collectors is not fixed, which made
	// mxl_flow_active a coin flip on every scrape of a healthy flow.
	d := newObservedFlow(t)

	first := d.Observe()
	second := d.Observe()
	require.Len(t, first, 1)
	require.Len(t, second, 1)

	require.Equal(t, first[0].Active, second[0].Active,
		"back-to-back Observe calls must agree; a second caller in the "+
			"same scrape must not be told the flow went idle because the "+
			"first caller already consumed the sample")
	require.True(t, first[0].Active,
		"a flow written moments ago must read as active")
}

func TestObserveActivityFollowsTheFlowClock(t *testing.T) {
	// Activity has to be a property of the flow, not of how often
	// anything asks. A flow whose last write has aged past the window
	// is inactive however many times it is observed, and a flow that is
	// still being written stays active however rarely it is.
	d := newObservedFlow(t)

	obs := d.Observe()
	require.Len(t, obs, 1)
	require.Equal(t, activeTestFlowID, obs[0].ID)
	require.True(t, obs[0].Active)
	require.Less(t, obs[0].WriteAge, activityWindow(obs[0].Info.Config.Common),
		"the write age is what decides activity, so it has to be inside "+
			"the window for a flow just written")

	// Nothing writes to the flow from here on, so it has to fall out of
	// the window on its own, with no scrape cadence involved.
	require.Eventually(t, func() bool {
		o := d.Observe()
		return len(o) == 1 && !o[0].Active
	}, 5*time.Second, 20*time.Millisecond,
		"a flow that stopped being written must go inactive once its "+
			"last write ages past the window")
}

func TestActivityWindowFollowsTheFlowRate(t *testing.T) {
	// A fixed window is wrong at both ends of the rate range: what is
	// three grains at 25 fps is a fifth of a grain at 1 fps, where a
	// healthy flow would read stalled between every frame.
	slow := mxl.CommonFlowConfig{
		Format:    mxl.FormatVideo,
		GrainRate: mxl.Rational{Num: 1, Den: 1},
	}
	require.Equal(t, activeGrains*time.Second, activityWindow(slow),
		"a 1 fps flow has to be allowed a full three seconds; anything "+
			"shorter reports every healthy frame gap as a stall")

	fast := mxl.CommonFlowConfig{
		Format:    mxl.FormatVideo,
		GrainRate: mxl.Rational{Num: 60, Den: 1},
	}
	require.Equal(t, minActivityWindow, activityWindow(fast),
		"three grains at 60 fps is shorter than the jitter between two "+
			"scrapes, so the floor has to take over")
}

func TestActivityWindowUsesCommitBatchForContinuousFlows(t *testing.T) {
	// On a continuous flow the grain rate is the sample rate. Treating
	// it as a write interval would give a 62 microsecond window at
	// 48 kHz; writes actually land one commit batch apart.
	audio := mxl.CommonFlowConfig{
		Format:                 mxl.FormatAudio,
		GrainRate:              mxl.Rational{Num: 48000, Den: 1},
		MaxCommitBatchSizeHint: 480,
	}
	require.Equal(t, minActivityWindow, activityWindow(audio),
		"three 10 ms batches is under the floor, so the floor applies")

	sparse := audio
	sparse.MaxCommitBatchSizeHint = 48000
	require.Equal(t, activeGrains*time.Second, activityWindow(sparse),
		"a flow committing one second of samples at a time must be "+
			"allowed three seconds, not the floor")
}

func TestActivityWindowFallsBackOnAnUnusableRate(t *testing.T) {
	require.Equal(t, minActivityWindow,
		activityWindow(mxl.CommonFlowConfig{Format: mxl.FormatVideo}),
		"a zero grain rate must not divide by zero or yield a zero window")
}
