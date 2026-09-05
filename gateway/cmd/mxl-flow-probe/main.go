// mxl-flow-probe samples a flow's head index at high rate and reports
// how evenly the flow is being delivered.
//
// Run it on the node that holds the flow. On the node a flow originates
// from it measures what the producer writes; on a node the flow is
// mirrored to it measures what the fabric delivered, and the difference
// between the two runs is the fabric's contribution.
//
//	mxl-flow-probe -domain /run/mxl/domain -flow <uuid> -duration 30s
//
// The exit status is 0 for a SMOOTH verdict and 1 for any other, so the
// probe can gate a change rather than only describe one.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/cadence"
)

func main() {
	code, err := run(os.Args[1:], os.Stdout, os.Stderr)
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "mxl-flow-probe:", err)
		os.Exit(2)
	}
	os.Exit(code)
}

func run(args []string, stdout, stderr io.Writer) (int, error) {
	fs := flag.NewFlagSet("mxl-flow-probe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		domain   = fs.String("domain", "/run/mxl/domain", "MXL domain directory")
		flowID   = fs.String("flow", "", "flow UUID to probe")
		duration = fs.Duration("duration", 30*time.Second, "how long to sample")
		interval = fs.Duration("interval", 500*time.Microsecond, "polling interval")
		window   = fs.Duration("window", cadence.DefaultWindow, "width of the windows delivery is scored over")
		stall    = fs.Duration("stall", cadence.DefaultStallThreshold, "gap between advances that counts as a stall")
		rateFlag = fs.Float64("rate", 0, "override the flow's own rate, in samples or grains per second")
		label    = fs.String("label", "", "free-form label echoed into the report, e.g. the node name")
		asJSON   = fs.Bool("json", false, "emit the report as JSON")
	)
	if err := fs.Parse(args); err != nil {
		return 2, err
	}
	if *flowID == "" {
		return 2, errors.New("missing -flow <uuid>")
	}

	inst, err := mxl.NewInstance(*domain, "")
	if err != nil {
		return 2, fmt.Errorf("NewInstance: %w", err)
	}
	defer inst.Close()

	reader, err := inst.NewReader(*flowID)
	if err != nil {
		return 2, fmt.Errorf("NewReader: %w", err)
	}
	defer reader.Close()

	info, err := reader.Info()
	if err != nil {
		return 2, fmt.Errorf("Info: %w", err)
	}

	rate := *rateFlag
	if rate == 0 {
		rate = info.Config.Common.GrainRate.Float64()
	}
	if rate <= 0 {
		return 2, errors.New("flow rate is zero; pass -rate")
	}

	samples, err := poll(reader, *duration, *interval)
	if err != nil {
		return 2, err
	}

	report := cadence.Analyse(samples, cadence.Params{
		SamplesPerSecond: rate,
		Window:           *window,
		StallThreshold:   *stall,
	})

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(struct {
			Label string `json:"label,omitempty"`
			Flow  string `json:"flow"`
			Rate  float64
			cadence.Report
		}{*label, *flowID, rate, report}); err != nil {
			return 2, err
		}
	} else {
		writeText(stdout, *label, *flowID, info, rate, report)
	}

	if report.Verdict == cadence.VerdictSmooth {
		return 0, nil
	}
	return 1, nil
}

// poll reads the head index as fast as the requested interval allows.
// Intervals below a millisecond are spun rather than slept: the runtime
// cannot sleep that precisely, and undersampling the head would blunt
// exactly the short gaps the probe exists to find.
func poll(reader *mxl.Reader, duration, interval time.Duration) ([]cadence.Sample, error) {
	out := make([]cadence.Sample, 0, int(duration/interval)+1)
	start := time.Now()
	deadline := start.Add(duration)
	next := start
	for {
		now := time.Now()
		if now.After(deadline) {
			break
		}
		rt, err := reader.Runtime()
		if err != nil {
			return nil, fmt.Errorf("Runtime after %s: %w", time.Since(start).Round(time.Millisecond), err)
		}
		out = append(out, cadence.Sample{At: now.Sub(start), Head: rt.HeadIndex})

		next = next.Add(interval)
		if d := time.Until(next); d > time.Millisecond {
			time.Sleep(d - 500*time.Microsecond)
		}
		for time.Now().Before(next) {
		}
	}
	if len(out) < 2 {
		return nil, errors.New("no samples collected")
	}
	return out, nil
}

func writeText(w io.Writer, label, flowID string, info mxl.FlowInfo, rate float64, r cadence.Report) {
	cfg := info.Config
	kind := "continuous"
	unit := "samples"
	if cfg.Common.Format.IsDiscrete() {
		kind, unit = "discrete", "grains"
	}
	if label != "" {
		fmt.Fprintf(w, "label      %s\n", label)
	}
	fmt.Fprintf(w, "flow       %s  %s  %.0f %s/s", flowID, kind, rate, unit)
	if !cfg.Common.Format.IsDiscrete() {
		fmt.Fprintf(w, "  channels=%d bufferLen=%d syncBatch=%d",
			cfg.Continuous.ChannelCount, cfg.Continuous.BufferLength, cfg.Common.MaxSyncBatchSizeHint)
	}
	fmt.Fprintf(w, "\nwindow     %s over %s, polled every %s (%d polls)\n\n",
		r.Duration.Round(time.Millisecond), time.Now().Format(time.RFC3339),
		(r.Duration / time.Duration(max(r.Polls-1, 1))).Round(time.Microsecond), r.Polls)

	fmt.Fprintf(w, "delivery\n")
	fmt.Fprintf(w, "  delivered            %d %s  (%.1f%% of real time)\n", r.Delivered, unit, 100*r.RealtimeRatio)
	fmt.Fprintf(w, "  advances             %d  (%.1f/s)", r.Advances, float64(r.Advances)/r.Duration.Seconds())
	if r.Regressions > 0 {
		fmt.Fprintf(w, "   head went backwards %d times", r.Regressions)
	}
	fmt.Fprintf(w, "\n  advance size         p50 %.0f  p90 %.0f  p99 %.0f  max %.0f\n",
		r.AdvanceSamples.P50, r.AdvanceSamples.P90, r.AdvanceSamples.P99, r.AdvanceSamples.Max)
	fmt.Fprintf(w, "  gap between them     p50 %.2fms  p90 %.2fms  p99 %.2fms  max %.2fms\n",
		r.GapMillis.P50, r.GapMillis.P90, r.GapMillis.P99, r.GapMillis.Max)
	fmt.Fprintf(w, "  stalls               %d  totalling %s  (%.1f%% of the window)\n",
		r.Stalls, r.StalledFor.Round(time.Millisecond), 100*r.StalledFraction)

	fmt.Fprintf(w, "\nsmoothness\n")
	fmt.Fprintf(w, "  starved windows      %d of %d  (%.1f%%)\n", r.StarvedWindows, r.Windows, 100*r.StarvedRatio)
	fmt.Fprintf(w, "  per-window variation %.2f\n", r.WindowCoV)
	fmt.Fprintf(w, "  worst deficit        %s behind real time\n", r.WorstDeficit.Round(time.Millisecond))

	fmt.Fprintf(w, "\nVERDICT: %s\n", r.Verdict)
}
