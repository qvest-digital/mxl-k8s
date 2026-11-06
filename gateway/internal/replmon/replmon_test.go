package replmon

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/mirror"
)

var (
	video = mxl.FlowConfig{Common: mxl.CommonFlowConfig{Format: mxl.FormatVideo, GrainRate: mxl.Rational{Num: 50, Den: 1}}}
	audio = mxl.FlowConfig{Common: mxl.CommonFlowConfig{Format: mxl.FormatAudio, GrainRate: mxl.Rational{Num: 48000, Den: 1}}}
	m1    = types.NamespacedName{Namespace: "ns", Name: "m1"}
	m2    = types.NamespacedName{Namespace: "ns", Name: "m2"}
)

const flowID = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"

func newMonitoring(t *testing.T) *Monitoring {
	t.Helper()
	m, err := New(logr.Discard())
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// sample is one series: its name, labels without node, and value.
type sample struct {
	name   string
	labels string
	value  float64
}

func samples(families map[string]*dto.MetricFamily, drop map[string]bool) map[string]float64 {
	out := map[string]float64{}
	for name, f := range families {
		if drop[name] {
			continue
		}
		for _, m := range f.GetMetric() {
			var ls []string
			for _, l := range m.GetLabel() {
				if l.GetName() != "node" {
					ls = append(ls, l.GetName()+"="+l.GetValue())
				}
			}
			sort.Strings(ls)
			base := name + "{" + strings.Join(ls, ",") + "}"
			switch f.GetType() {
			case dto.MetricType_COUNTER:
				out[base] = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				out[base] = m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				out[name+"_count{"+strings.Join(ls, ",")+"}"] = float64(h.GetSampleCount())
				// The library's text renders the sum to the microsecond.
				out[name+"_sum{"+strings.Join(ls, ",")+"}"] = math.Round(h.GetSampleSum()*1e6) / 1e6
				for _, b := range h.GetBucket() {
					// +Inf is the count; the text form lists it, a
					// gathered histogram leaves it implicit.
					if math.IsInf(b.GetUpperBound(), 1) {
						continue
					}
					out[fmt.Sprintf("%s_bucket{%s,le=%g}", name, strings.Join(ls, ","), b.GetUpperBound())] = float64(b.GetCumulativeCount())
				}
			}
		}
	}
	return out
}

func gather(t *testing.T, c prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	fams, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]*dto.MetricFamily{}
	for _, f := range fams {
		out[f.GetName()] = f
	}
	return out
}

// record drives both roles of two mirrors through every kind of call.
func record(m *Monitoring) {
	s := m.Source(m1, flowID, video, 20*time.Millisecond)
	now := mxl.CurrentIndex(video.Common.GrainRate)
	s.Read(nil, now, 1)
	s.Read(nil, now+1, 1)
	s.ReadContent(mirror.ContentInvalid, now+2, 3)
	s.ReadContent(mirror.ContentMissing, now+3, 1)
	s.Read(mxl.ErrOutOfRangeLate, now+4, 1)
	s.Skipped(5)

	a := m.Source(m2, flowID, audio, 10*time.Millisecond)
	a.Read(nil, mxl.CurrentIndex(audio.Common.GrainRate), 480)

	tg := m.Target(m1, flowID, video)
	tg.Write(nil, now, 1)
	tg.WriteContent(mirror.ContentInvalid, now+1, 1)
	tg.WriteContent(mirror.ContentMissing, now+2, 1)
}

// The collector's series are the library's own, plus node, without the
// two histograms the gateway never feeds.
func TestCollectorMatchesTheLibrarysPrometheusText(t *testing.T) {
	m := newMonitoring(t)
	record(m)
	require.NoError(t, m.m.Poll())

	text, err := m.m.PrometheusText()
	require.NoError(t, err)
	parser := expfmt.NewTextParser(model.UTF8Validation)
	lib, err := parser.TextToMetricFamilies(strings.NewReader(text))
	require.NoError(t, err)
	want := samples(lib, map[string]bool{
		"mxl_gateway_source_commit_seconds": true,
		"mxl_gateway_target_input_seconds":  true,
		// Measured at render time.
		"mxl_gateway_source_age_seconds": true,
		"mxl_gateway_target_age_seconds": true,
	})

	ours := gather(t, &Collector{NodeName: "n1", M: m})
	for name, f := range ours {
		for _, s := range f.GetMetric() {
			var node string
			for _, l := range s.GetLabel() {
				if l.GetName() == "node" {
					node = l.GetValue()
				}
			}
			assert.Equal(t, "n1", node, name)
		}
	}
	got := samples(ours, map[string]bool{
		"mxl_gateway_source_age_seconds": true,
		"mxl_gateway_target_age_seconds": true,
	})
	assert.Equal(t, want, got)
}

func TestCollectorExportsWhatWasRecorded(t *testing.T) {
	m := newMonitoring(t)
	record(m)
	got := samples(gather(t, &Collector{NodeName: "n1", M: m}), nil)

	for name := range got {
		assert.NotContains(t, name, "transfers_total")
		assert.NotContains(t, name, "gaps_total")
	}
	assert.Equal(t, 2.0, got["mxl_gateway_source_reads_total{flow=ns/m1,result=ok}"])
	assert.Equal(t, 3.0, got["mxl_gateway_source_reads_total{flow=ns/m1,result=invalid}"])
	assert.Equal(t, 1.0, got["mxl_gateway_source_reads_total{flow=ns/m1,result=missing}"])
	assert.Equal(t, 1.0, got["mxl_gateway_source_reads_total{flow=ns/m1,result=too_late}"])
	assert.Equal(t, 5.0, got["mxl_gateway_source_skipped_total{flow=ns/m1}"])
	assert.Equal(t, 1.0, got["mxl_gateway_target_writes_total{flow=ns/m1,result=ok}"])
	assert.Equal(t, 1.0, got["mxl_gateway_target_invalid_input_total{flow=ns/m1,issue=invalid}"])
	assert.Equal(t, 1.0, got["mxl_gateway_target_invalid_input_total{flow=ns/m1,issue=missing}"])
	assert.Equal(t, 1.0, got["mxl_gateway_source_flow_info{flow=ns/m1,media_type=video,unit=grains}"])
	assert.Equal(t, 1.0, got["mxl_gateway_source_flow_info{flow=ns/m2,media_type=audio,unit=samples}"])
	assert.Equal(t, 1.0, got["mxl_gateway_source_flow_state{flow=ns/m1,state=open}"])
	assert.Equal(t, 480.0, got["mxl_gateway_source_reads_total{flow=ns/m2,result=ok}"], "in samples")
}

// A grain the gateway waits for at the head is not the writer's fault;
// one still missing a grace period past its media timestamp is.
func TestSourceTimingCountsOnlyALateWriter(t *testing.T) {
	m := newMonitoring(t)
	s := m.Source(m1, flowID, video, 20*time.Millisecond)
	head := mxl.CurrentIndex(video.Common.GrainRate)
	s.Read(mxl.ErrTimeout, head+1, 1)
	s.Read(mxl.ErrTimeout, head-1000, 1)
	s.Read(mxl.ErrTimeout, head-1000, 1)
	got := samples(gather(t, &Collector{NodeName: "n1", M: m}), nil)
	assert.Equal(t, 1.0, got["mxl_gateway_source_reads_total{flow=ns/m1,result=too_early}"],
		"once per index, and only past the grace")
}

// A released mirror leaves no series, and a loop that records after its
// teardown records nothing rather than into a removed handle.
func TestReleaseRemovesTheMirror(t *testing.T) {
	m := newMonitoring(t)
	s := m.Source(m1, flowID, video, 20*time.Millisecond)
	tg := m.Target(m1, flowID, video)
	m.ReleaseSource(m1)
	m.ReleaseTarget(m1)
	s.Read(nil, 1, 1)
	tg.Write(nil, 1, 1)
	assert.Empty(t, gather(t, &Collector{NodeName: "n1", M: m}))

	again := m.Source(m1, flowID, video, 20*time.Millisecond)
	again.Read(nil, 2, 1)
	got := samples(gather(t, &Collector{NodeName: "n1", M: m}), nil)
	assert.Equal(t, 1.0, got["mxl_gateway_source_reads_total{flow=ns/m1,result=ok}"], "a new mirror of that name starts afresh")
}

// One handle per mirror and role: asking again for a mirror's source
// returns the handle it has, counted as one more open.
func TestSourceKeepsAMirrorsHandle(t *testing.T) {
	m := newMonitoring(t)
	a := m.Source(m1, flowID, video, 20*time.Millisecond)
	b := m.Source(m1, flowID, video, 20*time.Millisecond)
	assert.Same(t, a, b)
	got := samples(gather(t, &Collector{NodeName: "n1", M: m}), nil)
	assert.Equal(t, 2.0, got["mxl_gateway_source_opens_total{flow=ns/m1}"])
}

func TestTiming(t *testing.T) {
	grace, stall := Timing(20 * time.Millisecond)
	assert.Equal(t, 70*time.Millisecond, grace)
	assert.Equal(t, 100*time.Millisecond, stall)
	grace, stall = Timing(40 * time.Millisecond)
	assert.Equal(t, 90*time.Millisecond, grace)
	assert.Equal(t, 160*time.Millisecond, stall)
}
