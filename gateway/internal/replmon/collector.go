package replmon

import (
	mm "github.com/marquies/libmxl-monitoring/go"
	"github.com/prometheus/client_golang/prometheus"
)

// The families below are the ones libmxl-monitoring renders for its
// replication roles (src/Prometheus.cpp at the pinned version): the same
// names, help, label names and label values, with the node label every
// gateway series carries. Left out are the two histograms the gateway
// never feeds: the source's commit_seconds, which needs RecordCommit,
// and the target's input_seconds, which needs RecordInput.
//
// Series per mirror and role are fixed: one handle, label values from
// fixed sets, histogram buckets from the library's fixed bounds.

type key struct {
	name  string
	value func(*mm.Snapshot) uint64
}

func contentOf(s *mm.Snapshot, c mm.Content) uint64 { return s.Content[c] }

var readResults = []key{
	{"ok", func(s *mm.Snapshot) uint64 { return s.Reads[mm.ReadOK] }},
	{"too_early", func(s *mm.Snapshot) uint64 { return s.Reads[mm.ReadTooEarly] }},
	{"too_late", func(s *mm.Snapshot) uint64 { return s.Reads[mm.ReadTooLate] }},
	{"missing", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentMissing) }},
	{"invalid", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentInvalid) }},
	{"incomplete", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentIncomplete) }},
	{"wrong_size", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentWrongSize) }},
	{"wrong_format", func(s *mm.Snapshot) uint64 {
		return contentOf(s, mm.ContentWrongFormat) + contentOf(s, mm.ContentWrongResolution) + contentOf(s, mm.ContentWrongRate)
	}},
	{"illegal_values", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentIllegalValues) }},
	{"permission_error", func(s *mm.Snapshot) uint64 { return s.Reads[mm.ReadPermissionDenied] }},
	{"other_error", func(s *mm.Snapshot) uint64 { return s.Reads[mm.ReadError] }},
}

var writeResults = []key{
	{"ok", func(s *mm.Snapshot) uint64 { return s.Writes[mm.WriteOK] }},
	{"input_late", func(s *mm.Snapshot) uint64 { return s.Writes[mm.WriteInputLate] }},
	{"invalid_input", func(s *mm.Snapshot) uint64 { return s.Writes[mm.WriteInvalidInput] }},
	{"permission_error", func(s *mm.Snapshot) uint64 { return s.Writes[mm.WritePermissionDenied] }},
	{"out_of_space", func(s *mm.Snapshot) uint64 { return s.Writes[mm.WriteOutOfSpace] }},
	{"other_error", func(s *mm.Snapshot) uint64 { return s.Writes[mm.WriteError] }},
}

var inputIssues = []key{
	{"wrong_format", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentWrongFormat) }},
	{"wrong_resolution", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentWrongResolution) }},
	{"wrong_rate", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentWrongRate) }},
	{"wrong_size", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentWrongSize) }},
	{"missing", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentMissing) }},
	{"invalid", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentInvalid) }},
	{"incomplete", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentIncomplete) }},
	{"illegal_values", func(s *mm.Snapshot) uint64 { return contentOf(s, mm.ContentIllegalValues) }},
}

var openFailures = []key{
	{"domain", func(s *mm.Snapshot) uint64 { return s.AccessFailures[mm.AccessDomainUnavailable] }},
	{"not_found", func(s *mm.Snapshot) uint64 { return s.AccessFailures[mm.AccessFlowNotFound] }},
	{"permission", func(s *mm.Snapshot) uint64 { return s.AccessFailures[mm.AccessPermissionDenied] }},
	{"other", func(s *mm.Snapshot) uint64 {
		return s.AccessFailures[mm.AccessFlowInvalid] + s.AccessFailures[mm.AccessOther]
	}},
}

// family is one metric family of one role.
type family struct {
	desc  *prometheus.Desc
	label string
	keys  []key
}

type roleFamilies struct {
	info, state, delivering, age  *prometheus.Desc
	results, issues, openFailures family
	skipped, stalls, opens, lost  *prometheus.Desc
	histogram                     *prometheus.Desc
	histogramField                func(*mm.Snapshot) mm.Histogram
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, append([]string{"node", "flow"}, labels...), nil)
}

const (
	helpInfo         = "Media type and unit of a flow, once it has been opened."
	helpOpenFailures = "Attempts to reach a flow that failed, by reason."
	helpOpens        = "Times the flow was opened."
	helpLost         = "Times an open flow went away and was reopened."
)

var sourceFamilies = roleFamilies{
	info:       desc("mxl_gateway_source_flow_info", helpInfo, "media_type", "unit"),
	state:      desc("mxl_gateway_source_flow_state", "1 for the state the flow's reader is in.", "state"),
	delivering: desc("mxl_gateway_source_flow_delivering", "1 while reads land within a second."),
	results: family{desc("mxl_gateway_source_reads_total",
		"Reads by outcome, in grains (discrete) or samples (continuous); too_early and too_late count events.", "result"),
		"result", readResults},
	openFailures: family{desc("mxl_gateway_source_open_failures_total", helpOpenFailures, "reason"), "reason", openFailures},
	skipped: desc("mxl_gateway_source_skipped_total",
		"Grains or samples jumped over to reach the head again after the ring overtook the reader."),
	stalls:         desc("mxl_gateway_source_stalls_total", "Read timeouts that passed with no read at all."),
	opens:          desc("mxl_gateway_source_opens_total", helpOpens),
	lost:           desc("mxl_gateway_source_lost_total", helpLost),
	age:            desc("mxl_gateway_source_age_seconds", "Wall clock minus the media timestamp of the last read."),
	histogram:      desc("mxl_gateway_source_arrival_seconds", "Read time minus the media timestamp of what was read."),
	histogramField: func(s *mm.Snapshot) mm.Histogram { return s.ArrivalLatency },
}

var targetFamilies = roleFamilies{
	info:       desc("mxl_gateway_target_flow_info", helpInfo, "media_type", "unit"),
	state:      desc("mxl_gateway_target_flow_state", "1 for the state the flow's writer is in.", "state"),
	delivering: desc("mxl_gateway_target_flow_delivering", "1 while commits land within a second."),
	results: family{desc("mxl_gateway_target_writes_total",
		"Writes by outcome, in grains (discrete) or samples (continuous); permission_error and other_error count events.", "result"),
		"result", writeResults},
	issues:         family{desc("mxl_gateway_target_invalid_input_total", "Input found wrong before writing, by issue.", "issue"), "issue", inputIssues},
	openFailures:   family{desc("mxl_gateway_target_open_failures_total", helpOpenFailures, "reason"), "reason", openFailures},
	opens:          desc("mxl_gateway_target_opens_total", helpOpens),
	lost:           desc("mxl_gateway_target_lost_total", helpLost),
	age:            desc("mxl_gateway_target_age_seconds", "Wall clock minus the media timestamp of the last commit."),
	histogram:      desc("mxl_gateway_target_commit_seconds", "Commit time minus the media timestamp of what was committed."),
	histogramField: func(s *mm.Snapshot) mm.Histogram { return s.CommitLatency },
}

// Collector exports what Monitoring recorded.
type Collector struct {
	NodeName string
	M        *Monitoring
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, r := range []roleFamilies{sourceFamilies, targetFamilies} {
		for _, d := range []*prometheus.Desc{r.info, r.state, r.delivering, r.age, r.results.desc, r.openFailures.desc,
			r.opens, r.lost, r.histogram} {
			ch <- d
		}
		for _, d := range []*prometheus.Desc{r.issues.desc, r.skipped, r.stalls} {
			if d != nil {
				ch <- d
			}
		}
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snaps, err := c.M.m.Snapshot()
	if err != nil {
		c.M.log.Error(err, "snapshot libmxl-monitoring")
		return
	}
	for i := range snaps {
		s := &snaps[i]
		switch s.Role {
		case mm.RoleReplicationSource:
			c.collect(ch, sourceFamilies, s)
		case mm.RoleReplicationTarget:
			c.collect(ch, targetFamilies, s)
		}
	}
}

func (c *Collector) gauge(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, append([]string{c.NodeName}, labels...)...)
}

func (c *Collector) counter(ch chan<- prometheus.Metric, d *prometheus.Desc, v uint64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), append([]string{c.NodeName}, labels...)...)
}

func (c *Collector) collect(ch chan<- prometheus.Metric, r roleFamilies, s *mm.Snapshot) {
	flow := s.FlowID
	if s.Opens > 0 && s.MediaType != "" {
		unit := "grains"
		if s.Unit == mm.UnitSample {
			unit = "samples"
		}
		c.gauge(ch, r.info, 1, flow, s.MediaType, unit)
	}
	c.gauge(ch, r.state, 1, flow, s.Access.String())
	delivering := 0.0
	if s.Delivering {
		delivering = 1
	}
	c.gauge(ch, r.delivering, delivering, flow)
	for _, f := range []family{r.results, r.issues, r.openFailures} {
		if f.desc == nil {
			continue
		}
		for _, k := range f.keys {
			c.counter(ch, f.desc, k.value(s), flow, k.name)
		}
	}
	if r.skipped != nil {
		c.counter(ch, r.skipped, s.Skipped, flow)
		c.counter(ch, r.stalls, s.Stalls, flow)
	}
	c.counter(ch, r.opens, s.Opens, flow)
	c.counter(ch, r.lost, s.Losses, flow)
	if s.LastDataTime != 0 {
		c.gauge(ch, r.age, s.Age.Seconds(), flow)
	}
	h := r.histogramField(s)
	buckets := make(map[float64]uint64, mm.LatencyBuckets-1)
	var cumulative uint64
	for b := 0; b+1 < mm.LatencyBuckets; b++ {
		cumulative += h.Buckets[b]
		buckets[mm.LatencyBound(b).Seconds()] = cumulative
	}
	// The count from the buckets, as the library renders it, so +Inf is
	// never below the last bound.
	cumulative += h.Buckets[mm.LatencyBuckets-1]
	ch <- prometheus.MustNewConstHistogram(r.histogram, cumulative, h.Sum.Seconds(), buckets, c.NodeName, flow)
}
