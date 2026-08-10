// Package collector turns domain observations and MxlFlow topology
// into Prometheus metrics.
//
// The metric names and their meaning are ported from
// github.com/jonasohland/mxl-exporter (Apache-2.0) so dashboards built
// against that exporter keep working.
//
// A domain is shared by every pod on its node, so a series is per node
// and per flow. Anything scanning the same domain from more than one
// place on a node reports the same flows more than once.
package collector

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/qvest-digital/go-mxl/mxl"

	"github.com/qvest-digital/mxl-k8s/exporter/internal/domain"
)

// flowLabels identify a flow. The node is not among them: it is
// attached once, to the whole registry, from --node-name.
var flowLabels = []string{"flow_id", "domain"}

var metadataLabels = []string{
	"flow_id",
	"domain",
	"flow_label",
	"flow_description",
	"flow_group_name",
	"flow_group_type",
	"flow_data_type",
	"flow_format",
	"flow_payload_location",
	"flow_media_type",
	"flow_colorspace",
	"flow_version",
}

var (
	descMetadata = prometheus.NewDesc("mxl_flow_metadata",
		"Flow metadata as labels. The value is always 1.", metadataLabels, nil)
	descPresent = prometheus.NewDesc("mxl_flow_present",
		"1 while the flow's directory exists in the domain.", flowLabels, nil)
	descActive = prometheus.NewDesc("mxl_flow_active",
		"1 when the head index advanced since the previous scrape.", flowLabels, nil)

	descHeadIndex = prometheus.NewDesc("mxl_flow_head_index_grains",
		"Current head index of the flow, in grains.", flowLabels, nil)
	descLastRead = prometheus.NewDesc("mxl_flow_last_read_time_ns",
		"MXL clock timestamp of the last read, in nanoseconds.", flowLabels, nil)
	descLastWrite = prometheus.NewDesc("mxl_flow_last_write_time_ns",
		"MXL clock timestamp of the last write, in nanoseconds.", flowLabels, nil)
	descWriteAge = prometheus.NewDesc("mxl_flow_last_write_age_seconds",
		"Time since the last write, measured on the MXL clock at scrape time.", flowLabels, nil)
	descLatency = prometheus.NewDesc("mxl_flow_latency_grains",
		"Grains between where the MXL clock puts the head index and where it is.", flowLabels, nil)

	descRateNum = prometheus.NewDesc("mxl_flow_rate_num",
		"Numerator of the flow's grain rate.", flowLabels, nil)
	descRateDen = prometheus.NewDesc("mxl_flow_rate_den",
		"Denominator of the flow's grain rate.", flowLabels, nil)
	descCommitBatch = prometheus.NewDesc("mxl_flow_max_commit_batch_size_hint",
		"Flow maximum commit batch size hint.", flowLabels, nil)
	descSyncBatch = prometheus.NewDesc("mxl_flow_max_sync_batch_size_hint",
		"Flow maximum sync batch size hint.", flowLabels, nil)

	descSliceSize = prometheus.NewDesc("mxl_flow_payload_slice_size_bytes",
		"Size of one payload buffer slice, in bytes. Discrete flows only.",
		append(append([]string{}, flowLabels...), "payload_buffer_index"), nil)
	descRingGrains = prometheus.NewDesc("mxl_flow_ring_buffer_size_grains",
		"Ring buffer depth in grains. Discrete flows only.", flowLabels, nil)

	descChannels = prometheus.NewDesc("mxl_flow_channels_total",
		"Channel count. Continuous flows only.", flowLabels, nil)
	descRingSamples = prometheus.NewDesc("mxl_flow_ring_buffer_size_samples",
		"Ring buffer depth in samples. Continuous flows only.", flowLabels, nil)
)

// Source is what a FlowCollector reads on each scrape.
type Source interface {
	Observe() []domain.Observation
	Path() string
}

// FlowCollector exports one series set per flow in the domain.
type FlowCollector struct {
	src Source
}

// NewFlowCollector returns a collector over src.
func NewFlowCollector(src Source) *FlowCollector {
	return &FlowCollector{src: src}
}

// Describe implements prometheus.Collector.
func (c *FlowCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descMetadata, descPresent, descActive,
		descHeadIndex, descLastRead, descLastWrite, descWriteAge, descLatency,
		descRateNum, descRateDen, descCommitBatch, descSyncBatch,
		descSliceSize, descRingGrains, descChannels, descRingSamples,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *FlowCollector) Collect(ch chan<- prometheus.Metric) {
	dom := c.src.Path()
	for _, obs := range c.src.Observe() {
		c.collectFlow(ch, dom, obs)
	}
}

func (c *FlowCollector) collectFlow(ch chan<- prometheus.Metric, dom string, obs domain.Observation) {
	gauge := func(d *prometheus.Desc, v float64, extra ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v,
			append([]string{obs.ID, dom}, extra...)...)
	}

	gauge(descPresent, boolValue(obs.Present))
	gauge(descActive, boolValue(obs.Active))

	if !obs.HaveInfo {
		// A flow with no readable header still reports present/active,
		// so a directory that exists without a usable flow behind it is
		// visible rather than silently absent.
		return
	}

	cfg := obs.Info.Config
	rt := obs.Info.Runtime

	if obs.Def != nil {
		name, role := obs.Def.GroupHint()
		ch <- prometheus.MustNewConstMetric(descMetadata, prometheus.GaugeValue, 1,
			obs.ID, dom,
			obs.Def.Label,
			obs.Def.Description,
			name,
			role,
			cfg.Common.Format.String(),
			obs.Def.Format,
			payloadLocation(cfg.Common.PayloadLocation),
			obs.Def.MediaType,
			obs.Def.Colorspace,
			obs.Def.Version,
		)
	}

	gauge(descHeadIndex, float64(rt.HeadIndex))
	gauge(descLastRead, float64(rt.LastReadTime))
	gauge(descLastWrite, float64(rt.LastWriteTime))
	gauge(descWriteAge, obs.WriteAge.Seconds())
	gauge(descLatency, float64(obs.LatencyGrains))
	gauge(descRateNum, float64(cfg.Common.GrainRate.Num))
	gauge(descRateDen, float64(cfg.Common.GrainRate.Den))
	gauge(descCommitBatch, float64(cfg.Common.MaxCommitBatchSizeHint))
	gauge(descSyncBatch, float64(cfg.Common.MaxSyncBatchSizeHint))

	if cfg.Common.Format.IsDiscrete() {
		for i, size := range cfg.Discrete.SliceSizes {
			if size == 0 {
				// SliceSizes is a fixed-width array of planes; a zero
				// entry is an unused plane, not a zero-sized one.
				continue
			}
			gauge(descSliceSize, float64(size), strconv.Itoa(i))
		}
		gauge(descRingGrains, float64(cfg.Discrete.GrainCount))
		return
	}

	gauge(descChannels, float64(cfg.Continuous.ChannelCount))
	gauge(descRingSamples, float64(cfg.Continuous.BufferLength))
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func payloadLocation(l mxl.PayloadLocation) string {
	switch l {
	case mxl.PayloadHostMemory:
		return "host"
	case mxl.PayloadDeviceMemory:
		return "device"
	default:
		return "unknown"
	}
}
