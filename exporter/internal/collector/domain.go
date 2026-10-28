package collector

import (
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/qvest-digital/mxl-k8s/exporter/internal/domain"
)

var domainLabels = []string{"domain"}

var (
	descNumFlows = prometheus.NewDesc("mxl_domain_num_flows",
		"Flows currently present in the domain.", domainLabels, nil)
	descDomainSize = prometheus.NewDesc("mxl_domain_size_bytes",
		"Combined on-disk size of the domain's flow data.", domainLabels, nil)
	descFSTotal = prometheus.NewDesc("mxl_domain_fs_space_total_bytes",
		"Capacity of the filesystem backing the domain.", domainLabels, nil)
	descFSAvail = prometheus.NewDesc("mxl_domain_fs_space_available_bytes",
		"Free space on the filesystem backing the domain.", domainLabels, nil)
	descFSUsed = prometheus.NewDesc("mxl_domain_fs_space_used_bytes",
		"Used space on the filesystem backing the domain.", domainLabels, nil)
)

// DomainSource is what a DomainCollector reads on each scrape.
type DomainSource interface {
	Source
	SizeBytes() uint64
	Stat() (domain.FSStat, error)
}

// DomainCollector exports the domain-wide counters of each domain.
type DomainCollector struct {
	list func() []DomainSource
	log  logr.Logger
}

// NewDomainCollector returns a collector over the domains list returns
// at each scrape.
func NewDomainCollector(list func() []DomainSource, log logr.Logger) *DomainCollector {
	return &DomainCollector{list: list, log: log}
}

// Describe implements prometheus.Collector.
func (c *DomainCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descNumFlows, descDomainSize, descFSTotal, descFSAvail, descFSUsed,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *DomainCollector) Collect(ch chan<- prometheus.Metric) {
	for _, src := range c.list() {
		c.collectDomain(ch, src)
	}
}

func (c *DomainCollector) collectDomain(ch chan<- prometheus.Metric, src DomainSource) {
	dom := src.Path()

	var present int
	for _, obs := range src.Observe() {
		if obs.Present {
			present++
		}
	}
	ch <- prometheus.MustNewConstMetric(descNumFlows, prometheus.GaugeValue, float64(present), dom)
	ch <- prometheus.MustNewConstMetric(descDomainSize, prometheus.GaugeValue, float64(src.SizeBytes()), dom)

	st, err := src.Stat()
	if err != nil {
		// The domain directory can be absent on a node that has never
		// carried a flow. Skipping the filesystem series leaves a gap
		// rather than a wrong zero.
		c.log.Error(err, "statfs domain", "domain", dom)
		return
	}
	ch <- prometheus.MustNewConstMetric(descFSTotal, prometheus.GaugeValue, float64(st.TotalBytes), dom)
	ch <- prometheus.MustNewConstMetric(descFSAvail, prometheus.GaugeValue, float64(st.AvailableBytes), dom)
	ch <- prometheus.MustNewConstMetric(descFSUsed, prometheus.GaugeValue, float64(st.UsedBytes), dom)
}
