// Package topology exports which node holds which role for a flow,
// read from the MxlFlow CRs the agent publishes.
//
// A domain is shared by every pod on its node, so nothing in the
// domain itself says whether a flow is produced here or mirrored in
// for a local reader. The control plane already records that:
// status.locations carries one entry per node, and its phase is Origin
// exactly on the node the writer runs on. Joining the domain metrics
// against this on (flow_id, node) is what separates a producer from a
// consumer, without any media function having to label itself.
package topology

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

var descLocation = prometheus.NewDesc("mxl_flow_location_info",
	"1 for the phase this flow is in on this node. Phase is Origin on the node the writer runs on.",
	[]string{"flow_id", "phase"}, nil)

// Collector exports mxl_flow_location_info for the local node.
type Collector struct {
	reader   client.Reader
	nodeName string
	log      logr.Logger
}

// New returns a collector reading MxlFlow through reader and reporting
// only the entries for nodeName. Restricting to the local node is what
// keeps every node's exporter from re-exporting the whole cluster's
// flows as duplicate series.
func New(reader client.Reader, nodeName string, log logr.Logger) *Collector {
	return &Collector{reader: reader, nodeName: nodeName, log: log}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descLocation
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	var flows mxlv1alpha1.MxlFlowList
	if err := c.reader.List(context.Background(), &flows); err != nil {
		// Emitting nothing leaves a gap the dashboard shows as no
		// data, which is honest; emitting zeros would read as "no
		// producer anywhere".
		c.log.Error(err, "list MxlFlow")
		return
	}
	for i := range flows.Items {
		flow := &flows.Items[i]
		for _, loc := range flow.Status.Locations {
			if loc.NodeName != c.nodeName {
				continue
			}
			ch <- prometheus.MustNewConstMetric(descLocation, prometheus.GaugeValue, 1,
				flow.Spec.ID, string(loc.Phase))
		}
	}
}
