// Package heartbeat exports how long ago each mirror side last made
// progress, read from the heartbeat Leases the gateways renew.
//
// Each side of a mirror has its own Lease (api/v1alpha1
// MirrorLeaseName), renewed by the gateway serving that side with the
// side's progress time: the last completed transfer for the source,
// the last committed grain for the target. The mirror's status carries
// these only as of its last phase transition.
package heartbeat

import (
	"context"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	coordinationv1 "k8s.io/api/coordination/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

var descAge = prometheus.NewDesc("mxl_mirror_heartbeat_age_seconds",
	"Seconds since this side of the mirror last made progress, as renewed into its heartbeat Lease by the "+
		"gateway serving it: the last completed transfer for the source, the last committed grain for the "+
		"target. The gateway publishes it at 5s resolution.",
	[]string{"namespace", "mirror", "side"}, nil)

// Collector exports mxl_mirror_heartbeat_age_seconds for the mirror
// sides this node's gateway serves.
type Collector struct {
	reader   client.Reader
	nodeName string
	now      func() time.Time
	log      logr.Logger
}

// New returns a collector reading the heartbeat Leases through reader
// and reporting those whose holder is nodeName.
//
// The holder is the node of the gateway that renews the Lease, so each
// side is reported by exactly one exporter, the one beside the gateway
// serving it, and its age is taken against the clock that wrote the
// renew time.
func New(reader client.Reader, nodeName string, log logr.Logger) *Collector {
	return &Collector{reader: reader, nodeName: nodeName, now: time.Now, log: log}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descAge
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	var leases coordinationv1.LeaseList
	if err := c.reader.List(context.Background(), &leases,
		client.InNamespace(mxlv1alpha1.LeaseNamespace),
		client.HasLabels{mxlv1alpha1.LabelMirrorSide}); err != nil {
		// No series rather than stale ones: an alert on the age must
		// not read a failed list as fresh heartbeats.
		c.log.Error(err, "list mirror heartbeat Leases")
		return
	}
	now := c.now()
	for i := range leases.Items {
		l := &leases.Items[i]
		if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != c.nodeName || l.Spec.RenewTime == nil {
			continue
		}
		namespace, name, ok := strings.Cut(l.Annotations[mxlv1alpha1.AnnotationMirror], "/")
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(descAge, prometheus.GaugeValue,
			now.Sub(l.Spec.RenewTime.Time).Seconds(),
			namespace, name, l.Labels[mxlv1alpha1.LabelMirrorSide])
	}
}
