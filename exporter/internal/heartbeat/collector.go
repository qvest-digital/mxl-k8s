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
	"errors"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// syncWarnAfter is how long the Lease cache may take to sync before
// the exporter says why the metric is missing.
const syncWarnAfter = time.Minute

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
	// synced reports whether reader can answer. Nil means it always
	// can.
	synced func() bool
}

// Start brings up a cache of the mirror heartbeat Leases in namespace
// and returns a collector reading them, without waiting for the cache
// to sync. Until it has, and for as long as the Leases cannot be
// listed -- no Role, or a namespace the Role is not in -- the collector
// reports nothing, so the exporter's other metrics never wait on this
// one. Only Leases carrying the mirror side label are cached: the
// Origin Leases beside them are none of the exporter's business.
func Start(ctx context.Context, cfg *rest.Config, namespace, nodeName string, log logr.Logger) (*Collector, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	side, err := labels.NewRequirement(mxlv1alpha1.LabelMirrorSide, selection.Exists, nil)
	if err != nil {
		return nil, err
	}
	c, err := cache.New(cfg, cache.Options{
		Scheme: scheme,
		ByObject: map[client.Object]cache.ByObject{
			&coordinationv1.Lease{}: {
				Namespaces: map[string]cache.Config{namespace: {}},
				Label:      labels.NewSelector().Add(*side),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	// Registered before the cache starts, so this returns at once.
	inf, err := c.GetInformer(ctx, &coordinationv1.Lease{})
	if err != nil {
		return nil, err
	}
	go func() {
		if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error(err, "mirror heartbeat Lease cache")
		}
	}()
	go func() {
		wctx, cancel := context.WithTimeout(ctx, syncWarnAfter)
		defer cancel()
		if !c.WaitForCacheSync(wctx) && ctx.Err() == nil {
			log.Info("mirror heartbeat Leases not readable yet; mxl_mirror_heartbeat_age_seconds stays empty until they are",
				"namespace", namespace, "waited", syncWarnAfter.String())
		}
	}()
	hb := New(c, nodeName, log)
	hb.synced = inf.HasSynced
	return hb, nil
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
	if c.synced != nil && !c.synced() {
		// A List on an unsynced cache waits for the sync, which a
		// Lease the exporter cannot read never finishes.
		return
	}
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
