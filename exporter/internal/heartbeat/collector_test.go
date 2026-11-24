package heartbeat

import (
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

var now = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// heartbeat is the Lease a gateway on holder renews for one side of the
// mirror namespace/name, last at renewed.
func heartbeat(namespace, name string, side mxlv1alpha1.MirrorSide, holder string, renewed time.Time) *coordinationv1.Lease {
	at := metav1.NewMicroTime(renewed)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   mxlv1alpha1.LeaseNamespace,
			Name:        mxlv1alpha1.MirrorLeaseName(namespace, name, side),
			Labels:      map[string]string{mxlv1alpha1.LabelMirrorSide: string(side)},
			Annotations: map[string]string{mxlv1alpha1.AnnotationMirror: namespace + "/" + name},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, RenewTime: &at},
	}
}

func newCollector(t *testing.T, node string, objs ...client.Object) *Collector {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	c := New(fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(), node, testr.New(t))
	c.now = func() time.Time { return now }
	return c
}

const help = `
# HELP mxl_mirror_heartbeat_age_seconds Seconds since this side of the mirror last made progress, as renewed into its heartbeat Lease by the gateway serving it: the last completed transfer for the source, the last committed grain for the target. The gateway publishes it at 5s resolution.
# TYPE mxl_mirror_heartbeat_age_seconds gauge
`

// Each side is reported by the exporter beside the gateway that renews
// it, and by no other: the source and the target of one mirror sit on
// two nodes, and a cluster-wide report from every exporter would put
// each age on every node.
func TestCollectReportsOnlyTheSidesThisNodeServes(t *testing.T) {
	objs := []client.Object{
		heartbeat("prod", "video--node-b", mxlv1alpha1.MirrorSideSource, "node-a", now.Add(-2*time.Second)),
		heartbeat("prod", "video--node-b", mxlv1alpha1.MirrorSideTarget, "node-b", now.Add(-3*time.Second)),
		heartbeat("prod", "audio--node-a", mxlv1alpha1.MirrorSideTarget, "node-a", now.Add(-90*time.Second)),
	}

	require.NoError(t, testutil.CollectAndCompare(newCollector(t, "node-a", objs...), strings.NewReader(help+`
mxl_mirror_heartbeat_age_seconds{mirror="audio--node-a",mirror_namespace="prod",side="target"} 90
mxl_mirror_heartbeat_age_seconds{mirror="video--node-b",mirror_namespace="prod",side="source"} 2
`), "mxl_mirror_heartbeat_age_seconds"))

	require.NoError(t, testutil.CollectAndCompare(newCollector(t, "node-b", objs...), strings.NewReader(help+`
mxl_mirror_heartbeat_age_seconds{mirror="video--node-b",mirror_namespace="prod",side="target"} 3
`), "mxl_mirror_heartbeat_age_seconds"))
}

// The Origin Leases share the namespace. A Lease without the side label,
// without a renew time or without the mirror it belongs to is no
// heartbeat this metric can name.
func TestCollectSkipsLeasesThatAreNoHeartbeat(t *testing.T) {
	holder := "node-a"
	at := metav1.NewMicroTime(now)
	origin := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: mxlv1alpha1.LeaseNamespace, Name: "origin"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, RenewTime: &at},
	}
	unrenewed := heartbeat("prod", "a", mxlv1alpha1.MirrorSideSource, "node-a", now)
	unrenewed.Spec.RenewTime = nil
	unnamed := heartbeat("prod", "b", mxlv1alpha1.MirrorSideSource, "node-a", now)
	unnamed.Annotations = nil
	elsewhere := heartbeat("prod", "c", mxlv1alpha1.MirrorSideSource, "node-a", now)
	elsewhere.Namespace = "default"

	c := newCollector(t, "node-a", origin, unrenewed, unnamed, elsewhere)
	require.Equal(t, 0, testutil.CollectAndCount(c, "mxl_mirror_heartbeat_age_seconds"))
}

// Two mirrors may share a name in two namespaces, both served by one
// node. Each is its own series, told apart by the mirror's namespace.
func TestCollectSeparatesSameNamedMirrorsByNamespace(t *testing.T) {
	c := newCollector(t, "node-a",
		heartbeat("prod", "video--node-b", mxlv1alpha1.MirrorSideSource, "node-a", now.Add(-2*time.Second)),
		heartbeat("staging", "video--node-b", mxlv1alpha1.MirrorSideSource, "node-a", now.Add(-40*time.Second)),
	)

	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(help+`
mxl_mirror_heartbeat_age_seconds{mirror="video--node-b",mirror_namespace="prod",side="source"} 2
mxl_mirror_heartbeat_age_seconds{mirror="video--node-b",mirror_namespace="staging",side="source"} 40
`), "mxl_mirror_heartbeat_age_seconds"))
}

// A ServiceMonitor scrape sets these labels on every series to the
// scraped pod's own, and a series that already carries one keeps its
// value only as exported_<label>. The mirror's namespace under
// "namespace" would be read as the exporter's, and same-named mirrors
// of two namespaces would only be told apart by a label no query uses.
func TestCollectUsesNoLabelAScrapeOverwrites(t *testing.T) {
	c := newCollector(t, "node-a",
		heartbeat("prod", "video--node-b", mxlv1alpha1.MirrorSideSource, "node-a", now))
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	for _, m := range families[0].GetMetric() {
		for _, l := range m.GetLabel() {
			require.NotContains(t,
				[]string{"namespace", "pod", "service", "container", "endpoint", "job", "instance"},
				l.GetName())
		}
	}
}
