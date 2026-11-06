package placement

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func capsObj(name string, ifs ...mxlv1alpha1.MxlFabricInterface) *mxlv1alpha1.MxlNodeCapabilities {
	return &mxlv1alpha1.MxlNodeCapabilities{ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:   mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: name},
		Status: mxlv1alpha1.MxlNodeCapabilitiesStatus{Providers: []mxlv1alpha1.MxlFabricsProviderCapability{{Name: tcp, Interfaces: ifs}}}}
}

// The cache a placement reads lags the placement the previous reconcile
// wrote. Placed against that stale view alone, a second mirror sees the
// first one's interfaces as idle and lands beside it; the controller
// keeps its own writes until the cache shows them.
func TestReconcileCountsItsOwnWritesTheCacheHasNotShown(t *testing.T) {
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	flow := func(n int) *mxlv1alpha1.MxlFlow {
		return &mxlv1alpha1.MxlFlow{ObjectMeta: metav1.ObjectMeta{Name: flowID(n)},
			Spec:   mxlv1alpha1.MxlFlowSpec{ID: flowID(n), Definition: runtime.RawExtension{Raw: []byte(`{}`)}},
			Status: mxlv1alpha1.MxlFlowStatus{BitsPerSecond: 3 * gbit}}
	}
	mirror := func(n int) *mxlv1alpha1.MxlFlowMirror {
		return &mxlv1alpha1.MxlFlowMirror{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: flowID(n)},
			Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: flowID(n), SourceNode: "src", TargetNode: "tgt", Provider: tcp}}
	}
	objs := []client.Object{capsObj("src", twoRails(1)...), capsObj("tgt", twoRails(2)...), flow(1), flow(2), mirror(1), mirror(2)}

	// Every List of mirrors answers with the objects as they were before
	// anything was placed.
	var stale mxlv1alpha1.MxlFlowMirrorList
	for _, n := range []int{1, 2} {
		stale.Items = append(stale.Items, *mirror(n))
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlNodeCapabilities{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if l, ok := list.(*mxlv1alpha1.MxlFlowMirrorList); ok {
				stale.DeepCopyInto(l)
				return nil
			}
			return cl.List(ctx, list, opts...)
		}}).Build()
	r := &Reconciler{Client: c, Settings: defaults, DemandWait: time.Second}

	placementOf := func(n int) mxlv1alpha1.MxlFlowMirrorPlacement {
		_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: flowID(n)}})
		require.NoError(t, err)
		var m mxlv1alpha1.MxlFlowMirror
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: flowID(n)}, &m))
		require.NotNil(t, m.Spec.Placement)
		return *m.Spec.Placement
	}
	first, second := placementOf(1), placementOf(2)
	require.Equal(t, "192.0.2.1", first.SourceInterface)
	require.NotEqual(t, first.SourceInterface, second.SourceInterface, "the second mirror takes the rail the first one was just written to")
	require.NotEqual(t, first.TargetInterface, second.TargetInterface)
}

// placementSamples is the number of observations placementSeconds holds.
func placementSamples(t *testing.T) uint64 {
	t.Helper()
	var d dto.Metric
	require.NoError(t, placementSeconds.Write(&d))
	return d.GetHistogram().GetSampleCount()
}

// fakeMirrors builds a client holding two nodes with one rail each, a
// flow publishing its rate for each mirror, and the mirrors.
func fakeMirrors(t *testing.T, funcs interceptor.Funcs, mirrors ...*mxlv1alpha1.MxlFlowMirror) client.WithWatch {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	objs := []client.Object{capsObj("src", twoRails(1)...), capsObj("tgt", twoRails(2)...)}
	for _, m := range mirrors {
		objs = append(objs, &mxlv1alpha1.MxlFlow{ObjectMeta: metav1.ObjectMeta{Name: m.Spec.FlowID},
			Spec:   mxlv1alpha1.MxlFlowSpec{ID: m.Spec.FlowID, Definition: runtime.RawExtension{Raw: []byte(`{}`)}},
			Status: mxlv1alpha1.MxlFlowStatus{BitsPerSecond: gbit}}, m)
	}
	return fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlNodeCapabilities{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
}

func mirrorCreatedAt(n int, at time.Time) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: flowID(n), CreationTimestamp: metav1.NewTime(at)},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: flowID(n), SourceNode: "src", TargetNode: "tgt", Provider: tcp}}
}

// A mirror that existed before the controller started, and is placed
// for the first time once it runs, did not wait on placement for its
// whole age; only a mirror created since the start measures placement
// latency.
func TestReconcileObservesPlacementOnlyOfMirrorsCreatedSinceStart(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := start
	c := fakeMirrors(t, interceptor.Funcs{}, mirrorCreatedAt(1, start.Add(-time.Hour)), mirrorCreatedAt(2, start.Add(time.Second)))
	r := &Reconciler{Client: c, Settings: defaults, DemandWait: time.Second, now: func() time.Time { return now }}
	reconcileAndGet := func(n int) *mxlv1alpha1.MxlFlowMirror {
		key := types.NamespacedName{Namespace: "ns", Name: flowID(n)}
		_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		var m mxlv1alpha1.MxlFlowMirror
		require.NoError(t, c.Get(context.Background(), key, &m))
		return &m
	}

	before := placementSamples(t)
	require.NotNil(t, reconcileAndGet(1).Spec.Placement)
	require.Equal(t, before, placementSamples(t), "a mirror older than the controller adds no sample")

	now = start.Add(2 * time.Second)
	require.NotNil(t, reconcileAndGet(2).Spec.Placement)
	require.Equal(t, before+1, placementSamples(t), "a mirror created since the start adds one sample")
}

// The gateway rewrites lastSeen on every resync and the agents rewrite
// every flow's locations on every rescan; neither changes a placement.
func TestWatchPredicatesPassOnlyWhatPlacementReads(t *testing.T) {
	caps := capsObj("n", twoRails(1)...)
	seen := caps.DeepCopy()
	now := metav1.Now()
	seen.Status.LastSeen = &now
	require.False(t, placementInputsChanged().Update(event.UpdateEvent{ObjectOld: caps, ObjectNew: seen}))
	down := caps.DeepCopy()
	down.Status.Providers[0].Interfaces[0].LinkState = "down"
	require.True(t, placementInputsChanged().Update(event.UpdateEvent{ObjectOld: caps, ObjectNew: down}))
	util := caps.DeepCopy()
	util.Status.MaxUtilizationPercent = 50
	require.True(t, placementInputsChanged().Update(event.UpdateEvent{ObjectOld: caps, ObjectNew: util}))

	f := &mxlv1alpha1.MxlFlow{}
	rescan := f.DeepCopy()
	rescan.Status.Locations = []mxlv1alpha1.MxlFlowLocation{{NodeName: "n", Phase: mxlv1alpha1.MxlFlowLocationOrigin}}
	require.False(t, flowRateChanged().Update(event.UpdateEvent{ObjectOld: f, ObjectNew: rescan}))
	rate := f.DeepCopy()
	rate.Status.BitsPerSecond = gbit
	require.True(t, flowRateChanged().Update(event.UpdateEvent{ObjectOld: f, ObjectNew: rate}))
}

// A port that goes away loses its series; the others keep theirs
// throughout, rather than all of them vanishing between a reset and the
// next set.
func TestGaugesDeleteOnlyTheSeriesOfPortsGone(t *testing.T) {
	var g gauges
	nodes := map[string]Node{"a": twoPort("a", "192.0.2.201", "192.0.2.202", 10*gbit)}
	l := NewLedger([]Node{nodes["a"]}, []Placed{{SourceNode: "a", TargetNode: "b", Placement: place("192.0.2.201", "x"), Demand: gbit}})
	g.publish(l, nodes, defaults)
	require.Equal(t, float64(gbit), testutil.ToFloat64(committedBits.WithLabelValues("a", "192.0.2.201", "egress")))

	nodes["a"] = node("a", iface("192.0.2.201", railA, 10*gbit))
	g.publish(NewLedger([]Node{nodes["a"]}, nil), nodes, defaults)
	require.Equal(t, []string{"192.0.2.201"}, portsOf(t, "a"), "the gone port's series is deleted, the other's kept")
}

// portsOf lists the ports committedBits holds an egress series for on node.
func portsOf(t *testing.T, node string) []string {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	committedBits.Collect(ch)
	close(ch)
	var out []string
	for m := range ch {
		var d dto.Metric
		require.NoError(t, m.Write(&d))
		labels := map[string]string{}
		for _, l := range d.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["node"] == node && labels["direction"] == "egress" {
			out = append(out, labels["port"])
		}
	}
	return out
}
