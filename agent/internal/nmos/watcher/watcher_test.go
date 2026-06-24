package watcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestWatcherInitialSyncIndexesOriginFlowsByDomain(t *testing.T) {
	scheme := newScheme(t)
	flow := flow("flow-a", "node-a")
	domain := domain("domain-a", "node-a")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(flow, domain).Build()

	w := New(c)
	require.NoError(t, w.Sync(context.Background()))

	flows := w.GetDomainFlows("domain-a")
	require.Len(t, flows, 1)
	require.Equal(t, "flow-a", flows[0].Name)
	require.Len(t, w.GetFlows(), 1)

	gotFlow, ok := w.GetFlow("flow-a")
	require.True(t, ok)
	require.Equal(t, "flow-a", gotFlow.Name)

	gotDomain, ok := w.GetDomain("domain-a")
	require.True(t, ok)
	require.Equal(t, "node-a", gotDomain.Spec.NodeName)
}

func TestWatcherRunTracksFlowAndDomainCRUDAndEmitsEvents(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	w := New(c, WithEventBuffer(16))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()

	select {
	case <-w.Started():
	case <-time.After(time.Second):
		t.Fatal("watcher did not start")
	}

	require.NoError(t, createDomain(t, c, domain("domain-a", "node-a")))
	requireEvent(t, w.Events(), Event{Kind: EventCreate, Resource: ResourceDomain, ID: "domain-a"})

	require.NoError(t, c.Create(ctx, flow("flow-a", "node-a")))
	require.Eventually(t, func() bool {
		return len(w.GetDomainFlows("domain-a")) == 1
	}, time.Second, 10*time.Millisecond)
	requireEvent(t, w.Events(), Event{Kind: EventCreate, Resource: ResourceFlow, ID: "flow-a"})

	var updated mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "flow-a"}, &updated))
	updated.Status.Locations[0].Phase = mxlv1alpha1.MxlFlowLocationReady
	require.NoError(t, c.Update(ctx, &updated))
	require.Eventually(t, func() bool {
		flows := w.GetDomainFlows("domain-a")
		return len(flows) == 0
	}, time.Second, 10*time.Millisecond)
	requireEvent(t, w.Events(), Event{Kind: EventUpdate, Resource: ResourceFlow, ID: "flow-a"})

	require.NoError(t, c.Delete(ctx, &updated))
	require.Eventually(t, func() bool {
		_, ok := w.GetFlow("flow-a")
		return !ok
	}, time.Second, 10*time.Millisecond)
	requireEvent(t, w.Events(), Event{Kind: EventDelete, Resource: ResourceFlow, ID: "flow-a"})
}

func TestWatcherQueriesAreConcurrentSafe(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	w := New(c)
	require.NoError(t, w.Sync(context.Background()))

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				w.applyFlow(flow("flow-a", "node-a"), EventUpdate)
				_ = w.GetDomainFlows("domain-a")
				_ = w.GetFlows()
				_, _ = w.GetFlow("flow-a")
				_, _ = w.GetDomain("domain-a")
			}
		}()
	}
	wg.Wait()
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, mxlv1alpha1.AddToScheme(scheme))
	return scheme
}

func flow(name, originNode string) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: name},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{NodeName: originNode, Phase: mxlv1alpha1.MxlFlowLocationOrigin}},
		},
	}
}

func domain(name, node string) *mxlv1alpha1.MxlDomain {
	return &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       mxlv1alpha1.MxlDomainSpec{NodeName: node, HostPath: "/run/mxl/domain"},
	}
}

func createDomain(t *testing.T, c client.Client, d *mxlv1alpha1.MxlDomain) error {
	t.Helper()
	return c.Create(context.Background(), d)
}

func requireEvent(t *testing.T, events <-chan Event, want Event) {
	t.Helper()
	select {
	case got := <-events:
		require.Equal(t, want.Kind, got.Kind)
		require.Equal(t, want.Resource, got.Resource)
		require.Equal(t, want.ID, got.ID)
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for event %#v", want)
	}
}
