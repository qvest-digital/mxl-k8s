package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/qvest-digital/mxl-k8s/agent/internal/config"
)

// apiServer answers discovery for core/v1 pods and a Get of pod ns/p,
// and counts the requests for core/v1's resources.
func apiServer(t *testing.T, discovery *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
		case "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`))
		case "/api/v1":
			discovery.Add(1)
			_, _ = w.Write([]byte(`{"kind":"APIResourceList","groupVersion":"v1","resources":[` +
				`{"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":["get","list"]}]}`))
		case "/api/v1/namespaces/ns/pods/p":
			_, _ = w.Write([]byte(`{"kind":"Pod","apiVersion":"v1","metadata":{"name":"p","namespace":"ns"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Readers asking for many flows spend the intent client's budget, not
// the main one's: flow publishing and Lease renewal request the same
// resources and must not wait behind them. The intent client is held to
// its own limit, and the two share one REST mapping, so the resources
// are discovered once.
func TestTheIntentClientsLimitIsItsOwn(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	var discovery atomic.Int32
	srv := apiServer(t, &discovery)
	clients, err := newClients(&rest.Config{Host: srv.URL}, scheme, 1000, 1000, 2, 1)
	require.NoError(t, err)
	require.NotSame(t, clients.main, clients.intent)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	get := func(c client.Client) error {
		return c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "p"}, &corev1.Pod{})
	}
	require.NoError(t, get(clients.main))
	// The intent client's one request of burst is spent; the next ones
	// wait 500 ms each for its limiter.
	require.NoError(t, get(clients.intent))
	var wg sync.WaitGroup
	var throttled time.Duration
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()
		for range 3 {
			_ = get(clients.intent)
		}
		throttled = time.Since(start)
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	require.NoError(t, get(clients.main))
	require.Less(t, time.Since(start), 250*time.Millisecond, "the main client waited for the intent client's limit")
	wg.Wait()
	assert.GreaterOrEqual(t, throttled, 900*time.Millisecond, "the intent client was not held to its limit")
	assert.Equal(t, int32(1), discovery.Load(), "the clients discovered the API each")
}

// Pod lookups and on-demand mirrors are what readers ask of the API, so
// they run on the intent client, the dispatcher resolving pids through
// the resolver it is handed.
func TestReadersAskTheAPIThroughTheIntentClient(t *testing.T) {
	clients := apiClients{main: fake.NewClientBuilder().Build(), intent: fake.NewClientBuilder().Build()}
	pods := clients.pods("n1")
	assert.Same(t, clients.intent, pods.Client, "pod lookups run on the main client")
	assert.Equal(t, "n1", pods.NodeName)
	d := clients.dispatcher(&config.Config{NodeName: "n1", DomainPath: "/run/mxl/domain", IntentMirrorBurst: 8}, pods)
	assert.Same(t, clients.intent, d.Client, "on-demand mirrors run on the main client")
	assert.Same(t, pods, d.Resolver)
	assert.Equal(t, 8, d.Limits.Burst)
}
