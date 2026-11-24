package heartbeat

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// forbiddingAPIServer serves the discovery documents a client needs to
// find Leases and answers everything else, the Lease list among it,
// with 403: an exporter without the Role, or watching a namespace the
// Role is not in. lists counts the Lease list attempts.
func forbiddingAPIServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var lists atomic.Int32
	write := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api":
			write(w, http.StatusOK, metav1.APIVersions{
				TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"},
			})
		case "/apis":
			gv := metav1.GroupVersionForDiscovery{GroupVersion: "coordination.k8s.io/v1", Version: "v1"}
			write(w, http.StatusOK, metav1.APIGroupList{
				TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
				Groups: []metav1.APIGroup{{
					Name: "coordination.k8s.io", Versions: []metav1.GroupVersionForDiscovery{gv}, PreferredVersion: gv,
				}},
			})
		case "/apis/coordination.k8s.io/v1":
			write(w, http.StatusOK, metav1.APIResourceList{
				TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
				GroupVersion: "coordination.k8s.io/v1",
				APIResources: []metav1.APIResource{{
					Name: "leases", Namespaced: true, Kind: "Lease",
					Verbs: metav1.Verbs{"get", "list", "watch"},
				}},
			})
		default:
			if strings.HasSuffix(r.URL.Path, "/leases") {
				lists.Add(1)
			}
			write(w, http.StatusForbidden, metav1.Status{
				TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
				Status:   metav1.StatusFailure, Reason: metav1.StatusReasonForbidden, Code: http.StatusForbidden,
				Message: "leases is forbidden",
			})
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &lists
}

// The exporter serves the domain metrics it exists for; the heartbeat
// metric is an addition read from the API. A Lease it may not list must
// leave /metrics answering with everything else, rather than hold the
// exporter in a cache sync that never completes, which fails its
// liveness probe and takes the domain metrics down with it.
func TestStartServesMetricsWhileTheLeasesAreForbidden(t *testing.T) {
	srv, lists := forbiddingAPIServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	began := time.Now()
	hb, err := Start(ctx, &rest.Config{Host: srv.URL}, "mxl-system", "node-a", testr.New(t))
	require.NoError(t, err)
	require.Less(t, time.Since(began), 5*time.Second, "Start must not wait for the Lease cache")
	require.Eventually(t, func() bool { return lists.Load() > 0 }, 10*time.Second, 20*time.Millisecond,
		"the cache never tried to list the Leases, so the forbidden case was not reached")

	other := prometheus.NewGauge(prometheus.GaugeOpts{Name: "mxl_domain_num_flows", Help: "stand-in"})
	reg := prometheus.NewRegistry()
	reg.MustRegister(other, hb)

	served := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		served <- rec
	}()
	select {
	case rec := <-served:
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "mxl_domain_num_flows")
		require.NotContains(t, rec.Body.String(), "mxl_mirror_heartbeat_age_seconds{")
	case <-time.After(5 * time.Second):
		t.Fatal("/metrics did not answer while the Lease cache cannot sync")
	}
}
