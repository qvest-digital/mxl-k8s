// Command mxl-flow-exporter serves Prometheus metrics for the MXL
// domain on the node it runs on.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/collector"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/config"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/domain"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/heartbeat"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/numa"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/topology"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/wanted"
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	fs := flag.NewFlagSet("mxl-flow-exporter", flag.ContinueOnError)
	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(fs)

	cfg, err := config.FromFlags(fs, os.Args[1:])
	if err != nil {
		// The flags carry the logger's own configuration, so a
		// configuration error predates it. Installing a default logger
		// first keeps the reason the process refused to start from being
		// the one message it cannot emit.
		ctrl.SetLogger(zap.New())
		setupLog.Error(err, "configuration")
		os.Exit(1)
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, ctrl.Log.WithName("exporter")); err != nil {
		setupLog.Error(err, "exporter exited with error")
		os.Exit(1)
	}
}

// scannedCollector exports what its last Scan of the domains read.
type scannedCollector interface {
	prometheus.Collector
	Scan(paths []string)
}

// mxlfsCounters returns the collectors of the mxlfs module's counters
// that every scan reads: the waits for missing flows always, and the
// NUMA placement with numaMetrics.
func mxlfsCounters(numaMetrics bool, log logr.Logger) []scannedCollector {
	out := []scannedCollector{wanted.New(log.WithName("wanted"))}
	if numaMetrics {
		out = append(out, numa.New(log.WithName("numa")))
	}
	return out
}

func run(ctx context.Context, cfg *config.Config, log logr.Logger) error {
	doms, err := domain.OpenSet(cfg.DomainPath, cfg.FlowLifetime)
	if err != nil {
		return err
	}
	defer func() { _ = doms.Close() }()

	// The node name labels every series this process emits, so a flow
	// mirrored onto three nodes is three samples rather than three
	// indistinguishable copies.
	reg := prometheus.NewRegistry()
	nodeReg := prometheus.WrapRegistererWith(prometheus.Labels{"node": cfg.NodeName}, reg)

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	flows := func() []collector.Source {
		var out []collector.Source
		for _, d := range doms.Domains() {
			out = append(out, d)
		}
		return out
	}
	domains := func() []collector.DomainSource {
		var out []collector.DomainSource
		for _, d := range doms.Domains() {
			out = append(out, d)
		}
		return out
	}
	nodeReg.MustRegister(
		collector.NewFlowCollector(flows),
		collector.NewDomainCollector(domains, log.WithName("domain")),
	)

	if cfg.TopologyEnabled || cfg.MirrorHeartbeats {
		reader, err := startCache(ctx, cfg, log)
		if err != nil {
			return err
		}
		if cfg.TopologyEnabled {
			nodeReg.MustRegister(topology.New(reader, cfg.NodeName, log.WithName("topology")))
		}
		if cfg.MirrorHeartbeats {
			nodeReg.MustRegister(heartbeat.New(reader, cfg.NodeName, log.WithName("heartbeat")))
		}
	}

	counters := mxlfsCounters(cfg.MxlfsNumaMetrics, log)
	for _, c := range counters {
		nodeReg.MustRegister(c)
	}
	scan := func() error {
		err := doms.Scan()
		var paths []string
		for _, d := range doms.Domains() {
			paths = append(paths, d.Path())
		}
		for _, c := range counters {
			c.Scan(paths)
		}
		return err
	}
	if err := scan(); err != nil {
		// A node that has never carried a flow has no domain directory
		// yet. Failing to boot on that would take the DaemonSet down
		// on exactly the nodes with nothing to report.
		log.Error(err, "initial domain scan", "primary", cfg.DomainPath)
	}
	go scanLoop(ctx, scan, cfg.ScanPeriod, log)

	return serve(ctx, cfg, reg, log)
}

// startCache brings up a controller-runtime cache over what the
// enabled collectors read, MxlFlow and the mirror heartbeat Leases, so
// a scrape reads from a watch-backed store instead of listing against
// the API server. Both are watched from the start, so the exporter
// serves only once they have synced.
//
// The Leases are watched in their namespace and only those carrying
// the mirror side label: the Origin Leases there are none of the
// exporter's business.
func startCache(ctx context.Context, cfg *config.Config, log logr.Logger) (client.Reader, error) {
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := mxlv1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	mirrorSide, err := labels.NewRequirement(mxlv1alpha1.LabelMirrorSide, selection.Exists, nil)
	if err != nil {
		return nil, err
	}
	c, err := cache.New(restCfg, cache.Options{
		Scheme: scheme,
		ByObject: map[client.Object]cache.ByObject{
			&coordinationv1.Lease{}: {
				Namespaces: map[string]cache.Config{mxlv1alpha1.LeaseNamespace: {}},
				Label:      labels.NewSelector().Add(*mirrorSide),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	var watched []client.Object
	if cfg.TopologyEnabled {
		watched = append(watched, &mxlv1alpha1.MxlFlow{})
	}
	if cfg.MirrorHeartbeats {
		watched = append(watched, &coordinationv1.Lease{})
	}
	for _, obj := range watched {
		if _, err := c.GetInformer(ctx, obj); err != nil {
			return nil, err
		}
	}
	go func() {
		if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error(err, "API cache")
		}
	}()
	if !c.WaitForCacheSync(ctx) {
		return nil, errors.New("API cache did not sync")
	}
	return c, nil
}

// scanLoop re-lists the domains until ctx is done.
func scanLoop(ctx context.Context, scan func() error, period time.Duration, log logr.Logger) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := scan(); err != nil {
				log.Error(err, "scan domains")
			}
		}
	}
}

// serve runs the metrics and probe endpoints until ctx is done.
func serve(ctx context.Context, cfg *config.Config, reg *prometheus.Registry, log logr.Logger) error {
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}))

	probeMux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	probeMux.HandleFunc("/healthz", ok)
	probeMux.HandleFunc("/readyz", ok)

	metrics := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux, ReadHeaderTimeout: 3 * time.Second}
	probes := &http.Server{Addr: cfg.ProbeAddr, Handler: probeMux, ReadHeaderTimeout: 3 * time.Second}

	errs := make(chan error, 2)
	for _, srv := range []*http.Server{metrics, probes} {
		go func(s *http.Server) {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs <- err
			}
		}(srv)
	}
	log.Info("listening", "metrics", cfg.MetricsAddr, "probes", cfg.ProbeAddr, "domain", cfg.DomainPath, "node", cfg.NodeName)

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metrics.Shutdown(shutdownCtx)
	_ = probes.Shutdown(shutdownCtx)
	return nil
}
