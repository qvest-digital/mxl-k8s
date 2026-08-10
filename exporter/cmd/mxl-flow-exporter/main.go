// Command mxl-flow-exporter serves Prometheus metrics for the MXL
// domain on the node it runs on.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/collector"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/config"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/domain"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/topology"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.FromFlags(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:])
	if err != nil {
		log.Error("configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("exporter", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	dom, err := domain.Open(cfg.DomainPath, cfg.FlowLifetime)
	if err != nil {
		return err
	}
	defer func() { _ = dom.Close() }()

	// The node name labels every series this process emits, so a flow
	// mirrored onto three nodes is three samples rather than three
	// indistinguishable copies.
	reg := prometheus.NewRegistry()
	nodeReg := prometheus.WrapRegistererWith(prometheus.Labels{"node": cfg.NodeName}, reg)

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	nodeReg.MustRegister(
		collector.NewFlowCollector(dom),
		collector.NewDomainCollector(dom, log),
	)

	if cfg.TopologyEnabled {
		reader, err := startTopologyCache(ctx, cfg, log)
		if err != nil {
			return err
		}
		nodeReg.MustRegister(topology.New(reader, cfg.NodeName, log))
	}

	if err := dom.Scan(); err != nil {
		// A node that has never carried a flow has no domain directory
		// yet. Failing to boot on that would take the DaemonSet down
		// on exactly the nodes with nothing to report.
		log.Warn("initial domain scan", "domain", cfg.DomainPath, "error", err)
	}
	go scanLoop(ctx, dom, cfg.ScanPeriod, log)

	return serve(ctx, cfg, reg, log)
}

// startTopologyCache brings up a controller-runtime cache over
// MxlFlow so a scrape reads from a watch-backed store instead of
// listing against the API server.
func startTopologyCache(ctx context.Context, cfg *config.Config, log *slog.Logger) (client.Reader, error) {
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
	c, err := cache.New(restCfg, cache.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	go func() {
		if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("topology cache", "error", err)
		}
	}()
	if !c.WaitForCacheSync(ctx) {
		return nil, errors.New("topology cache did not sync")
	}
	return c, nil
}

// scanLoop re-lists the domain until ctx is done.
func scanLoop(ctx context.Context, dom *domain.Domain, period time.Duration, log *slog.Logger) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := dom.Scan(); err != nil {
				log.Warn("scan domain", "error", err)
			}
		}
	}
}

// serve runs the metrics and probe endpoints until ctx is done.
func serve(ctx context.Context, cfg *config.Config, reg *prometheus.Registry, log *slog.Logger) error {
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
