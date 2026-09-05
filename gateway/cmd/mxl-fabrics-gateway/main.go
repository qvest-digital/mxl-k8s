package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on http.DefaultServeMux
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/capabilities"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/config"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/domaingc"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/mirror"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/rdma"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(mxlv1alpha1.AddToScheme(scheme))
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		setupLog.Error(err, "gateway exited with error")
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("mxl-fabrics-gateway", flag.ContinueOnError)
	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(fs)
	cfg, err := config.FromFlags(fs, args)
	if err != nil {
		return err
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	// client-go defaults to 5 QPS / 10 burst when the limits are left
	// zero; the per-mirror status flushers alone exceed that with a
	// handful of flowing mirrors on the node.
	restCfg.QPS = float32(cfg.KubeAPIQPS)
	restCfg.Burst = cfg.KubeAPIBurst

	// Open libmxl handles up front so any misconfiguration (bad
	// domain path, missing .so) fails before the manager comes up.
	handles, err := instance.Open(cfg.DomainPath)
	if err != nil {
		return fmt.Errorf("open libmxl: %w", err)
	}
	defer func() { _ = handles.Close() }()

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: cfg.MetricsAddr},
		HealthProbeBindAddress: cfg.ProbeAddr,
	})
	if err != nil {
		return fmt.Errorf("construct manager: %w", err)
	}

	if cfg.PprofAddr != "" {
		pprofSrv := &http.Server{
			Addr:              cfg.PprofAddr,
			Handler:           http.DefaultServeMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				setupLog.Error(err, "pprof server")
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pprofSrv.Shutdown(shutdownCtx)
		}()
	}

	// One selector for the whole process: the reconcilers bind what
	// the publisher advertises, and nothing else.
	selector := cfg.Selector()

	targetReconciler := &mirror.TargetReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		NodeName:      cfg.NodeName,
		Selector:      selector,
		Handles:       handles,
		DomainPath:    cfg.DomainPath,
		DegradedAfter: cfg.DegradedAfter,
		Recorder:      mgr.GetEventRecorderFor("mxl-target-gateway"),
	}
	if err := targetReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup target reconciler: %w", err)
	}
	sourceReconciler := &mirror.SourceReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		NodeName:         cfg.NodeName,
		Selector:         selector,
		Handles:          handles,
		ReaderStallAfter: cfg.ReaderStallAfter,
		PacingFraction:   cfg.PacingFraction,
		PacingChunks:     cfg.PacingChunks,
		Recorder:         mgr.GetEventRecorderFor("mxl-source-gateway"),
	}
	if err := sourceReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup source reconciler: %w", err)
	}

	// Both reconcilers hold per-mirror byte counters the collector
	// reads on scrape, so the metrics server already listening on
	// --metrics-bind-address serves them beside controller-runtime's.
	metrics.Registry.MustRegister(&mirror.ThroughputCollector{
		NodeName: cfg.NodeName,
		Source:   sourceReconciler,
		Target:   targetReconciler,
	})

	// Built whether or not it gates anything, so the publisher's
	// observer wiring does not depend on the flag; only the health
	// check registration below does.
	enumerationGate := &capabilities.EnumerationGate{Grace: cfg.RDMAEnumerationGrace}

	// MxlNodeCapabilities publisher runs as a Manager Runnable so it
	// joins the leader-election / shutdown lifecycle and only fires
	// once the cache has synced.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		pub := &capabilities.Publisher{
			Client:             mgr.GetClient(),
			APIReader:          mgr.GetAPIReader(),
			Recorder:           mgr.GetEventRecorderFor("mxl-node-capabilities"),
			NodeName:           cfg.NodeName,
			Providers:          cfg.Providers,
			Lister:             handles.Fabrics(),
			Selector:           selector,
			HostDevices:        rdma.Inventory{},
			HostDeviceObserver: enumerationGate,
			ProbePeriod:        cfg.ProbePeriod,
		}
		if err := pub.EnsureExists(ctx); err != nil {
			return err
		}
		pub.RunRefreshLoop(ctx, cfg.ResyncPeriod)
		return nil
	})); err != nil {
		return fmt.Errorf("register capabilities runnable: %w", err)
	}

	// Reclaiming abandoned flow directories is a Manager Runnable so
	// it starts only once the caches have synced, which is the first
	// half of not collecting a mirror copy the target reconciler has
	// yet to re-establish; the sweeper's own grace is the second.
	sweeper := &domaingc.Sweeper{
		DomainPath:    cfg.DomainPath,
		Interval:      cfg.DomainGCInterval,
		Grace:         cfg.DomainGCGrace,
		ScaffoldGrace: cfg.DomainGCScaffoldGrace,
		Log:           ctrl.Log.WithName("domaingc"),
	}
	if inst := handles.MXL(); inst != nil {
		sweeper.Collector = inst
	}
	if err := mgr.Add(manager.RunnableFunc(sweeper.Start)); err != nil {
		return fmt.Errorf("add domain gc sweeper: %w", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("register healthz: %w", err)
	}
	// Registered on healthz rather than readyz because the remedy is a
	// restart: the enumeration is fixed for the life of the process,
	// and withdrawing the pod from its Service would stop its counters
	// being collected without revisiting it.
	if cfg.RDMAEnumerationLiveness {
		if err := mgr.AddHealthzCheck("rdma-enumeration", enumerationGate.Check); err != nil {
			return fmt.Errorf("register rdma enumeration healthz: %w", err)
		}
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("register readyz: %w", err)
	}

	setupLog.Info("gateway started",
		"node", cfg.NodeName,
		"domainPath", cfg.DomainPath,
		"bindAddress", cfg.BindAddress,
		"providers", providerNames(cfg.Providers),
		"fabricCIDRs", cfg.FabricCIDRs,
		"fabricDevices", cfg.FabricDevices,
		"fabricMinLinkSpeed", cfg.FabricMinLinkSpeed,
		"probeAddr", cfg.ProbeAddr,
		"pprofAddr", cfg.PprofAddr,
		"resyncPeriod", cfg.ResyncPeriod,
		"probePeriod", cfg.ProbePeriod,
		"rdmaEnumerationLiveness", cfg.RDMAEnumerationLiveness,
		"rdmaEnumerationGrace", cfg.RDMAEnumerationGrace)

	return mgr.Start(ctrl.SetupSignalHandler())
}

func providerNames(providers []fabrics.Provider) []string {
	out := make([]string, 0, len(providers))
	for _, p := range providers {
		out = append(out, p.String())
	}
	return out
}
