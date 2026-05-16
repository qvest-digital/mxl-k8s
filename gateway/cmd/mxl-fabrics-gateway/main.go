package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/capabilities"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/config"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/mirror"
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

	if err := (&mirror.TargetReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		NodeName:    cfg.NodeName,
		BindAddress: cfg.BindAddress,
		Handles:     handles,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup target reconciler: %w", err)
	}
	if err := (&mirror.SourceReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		NodeName:    cfg.NodeName,
		BindAddress: cfg.BindAddress,
		Handles:     handles,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup source reconciler: %w", err)
	}

	// MxlNodeCapabilities publisher runs as a Manager Runnable so it
	// joins the leader-election / shutdown lifecycle and only fires
	// once the cache has synced.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		pub := &capabilities.Publisher{
			Client:    mgr.GetClient(),
			NodeName:  cfg.NodeName,
			Providers: cfg.Providers,
		}
		if err := pub.EnsureExists(ctx); err != nil {
			return err
		}
		pub.RunRefreshLoop(ctx, cfg.ResyncPeriod)
		return nil
	})); err != nil {
		return fmt.Errorf("register capabilities runnable: %w", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("register healthz: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("register readyz: %w", err)
	}

	setupLog.Info("gateway started",
		"node", cfg.NodeName,
		"domainPath", cfg.DomainPath,
		"bindAddress", cfg.BindAddress,
		"providers", providerNames(cfg.Providers),
		"probeAddr", cfg.ProbeAddr,
		"resyncPeriod", cfg.ResyncPeriod)

	return mgr.Start(ctrl.SetupSignalHandler())
}

func providerNames(providers []fabrics.Provider) []string {
	out := make([]string, 0, len(providers))
	for _, p := range providers {
		out = append(out, p.String())
	}
	return out
}
