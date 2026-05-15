package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/capabilities"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/config"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
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
	kClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())
	defer cancel()

	pub := &capabilities.Publisher{
		Client:    kClient,
		NodeName:  cfg.NodeName,
		Providers: cfg.Providers,
	}
	if err := pub.EnsureExists(ctx); err != nil {
		return err
	}

	var ready atomic.Bool
	ready.Store(true)

	go pub.RunRefreshLoop(ctx, cfg.ResyncPeriod)
	go runProbes(ctx, cfg.ProbeAddr, &ready)

	setupLog.Info("gateway started",
		"node", cfg.NodeName,
		"providers", providerNames(cfg.Providers),
		"probeAddr", cfg.ProbeAddr,
		"resyncPeriod", cfg.ResyncPeriod)

	<-ctx.Done()
	return nil
}

func providerNames(providers []fabrics.Provider) []string {
	out := make([]string, 0, len(providers))
	for _, p := range providers {
		out = append(out, p.String())
	}
	return out
}

// runProbes serves /healthz and /readyz on addr.
func runProbes(ctx context.Context, addr string, ready *atomic.Bool) {
	l := ctrl.Log.WithName("probes")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		l.Error(err, "probe server exited")
	}
}
