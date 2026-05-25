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

	"github.com/qvest-digital/mxl-k8s/agent/internal/config"
	"github.com/qvest-digital/mxl-k8s/agent/internal/domainpublisher"
	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
	"github.com/qvest-digital/mxl-k8s/agent/internal/flowpublisher"
	"github.com/qvest-digital/mxl-k8s/agent/internal/intent"
	"github.com/qvest-digital/mxl-k8s/agent/internal/intentsock"
	"github.com/qvest-digital/mxl-k8s/agent/internal/originlease"
	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	"github.com/qvest-digital/mxl-k8s/agent/internal/statfs"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
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
		setupLog.Error(err, "agent exited with error")
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("mxl-domain-agent", flag.ContinueOnError)
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

	// fanotify-readiness flag, observable by the domain publisher.
	var ready atomic.Bool

	domainPub := &domainpublisher.Publisher{
		Client:        kClient,
		NodeName:      cfg.NodeName,
		HostPath:      cfg.DomainPath,
		Stats:         statfs.Stats,
		FanotifyReady: ready.Load,
	}
	if err := domainPub.EnsureExists(ctx); err != nil {
		return err
	}

	leaseMgr := originlease.New(kClient, cfg.NodeName)
	flowPub := &flowpublisher.Publisher{
		Client:     kClient,
		DomainPath: cfg.DomainPath,
		NodeName:   cfg.NodeName,
		Lease:      leaseMgr,
	}
	if err := flowPub.InitialSync(ctx); err != nil {
		// Initial sync failures are logged, not fatal -- the fanotify
		// stream will reconcile when entries change.
		setupLog.Error(err, "initial flow sync failed")
	}

	w, err := fanotify.New()
	if err != nil {
		return fmt.Errorf("fanotify init: %w", err)
	}
	defer w.Close()
	if err := w.MarkInode(cfg.DomainPath,
		fanotify.MaskCreate|fanotify.MaskMovedTo|fanotify.MaskDelete|fanotify.MaskMovedFrom|fanotify.MaskOnDir,
	); err != nil {
		return fmt.Errorf("fanotify mark %s: %w", cfg.DomainPath, err)
	}
	ready.Store(true)

	events := make(chan fanotify.Event, 32)
	watchErr := make(chan error, 1)
	go func() { watchErr <- w.Run(ctx, events) }()

	go runDispatcher(ctx, events, flowPub)

	go runProbes(ctx, cfg.ProbeAddr, &ready)

	go domainPub.RunRefreshLoop(ctx, cfg.ResyncPeriod)

	go flowPub.RunRenewLoop(ctx, originlease.DefaultRenewInterval)

	go flowPub.RunLocalRescan(ctx, 30*time.Second)

	if cfg.IntentSocketPath != "" {
		intentDispatcher := &intent.Dispatcher{
			Client:             kClient,
			Resolver:           &podlookup.Resolver{Client: kClient, NodeName: cfg.NodeName},
			DomainPath:         cfg.DomainPath,
			NodeName:           cfg.NodeName,
			MaterializeTimeout: cfg.MaterializeTimeout,
			Lease:              leaseMgr,
		}
		intentServer := &intentsock.Server{
			SocketPath: cfg.IntentSocketPath,
			Dispatcher: intentDispatcher,
		}
		go func() {
			if err := intentServer.Run(ctx); err != nil {
				setupLog.Error(err, "intent socket exited")
			}
		}()
	}

	setupLog.Info("agent started",
		"node", cfg.NodeName,
		"domainPath", cfg.DomainPath,
		"probeAddr", cfg.ProbeAddr,
		"resyncPeriod", cfg.ResyncPeriod,
		"intentSocket", cfg.IntentSocketPath)

	select {
	case <-ctx.Done():
		return nil
	case err := <-watchErr:
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("fanotify watcher: %w", err)
		}
		return nil
	}
}

// runDispatcher routes fanotify events to the flow publisher.
func runDispatcher(ctx context.Context, in <-chan fanotify.Event, fp *flowpublisher.Publisher) {
	l := ctrl.Log.WithName("dispatcher")
	for ev := range in {
		switch {
		case ev.IsCreate():
			if err := fp.PublishAppeared(ctx, ev.Name); err != nil {
				l.Error(err, "PublishAppeared", "name", ev.Name)
			}
		case ev.IsRemove():
			if err := fp.PublishVanished(ctx, ev.Name); err != nil {
				l.Error(err, "PublishVanished", "name", ev.Name)
			}
		}
	}
}

// runProbes serves /healthz and /readyz on addr. /readyz returns 503
// until fanotify is initialized.
func runProbes(ctx context.Context, addr string, ready *atomic.Bool) {
	l := ctrl.Log.WithName("probes")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "fanotify not ready", http.StatusServiceUnavailable)
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
