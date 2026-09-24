package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
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
	"github.com/qvest-digital/mxl-k8s/agent/internal/shiminstall"
	"github.com/qvest-digital/mxl-k8s/agent/internal/statfs"
	"github.com/qvest-digital/mxl-k8s/agent/internal/tracker"
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

	// Before anything that can block on the apiserver: the shim is
	// node-local, and a consumer pod may already be waiting on it.
	if cfg.ShimPath != "" {
		if err := shiminstall.Install(shiminstall.ImagePath, cfg.ShimPath); err != nil {
			return fmt.Errorf("install intent shim: %w", err)
		}
	}

	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	// client-go defaults to 5 QPS / 10 burst when the limits are left
	// zero; flow appear/vanish bursts on a busy domain exceed that.
	restCfg.QPS = float32(cfg.KubeAPIQPS)
	restCfg.Burst = cfg.KubeAPIBurst
	kClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())
	defer cancel()

	// fanotify-readiness flag, observable by the domain publisher.
	var ready atomic.Bool

	leaseMgr := originlease.New(kClient, cfg.NodeName)

	// Every domain written on the node other than the primary gets its own
	// tracker: a fanotify mark, a flow publisher and its lease loops, the
	// same set the primary domain gets below.
	root := filepath.Dir(filepath.Clean(cfg.DomainPath))
	primaryDir := filepath.Base(filepath.Clean(cfg.DomainPath))
	trackers := &tracker.Manager{
		PrimaryDir: primaryDir,
		Start: func(ctx context.Context, name, dir string) (tracker.Tracker, error) {
			return startTracker(ctx, kClient, cfg.NodeName, leaseMgr, name, filepath.Join(root, dir))
		},
	}
	defer trackers.StopAll()

	var domainsMu sync.Mutex
	current := intent.Domains{Root: root, Primary: primaryDir, ByDir: map[string]string{}}

	domainPub := domainpublisher.NewFromDomainPath(kClient, cfg.NodeName,
		cfg.DomainPath, statfs.Stats, ready.Load)
	domainPub.Tracked = trackers.Tracked
	domainPub.OnMaterialised = func(all map[string]string) {
		m := intent.Mirrored(primaryDir, all)
		byDir := make(map[string]string, len(m))
		for name, dir := range m {
			byDir[dir] = name
		}
		domainsMu.Lock()
		current = intent.Domains{Root: root, Primary: primaryDir, ByDir: byDir}
		domainsMu.Unlock()
		trackers.Sync(ctx, m)
	}
	// Before the fanotify mark: the mirrored directory is one of the
	// domains, and a fresh node has none yet. A failure here is logged,
	// not fatal -- the sync loop retries, and the mark below fails
	// loudly if the directory is still missing.
	if err := domainPub.Sync(ctx); err != nil {
		setupLog.Error(err, "initial domain sync failed")
	}

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

	go domainPub.RunSyncLoop(ctx, cfg.ResyncPeriod)

	go flowPub.RunRenewLoop(ctx, originlease.DefaultRenewInterval)

	go flowPub.RunLocalRescan(ctx, 30*time.Second)

	if cfg.IntentSocketPath != "" {
		intentDispatcher := &intent.Dispatcher{
			Client:             kClient,
			Resolver:           &podlookup.Resolver{Client: kClient, NodeName: cfg.NodeName},
			DomainPath:         cfg.DomainPath,
			NodeName:           cfg.NodeName,
			Provider:           mxlv1alpha1.MxlFabricsProvider(cfg.Provider),
			MaterializeTimeout: cfg.MaterializeTimeout,
			Lease:              leaseMgr,
			Origin:             originClaims{primary: flowPub, trackers: trackers},
			Domains: func() intent.Domains {
				domainsMu.Lock()
				defer domainsMu.Unlock()
				return current
			},
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
		"intentSocket", cfg.IntentSocketPath,
		"shimPath", cfg.ShimPath)

	select {
	case <-ctx.Done():
	case err := <-watchErr:
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("fanotify watcher: %w", err)
		}
	}

	// Release every Lease this node currently holds so the operator's
	// freshness check sees the Origins drop immediately on graceful
	// shutdown. Without this the Leases sit unrenewed and the operator
	// only notices after the 30s window elapses - well past pod
	// eviction in the common case. Bounded to 5s so a partitioned
	// apiserver does not stretch terminationGracePeriodSeconds.
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRelease()
	if err := flowPub.ReleaseAll(releaseCtx); err != nil {
		setupLog.Error(err, "release leases on shutdown")
	}
	return nil
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

// startTracker tracks the flows of one domain other than the primary: the
// same fanotify mark, flow publisher and lease loops the primary domain gets.
// It returns once the mark is in place, so a domain is reported mirrored only
// when its flows are being tracked.
func startTracker(ctx context.Context, c client.Client, node string,
	lease *originlease.Manager, name, dir string) (tracker.Tracker, error) {

	l := ctrl.Log.WithName("tracker").WithValues("domain", name, "directory", dir)
	fp := &flowpublisher.Publisher{
		Client:     c,
		DomainPath: dir,
		Domain:     name,
		NodeName:   node,
		Lease:      lease,
	}
	w, err := fanotify.New()
	if err != nil {
		return tracker.Tracker{}, fmt.Errorf("fanotify init: %w", err)
	}
	if err := w.MarkInode(dir,
		fanotify.MaskCreate|fanotify.MaskMovedTo|fanotify.MaskDelete|fanotify.MaskMovedFrom|fanotify.MaskOnDir,
	); err != nil {
		w.Close()
		return tracker.Tracker{}, fmt.Errorf("fanotify mark %s: %w", dir, err)
	}
	if err := fp.InitialSync(ctx); err != nil {
		l.Error(err, "initial flow sync failed")
	}

	tctx, cancel := context.WithCancel(ctx)
	events := make(chan fanotify.Event, 32)
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		// Run closes events when it returns, which ends the dispatcher.
		if err := w.Run(tctx, events); err != nil && tctx.Err() == nil {
			l.Error(err, "fanotify watcher")
		}
	}()
	go func() { defer wg.Done(); runDispatcher(tctx, events, fp) }()
	go func() { defer wg.Done(); fp.RunRenewLoop(tctx, originlease.DefaultRenewInterval) }()
	go func() { defer wg.Done(); fp.RunLocalRescan(tctx, 30*time.Second) }()
	l.Info("tracking domain")

	stop := func() {
		cancel()
		wg.Wait()
		w.Close()
		// The node no longer serves this domain's flows as their origin.
		rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		if err := fp.ReleaseAll(rctx); err != nil {
			l.Error(err, "releasing leases")
		}
		l.Info("stopped tracking domain")
	}
	return tracker.Tracker{Stop: stop, ClaimOrigin: fp.ClaimOrigin}, nil
}

// originClaims sends a producer's claim to the publisher of its flow's
// domain: the primary one, or the tracker of any other.
type originClaims struct {
	primary  *flowpublisher.Publisher
	trackers *tracker.Manager
}

func (o originClaims) ClaimOrigin(ctx context.Context, ref mxlv1alpha1.FlowRef) error {
	if ref.Domain == "" {
		return o.primary.ClaimOrigin(ctx, ref.ID)
	}
	return o.trackers.ClaimOrigin(ctx, ref.Domain, ref.ID)
}
