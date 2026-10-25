package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/qvest-digital/mxl-k8s/agent/internal/config"
	"github.com/qvest-digital/mxl-k8s/agent/internal/csi"
	"github.com/qvest-digital/mxl-k8s/agent/internal/domainfs"
	"github.com/qvest-digital/mxl-k8s/agent/internal/domainpublisher"
	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
	"github.com/qvest-digital/mxl-k8s/agent/internal/flowpublisher"
	"github.com/qvest-digital/mxl-k8s/agent/internal/intent"
	"github.com/qvest-digital/mxl-k8s/agent/internal/intentsock"
	"github.com/qvest-digital/mxl-k8s/agent/internal/mxlfsmount"
	"github.com/qvest-digital/mxl-k8s/agent/internal/mxlfswatch"
	"github.com/qvest-digital/mxl-k8s/agent/internal/observed"
	"github.com/qvest-digital/mxl-k8s/agent/internal/originlease"
	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	"github.com/qvest-digital/mxl-k8s/agent/internal/redundancy"
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
	flowPub, sets := newFlowPublisher(cfg, kClient, leaseMgr)

	// Every domain written on the node other than the primary gets its own
	// tracker: a fanotify mark, a flow publisher and its lease loops, the
	// same set the primary domain gets below, and on mxlfs what runMxlfs
	// starts. Each starts as its domain is materialised, below.
	root := filepath.Dir(filepath.Clean(cfg.DomainPath))
	primaryDir := filepath.Base(filepath.Clean(cfg.DomainPath))
	trackers := &tracker.Manager{PrimaryDir: primaryDir}
	defer trackers.StopAll()

	var domainsMu sync.Mutex
	current := intent.Domains{Root: root, Primary: primaryDir, ByDir: map[string]string{}}

	pods := &podlookup.Resolver{Client: kClient, NodeName: cfg.NodeName}
	ns, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	own := ownPod(strings.TrimSpace(string(ns)))
	var intentDispatcher *intent.Dispatcher
	if cfg.IntentSocketPath != "" || cfg.Mxlfs {
		intentDispatcher = &intent.Dispatcher{
			Client:             kClient,
			Resolver:           pods,
			DomainPath:         cfg.DomainPath,
			NodeName:           cfg.NodeName,
			Provider:           mxlv1alpha1.MxlFabricsProvider(cfg.Provider),
			MaterializeTimeout: cfg.MaterializeTimeout,
			Lease:              leaseMgr,
			Origin:             originClaims{primary: flowPub, trackers: trackers},
			Own:                own,
			Domains: func() intent.Domains {
				domainsMu.Lock()
				defer domainsMu.Unlock()
				return current
			},
		}
	}
	// A redundancy set's output is merged where a reader waits for it, in
	// whichever domain it is in, rather than mirrored.
	claim := func(ctx context.Context, flowDir string) bool {
		domainsMu.Lock()
		doms := current
		domainsMu.Unlock()
		ref, ok := doms.FlowRefFromPath(flowDir)
		if !ok {
			return false
		}
		isOutput, err := sets.Want(ctx, ref.Domain, ref.ID)
		if err != nil {
			setupLog.Error(err, "record redundancy set demand", "flowID", ref.ID)
		}
		return isOutput
	}
	startMxlfs := func(ctx context.Context, dir string, fp *flowpublisher.Publisher) (func(), error) {
		return runMxlfs(ctx, dir, fp, intentDispatcher, claim, ours(pods, own))
	}
	tracking := domainTracking{
		Client: kClient,
		Node:   cfg.NodeName,
		Lease:  leaseMgr,
		WatchFlows: func(ctx context.Context, dir string, fp *flowpublisher.Publisher) (func(), error) {
			return watchFlows(ctx, dir, fp, ctrl.Log.WithName("tracker").WithValues("directory", dir))
		},
	}
	if cfg.Mxlfs {
		tracking.Mxlfs = &trackerMxlfs{IsMxlfs: mxlfswatch.IsMxlfs, IsOutput: sets.IsOutput, Run: startMxlfs}
	}
	trackers.Start = func(ctx context.Context, name, dir string) (tracker.Tracker, error) {
		return tracking.start(ctx, name, filepath.Join(root, dir))
	}

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
	// Every domain directory is mounted as an mxlfs before anything is
	// written into it or watched there: the primary one now, any other
	// as it is materialised. Without mxlfs a marker left by an earlier
	// run with it goes, so no gateway or exporter waits on it.
	var mounts *mxlfsmount.Mounter
	if cfg.Mxlfs {
		if err := requireKind(kClient.RESTMapper(), "MxlFlowRedundancySet"); err != nil {
			return fmt.Errorf("--mxlfs needs the redundancy set CRD: %w", err)
		}
		mounts = &mxlfsmount.Mounter{
			Fallback: mxlfsmount.NodeEvent(kClient, cfg.NodeName, setupLog),
			Log:      ctrl.Log.WithName("mxlfsmount"),
		}
		if err := mounts.Ensure(cfg.DomainPath); err != nil {
			return err
		}
		domainPub.Apply = func(base string, spec *mxlv1alpha1.MxlDomainSpec) (domainfs.Result, error) {
			if err := mounts.Ensure(filepath.Join(base, spec.Path())); err != nil {
				return domainfs.Result{}, err
			}
			return domainfs.Apply(base, spec)
		}
	} else if err := mxlfsmount.ClearReady(root); err != nil {
		setupLog.Error(err, "remove mxlfs ready marker")
	}

	// Before the fanotify mark: the mirrored directory is one of the
	// domains, and a fresh node has none yet. A failure here is logged,
	// not fatal -- the sync loop retries, and the mark below fails
	// loudly if the directory is still missing.
	if err := domainPub.Sync(ctx); err != nil {
		setupLog.Error(err, "initial domain sync failed")
	}
	// The gateway and the exporter wait for this before they open a
	// domain, so they never hold the directory beneath a later mount.
	if mounts != nil {
		if err := mxlfsmount.MarkReady(root); err != nil {
			return fmt.Errorf("write mxlfs ready marker: %w", err)
		}
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

	if cfg.Mxlfs && mxlfswatch.IsMxlfs(cfg.DomainPath) {
		if _, err := startMxlfs(ctx, cfg.DomainPath, flowPub); err != nil {
			return err
		}
	}

	scanner := &observed.Scanner{Domains: func() map[string]string {
		domainsMu.Lock()
		doms := current
		domainsMu.Unlock()
		byDev := map[string]string{}
		if dev, err := observed.Device(cfg.DomainPath); err == nil {
			byDev[dev] = ""
		}
		for dir, name := range doms.ByDir {
			if dev, err := observed.Device(filepath.Join(doms.Root, dir)); err == nil {
				byDev[dev] = name
			}
		}
		return byDev
	}}
	if sets != nil {
		inUse := func() (map[mxlv1alpha1.FlowRef]bool, error) {
			return flowsInUse(ctx, kClient, cfg.NodeName, own, scanner.Scan)
		}
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				if err := sets.Release(ctx, inUse, redundancy.ReleaseGrace); err != nil {
					setupLog.Error(err, "release unused redundancy set outputs")
				}
			}
		}()
	}

	if cfg.ClaimStatusLabel != "" {
		claims := &observed.ClaimWriter{
			Client:   kClient,
			NodeName: cfg.NodeName,
			Label:    cfg.ClaimStatusLabel,
			Log:      ctrl.Log.WithName("observed"),
			Scanner:  scanner,
		}
		go claims.Run(ctx, 10*time.Second)
	}

	if cfg.CSIEndpoint != "" {
		plugin := &csi.Plugin{
			NodeName: cfg.NodeName,
			Mounter:  csi.SysMounter{},
			Ready:    csiReady(mounts),
			DomainDir: func(ctx context.Context, domain string) (string, error) {
				if domain == "" {
					return cfg.DomainPath, nil
				}
				var d mxlv1alpha1.MxlDomain
				if err := kClient.Get(ctx, client.ObjectKey{Name: domain}, &d); err != nil {
					return "", err
				}
				return filepath.Join(filepath.Dir(cfg.DomainPath), d.Spec.Path()), nil
			},
		}
		go func() {
			if err := plugin.Serve(ctx, cfg.CSIEndpoint); err != nil && ctx.Err() == nil {
				setupLog.Error(err, "CSI node plugin exited")
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

// newFlowPublisher builds the primary domain's flow publisher. With
// mxlfs it keeps redundancy set outputs out of what it publishes, and
// the sets it consults are returned; without, it publishes every flow
// and asks the API server about no set.
func newFlowPublisher(cfg *config.Config, c client.Client, lease *originlease.Manager) (*flowpublisher.Publisher, *redundancy.Sets) {
	fp := &flowpublisher.Publisher{
		Client:     c,
		DomainPath: cfg.DomainPath,
		NodeName:   cfg.NodeName,
		Lease:      lease,
	}
	if !cfg.Mxlfs {
		return fp, nil
	}
	sets := &redundancy.Sets{Client: c, NodeName: cfg.NodeName}
	fp.IsOutput = sets.IsOutput
	return fp, sets
}

// csiReady holds a pod's bind of a domain until the domain is mounted or
// known to stay plain. Without mxlfs nothing is mounted, and nothing waits.
func csiReady(mounts *mxlfsmount.Mounter) func(dir string) bool {
	if mounts == nil {
		return nil
	}
	return mounts.Decided
}

// requireKind fails when the API server does not serve kind: every
// request for it would fail for as long as the agent runs.
func requireKind(mapper meta.RESTMapper, kind string) error {
	gk := schema.GroupKind{Group: mxlv1alpha1.GroupVersion.Group, Kind: kind}
	if _, err := mapper.RESTMapping(gk, mxlv1alpha1.GroupVersion.Version); err != nil {
		return fmt.Errorf("%s is not served (is its CRD installed?): %w", gk, err)
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

// domainTracking starts the tracker of one domain other than the primary.
type domainTracking struct {
	Client client.Client
	Node   string
	Lease  flowpublisher.LeaseRenewer
	// WatchFlows feeds fp the flow directories coming and going in dir
	// until ctx ends. It returns once they are watched, with what waits
	// for the watch to end.
	WatchFlows func(ctx context.Context, dir string, fp *flowpublisher.Publisher) (wait func(), err error)
	// Mxlfs is nil without --mxlfs.
	Mxlfs *trackerMxlfs
}

// trackerMxlfs is what a tracked domain on mxlfs gets beyond its flow
// watch: the same placeholder and redundancy set handling as the
// primary domain.
type trackerMxlfs struct {
	IsMxlfs  func(dir string) bool
	IsOutput func(ctx context.Context, domain, flowID string) bool
	// Run starts runMxlfs for dir and returns what waits for it to end
	// once ctx has.
	Run func(ctx context.Context, dir string, fp *flowpublisher.Publisher) (wait func(), err error)
}

// start tracks the flows of the domain name in dir: the same fanotify
// mark, flow publisher and lease loops the primary domain gets, and on
// mxlfs what runMxlfs starts. It returns once all of it is in place, so a
// domain is reported mirrored only when its flows are being tracked.
func (d domainTracking) start(ctx context.Context, name, dir string) (tracker.Tracker, error) {
	l := ctrl.Log.WithName("tracker").WithValues("domain", name, "directory", dir)
	fp := &flowpublisher.Publisher{
		Client:     d.Client,
		DomainPath: dir,
		Domain:     name,
		NodeName:   d.Node,
		Lease:      d.Lease,
	}
	onMxlfs := d.Mxlfs != nil && d.Mxlfs.IsMxlfs(dir)
	if onMxlfs {
		fp.IsOutput = d.Mxlfs.IsOutput
	}

	tctx, cancel := context.WithCancel(ctx)
	waitWatch, err := d.WatchFlows(tctx, dir, fp)
	if err != nil {
		cancel()
		return tracker.Tracker{}, err
	}
	if err := fp.InitialSync(tctx); err != nil {
		l.Error(err, "initial flow sync failed")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); fp.RunRenewLoop(tctx, originlease.DefaultRenewInterval) }()
	go func() { defer wg.Done(); fp.RunLocalRescan(tctx, 30*time.Second) }()
	waitMxlfs := func() {}
	if onMxlfs {
		if waitMxlfs, err = d.Mxlfs.Run(tctx, dir, fp); err != nil {
			cancel()
			wg.Wait()
			waitWatch()
			return tracker.Tracker{}, err
		}
	}
	l.Info("tracking domain", "mxlfs", onMxlfs)

	stop := func() {
		cancel()
		wg.Wait()
		waitWatch()
		waitMxlfs()
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

// watchFlows marks dir for flow directories coming and going and feeds
// them to fp until ctx ends.
func watchFlows(ctx context.Context, dir string, fp *flowpublisher.Publisher, l logr.Logger) (func(), error) {
	w, err := fanotify.New()
	if err != nil {
		return nil, fmt.Errorf("fanotify init: %w", err)
	}
	if err := w.MarkInode(dir,
		fanotify.MaskCreate|fanotify.MaskMovedTo|fanotify.MaskDelete|fanotify.MaskMovedFrom|fanotify.MaskOnDir,
	); err != nil {
		w.Close()
		return nil, fmt.Errorf("fanotify mark %s: %w", dir, err)
	}
	events := make(chan fanotify.Event, 32)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Run closes events when it returns, which ends the dispatcher.
		if err := w.Run(ctx, events); err != nil && ctx.Err() == nil {
			l.Error(err, "fanotify watcher")
		}
	}()
	go func() { defer wg.Done(); runDispatcher(ctx, events, fp) }()
	return func() { wg.Wait(); w.Close() }, nil
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

// ownComponents are the mxl-k8s components that open placeholders: the
// gateway creating and filling them, the exporter reading them.
var ownComponents = map[string]bool{"agent": true, "gateway": true, "exporter": true}

// ownPod reports a pod as mxl-k8s's own when it is one of ownComponents
// in the agent's namespace. Any other pod there is a consumer like any
// other.
func ownPod(namespace string) func(*corev1.Pod) bool {
	return func(pod *corev1.Pod) bool {
		return pod.Namespace == namespace && ownComponents[pod.Labels["app.kubernetes.io/component"]]
	}
}

// ours reports a pid as mxl-k8s's own when own reports its pod so. A pid
// whose pod cannot be resolved is not known to be ours: the dispatcher
// resolves it again and reports why no mirror starts.
func ours(pods interface {
	PodForPID(ctx context.Context, pid int32) (*corev1.Pod, error)
}, own func(*corev1.Pod) bool) func(ctx context.Context, pid int32) bool {
	return func(ctx context.Context, pid int32) bool {
		pod, err := pods.PodForPID(ctx, pid)
		return err == nil && own(pod)
	}
}

// flowsInUse returns the flows processes on this node map, leaving out
// mxl-k8s's own: the gateway writing a merged output and the exporter
// reading every flow would otherwise keep each output in use for good.
func flowsInUse(ctx context.Context, c client.Client, node string, own func(*corev1.Pod) bool,
	scan func() (map[string][]observed.Use, error)) (map[mxlv1alpha1.FlowRef]bool, error) {
	uses, err := scan()
	if err != nil {
		return nil, err
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node}); err != nil {
		return nil, err
	}
	mine := map[string]bool{}
	for _, p := range pods.Items {
		if own(&p) {
			mine[string(p.UID)] = true
		}
	}
	out := map[mxlv1alpha1.FlowRef]bool{}
	for uid, us := range uses {
		if mine[uid] {
			continue
		}
		for _, u := range us {
			out[mxlv1alpha1.FlowRef{Domain: u.Domain, ID: u.FlowID}] = true
		}
	}
	return out, nil
}

// runMxlfs starts what a domain on mxlfs needs beyond fanotify on the
// domain directory: a mirror for every placeholder a pod opens, one for
// every flow a path walk waits for, and a publish for every placeholder
// that stops being one. It returns what waits for all of it to end once
// ctx has.
func runMxlfs(ctx context.Context, domain string, fp *flowpublisher.Publisher,
	d *intent.Dispatcher, claim func(context.Context, string) bool,
	ours func(ctx context.Context, pid int32) bool) (func(), error) {

	l := ctrl.Log.WithName("mxlfs").WithValues("domain", domain)
	markers, err := mxlfswatch.MarkerDir(domain)
	if err != nil {
		return nil, fmt.Errorf("placeholder marker directory: %w", err)
	}
	w, err := fanotify.New()
	if err != nil {
		return nil, fmt.Errorf("fanotify init: %w", err)
	}
	if err := w.MarkInode(markers, fanotify.MaskDelete|fanotify.MaskMovedFrom|fanotify.MaskCreate|fanotify.MaskMovedTo); err != nil {
		w.Close()
		return nil, fmt.Errorf("fanotify mark %s: %w", markers, err)
	}

	trigger := &mxlfswatch.Trigger{Dispatcher: d, Ours: ours, Log: l}
	events := make(chan fanotify.Event, 32)
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		if err := trigger.Run(ctx, domain); err != nil && ctx.Err() == nil {
			l.Error(err, "placeholder open watcher exited")
		}
	}()
	go func() {
		defer wg.Done()
		if err := mxlfswatch.RunWanted(ctx, domain, d, claim, ours, l); err != nil && ctx.Err() == nil {
			l.Error(err, "wanted flow loop exited")
		}
	}()
	go func() {
		defer wg.Done()
		defer w.Close()
		if err := w.Run(ctx, events); err != nil && ctx.Err() == nil {
			l.Error(err, "placeholder marker watcher exited")
		}
	}()
	go func() {
		defer wg.Done()
		for ev := range events {
			if err := mxlfswatch.MarkerChanged(ctx, fp, ev.Name, ev.IsCreate()); err != nil {
				l.Error(err, "publish placeholder change", "flowID", ev.Name)
			}
		}
	}()
	l.Info("watching mxlfs placeholders")
	return wg.Wait, nil
}
