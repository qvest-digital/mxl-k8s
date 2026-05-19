package mirror

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

// SourceFinalizerName is the finalizer the source-side reconciler
// adds so the initiator + transfer goroutine get torn down before
// the CR is removed from the API.
const SourceFinalizerName = "gateway.mxl.qvest-digital.com/source-side"

// SourceReconciler reconciles MxlFlowMirror resources from the
// sending side. See the package doc.
type SourceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NodeName is the Kubernetes node this gateway runs on. Mirrors
	// with spec.sourceNode set to a different node are ignored.
	NodeName string

	// BindAddress is the libmxl-fabrics endpoint node passed to the
	// Initiator Setup. Empty means "bind all interfaces".
	BindAddress string

	// Handles owns the long-lived mxl + fabrics instances.
	Handles *instance.Handles

	// ProgressInterval is how often the per-flow transfer goroutine
	// calls MakeProgress + polls FlowRuntime for new grain indices.
	// Defaults to 2ms.
	ProgressInterval time.Duration

	mu      sync.Mutex
	sources map[types.NamespacedName]*sourceEntry
}

// sourceEntry holds the live libmxl + fabrics handles and the
// transfer-loop control plumbing for one source-side mirror.
type sourceEntry struct {
	reader    *mxl.Reader
	regions   *fabrics.Regions
	initiator *fabrics.Initiator
	info      *fabrics.TargetInfo
	// infoStr is the serialized TargetInfo the initiator was set up
	// with. We keep it so a later reconcile can detect that the
	// target has rotated its info (e.g. the target-side gateway pod
	// restarted and re-opened the FlowWriter) and reopen the
	// initiator against the fresh address before the source's writes
	// keep getting refused.
	infoStr string

	// cancel stops the per-flow transfer goroutine; done is closed
	// when the goroutine returns.
	cancel context.CancelFunc
	done   chan struct{}
}

// Reconcile drives one MxlFlowMirror through its source-side
// lifecycle.
func (r *SourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)

	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &mirror); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if mirror.Spec.SourceNode != r.NodeName {
		return ctrl.Result{}, nil
	}

	if !mirror.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&mirror, SourceFinalizerName) {
			return ctrl.Result{}, nil
		}
		r.closeEntry(req.NamespacedName)
		controllerutil.RemoveFinalizer(&mirror, SourceFinalizerName)
		if err := r.Update(ctx, &mirror); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
		l.Info("torn down source-side mirror")
		return ctrl.Result{}, nil
	}

	// Concurrent reconcilers (target-side gateway, agent intent
	// dispatcher) routinely race us on the same MxlFlowMirror;
	// surface the conflict as a benign requeue instead of a
	// stacktraced Reconciler error.
	if !controllerutil.ContainsFinalizer(&mirror, SourceFinalizerName) {
		controllerutil.AddFinalizer(&mirror, SourceFinalizerName)
		if err := r.Update(ctx, &mirror); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Wait for the target side to publish its TargetInfo. The status
	// update will trigger another reconcile.
	if mirror.Status.TargetInfo == "" {
		return ctrl.Result{}, nil
	}

	// Already set up against this exact target info?
	r.mu.Lock()
	existing := r.sources[req.NamespacedName]
	r.mu.Unlock()
	if existing != nil {
		if existing.infoStr == mirror.Status.TargetInfo {
			return ctrl.Result{}, nil
		}
		// TargetInfo rotated under us (typically: target gateway
		// restarted and rebuilt the writer on a fresh ephemeral
		// port). Tear down the stale initiator so we re-open
		// against the new address.
		l.Info("target info rotated, rebuilding initiator")
		r.closeEntry(req.NamespacedName)
	}

	provider := mapProvider(mirror.Spec.Provider)

	entry, err := r.openInitiator(mirror.Spec.FlowID, mirror.Status.TargetInfo, provider)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("open initiator: %w", err)
	}

	r.mu.Lock()
	if dup := r.sources[req.NamespacedName]; dup != nil {
		r.mu.Unlock()
		closeSourceHandles(entry)
		return ctrl.Result{}, nil
	}
	r.sources[req.NamespacedName] = entry
	r.mu.Unlock()

	l.Info("initiator running",
		"flowID", mirror.Spec.FlowID,
		"targetNode", mirror.Spec.TargetNode,
		"provider", provider.String())
	return ctrl.Result{}, nil
}

// openInitiator opens a FlowReader on the local flow, registers its
// regions with libmxl-fabrics, creates and sets up an Initiator,
// AddTarget()s the remote info, and starts the per-flow transfer
// goroutine.
func (r *SourceReconciler) openInitiator(flowID, targetInfoStr string, provider fabrics.Provider) (*sourceEntry, error) {
	mxlInst := r.Handles.MXL()
	if mxlInst == nil {
		return nil, fmt.Errorf("mxl instance closed")
	}
	fabInst := r.Handles.Fabrics()
	if fabInst == nil {
		return nil, fmt.Errorf("fabrics instance closed")
	}

	reader, err := mxlInst.NewReader(flowID)
	if err != nil {
		return nil, fmt.Errorf("NewReader: %w", err)
	}
	regions, err := fabrics.RegionsForFlowReader(reader)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("RegionsForFlowReader: %w", err)
	}
	initiator, err := fabInst.NewInitiator()
	if err != nil {
		_ = regions.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("NewInitiator: %w", err)
	}
	if err := initiator.Setup(fabrics.InitiatorConfig{
		Endpoint: fabrics.EndpointAddress{Node: r.BindAddress},
		Provider: provider,
		Regions:  regions,
	}); err != nil {
		_ = initiator.Close()
		_ = regions.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("Initiator.Setup: %w", err)
	}
	info, err := fabrics.ParseTargetInfo(targetInfoStr)
	if err != nil {
		_ = initiator.Close()
		_ = regions.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("ParseTargetInfo: %w", err)
	}
	if err := initiator.AddTarget(info); err != nil {
		_ = info.Close()
		_ = initiator.Close()
		_ = regions.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("AddTarget: %w", err)
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	entry := &sourceEntry{
		reader:    reader,
		regions:   regions,
		initiator: initiator,
		info:      info,
		infoStr:   targetInfoStr,
		cancel:    cancel,
		done:      done,
	}

	progressInterval := r.ProgressInterval
	if progressInterval <= 0 {
		progressInterval = 2 * time.Millisecond
	}
	runtimeFn := func() (uint64, error) {
		rt, err := reader.Runtime()
		if err != nil {
			return 0, err
		}
		return rt.HeadIndex, nil
	}
	transferFn := func(idx uint64) (bool, error) {
		grain, err := reader.GetGrainNonBlocking(idx)
		if err != nil {
			// Not yet committed or already aged out; signal the loop
			// to break and re-try on the next tick.
			return false, err
		}
		if grain.TotalSlices == 0 {
			// Continuous flows or grains with no slice subdivision
			// report TotalSlices==0; v0 sends only fully-discrete
			// grains and skips these.
			return true, nil
		}
		return false, initiator.TransferGrain(idx, 0, grain.TotalSlices)
	}
	progressFn := initiator.MakeProgressNonBlocking
	go runTransferLoop(loopCtx, done, flowID, runtimeFn, transferFn, progressFn, progressInterval)

	return entry, nil
}

// runTransferLoop pumps grains that appear on the source flow into
// the initiator until ctx is canceled. Closes done on exit.
//
// The loop is parameterised by three injected functions so the
// cgo-dependent calls stay isolated from the state machine: tests
// can drive it with closures that return canned indices and record
// every interaction.
func runTransferLoop(
	ctx context.Context,
	done chan struct{},
	flowID string,
	probeRuntime RuntimeProbe,
	transferGrain TransferFunc,
	makeProgress ProgressFunc,
	interval time.Duration,
) {
	defer close(done)

	l := ctrl.Log.WithName("transfer").WithValues("flowID", flowID)

	// Discover the current head and start from there: a freshly
	// attached mirror tails the live flow rather than replaying
	// historical grains the producer may already have aged out of
	// the ring.
	head0, err := probeRuntime()
	if err != nil {
		l.Error(err, "initial Runtime")
		return
	}
	lastSent := int64(head0)

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		head, err := probeRuntime()
		if err != nil {
			l.Error(err, "Runtime")
			continue
		}
		for idx := lastSent + 1; idx <= int64(head); idx++ {
			if _, err := transferGrain(uint64(idx)); err != nil {
				l.Error(err, "TransferGrain", "index", idx)
				break
			}
			lastSent = idx
		}

		if err := makeProgress(); err != nil && !errors.Is(err, fabrics.ErrNotReady) {
			l.Error(err, "MakeProgress")
		}
	}
}

func (r *SourceReconciler) closeEntry(key types.NamespacedName) {
	r.mu.Lock()
	entry := r.sources[key]
	delete(r.sources, key)
	r.mu.Unlock()
	if entry == nil {
		return
	}
	closeSourceHandles(entry)
}

func closeSourceHandles(e *sourceEntry) {
	if e.cancel != nil {
		e.cancel()
	}
	if e.done != nil {
		<-e.done
	}
	if e.info != nil {
		_ = e.info.Close()
	}
	if e.initiator != nil {
		_ = e.initiator.Close()
	}
	if e.regions != nil {
		_ = e.regions.Close()
	}
	if e.reader != nil {
		_ = e.reader.Close()
	}
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
//
// The Watches on MxlFlow closes the writer-startup race: when an
// agent on the source node publishes a new MxlFlow (or updates its
// Origin location) the reconciler is woken immediately for any
// MxlFlowMirror with the matching flowID, instead of waiting out
// the exponential backoff that started when the local FlowReader
// open returned FLOW_NOT_FOUND before the writer had created it.
func (r *SourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.sources == nil {
		r.sources = make(map[types.NamespacedName]*sourceEntry)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Watches(
			&mxlv1alpha1.MxlFlow{},
			handler.EnqueueRequestsFromMapFunc(r.flowToMirrors),
		).
		Named("mxlflowmirror-source").
		Complete(r)
}

// flowToMirrors maps an MxlFlow event to reconcile requests for any
// source-side MxlFlowMirror whose spec.flowID matches and whose
// sourceNode is this gateway's node. Cluster-wide list because
// mirrors live in arbitrary user namespaces.
func (r *SourceReconciler) flowToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	flow, ok := obj.(*mxlv1alpha1.MxlFlow)
	if !ok {
		return nil
	}
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors); err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range mirrors.Items {
		m := &mirrors.Items[i]
		if m.Spec.FlowID == flow.Spec.ID && m.Spec.SourceNode == r.NodeName {
			out = append(out, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: m.Namespace,
					Name:      m.Name,
				},
			})
		}
	}
	return out
}
