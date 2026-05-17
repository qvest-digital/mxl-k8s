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
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

// TargetFinalizerName is the finalizer the target-side reconciler
// adds so libmxl-fabrics handles get torn down before the CR is
// removed from the API.
const TargetFinalizerName = "gateway.mxl.qvest-digital.com/target-side"

// TargetReconciler reconciles MxlFlowMirror resources from the
// receiving side. See the package doc.
type TargetReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NodeName is the Kubernetes node this gateway runs on. Mirrors
	// with spec.targetNode set to a different node are ignored.
	NodeName string

	// BindAddress is the libmxl-fabrics endpoint node passed to each
	// Target Setup. Empty means "bind all interfaces" per
	// libmxl-fabrics semantics.
	BindAddress string

	// Handles owns the long-lived mxl + fabrics instances.
	Handles *instance.Handles

	mu      sync.Mutex
	targets map[types.NamespacedName]*targetEntry
}

// targetEntry holds the live libmxl handles for one target-side
// mirror plus the goroutine that drives the target's progress loop.
// Closed together by closeTargetHandles.
type targetEntry struct {
	// writer owns the local flow file. Its lifetime spans recoveries:
	// closing it would invalidate the FlowReader handles in consumer
	// pods, so the recovery path leaves it alone and only rebuilds
	// the fabric side.
	writer *mxl.Writer

	// fabric-side handles, rebuilt by recoverFromFatalError when
	// ReadGrain reports a non-recoverable error.
	regions *fabrics.Regions
	target  *fabrics.Target
	info    *fabrics.TargetInfo
	infoStr string

	// provider records the configuration the entry was opened with,
	// so the recovery path can rebuild the fabric side identically.
	provider fabrics.Provider

	// cancel stops the per-mirror progress goroutine; done is closed
	// when the goroutine returns. Without this loop the libmxl-fabrics
	// Target never advances its event/completion queues, so remote
	// initiators never get an FI_CONNECTED back and grains never land.
	cancel context.CancelFunc
	done   chan struct{}
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/finalizers,verbs=update
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch

// Reconcile drives one MxlFlowMirror through its target-side
// lifecycle.
func (r *TargetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)

	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &mirror); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Other-node mirrors are not ours.
	if mirror.Spec.TargetNode != r.NodeName {
		return ctrl.Result{}, nil
	}

	// Deletion path: tear down libmxl handles, drop the finalizer.
	if !mirror.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&mirror, TargetFinalizerName) {
			return ctrl.Result{}, nil
		}
		r.closeEntry(req.NamespacedName)
		controllerutil.RemoveFinalizer(&mirror, TargetFinalizerName)
		if err := r.Update(ctx, &mirror); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
		l.Info("torn down target-side mirror")
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is in place before we own any handles.
	if !controllerutil.ContainsFinalizer(&mirror, TargetFinalizerName) {
		controllerutil.AddFinalizer(&mirror, TargetFinalizerName)
		if err := r.Update(ctx, &mirror); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Idempotent fast-path. Requires *both* a Ready status and a live
	// in-memory entry: a gateway restart preserves status but loses
	// the libmxl FlowWriter, and closing that writer on shutdown
	// removes the on-disk flow definition. Re-opening here restores
	// the flow file and rotates TargetInfo, which the source side
	// picks up via the MxlFlowMirror watch.
	r.mu.Lock()
	live := r.targets[req.NamespacedName] != nil
	r.mu.Unlock()
	if live && mirror.Status.Phase == mxlv1alpha1.MxlFlowMirrorReady && mirror.Status.TargetInfo != "" {
		return ctrl.Result{}, nil
	}

	// Resolve the flow definition.
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: mirror.Spec.FlowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.markMaterializing(ctx, &mirror); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get MxlFlow %s: %w", mirror.Spec.FlowID, err)
	}
	if len(flow.Spec.Definition.Raw) == 0 {
		return ctrl.Result{}, fmt.Errorf("MxlFlow %s has empty spec.definition", mirror.Spec.FlowID)
	}

	provider := mapProvider(mirror.Spec.Provider)

	entry, err := r.openTarget(req.NamespacedName, string(flow.Spec.Definition.Raw), provider)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("open target: %w", err)
	}

	r.mu.Lock()
	if existing := r.targets[req.NamespacedName]; existing != nil {
		// Concurrent reconcile produced a stray entry; close the new
		// one and reuse the existing.
		r.mu.Unlock()
		closeTargetHandles(entry)
		return ctrl.Result{}, nil
	}
	r.targets[req.NamespacedName] = entry
	r.mu.Unlock()

	mirror.Status.TargetInfo = entry.infoStr
	mirror.Status.Phase = mxlv1alpha1.MxlFlowMirrorReady
	mirror.Status.ObservedGeneration = mirror.Generation
	if err := r.Status().Update(ctx, &mirror); err != nil {
		// Status update lost; close the entry so the next pass can
		// retry cleanly.
		r.closeEntry(req.NamespacedName)
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	l.Info("target ready",
		"flowID", mirror.Spec.FlowID,
		"sourceNode", mirror.Spec.SourceNode,
		"provider", provider.String())
	return ctrl.Result{}, nil
}

// openTarget walks the libmxl handshake: open FlowWriter, register
// memory regions, create + setup fabrics.Target, marshal TargetInfo,
// and start the progress goroutine.
//
// key identifies the MxlFlowMirror whose target we're opening; the
// progress goroutine uses it to invoke recovery if the libmxl-fabrics
// Target dies (the writer is retained across recoveries to keep the
// flow file valid for consumer pods).
func (r *TargetReconciler) openTarget(key types.NamespacedName, flowDef string, provider fabrics.Provider) (*targetEntry, error) {
	mxlInst := r.Handles.MXL()
	if mxlInst == nil {
		return nil, fmt.Errorf("mxl instance closed")
	}

	writer, _, err := mxlInst.NewWriter(flowDef)
	if err != nil {
		return nil, fmt.Errorf("NewWriter: %w", err)
	}
	regions, target, info, s, err := r.openFabricSide(writer, provider)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}

	entry := &targetEntry{
		writer:   writer,
		regions:  regions,
		target:   target,
		info:     info,
		infoStr:  s,
		provider: provider,
	}
	r.startProgressLoop(entry, key)
	return entry, nil
}

// openFabricSide creates the regions + fabrics.Target + TargetInfo
// on an already-open mxl.Writer. Used both by initial openTarget and
// by recoverFromFatalError when the fabric side died but the writer
// is still good.
func (r *TargetReconciler) openFabricSide(writer *mxl.Writer, provider fabrics.Provider) (*fabrics.Regions, *fabrics.Target, *fabrics.TargetInfo, string, error) {
	fabInst := r.Handles.Fabrics()
	if fabInst == nil {
		return nil, nil, nil, "", fmt.Errorf("fabrics instance closed")
	}
	regions, err := fabrics.RegionsForFlowWriter(writer)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("RegionsForFlowWriter: %w", err)
	}
	target, err := fabInst.NewTarget()
	if err != nil {
		_ = regions.Close()
		return nil, nil, nil, "", fmt.Errorf("NewTarget: %w", err)
	}
	info, err := target.Setup(fabrics.TargetConfig{
		Endpoint: fabrics.EndpointAddress{Node: r.BindAddress},
		Provider: provider,
		Regions:  regions,
	})
	if err != nil {
		_ = target.Close()
		_ = regions.Close()
		return nil, nil, nil, "", fmt.Errorf("Target.Setup: %w", err)
	}
	s, err := info.MarshalString()
	if err != nil {
		_ = info.Close()
		_ = target.Close()
		_ = regions.Close()
		return nil, nil, nil, "", fmt.Errorf("TargetInfo.MarshalString: %w", err)
	}
	return regions, target, info, s, nil
}

// startProgressLoop wires the progress goroutine for an entry and
// arms the recovery callback. Called after openTarget and again
// after every successful in-place fabric rebuild.
func (r *TargetReconciler) startProgressLoop(entry *targetEntry, key types.NamespacedName) {
	loopCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	entry.cancel = cancel
	entry.done = done
	target := entry.target
	writer := entry.writer
	go runTargetProgressLoop(loopCtx, done, target, writer, func() {
		// Detach the recovery work from the goroutine that's
		// exiting so the recovery's wait-for-done doesn't deadlock
		// on its own done channel.
		go r.recoverFromFatalError(key)
	})
}

// runTargetProgressLoop drives the libmxl-fabrics Target until ctx is
// canceled or ReadGrain reports a non-recoverable error. Each
// ReadGrain call internally advances the libfabric event +
// completion queues — without it the target never accepts incoming
// connections nor signals grain arrivals.
//
// For every grain ReadGrain reports as received, we also OpenGrain
// followed by Commit on the local FlowWriter: the initiator side
// has already RDMA'd payload + header into the ring slot, so the
// commit only advances the flow's HeadIndex so local FlowReaders
// (consumer pods) can see the new grain. Mirrors the pattern from
// upstream `mxl-fabrics-demo` tools/mxl-fabrics-demo/demo.cpp.
//
// On any error other than ErrNotReady the underlying Target is no
// longer safe to poll — libmxl-fabrics has been observed to dangle
// internal state after the remote endpoint drops, and the next
// ReadGrain call segfaults inside cgo. We exit the loop and call
// onFatal so the reconciler can drop the entry and re-open against
// a fresh Target.
func runTargetProgressLoop(ctx context.Context, done chan struct{}, target *fabrics.Target, writer *mxl.Writer, onFatal func()) {
	defer close(done)
	l := ctrl.Log.WithName("target-progress")
	const tick = 100 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		idx, err := target.ReadGrain(tick)
		switch {
		case err == nil:
			if err := commitArrivedGrain(writer, idx); err != nil {
				l.Error(err, "commit received grain", "idx", idx)
			}
		case errors.Is(err, fabrics.ErrNotReady):
			// idle tick, keep polling.
		default:
			l.Error(err, "ReadGrain — target is no longer safe to poll, exiting loop")
			if onFatal != nil {
				onFatal()
			}
			return
		}
	}
}

// commitArrivedGrain advances the local flow's HeadIndex for a grain
// whose payload + header were already filled in by the remote
// initiator's RDMA write. OpenGrain returns a writable handle aliasing
// the ring slot — we leave the slot bytes untouched and Commit so the
// flow metadata catches up.
func commitArrivedGrain(writer *mxl.Writer, idx uint64) error {
	ga, err := writer.OpenGrain(idx)
	if err != nil {
		return fmt.Errorf("OpenGrain(%d): %w", idx, err)
	}
	if err := ga.Commit(ga.TotalSlices, 0); err != nil {
		return fmt.Errorf("Commit(%d): %w", idx, err)
	}
	return nil
}

func (r *TargetReconciler) closeEntry(key types.NamespacedName) {
	r.mu.Lock()
	entry := r.targets[key]
	delete(r.targets, key)
	r.mu.Unlock()
	if entry == nil {
		return
	}
	closeTargetHandles(entry)
}

// recoverFromFatalError rebuilds the libmxl-fabrics side of a mirror
// whose Target died asynchronously, keeping the mxl.Writer alive so
// consumer FlowReaders stay valid across the recovery. The new
// TargetInfo is published to mirror.status so the source side picks
// up the rotation through its existing watch.
//
// Must be invoked from a goroutine other than the progress loop
// itself: we wait on the loop's done channel before touching the
// entry's resources.
func (r *TargetReconciler) recoverFromFatalError(key types.NamespacedName) {
	l := ctrl.Log.WithName("target-recover").WithValues("mirror", key)

	r.mu.Lock()
	entry := r.targets[key]
	r.mu.Unlock()
	if entry == nil {
		return
	}

	// Wait for the previous progress loop to finish before swapping
	// its target/regions/info pointers.
	if entry.done != nil {
		<-entry.done
	}

	// Tear down the fabric side; KEEP the writer so the flow file
	// on disk and consumer FlowReader handles stay valid.
	if entry.info != nil {
		_ = entry.info.Close()
	}
	if entry.target != nil {
		_ = entry.target.Close()
	}
	if entry.regions != nil {
		_ = entry.regions.Close()
	}
	entry.info, entry.target, entry.regions, entry.infoStr = nil, nil, nil, ""

	regions, target, info, s, err := r.openFabricSide(entry.writer, entry.provider)
	if err != nil {
		l.Error(err, "rebuild fabric side")
		// Drop the entry so the next Reconcile rebuilds from scratch
		// (closing the writer too, which will invalidate readers).
		r.mu.Lock()
		delete(r.targets, key)
		r.mu.Unlock()
		_ = entry.writer.Close()
		return
	}
	entry.regions = regions
	entry.target = target
	entry.info = info
	entry.infoStr = s
	r.startProgressLoop(entry, key)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, key, &mirror); err != nil {
		if !apierrors.IsNotFound(err) {
			l.Error(err, "get mirror during recovery")
		}
		return
	}
	mirror.Status.TargetInfo = s
	mirror.Status.Phase = mxlv1alpha1.MxlFlowMirrorReady
	if err := r.Status().Update(ctx, &mirror); err != nil {
		l.Error(err, "publish rebuilt TargetInfo")
		return
	}
	l.Info("rebuilt fabric side after fatal ReadGrain")
}

func closeTargetHandles(e *targetEntry) {
	if e.cancel != nil {
		e.cancel()
	}
	if e.done != nil {
		<-e.done
	}
	if e.info != nil {
		_ = e.info.Close()
	}
	if e.target != nil {
		_ = e.target.Close()
	}
	if e.regions != nil {
		_ = e.regions.Close()
	}
	if e.writer != nil {
		_ = e.writer.Close()
	}
}

func (r *TargetReconciler) markMaterializing(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror) error {
	if mirror.Status.Phase == mxlv1alpha1.MxlFlowMirrorMaterializing {
		return nil
	}
	mirror.Status.Phase = mxlv1alpha1.MxlFlowMirrorMaterializing
	return r.Status().Update(ctx, mirror)
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *TargetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.targets == nil {
		r.targets = make(map[types.NamespacedName]*targetEntry)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Named("mxlflowmirror-target").
		Complete(r)
}
