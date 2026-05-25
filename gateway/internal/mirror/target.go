package mirror

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

// targetFieldOwner is the server-side-apply field manager owning the
// TargetProgress condition and the target-side status fields the
// flusher writes. Distinct from the source-side manager so the two
// gateways never collide on the same conditions entry.
const targetFieldOwner = "mxl-target-gateway"

// defaultDegradedAfter is the duration of grain-commit inactivity
// after which the flusher demotes a Ready mirror to Degraded.
const defaultDegradedAfter = 10 * time.Second

// defaultTargetFlushInterval is how often the per-mirror flusher
// re-evaluates targetEntry trackers and publishes TargetProgress on
// transition.
const defaultTargetFlushInterval = 1 * time.Second

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

	// FlushInterval is how often the per-mirror status flusher
	// inspects the targetEntry trackers and publishes TargetProgress
	// when the observed phase has transitioned. Defaults to 1s.
	FlushInterval time.Duration

	// DegradedAfter is the duration of grain-commit inactivity after
	// which the flusher demotes the mirror from Ready to Degraded.
	// The same threshold gates the Reconcile fast-path: a Ready
	// status whose LastGrainAt is older than this falls through to
	// re-establish instead of short-circuiting. Defaults to 10s.
	DegradedAfter time.Duration

	// openFabricSideFn is overridable so tests exercise the recovery
	// path without a real libmxl-fabrics. Production leaves it nil
	// and the reconciler falls back to (*TargetReconciler).openFabricSide.
	openFabricSideFn func(writer *mxl.Writer, provider fabrics.Provider) (*fabrics.Regions, *fabrics.Target, *fabrics.TargetInfo, string, error)

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

	// commits counts grains the progress loop has successfully handed
	// to commitArrivedGrain. lastCommitAt records the wall-clock time
	// of the most recent successful commit. Both feed the per-mirror
	// status flusher.
	commits      atomic.Uint64
	lastCommitAt atomic.Pointer[time.Time]

	// recovering, set during recoverFromFatalError, tells the flusher
	// to back off so its writes do not race the rebuild's own status
	// publish. The recovery path clears it once the fabric side is
	// rebuilt and the new progress loop is running.
	recovering atomic.Bool

	// cancel stops the per-mirror progress goroutine; done is closed
	// when the goroutine returns. Without this loop the libmxl-fabrics
	// Target never advances its event/completion queues, so remote
	// initiators never get an FI_CONNECTED back and grains never land.
	cancel context.CancelFunc
	done   chan struct{}

	// flusherCancel stops the per-mirror status flusher; flusherDone
	// is closed when it returns.
	flusherCancel context.CancelFunc
	flusherDone   chan struct{}
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
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
		l.Info("torn down target-side mirror")
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is in place before we own any handles.
	// Concurrent reconcilers (source-side gateway, agent intent
	// dispatcher) routinely race us on the same MxlFlowMirror in
	// the moments after creation; treat an optimistic-concurrency
	// conflict as a benign requeue rather than surfacing it as a
	// stacktraced Reconciler error.
	if !controllerutil.ContainsFinalizer(&mirror, TargetFinalizerName) {
		controllerutil.AddFinalizer(&mirror, TargetFinalizerName)
		if err := r.Update(ctx, &mirror); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Idempotent fast-path. Requires a live in-memory entry, a Ready
	// status with non-empty TargetInfo, *and* fresh grain activity:
	// a gateway restart preserves status but loses the libmxl
	// FlowWriter (closing it removes the on-disk flow definition);
	// re-opening here restores the flow file and rotates TargetInfo,
	// which the source side picks up via the MxlFlowMirror watch.
	// The freshness check forces a re-establish when LastGrainAt has
	// fallen outside the degraded window: a Ready status without
	// recent commits means the fabric side has likely died silently
	// (no fatal ReadGrain error to trigger recoverFromFatalError) and
	// the flow file in the consumer pod is no longer being filled.
	r.mu.Lock()
	live := r.targets[req.NamespacedName] != nil
	r.mu.Unlock()
	if live && mirror.Status.Phase == mxlv1alpha1.MxlFlowMirrorReady && mirror.Status.TargetInfo != "" &&
		r.lastGrainFresh(mirror.Status.LastGrainAt) {
		return ctrl.Result{}, nil
	}

	// Resolve the flow definition.
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: mirror.Spec.FlowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			if mirror.Status.Phase != mxlv1alpha1.MxlFlowMirrorMaterializing {
				if err := r.applyTargetStatus(ctx, &mirror, mxlv1alpha1.MxlFlowMirrorMaterializing, nil, nil, nil); err != nil {
					return ctrl.Result{}, err
				}
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

	if err := r.applyTargetStatus(ctx, &mirror, mxlv1alpha1.MxlFlowMirrorReady, &entry.infoStr, nil, nil); err != nil {
		// Status update lost; close the entry so the next pass can
		// retry cleanly.
		r.closeEntry(req.NamespacedName)
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	r.startFlusher(req.NamespacedName, entry)

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
	regions, target, info, s, err := r.openFabricSideDispatch(writer, provider)
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

// openFabricSideDispatch routes the fabric-side open through the
// test seam when set, falling back to the cgo openFabricSide in
// production. The source reconciler routes the equivalent libmxl-
// fabrics Initiator setup through the initiatorOpener interface
// instead, but the seam serves the same purpose.
func (r *TargetReconciler) openFabricSideDispatch(writer *mxl.Writer, provider fabrics.Provider) (*fabrics.Regions, *fabrics.Target, *fabrics.TargetInfo, string, error) {
	if r.openFabricSideFn != nil {
		return r.openFabricSideFn(writer, provider)
	}
	return r.openFabricSide(writer, provider)
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
	readFn := target.ReadGrainNonBlocking
	commitFn := func(idx uint64) error { return commitArrivedGrain(writer, idx) }
	go runTargetProgressLoop(loopCtx, done, readFn, commitFn, func() {
		// Detach the recovery work from the goroutine that's
		// exiting so the recovery's wait-for-done doesn't deadlock
		// on its own done channel.
		go r.recoverFromFatalError(key)
	}, entry)
}

// commitTracker is the subset of targetEntry the progress loop
// updates after every successful commit. Defined as an interface so
// tests can drive runTargetProgressLoop with a stub.
type commitTracker interface {
	recordCommit(idx uint64, at time.Time)
}

func (e *targetEntry) recordCommit(_ uint64, at time.Time) {
	e.commits.Add(1)
	t := at
	e.lastCommitAt.Store(&t)
}

// runTargetProgressLoop drives the libmxl-fabrics Target until ctx
// is canceled or ReadGrain reports a non-recoverable error. Each
// ReadGrain call internally advances the libfabric event +
// completion queues - without it the target never accepts incoming
// connections nor signals grain arrivals.
//
// We use the non-blocking ReadGrain plus a Go-side sleep on idle
// rather than the blocking variant. libfabric's util_wait.c (line
// ~404) returns -EINTR from epoll_wait as fatal "poll failed" in
// release builds (the EINTR filter only fires under ENABLE_DEBUG);
// Go's async preemption sends SIGURG to running goroutines ~50/sec
// since Go 1.14, and that's the signal the blocking ReadGrain
// receives via the libfabric thread, tearing the endpoint down
// every 10-60 seconds in steady state. Polling from Go avoids
// blocking in libfabric, which sidesteps the signal-interaction.
//
// For every grain ReadGrain reports as received, commitFn does the
// OpenGrain + Commit dance on the local FlowWriter so consumer
// FlowReaders see the arrived grain.
//
// On any non-ErrNotReady error from ReadGrain the underlying Target
// is no longer safe to poll - libmxl-fabrics has been observed to
// dangle internal state after the remote endpoint drops, and the
// next ReadGrain call segfaults inside cgo. We exit the loop and
// call onFatal so the reconciler can rebuild the fabric side.
//
// The loop takes readFn and commitFn as injected closures so the
// state machine - the only piece prone to bugs and the only piece
// worth unit-testing - is isolated from cgo. Production passes a
// closure over fabrics.Target.ReadGrainNonBlocking and a closure
// over commitArrivedGrain(writer, ...).
func runTargetProgressLoop(
	ctx context.Context,
	done chan struct{},
	readGrain ReadGrainFunc,
	commit CommitFunc,
	onFatal func(),
	tracker commitTracker,
) {
	defer close(done)
	l := ctrl.Log.WithName("target-progress")
	const idleSleep = 1 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		idx, err := readGrain()
		switch {
		case err == nil:
			if err := commit(idx); err != nil {
				l.Error(err, "commit received grain", "idx", idx)
				break
			}
			if tracker != nil {
				tracker.recordCommit(idx, time.Now())
			}
		case errors.Is(err, fabrics.ErrNotReady):
			select {
			case <-ctx.Done():
				return
			case <-time.After(idleSleep):
			}
		default:
			l.Error(err, "ReadGrain - target is no longer safe to poll, exiting loop")
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
// the ring slot -- we leave the slot bytes untouched and Commit so the
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

	// Park the flusher: a Degraded transition published while the
	// fabric side is being rebuilt would race the Materializing write
	// below and oscillate the phase under load.
	entry.recovering.Store(true)
	defer entry.recovering.Store(false)

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

	// Publish Materializing so observers see the in-flight rebuild.
	// The flusher flips Phase back to Ready on the first commit the
	// new progress loop records.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if mirror, err := r.fetchMirror(ctx, key); err != nil {
		if !apierrors.IsNotFound(err) {
			l.Error(err, "get mirror during recovery")
		}
	} else if mirror.Status.Phase != mxlv1alpha1.MxlFlowMirrorMaterializing {
		if err := r.applyTargetStatus(ctx, mirror, mxlv1alpha1.MxlFlowMirrorMaterializing, nil, nil, nil); err != nil && !apierrors.IsNotFound(err) {
			l.Error(err, "mark Materializing during recovery")
		}
	}

	regions, target, info, s, err := r.openFabricSideDispatch(entry.writer, entry.provider)
	if err != nil {
		l.Error(err, "rebuild fabric side")
		// Drop the entry so the next Reconcile rebuilds from scratch
		// (closing the writer too, which will invalidate readers).
		r.mu.Lock()
		delete(r.targets, key)
		r.mu.Unlock()
		if entry.writer != nil {
			_ = entry.writer.Close()
		}
		return
	}
	entry.regions = regions
	entry.target = target
	entry.info = info
	entry.infoStr = s
	r.startProgressLoop(entry, key)

	mirror, err := r.fetchMirror(ctx, key)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			l.Error(err, "get mirror during recovery")
		}
		return
	}
	if err := r.applyTargetStatus(ctx, mirror, mxlv1alpha1.MxlFlowMirrorReady, &s, nil, nil); err != nil {
		l.Error(err, "publish rebuilt TargetInfo")
		return
	}
	l.Info("rebuilt fabric side after fatal ReadGrain")
}

// fetchMirror reads the freshest cached MxlFlowMirror so the SSA
// payload built from it carries the current Generation. Status writes
// must never reuse a stale Generation: the operator's
// observedGeneration gate depends on it being current.
func (r *TargetReconciler) fetchMirror(ctx context.Context, key types.NamespacedName) (*mxlv1alpha1.MxlFlowMirror, error) {
	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, key, &mirror); err != nil {
		return nil, err
	}
	return &mirror, nil
}

func closeTargetHandles(e *targetEntry) {
	if e.flusherCancel != nil {
		e.flusherCancel()
	}
	if e.flusherDone != nil {
		<-e.flusherDone
	}
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

// applyTargetStatus writes mirror.status via server-side apply with
// FieldOwner=mxl-target-gateway. It is the only path that mutates
// status on this reconciler: routing every write through one field
// manager keeps LastTransitionTime stable across reconciles and lets
// every write stamp observedGeneration off the freshly-cached object.
//
// targetInfo, lastGrainAt and cond are optional; nil pointers omit
// the corresponding key from the SSA payload so the manager does not
// claim ownership of fields it has nothing to say about.
func (r *TargetReconciler) applyTargetStatus(
	ctx context.Context,
	mirror *mxlv1alpha1.MxlFlowMirror,
	phase mxlv1alpha1.MxlFlowMirrorPhase,
	targetInfo *string,
	lastGrainAt *time.Time,
	cond *metav1.Condition,
) error {
	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirror"))
	patch.SetNamespace(mirror.Namespace)
	patch.SetName(mirror.Name)
	status := map[string]any{
		"phase":              string(phase),
		"observedGeneration": mirror.Generation,
	}
	if targetInfo != nil {
		status["targetInfo"] = *targetInfo
	}
	if lastGrainAt != nil {
		status["lastGrainAt"] = lastGrainAt.UTC().Format(time.RFC3339)
	}
	if cond != nil {
		status["conditions"] = []any{map[string]any{
			"type":               cond.Type,
			"status":             string(cond.Status),
			"reason":             cond.Reason,
			"message":            cond.Message,
			"lastTransitionTime": cond.LastTransitionTime.UTC().Format(time.RFC3339),
		}}
	}
	if err := unstructured.SetNestedField(patch.Object, status, "status"); err != nil {
		return fmt.Errorf("build SSA payload: %w", err)
	}
	return r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(targetFieldOwner),
		client.ForceOwnership,
	)
}

// degradedAfter returns the configured grain-commit freshness window,
// falling back to defaultDegradedAfter when unset.
func (r *TargetReconciler) degradedAfter() time.Duration {
	if r.DegradedAfter > 0 {
		return r.DegradedAfter
	}
	return defaultDegradedAfter
}

// flushInterval returns the configured per-mirror flusher tick,
// falling back to defaultTargetFlushInterval when unset.
func (r *TargetReconciler) flushInterval() time.Duration {
	if r.FlushInterval > 0 {
		return r.FlushInterval
	}
	return defaultTargetFlushInterval
}

// lastGrainFresh reports whether the recorded LastGrainAt timestamp
// is within the degraded window. A nil pointer (no grain ever
// observed) counts as stale - the fast-path must fall through so a
// fresh handshake gets a chance to produce one.
func (r *TargetReconciler) lastGrainFresh(t *metav1.Time) bool {
	if t == nil {
		return false
	}
	return time.Since(t.Time) < r.degradedAfter()
}

// targetProgressState is the TargetProgress condition + status
// fields the per-mirror flusher publishes via server-side apply.
type targetProgressState struct {
	phase        mxlv1alpha1.MxlFlowMirrorPhase
	status       metav1.ConditionStatus
	reason       string
	message      string
	lastCommitAt *time.Time
}

// startFlusher launches the per-mirror status flusher. The flusher
// ticks at r.flushInterval() and publishes TargetProgress only when
// the observed phase has transitioned, so a steady-state mirror
// produces zero status writes.
func (r *TargetReconciler) startFlusher(key types.NamespacedName, entry *targetEntry) {
	if entry.flusherCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	entry.flusherCancel = cancel
	entry.flusherDone = done
	go r.runFlusher(ctx, done, key, entry)
}

// runFlusher is the per-mirror status flusher loop. Tracks the most
// recently published state so a steady stream of grains does not
// turn into a steady stream of API writes.
//
// `last` is updated *before* the publish call: a transient publish
// failure leaves the next tick with a correct previous-state
// reference, so a subsequent Ready->Degraded->Recovered transition
// renders the correct reason even when an external observer races
// the post-publish bookkeeping. If the publish itself fails the
// next tick re-derives state from entry.lastCommitAt and re-attempts
// only when state genuinely changes.
func (r *TargetReconciler) runFlusher(ctx context.Context, done chan struct{}, key types.NamespacedName, entry *targetEntry) {
	defer close(done)
	t := time.NewTicker(r.flushInterval())
	defer t.Stop()

	var last targetProgressState
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// The recovery path owns Phase + Conditions while it rebuilds
		// the fabric side. Skip the tick so the flusher's Degraded
		// write does not race the recovery's Materializing publish.
		if entry.recovering.Load() {
			continue
		}
		state := observedTargetState(entry, r.degradedAfter(), last)
		// Nothing observed yet: avoid publishing a placeholder
		// TargetProgress before the handshake has had a chance to
		// hand any grain to the writer.
		if state.reason == "" {
			continue
		}
		if !first && targetStateEqual(state, last) {
			continue
		}
		last = state
		first = false
		if err := r.publishTargetProgress(ctx, key, state); err != nil {
			ctrl.Log.WithName("target-flush").Error(err, "publish",
				"mirror", key, "reason", state.reason)
		}
	}
}

// observedTargetState derives the TargetProgress state the flusher
// should publish from the entry's atomics. previous is consulted so
// a Degraded->Ready transition can publish ReasonRecovered instead
// of ReasonHandshakeComplete, and so that Recovered stays sticky
// across subsequent in-Ready ticks instead of churning back to
// HandshakeComplete and clobbering the recovery signal.
func observedTargetState(entry *targetEntry, degradedAfter time.Duration, previous targetProgressState) targetProgressState {
	lastAt := entry.lastCommitAt.Load()
	if lastAt == nil {
		// No commit observed yet. Leave Phase + Condition unset so the
		// flusher does not publish before the handshake has produced
		// any grain - the initial Reconcile already set Phase=Ready.
		return targetProgressState{}
	}
	if time.Since(*lastAt) < degradedAfter {
		reason := mxlv1alpha1.ReasonHandshakeComplete
		message := "grain commits observed"
		switch {
		case previous.phase == mxlv1alpha1.MxlFlowMirrorDegraded:
			reason = mxlv1alpha1.ReasonRecovered
			message = "grain commits resumed after stall"
		case previous.reason == mxlv1alpha1.ReasonRecovered:
			// Stay sticky on Recovered: a flap back to
			// HandshakeComplete would erase the "this mirror has
			// recovered from a stall" signal that operators rely on.
			reason = mxlv1alpha1.ReasonRecovered
			message = previous.message
		}
		t := *lastAt
		return targetProgressState{
			phase:        mxlv1alpha1.MxlFlowMirrorReady,
			status:       metav1.ConditionTrue,
			reason:       reason,
			message:      message,
			lastCommitAt: &t,
		}
	}
	t := *lastAt
	return targetProgressState{
		phase:        mxlv1alpha1.MxlFlowMirrorDegraded,
		status:       metav1.ConditionFalse,
		reason:       mxlv1alpha1.ReasonNoGrains,
		message:      "no grain commits within freshness window",
		lastCommitAt: &t,
	}
}

// targetStateEqual reports whether two states would render the same
// SSA patch. lastCommitAt is included because publishing a fresher
// LastGrainAt is the flusher's primary job - the Ready/Ready ticks
// must keep moving status forward even when phase and reason are
// stable, otherwise an external observer cannot distinguish a stuck
// gateway from a live one.
func targetStateEqual(a, b targetProgressState) bool {
	if a.phase != b.phase || a.status != b.status || a.reason != b.reason || a.message != b.message {
		return false
	}
	if (a.lastCommitAt == nil) != (b.lastCommitAt == nil) {
		return false
	}
	if a.lastCommitAt != nil && !a.lastCommitAt.Equal(*b.lastCommitAt) {
		return false
	}
	return true
}

// publishTargetProgress writes the TargetProgress condition, Phase,
// and LastGrainAt onto the MxlFlowMirror's status. The mirror is
// re-fetched from the cache on every call so the SSA payload stamps
// the current Generation rather than a value captured at flusher
// start.
func (r *TargetReconciler) publishTargetProgress(ctx context.Context, key types.NamespacedName, state targetProgressState) error {
	mirror, err := r.fetchMirror(ctx, key)
	if err != nil {
		return fmt.Errorf("get mirror for status flush: %w", err)
	}
	cond := metav1.Condition{
		Type:               mxlv1alpha1.ConditionTypeTargetProgress,
		Status:             state.status,
		Reason:             state.reason,
		Message:            state.message,
		LastTransitionTime: metav1.Now(),
	}
	return r.applyTargetStatus(ctx, mirror, state.phase, nil, state.lastCommitAt, &cond)
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
