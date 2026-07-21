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

// defaultStuckHandshakeAfter is the duration without any grain
// commit since the fabric side was last opened that the flusher
// treats as a silent libmxl-fabrics wedge: ReadGrain keeps reporting
// ErrNotReady forever, so onFatal never fires, but no FI_CONNECTED
// has landed and no grain has ever arrived. After this much time the
// flusher escalates into recoverFromFatalError instead of waiting on
// a fatal signal that will never come. 20 s sits between the typical
// post-DaemonSet-rollout reconnect window (~15 s observed) and the
// kind testcase-60 ceiling (STUCK_SECS=45 s).
const defaultStuckHandshakeAfter = 20 * time.Second

// maxStuckRebuilds bounds the number of consecutive recovery spawns
// the watchdog will issue without seeing a grain commit land. On the
// (maxStuckRebuilds+1)th observation of a stuck handshake the flusher
// publishes Phase=Failed with reason StuckHandshakeCapReached, drops
// the entry, closes the writer (invalidating consumer FlowReaders),
// and exits; the next Reconcile rebuilds from scratch. The counter
// resets on the first commit after each successful fabric open, so
// the cap counts *consecutive* failed rebuilds, not lifetime ones.
const maxStuckRebuilds uint32 = 3

// ReasonStuckHandshakeCapReached marks a target-side mirror whose
// libmxl-fabrics handshake never produced a grain commit across
// maxStuckRebuilds consecutive watchdog-driven rebuilds. Distinct
// from ReasonNoGrains: NoGrains is a recoverable Degraded state, this
// reason accompanies a terminal Phase=Failed and signals that the
// gateway has given up rebuilding the target in place.
const ReasonStuckHandshakeCapReached = "StuckHandshakeCapReached"

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

	// StuckHandshakeAfter is the duration without any grain commit
	// since the fabric side was opened that the flusher treats as a
	// silent libmxl-fabrics wedge (ErrNotReady forever, no fatal
	// signal). On reaching it the flusher spawns recoverFromFatalError
	// instead of waiting on a fatal that will not come. Defaults to
	// defaultStuckHandshakeAfter.
	StuckHandshakeAfter time.Duration

	// openFabricSideFn is overridable so tests exercise the recovery
	// path without a real libmxl-fabrics. Production leaves it nil
	// and the reconciler falls back to (*TargetReconciler).openFabricSide.
	openFabricSideFn func(writer *mxl.Writer, provider fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error)

	// recoverFn is the seam the stuck-handshake watchdog uses to
	// spawn its recovery work. Production leaves it nil and the
	// flusher invokes recoverFromFatalError directly; tests inject a
	// stub so the watchdog's spawn/cap behavior can be observed
	// without driving a real libmxl-fabrics rebuild. Kept distinct
	// from openFabricSideFn because the watchdog's contract is
	// "fire-and-forget" — it does not own the recovery result, only
	// the gate that prevents double spawns.
	recoverFn func(key types.NamespacedName)

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

	// fabricOpenedAt is the wall-clock the current fabric side became
	// live (initial openTarget or a recoverFromFatalError rebuild).
	// commitsAtFabricOpen snapshots the commits counter at the same
	// moment. Together they let the flusher discriminate "no commits
	// yet because we just opened" from "no commits because the
	// handshake is silently wedged" without needing a separate
	// state-machine flag.
	//
	// Atomic because the recovery goroutine swaps them inside
	// startProgressLoop while the flusher tick reads them. The
	// existing entry.recovering atomic gates the flusher's read but
	// only after the recovery has cleared it; the watchdog block
	// reads these values from the flusher without ever taking r.mu.
	// nil pointer (zero value) means "fabric side never opened".
	fabricOpenedAt      atomic.Pointer[time.Time]
	commitsAtFabricOpen atomic.Uint64

	// recoveryAttempts counts consecutive watchdog-spawned recovery
	// invocations that did not result in a fresh commit. recordCommit
	// resets it to zero on the first commit after each fabric open,
	// so the cap counts *consecutive* failed rebuilds rather than
	// lifetime ones. The flusher caps spawns at maxStuckRebuilds.
	recoveryAttempts atomic.Uint32

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
		// The MxlFlow exists but the producer has not published its
		// definition yet. Treat it like a not-yet-materialized flow:
		// surface the reason and requeue instead of returning a bare
		// error that leaves the mirror sitting at an empty phase.
		r.surfaceTargetFailure(ctx, &mirror, mxlv1alpha1.ReasonFlowDefinitionEmpty,
			fmt.Sprintf("MxlFlow %s has empty spec.definition", mirror.Spec.FlowID))
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	provider, err := providerForSetup(&mirror)
	if err != nil {
		// Never forward auto into libmxl-fabrics: surface the reason on
		// status and stop. The agent or operator patches spec.provider to
		// a concrete value, which wakes this reconciler through its watch.
		r.surfaceTargetFailure(ctx, &mirror, mxlv1alpha1.ReasonProviderUnresolved, err.Error())
		l.Info("refusing target setup: mirror provider is unresolved", "error", err.Error())
		return ctrl.Result{}, nil
	}

	entry, err := r.openTarget(req.NamespacedName, string(flow.Spec.Definition.Raw), provider)
	if err != nil {
		// openTarget wraps NewWriter + the libmxl-fabrics Target.Setup,
		// whose errors otherwise land only in the gateway log — the mirror
		// is left at an empty phase, so the consumer (and cluster
		// diagnostics, which only see the CR) get no signal for why it
		// never went Ready. Surface the cause, then let the rate-limited
		// requeue retry.
		r.surfaceTargetFailure(ctx, &mirror, mxlv1alpha1.ReasonOpenTargetFailed, err.Error())
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

// openTarget walks the libmxl handshake: open FlowWriter, create +
// setup fabrics.Target against it, marshal TargetInfo, and start the
// progress goroutine.
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
	target, info, s, err := r.openFabricSideDispatch(writer, provider)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}

	entry := &targetEntry{
		writer:   writer,
		target:   target,
		info:     info,
		infoStr:  s,
		provider: provider,
	}
	r.startProgressLoop(entry, key)
	return entry, nil
}

// openFabricSide creates the fabrics.Target + TargetInfo on an
// already-open mxl.Writer. Used both by initial openTarget and by
// recoverFromFatalError when the fabric side died but the writer is
// still good.
func (r *TargetReconciler) openFabricSide(writer *mxl.Writer, provider fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
	fabInst := r.Handles.Fabrics()
	if fabInst == nil {
		return nil, nil, "", fmt.Errorf("fabrics instance closed")
	}
	target, err := fabInst.NewTarget()
	if err != nil {
		return nil, nil, "", fmt.Errorf("NewTarget: %w", err)
	}
	info, err := target.Setup(fabrics.TargetConfig{
		Interface: fabrics.InterfaceConfig{
			Provider: provider,
			Address:  fabrics.EndpointAddress{Node: r.BindAddress},
		},
		Writer: writer,
	})
	if err != nil {
		_ = target.Close()
		return nil, nil, "", fmt.Errorf("Target.Setup: %w", err)
	}
	s, err := info.MarshalString()
	if err != nil {
		_ = info.Close()
		_ = target.Close()
		return nil, nil, "", fmt.Errorf("TargetInfo.MarshalString: %w", err)
	}
	return target, info, s, nil
}

// openFabricSideDispatch routes the fabric-side open through the
// test seam when set, falling back to the cgo openFabricSide in
// production. The source reconciler routes the equivalent libmxl-
// fabrics Initiator setup through the initiatorOpener interface
// instead, but the seam serves the same purpose.
func (r *TargetReconciler) openFabricSideDispatch(writer *mxl.Writer, provider fabrics.Provider) (*fabrics.Target, *fabrics.TargetInfo, string, error) {
	if r.openFabricSideFn != nil {
		return r.openFabricSideFn(writer, provider)
	}
	return r.openFabricSide(writer, provider)
}

// startProgressLoop wires the progress goroutine for an entry and
// arms the recovery callback. Called after openTarget and again
// after every successful in-place fabric rebuild.
func (r *TargetReconciler) startProgressLoop(entry *targetEntry, key types.NamespacedName) {
	// Re-arm the stuck-handshake watchdog reference: every fabric
	// open (initial or rebuilt) gets its own elapsed window measured
	// against its own commits baseline. Without resetting here the
	// post-recovery flusher would trip the watchdog immediately on
	// the carried-over (now-stale) fabricOpenedAt.
	now := time.Now()
	entry.fabricOpenedAt.Store(&now)
	entry.commitsAtFabricOpen.Store(entry.commits.Load())

	loopCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	entry.cancel = cancel
	entry.done = done
	target := entry.target
	writer := entry.writer
	onFatal := func() {
		// Detach the recovery work from the goroutine that's exiting
		// so the recovery's wait-for-done doesn't deadlock on its own
		// done channel.
		go r.recoverFromFatalError(key)
	}
	// A continuous (audio) flow receives sample runs, not grains; pick
	// the progress path from the flow's data format. The writer's
	// cached config is the source of truth for which API is valid.
	if writer.Config().Common.Format == mxl.FormatAudio {
		readFn := target.ReadSamplesNonBlocking
		commitFn := func(head uint64, count int) error {
			return commitArrivedSamples(writer, head, count)
		}
		go runTargetSampleProgressLoop(loopCtx, done, readFn, commitFn, onFatal, entry)
	} else {
		readFn := target.ReadGrainNonBlocking
		commitFn := func(idx uint64) error { return commitArrivedGrain(writer, idx) }
		go runTargetProgressLoop(loopCtx, done, readFn, commitFn, onFatal, entry)
	}
}

// commitTracker is the subset of targetEntry the progress loop
// updates after every successful commit. Defined as an interface so
// tests can drive runTargetProgressLoop with a stub.
type commitTracker interface {
	recordCommit(idx uint64, at time.Time)
}

func (e *targetEntry) recordCommit(_ uint64, at time.Time) {
	n := e.commits.Add(1)
	t := at
	e.lastCommitAt.Store(&t)
	// First commit after this fabric side opened disarms the
	// stuck-handshake cap so a previously-rebuilt-successfully entry
	// gets a fresh budget if it later wedges again. recoveryAttempts
	// thus counts *consecutive* unsuccessful rebuilds, not lifetime
	// ones.
	if n == e.commitsAtFabricOpen.Load()+1 {
		e.recoveryAttempts.Store(0)
	}
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

// runTargetSampleProgressLoop drives a libmxl-fabrics Target for a
// continuous (audio) flow until ctx is canceled or ReadSamples reports
// a non-recoverable error. It mirrors runTargetProgressLoop: the
// non-blocking ReadSamples advances the libfabric event + completion
// queues on every call, and the same Go-side idle sleep avoids the
// blocking-variant SIGURG interaction documented there. For every run
// of samples ReadSamples reports as arrived, commit does the
// OpenSamples + Commit dance on the local FlowWriter so consumer
// FlowReaders see them. Any non-ErrNotReady error is fatal and fires
// onFatal so the reconciler rebuilds the fabric side.
func runTargetSampleProgressLoop(
	ctx context.Context,
	done chan struct{},
	readSamples ReadSamplesFunc,
	commit CommitSamplesFunc,
	onFatal func(),
	tracker commitTracker,
) {
	defer close(done)
	l := ctrl.Log.WithName("target-sample-progress")
	const idleSleep = 1 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		head, count, err := readSamples()
		switch {
		case err == nil:
			if err := commit(head, count); err != nil {
				l.Error(err, "commit received samples", "headIndex", head, "count", count)
				break
			}
			if tracker != nil {
				tracker.recordCommit(head, time.Now())
			}
		case errors.Is(err, fabrics.ErrNotReady):
			select {
			case <-ctx.Done():
				return
			case <-time.After(idleSleep):
			}
		default:
			l.Error(err, "ReadSamples - target is no longer safe to poll, exiting loop")
			if onFatal != nil {
				onFatal()
			}
			return
		}
	}
}

// commitArrivedSamples advances the local flow's head for a run of
// samples whose payload was already filled in by the remote
// initiator's RDMA write. OpenSamples returns a writable view aliasing
// the ring; we leave the bytes untouched and Commit so the flow
// metadata catches up, mirroring commitArrivedGrain.
func commitArrivedSamples(writer *mxl.Writer, head uint64, count int) error {
	sa, err := writer.OpenSamples(head, count)
	if err != nil {
		return fmt.Errorf("OpenSamples(%d,%d): %w", head, count, err)
	}
	if err := sa.Commit(); err != nil {
		return fmt.Errorf("CommitSamples(%d,%d): %w", head, count, err)
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

// dispatchRecovery routes the stuck-handshake watchdog's spawn
// through the recoverFn seam when set, falling back to the cgo-
// dependent recoverFromFatalError in production. The runTargetProgressLoop
// onFatal callback continues to invoke recoverFromFatalError
// directly: that path only fires on a fatal ReadGrain return, which
// the tests cannot reach without real libmxl-fabrics anyway.
func (r *TargetReconciler) dispatchRecovery(key types.NamespacedName) {
	if r.recoverFn != nil {
		r.recoverFn(key)
		return
	}
	r.recoverFromFatalError(key)
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

	// Cancel the previous progress loop before waiting on its done
	// channel. The onFatal caller has already exited the loop, so the
	// cancel is a no-op for that path. The stuck-handshake watchdog
	// caller has not: ReadGrain keeps reporting ErrNotReady on a wedge
	// that no fatal signal will resolve, so without the explicit cancel
	// the wait below would block forever and pin entry.recovering=true.
	// The flusher would then skip every subsequent tick, no further
	// recovery would ever spawn, and the cap branch could never fire.
	if entry.cancel != nil {
		entry.cancel()
	}

	// Wait for the previous progress loop to finish before swapping
	// its target/info pointers.
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
	entry.info, entry.target, entry.infoStr = nil, nil, ""

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

	target, info, s, err := r.openFabricSideDispatch(entry.writer, entry.provider)
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

// surfaceTargetFailure publishes Phase=Materializing plus a
// TargetProgress=False condition carrying the reason the target side
// could not be established yet. Best-effort: a failed status write must
// not mask the original error the caller returns/requeues on, so the
// result is intentionally ignored. It exists so a target-open failure
// shows up in MxlFlowMirror status (and `kubectl describe`) instead of
// the mirror wedging silently at an empty phase — the producer, the
// consumer, and the cluster diagnostics only observe the CR, never the
// gateway log. Only reached on the pre-Ready path (the Reconcile
// fast-path returns earlier for a live, fresh, Ready mirror), so this
// never demotes a healthy mirror.
func (r *TargetReconciler) surfaceTargetFailure(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror, reason, message string) {
	_ = r.applyTargetStatus(ctx, mirror, mxlv1alpha1.MxlFlowMirrorMaterializing, nil, nil, &metav1.Condition{
		Type:               mxlv1alpha1.ConditionTypeTargetProgress,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	})
}

// degradedAfter returns the configured grain-commit freshness window,
// falling back to defaultDegradedAfter when unset.
func (r *TargetReconciler) degradedAfter() time.Duration {
	if r.DegradedAfter > 0 {
		return r.DegradedAfter
	}
	return defaultDegradedAfter
}

// stuckHandshakeAfter returns the configured silent-wedge timeout the
// flusher uses to escalate a non-progressing target into recovery,
// falling back to defaultStuckHandshakeAfter when unset.
func (r *TargetReconciler) stuckHandshakeAfter() time.Duration {
	if r.StuckHandshakeAfter > 0 {
		return r.StuckHandshakeAfter
	}
	return defaultStuckHandshakeAfter
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
		// Stuck-handshake watchdog. Two wedge shapes need recovery:
		//
		// neverHandshook: a libmxl-fabrics target whose remote
		// initiator never sent FI_CONNECTED keeps reporting
		// ErrNotReady forever - the progress loop never raises
		// onFatal, so recoverFromFatalError never fires, and the
		// flusher would otherwise just oscillate between Ready and
		// Degraded as time goes by. Spawn recovery explicitly when
		// no commit has landed since the fabric side opened and the
		// stuck-handshake window has elapsed.
		//
		// postHandshakeWedge: the handshake succeeded and at least
		// one grain committed, then the fabric side wedged silently.
		// commits > commitsAtFabricOpen makes neverHandshook false
		// forever, so it cannot catch this. Cross-side coordination
		// via mirror.Status.LastSentAt distinguishes a wedge ("source
		// is sending but target is not committing") from a legitimately
		// idle flow ("source is not sending either"); idle flows must
		// not trigger recovery.
		//
		// A nil fabricOpenedAt guards entries that the flusher runs
		// against without a real openTarget/recovery handoff (existing
		// flusher tests construct bare targetEntry values and drive
		// the flusher directly): the watchdog must not fire on an
		// entry whose fabric side was never actually opened.
		openedAt := entry.fabricOpenedAt.Load()
		neverHandshook := openedAt != nil &&
			entry.commits.Load() == entry.commitsAtFabricOpen.Load() &&
			time.Since(*openedAt) > r.stuckHandshakeAfter()
		postHandshakeWedge := false
		if openedAt != nil && entry.commits.Load() > entry.commitsAtFabricOpen.Load() {
			lastCommit := entry.lastCommitAt.Load()
			if lastCommit != nil {
				if mirror, err := r.fetchMirror(ctx, key); err == nil &&
					mirror.Status.LastSentAt != nil &&
					mirror.Status.LastSentAt.Time.Sub(*lastCommit) > r.stuckHandshakeAfter() {
					postHandshakeWedge = true
				}
			}
		}
		if neverHandshook || postHandshakeWedge {
			if entry.recoveryAttempts.Load() >= maxStuckRebuilds {
				// Cap reached: rebuilds keep failing to attract a
				// commit. Publish a terminal Phase=Failed so operators
				// see an explicit dead state, tear down the entry the
				// way recoverFromFatalError's failure path does, and
				// exit the flusher. The next Reconcile rebuilds the
				// entry from scratch through openTarget.
				//
				// closeEntry must NOT be called from here:
				// closeTargetHandles waits on flusherDone and the
				// flusher is the goroutine that closes it on return,
				// so a closeEntry from this point would deadlock the
				// flusher on its own done channel. The teardown below
				// matches recoverFromFatalError's drop-entry exit
				// shape: cancel progress loop, wait for done, delete
				// from r.targets, close writer, return.
				log.FromContext(ctx).Error(nil,
					"stuck handshake; recovery cap reached, dropping entry",
					"mirror", key,
					"attempts", entry.recoveryAttempts.Load())
				if mirror, err := r.fetchMirror(ctx, key); err == nil {
					_ = r.applyTargetStatus(ctx, mirror, mxlv1alpha1.MxlFlowMirrorFailed, &entry.infoStr, nil, &metav1.Condition{
						Type:               mxlv1alpha1.ConditionTypeTargetProgress,
						Status:             metav1.ConditionFalse,
						Reason:             ReasonStuckHandshakeCapReached,
						Message:            fmt.Sprintf("no commits in %s across %d rebuild attempts", r.stuckHandshakeAfter(), maxStuckRebuilds),
						LastTransitionTime: metav1.Now(),
					})
				}
				if entry.cancel != nil {
					entry.cancel()
				}
				if entry.done != nil {
					// Mirror recoverFromFatalError's ordering
					// (target.go drop-entry exit): the progress loop
					// must release its libmxl handles before Close
					// runs underneath it.
					<-entry.done
				}
				r.mu.Lock()
				delete(r.targets, key)
				r.mu.Unlock()
				if entry.writer != nil {
					// Invalidates consumer FlowReaders, matching
					// recoverFromFatalError's failure exit. A
					// concurrent closeEntry+closeTargetHandles would
					// also Close this writer; double-close on
					// *mxl.Writer is safe per the existing precedent
					// in closeTargetHandles.
					_ = entry.writer.Close()
				}
				return
			}
			if entry.recovering.CompareAndSwap(false, true) {
				// CompareAndSwap, not Load+Store: two flusher ticks
				// observing "not recovering" simultaneously would
				// otherwise both spawn recoverFromFatalError on the
				// same entry. CAS makes the spawn atomic. The
				// goroutine clears recovering via the deferred
				// Store(false) inside recoverFromFatalError.
				//
				// Re-check the wedge predicate after acquiring the
				// gate: a progress-loop commit landing between the
				// read above and the CAS would update commits +
				// lastCommitAt and reset recoveryAttempts via
				// recordCommit. Without the re-check the watchdog
				// would still spawn a rebuild against an entry that
				// just made progress, which both wastes the rebuild
				// budget and would be visible as an extra
				// recoveryAttempts bump after the reset. Re-fetching
				// the mirror covers the post-handshake variant: a
				// LastSentAt change that lands in the same window
				// must be observed before the spawn commits.
				openedAt2 := entry.fabricOpenedAt.Load()
				neverHandshook2 := openedAt2 != nil &&
					entry.commits.Load() == entry.commitsAtFabricOpen.Load() &&
					time.Since(*openedAt2) > r.stuckHandshakeAfter()
				postHandshakeWedge2 := false
				if openedAt2 != nil && entry.commits.Load() > entry.commitsAtFabricOpen.Load() {
					lastCommit2 := entry.lastCommitAt.Load()
					if lastCommit2 != nil {
						if mirror, err := r.fetchMirror(ctx, key); err == nil &&
							mirror.Status.LastSentAt != nil &&
							mirror.Status.LastSentAt.Time.Sub(*lastCommit2) > r.stuckHandshakeAfter() {
							postHandshakeWedge2 = true
						}
					}
				}
				if !neverHandshook2 && !postHandshakeWedge2 {
					entry.recovering.Store(false)
					continue
				}
				attempt := entry.recoveryAttempts.Add(1)
				log.FromContext(ctx).Info("stuck handshake; triggering recovery",
					"mirror", key, "attempt", attempt)
				go r.dispatchRecovery(key)
			}
			// Skip the Degraded write this tick: publishing it would
			// flap Ready->Degraded->Materializing->Ready around the
			// rebuild. The watchdog and the would-be Degraded write
			// fire on the same discriminator, so the recovery must
			// take precedence.
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
		if err := r.publishTargetProgress(ctx, key, state, entry); err != nil {
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
// LastGrainAt, and TargetInfo onto the MxlFlowMirror's status. The
// mirror is re-fetched from the cache on every call so the SSA
// payload stamps the current Generation rather than a value captured
// at flusher start. TargetInfo is re-stamped on every flush because
// SSA with a single FieldOwner releases ownership of fields omitted
// from a subsequent payload; without re-stamping the apiserver would
// strip status.targetInfo after the second flush.
func (r *TargetReconciler) publishTargetProgress(ctx context.Context, key types.NamespacedName, state targetProgressState, entry *targetEntry) error {
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
	return r.applyTargetStatus(ctx, mirror, state.phase, &entry.infoStr, state.lastCommitAt, &cond)
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
