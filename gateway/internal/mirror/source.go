package mirror

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
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

// sourceFieldOwner is the server-side-apply field manager owning the
// SourceProgress condition on MxlFlowMirror status. The target-side
// reconciler uses a different manager so the two never collide on
// the same conditions entry.
const sourceFieldOwner = "mxl-source-gateway"

// mirrorFlowIDIndex is the field-index key registered against
// MxlFlowMirror on spec.flowID. The Lease watch uses it to map a
// Lease event back to the matching MxlFlowMirrors without a cluster-
// wide scan. The string matches the operator's flowIDIndex on
// MxlReceiver verbatim so a cross-package audit can confirm both
// indexers point at the same logical key.
const mirrorFlowIDIndex = "spec.flowID"

// errAddTargetFailed wraps a libmxl-fabrics AddTarget failure so the
// reconciler can errors.Is detect it and engage bounded backoff
// without rebuilding the initiator from scratch on every tick.
var errAddTargetFailed = errors.New("AddTarget failed")

// readerAgedOutMarker is the substring libmxl returns when
// GetGrainNonBlocking is called for an index the writer has already
// overwritten in the ring. Matched on Error() until go-mxl exposes
// a typed sentinel.
//
// TODO(go-mxl): swap to typed sentinel when wrapper exposes one.
const readerAgedOutMarker = "out of range (too late)"

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

	// FlushInterval is how often the per-mirror status flusher
	// checks the sourceEntry trackers and publishes SourceProgress
	// when the observed state has transitioned. Defaults to 1s.
	FlushInterval time.Duration

	// opener is the seam onto the cgo-dependent libmxl-fabrics
	// Initiator setup path. SetupWithManager binds it to a
	// libmxlOpener built from Handles + NodeName + BindAddress;
	// tests build their own inline fake. The production binary
	// therefore never carries a swappable function pointer that a
	// later caller could redirect.
	opener initiatorOpener

	mu       sync.Mutex
	sources  map[types.NamespacedName]*sourceEntry
	attempts map[types.NamespacedName]uint32
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

	// progress counts grains the transfer loop has successfully
	// handed to TransferGrain. lastSentAt records the wall-clock
	// time of the most recent successful transfer. Both feed the
	// per-mirror status flusher.
	progress   atomic.Uint64
	lastSentAt atomic.Pointer[time.Time]

	// agedOutAt records the wall-clock of the most recent
	// reader-aged-out skip the transfer loop has had to perform.
	// Used by the flusher to publish SourceProgress with reason
	// ReaderAgedOut on the transition.
	agedOutAt atomic.Pointer[time.Time]

	// addTargetAttempts mirrors r.attempts for this key into the
	// flusher's view. lastError records the most recent reconcile
	// error message for the same purpose.
	addTargetAttempts atomic.Uint32
	lastError         atomic.Pointer[string]

	// lastObservedOriginAt records the MxlFlow status.locations
	// entry's LastObserved timestamp for r.NodeName at the moment
	// the FlowReader was opened. A subsequent reconcile that sees
	// a newer timestamp tears the reader down and reopens it so
	// the gateway tails the freshly rebound writer.
	lastObservedOriginAt atomic.Pointer[time.Time]

	// cancel stops the per-flow transfer goroutine; done is closed
	// when the goroutine returns.
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

// Reconcile drives one MxlFlowMirror through its source-side
// lifecycle.
func (r *SourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)

	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &mirror); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if mirror.Spec.SourceNode != r.NodeName {
		// spec.sourceNode used to name this node and was mutated to
		// another; the in-memory sources map keeps an RCInitiator open
		// against a producer-less .mxl-flow until the pod is bounced.
		// closeEntry is a no-op when key absent (def at line 592).
		r.closeEntry(req.NamespacedName)
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

	// originAt is the MxlFlow's most recent LastObserved timestamp
	// for the Origin location on this node. It is read once here so
	// the fast-path comparison below and the openInitiator handoff
	// both observe the same value, and a later rotation is detected
	// even if the MxlFlow watch fires before status propagates.
	originAt := r.observedOriginAt(ctx, mirror.Spec.FlowID)

	// Already set up against this exact target info?
	r.mu.Lock()
	existing := r.sources[req.NamespacedName]
	r.mu.Unlock()
	if existing != nil {
		// MxlFlow Origin location for this node has rotated since
		// the FlowReader was opened (the writer-side agent
		// re-registered the flow, typically after a pod restart).
		// Tear down + reopen so the reader tails the fresh writer
		// instead of holding a handle on a now-invalid ring.
		if originRotated(existing.lastObservedOriginAt.Load(), originAt) {
			l.Info("flow origin rotated, reopening reader")
			r.closeEntry(req.NamespacedName)
		} else if existing.infoStr == mirror.Status.TargetInfo {
			return ctrl.Result{}, nil
		} else {
			// TargetInfo rotated under us (typically: target gateway
			// restarted and rebuilt the writer on a fresh ephemeral
			// port). Tear down the stale initiator so we re-open
			// against the new address. The target side also lands
			// here after recoverFromFatalError republishes
			// status.targetInfo via SSA on its Ready transition --
			// the infoStr comparison above is the source initiator's
			// only wake-up signal for that rotation, so do not add a
			// spec-only predicate to the MxlFlowMirror watch or
			// recovery wakes will be silently dropped.
			l.Info("target info rotated, rebuilding initiator")
			r.closeEntry(req.NamespacedName)
		}
	}

	provider := mapProvider(mirror.Spec.Provider)

	// Seed the in-memory AddTarget attempts counter from the persisted
	// status.attemptCount so a gateway pod restart does not reset the
	// backoff for a target that has been unreachable across the bounce.
	// Only the counter is restored: the next attempt fires immediately
	// rather than waiting out a remembered backoff window, which keeps
	// the design's "one free retry on restart" budget intact.
	r.mu.Lock()
	if _, ok := r.attempts[req.NamespacedName]; !ok && mirror.Status.AttemptCount > 0 {
		r.attempts[req.NamespacedName] = uint32(mirror.Status.AttemptCount)
	}
	r.mu.Unlock()

	entry, err := r.opener.open(mirror.Spec.FlowID, mirror.Status.TargetInfo, provider)
	if err != nil {
		if errors.Is(err, errAddTargetFailed) {
			return r.handleAddTargetFailure(ctx, req.NamespacedName, err)
		}
		return ctrl.Result{}, fmt.Errorf("open initiator: %w", err)
	}
	if originAt != nil {
		t := *originAt
		entry.lastObservedOriginAt.Store(&t)
	}

	r.mu.Lock()
	if dup := r.sources[req.NamespacedName]; dup != nil {
		r.mu.Unlock()
		closeSourceHandles(entry)
		return ctrl.Result{}, nil
	}
	r.sources[req.NamespacedName] = entry
	delete(r.attempts, req.NamespacedName)
	r.mu.Unlock()

	r.startFlusher(req.NamespacedName, entry)

	l.Info("initiator running",
		"flowID", mirror.Spec.FlowID,
		"targetNode", mirror.Spec.TargetNode,
		"provider", provider.String())
	return ctrl.Result{}, nil
}

// handleAddTargetFailure records the failed AddTarget attempt and
// returns a bounded-backoff RequeueAfter so the libmxl-fabrics
// initiator is not torn down and rebuilt every controller-runtime
// tick while the target is unreachable.
func (r *SourceReconciler) handleAddTargetFailure(ctx context.Context, key types.NamespacedName, addErr error) (ctrl.Result, error) {
	r.mu.Lock()
	r.attempts[key]++
	attempts := r.attempts[key]
	r.mu.Unlock()

	msg := addErr.Error()
	if err := r.publishSourceProgress(ctx, key, sourceProgressState{
		status:   metav1.ConditionFalse,
		reason:   mxlv1alpha1.ReasonAddTargetFailed,
		message:  msg,
		attempts: attempts,
	}); err != nil {
		log.FromContext(ctx).Error(err, "publish SourceProgress")
	}
	return ctrl.Result{RequeueAfter: backoffFor(attempts)}, nil
}

// backoffFor returns 100ms * 2^(attempts-1) capped at 30s. The cap
// matches the controller-runtime default rate-limiter ceiling so a
// permanently unreachable target does not consume more than one
// reconcile per 30s.
func backoffFor(attempts uint32) time.Duration {
	if attempts == 0 {
		return 100 * time.Millisecond
	}
	const cap = 30 * time.Second
	d := 100 * time.Millisecond
	for i := uint32(1); i < attempts; i++ {
		d *= 2
		if d >= cap {
			return cap
		}
	}
	return d
}

// observedOriginAt returns the LastObserved timestamp of the Origin
// location entry on r.NodeName for the given flow, or nil if the
// flow or location is missing. Errors are logged but not fatal: a
// missing flow means the reconciler will wait for the MxlFlow watch
// to fire.
func (r *SourceReconciler) observedOriginAt(ctx context.Context, flowID string) *time.Time {
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		return nil
	}
	for _, loc := range flow.Status.Locations {
		if loc.NodeName != r.NodeName || loc.Phase != mxlv1alpha1.MxlFlowLocationOrigin {
			continue
		}
		if loc.LastObserved == nil {
			return nil
		}
		t := loc.LastObserved.Time
		return &t
	}
	return nil
}

// originRotated reports whether the freshly observed origin
// timestamp is strictly after the one recorded on the entry. A nil
// observation means "no data yet" and never counts as a rotation;
// a nil baseline (entry never saw an origin timestamp) means the
// first observation is not a rotation either.
func originRotated(baseline, observed *time.Time) bool {
	if observed == nil || baseline == nil {
		return false
	}
	return observed.After(*baseline)
}

// libmxlOpener is the production initiatorOpener implementation. It
// opens a FlowReader on the local flow, registers its regions with
// libmxl-fabrics, creates and sets up an Initiator, AddTarget()s the
// remote info, and starts the per-flow transfer goroutine. The
// fields are the subset of SourceReconciler state the open path
// reads; the reconciler hands a fully populated value at
// SetupWithManager time.
type libmxlOpener struct {
	Handles          *instance.Handles
	NodeName         string
	BindAddress      string
	ProgressInterval time.Duration
}

func (o *libmxlOpener) open(flowID, targetInfoStr string, provider fabrics.Provider) (*sourceEntry, error) {
	mxlInst := o.Handles.MXL()
	if mxlInst == nil {
		return nil, fmt.Errorf("mxl instance closed")
	}
	fabInst := o.Handles.Fabrics()
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
		Endpoint: fabrics.EndpointAddress{Node: o.BindAddress},
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
		return nil, fmt.Errorf("%w: %w", errAddTargetFailed, err)
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

	progressInterval := o.ProgressInterval
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
	go runTransferLoop(loopCtx, done, flowID, runtimeFn, transferFn, progressFn, progressInterval, entry)

	return entry, nil
}

// progressTracker is the subset of sourceEntry the transfer loop
// updates. Defined as an interface so tests can inject a stub
// without constructing a full sourceEntry.
type progressTracker interface {
	recordTransfer(idx uint64, at time.Time)
	recordAgedOut(at time.Time)
}

func (e *sourceEntry) recordTransfer(idx uint64, at time.Time) {
	e.progress.Add(1)
	t := at
	e.lastSentAt.Store(&t)
}

func (e *sourceEntry) recordAgedOut(at time.Time) {
	t := at
	e.agedOutAt.Store(&t)
}

// runTransferLoop pumps grains that appear on the source flow into
// the initiator until ctx is canceled. Closes done on exit.
//
// The loop is parameterised by three injected functions so the
// cgo-dependent calls stay isolated from the state machine: tests
// can drive it with closures that return canned indices and record
// every interaction. The tracker receives per-grain progress and
// the aged-out skip signal so the per-mirror flusher can surface
// them on status without touching the cgo path.
func runTransferLoop(
	ctx context.Context,
	done chan struct{},
	flowID string,
	probeRuntime RuntimeProbe,
	transferGrain TransferFunc,
	makeProgress ProgressFunc,
	interval time.Duration,
	tracker progressTracker,
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
				// libmxl reports "out of range (too late)" when the
				// writer has already overwritten the slot the reader
				// is asking for. The grains between lastSent+1 and
				// head are unrecoverable; advance lastSent to head
				// so the loop tails the live flow again, and signal
				// the tracker so the reconciler can publish the
				// SourceProgress=ReaderAgedOut condition.
				//
				// TODO(go-mxl): swap to typed sentinel when wrapper
				// exposes one.
				if isReaderAgedOut(err) {
					l.Info("reader aged out, skipping grains",
						"fromIndex", lastSent+1, "toIndex", int64(head))
					if tracker != nil {
						tracker.recordAgedOut(time.Now())
					}
					lastSent = int64(head)
					break
				}
				l.Error(err, "TransferGrain", "index", idx)
				break
			}
			if tracker != nil {
				tracker.recordTransfer(uint64(idx), time.Now())
			}
			lastSent = idx
		}

		if err := makeProgress(); err != nil && !errors.Is(err, fabrics.ErrNotReady) {
			l.Error(err, "MakeProgress")
		}
	}
}

// isReaderAgedOut reports whether the error returned by
// GetGrainNonBlocking / TransferGrain is the libmxl "the writer
// has lapped the reader" signal.
func isReaderAgedOut(err error) bool {
	return err != nil && strings.Contains(err.Error(), readerAgedOutMarker)
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

// sourceProgressState is the SourceProgress condition the source
// gateway publishes via server-side apply.
type sourceProgressState struct {
	status   metav1.ConditionStatus
	reason   string
	message  string
	attempts uint32
	// lastSentAt carries the wall-clock of the most recent successful
	// TransferGrain forward into the SSA payload. The target-side
	// stuck-handshake watchdog reads it to discriminate "source is
	// sending but target is wedged" from "source is idle". Unset
	// before the first transfer.
	lastSentAt *time.Time
}

// publishSourceProgress writes a single SourceProgress condition
// onto the MxlFlowMirror's status using server-side apply with
// FieldOwner=mxl-source-gateway, so the writer never collides with
// the target-side gateway or the operator on neighbouring fields.
// The payload is the conditions slice and nothing else, so a stray
// zero value never overwrites a foreign-owned status field.
func (r *SourceReconciler) publishSourceProgress(ctx context.Context, key types.NamespacedName, state sourceProgressState) error {
	cond := metav1.Condition{
		Type:               mxlv1alpha1.ConditionTypeSourceProgress,
		Status:             state.status,
		Reason:             state.reason,
		Message:            state.message,
		LastTransitionTime: metav1.Now(),
	}
	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirror"))
	patch.SetNamespace(key.Namespace)
	patch.SetName(key.Name)
	conditionsField := []any{map[string]any{
		"type":               cond.Type,
		"status":             string(cond.Status),
		"reason":             cond.Reason,
		"message":            cond.Message,
		"lastTransitionTime": cond.LastTransitionTime.UTC().Format(time.RFC3339),
	}}
	status := map[string]any{
		"conditions": conditionsField,
	}
	if state.reason == mxlv1alpha1.ReasonAddTargetFailed {
		status["attemptCount"] = int64(state.attempts)
		status["lastError"] = state.message
	} else {
		status["attemptCount"] = int64(0)
		status["lastError"] = ""
	}
	if state.lastSentAt != nil {
		// Re-stamped on every publish that carries it: SSA with a
		// single FieldOwner releases ownership of fields omitted from
		// a later payload, so dropping the key after the first
		// publish would let the apiserver strip it and break the
		// target-side stuck-handshake discriminator.
		status["lastSentAt"] = state.lastSentAt.UTC().Format(time.RFC3339)
	}
	if err := unstructured.SetNestedField(patch.Object, status, "status"); err != nil {
		return fmt.Errorf("build SSA payload: %w", err)
	}
	return r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(sourceFieldOwner),
		client.ForceOwnership,
	)
}

// startFlusher launches the per-mirror status flusher. The flusher
// ticks at r.FlushInterval and publishes SourceProgress only when
// the observed state has transitioned, so a steady-state mirror
// produces zero status writes.
func (r *SourceReconciler) startFlusher(key types.NamespacedName, entry *sourceEntry) {
	if entry.flusherCancel != nil {
		return
	}
	interval := r.FlushInterval
	if interval <= 0 {
		interval = 1 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	entry.flusherCancel = cancel
	entry.flusherDone = done
	go r.runFlusher(ctx, done, key, entry, interval)
}

// runFlusher is the per-mirror status flusher loop. Tracks the most
// recently published condition so a steady stream of grains does
// not turn into a steady stream of API writes.
func (r *SourceReconciler) runFlusher(ctx context.Context, done chan struct{}, key types.NamespacedName, entry *sourceEntry, interval time.Duration) {
	defer close(done)
	t := time.NewTicker(interval)
	defer t.Stop()

	var last sourceProgressState
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		state := observedState(entry)
		if !first && sourceStateEqual(state, last) {
			continue
		}
		if err := r.publishSourceProgress(ctx, key, state); err != nil {
			ctrl.Log.WithName("source-flush").Error(err, "publish",
				"mirror", key, "reason", state.reason)
			continue
		}
		last = state
		first = false
	}
}

// sourceStateEqual reports whether two sourceProgressState values
// would publish identical SSA payloads. lastSentAt is compared by
// value rather than pointer identity: observedState allocates a
// fresh *time.Time on every tick (copying the atomic-loaded value),
// so a struct-level == on the two states would always disagree once
// lastSentAt is set, defeating the dedupe and turning a single
// successful transfer into a status write every flusher tick.
func sourceStateEqual(a, b sourceProgressState) bool {
	if a.status != b.status || a.reason != b.reason ||
		a.message != b.message || a.attempts != b.attempts {
		return false
	}
	if (a.lastSentAt == nil) != (b.lastSentAt == nil) {
		return false
	}
	if a.lastSentAt != nil && !a.lastSentAt.Equal(*b.lastSentAt) {
		return false
	}
	return true
}

// observedState derives the SourceProgress condition the flusher
// should publish from the atomics on the entry. lastSentAt is
// propagated into every state so the target-side watchdog gets a
// freshness signal regardless of which condition is currently being
// published.
func observedState(entry *sourceEntry) sourceProgressState {
	var sent *time.Time
	if p := entry.lastSentAt.Load(); p != nil {
		t := *p
		sent = &t
	}
	attempts := entry.addTargetAttempts.Load()
	if attempts > 0 {
		msg := ""
		if p := entry.lastError.Load(); p != nil {
			msg = *p
		}
		return sourceProgressState{
			status:     metav1.ConditionFalse,
			reason:     mxlv1alpha1.ReasonAddTargetFailed,
			message:    msg,
			attempts:   attempts,
			lastSentAt: sent,
		}
	}
	if entry.agedOutAt.Load() != nil {
		return sourceProgressState{
			status:     metav1.ConditionFalse,
			reason:     mxlv1alpha1.ReasonReaderAgedOut,
			message:    "source reader fell behind writer; advanced past missed grains",
			lastSentAt: sent,
		}
	}
	if entry.progress.Load() > 0 {
		return sourceProgressState{
			status:     metav1.ConditionTrue,
			reason:     mxlv1alpha1.ReasonRecovered,
			message:    "grain progress observed",
			lastSentAt: sent,
		}
	}
	return sourceProgressState{
		status:     metav1.ConditionTrue,
		reason:     mxlv1alpha1.ReasonHandshakeComplete,
		message:    "initiator running",
		lastSentAt: sent,
	}
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
//
// The MxlFlow watch closes the writer-startup race: when an agent
// on the source node publishes a new MxlFlow (or updates its Origin
// location) the reconciler is woken immediately for any
// MxlFlowMirror with the matching flowID, instead of waiting out
// the exponential backoff that started when the local FlowReader
// open returned FLOW_NOT_FOUND before the writer had created it.
//
// The Lease watch covers the symmetric case on the renew side:
// without it a freshly-renewed Origin Lease only reaches the source
// reconciler at the next MxlFlow status flush, so the same liveness
// signal that wakes the receiver lags arbitrarily on the source.
// The spec.flowID field index keeps the per-event work O(matches)
// instead of a cluster-wide MxlFlowMirror scan.
//
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch
func (r *SourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.sources == nil {
		r.sources = make(map[types.NamespacedName]*sourceEntry)
	}
	if r.attempts == nil {
		r.attempts = make(map[types.NamespacedName]uint32)
	}
	if r.opener == nil {
		r.opener = &libmxlOpener{
			Handles:          r.Handles,
			NodeName:         r.NodeName,
			BindAddress:      r.BindAddress,
			ProgressInterval: r.ProgressInterval,
		}
	}
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&mxlv1alpha1.MxlFlowMirror{},
		mirrorFlowIDIndex,
		func(obj client.Object) []string {
			m, ok := obj.(*mxlv1alpha1.MxlFlowMirror)
			if !ok || m.Spec.FlowID == "" {
				return nil
			}
			return []string{m.Spec.FlowID}
		},
	); err != nil {
		return fmt.Errorf("index MxlFlowMirror by %s: %w", mirrorFlowIDIndex, err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Watches(
			&mxlv1alpha1.MxlFlow{},
			handler.EnqueueRequestsFromMapFunc(r.flowToMirrors),
		).
		Watches(
			&coordinationv1.Lease{},
			handler.EnqueueRequestsFromMapFunc(r.leaseToMirrors),
			builder.WithPredicates(leaseInMxlSystem()),
		).
		Named("mxlflowmirror-source").
		Complete(r)
}

// leaseInMxlSystem confines the Lease watch to the namespace the
// agent publishes Origin Leases in. Leader-election Leases in
// kube-system and node Leases in kube-node-lease would otherwise
// wake every source reconciler on every renew tick. The receiver
// reconciler uses the same predicate; the two packages cannot share
// it because gateway and operator are separate modules with no
// shared internal package.
func leaseInMxlSystem() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == mxlv1alpha1.LeaseNamespace
	})
}

// leaseToMirrors maps an Origin Lease event to reconcile requests
// for source-side MxlFlowMirrors on this gateway's node. The Lease
// name encodes (flowID, nodeName); a Lease whose nodeName is not
// r.NodeName is dropped so an N-pod, M-flow cluster does not see
// N*M wakeups per renew tick. Matches are narrowed to mirrors whose
// SourceNode equals r.NodeName: the field index is single-key on
// spec.flowID, the SourceNode filter is the in-memory second pass.
func (r *SourceReconciler) leaseToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	lease, ok := obj.(*coordinationv1.Lease)
	if !ok {
		return nil
	}
	flowID, nodeName, ok := mxlv1alpha1.ParseLeaseName(lease.Name)
	if !ok {
		return nil
	}
	if nodeName != r.NodeName {
		return nil
	}
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors, client.MatchingFields{mirrorFlowIDIndex: flowID}); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(mirrors.Items))
	for i := range mirrors.Items {
		m := &mirrors.Items[i]
		if m.Spec.SourceNode != r.NodeName {
			continue
		}
		out = append(out, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(m),
		})
	}
	return out
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
