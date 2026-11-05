package mirror

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"k8s.io/client-go/tools/record"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/fabric"
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

// indexMirrorByFlow indexes a mirror under the MxlFlow object name of
// the flow it copies, which carries the domain, so a Lease or flow in
// one domain never wakes the mirrors of the same id in another.
func indexMirrorByFlow(obj client.Object) []string {
	m, ok := obj.(*mxlv1alpha1.MxlFlowMirror)
	if !ok || m.Spec.FlowID == "" {
		return nil
	}
	return []string{m.Ref().Name()}
}

// errAddTargetFailed wraps a libmxl-fabrics AddTarget failure so the
// reconciler can errors.Is detect it and engage bounded backoff
// without reopening the reader on every tick.
var errAddTargetFailed = errors.New("AddTarget failed")

// readerAgedOutMarker is the substring libmxl returns when
// a grain is read at an index the writer has already
// overwritten in the ring. Matched on Error() until go-mxl exposes
// a typed sentinel.
//
// TODO(go-mxl): swap to typed sentinel when wrapper exposes one.
const readerAgedOutMarker = "out of range (too late)"

// maxReaderRebuilds bounds the number of consecutive reader reopens
// the stall watchdog performs for one mirror. Past the cap the
// gateway keeps the entry in place and publishes
// ReasonReaderRebuildCapReached, so a flow whose producer is
// genuinely gone stops costing a reopen every stall window. The
// counter is cleared as soon as a grain transfers, so it counts
// consecutive failed reopens rather than lifetime ones.
const maxReaderRebuilds = 5

// writerAbsentRetry is how long a mirror waits before asking again
// whether its flow has a writer. It only paces the reopen of a mirror
// whose producer is gone, so it is set for a cheap steady state rather
// than a prompt one: the probe is an open plus a non-blocking flock,
// and a producer that comes back on a fresh flow directory rotates the
// Origin and wakes the reconciler through its watch well before this
// fires.
const writerAbsentRetry = 30 * time.Second

// ReasonReaderRebuildCapReached marks a mirror whose source-side
// reader stayed wedged across maxReaderRebuilds consecutive reopens.
// Distinct from ReasonReaderNotAdvancing and ReasonReaderAgedOut:
// those are the recoverable observations that spend the budget, this
// one says the gateway has stopped reopening the reader.
const ReasonReaderRebuildCapReached = "ReaderRebuildCapReached"

// SourceReconciler reconciles MxlFlowMirror resources from the
// sending side. See the package doc.
type SourceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// APIReader is an uncached reader for the Node lookup that gates
	// orphaned-finalizer reaping. Nil falls back to Client.
	APIReader client.Reader

	// NodeName is the Kubernetes node this gateway runs on. Mirrors
	// with spec.sourceNode set to a different node are ignored.
	NodeName string

	// Selector narrows the interfaces a setup may bind to the fabric
	// this node is allowed to carry MXL traffic on. The capability
	// publisher applies the same one.
	Selector fabric.Selector

	// Domains holds the long-lived mxl + fabrics instances, one per
	// MxlDomain a mirror reads from.
	Domains *instance.Set

	// PollInterval is how long a mirror's transfer loop waits after a
	// read or post that failed, and the shortest wait of an audio loop
	// for its next batch. Defaults to 2ms, and to half a transfer batch
	// for a continuous flow.
	PollInterval time.Duration

	// FlushInterval is how often the per-mirror status flusher
	// checks the sourceEntry trackers and publishes SourceProgress
	// when the observed state has transitioned. Defaults to 1s.
	FlushInterval time.Duration

	// Recorder publishes mirror-level events for the source side. Nil
	// records nothing.
	Recorder record.EventRecorder

	// ReaderStallAfter is how long a reader may report an unchanged
	// head index, having never transferred a grain, before
	// SourceProgress reports ReaderNotAdvancing. Defaults to
	// defaultReaderStallAfter.
	ReaderStallAfter time.Duration

	opener initiatorOpener

	// rebuildFn is the seam the reader-stall watchdog uses to spawn
	// its reopen. Production leaves it nil and the flusher invokes
	// rebuildWedgedReader directly; tests inject a recorder so they
	// can assert the watchdog fired without driving a real libmxl
	// reader. Mirrors the target reconciler's recoverFn.
	rebuildFn func(key types.NamespacedName)

	// nowFn is the clock the AddTarget wait is measured against.
	// Production leaves it nil.
	nowFn func() time.Time

	// writerLiveFn answers whether a flow still has a live writer.
	// SetupWithManager binds it to libmxl's own mxlIsFlowActive, which
	// is an authoritative answer rather than an inference from a
	// stalled head. Nil disables the check, which leaves the previous
	// reopen-and-hope behaviour and is what tests wire when the
	// question is not what they are exercising.
	writerLiveFn func(ref mxlv1alpha1.FlowRef) (bool, error)

	mu sync.Mutex
	// sources holds the per-mirror half, keyed by MxlFlowMirror;
	// shared holds what the mirrors of one flow on one provider have in
	// common.
	sources  map[types.NamespacedName]*sourceEntry
	shared   map[sourceKey]*sharedSource
	attempts attemptTable[sourceAddInputs]
	// redials holds, per mirror, the consecutive re-adds of a target its
	// loop gave up and the earliest the next one may run. Cleared
	// once a transfer to the target completes or the mirror's TargetInfo
	// changes.
	redials map[types.NamespacedName]redial

	// redialEvents brings a mirror whose loop gave its target up back
	// through the workqueue, so the re-add runs on the controller's
	// worker for that key and never beside it. Nil sends nothing.
	redialEvents chan event.GenericEvent
	// rebuilds counts consecutive reader reopens per shared source. It
	// outlives the sharedSource the watchdog tears down, so the budget
	// survives the reopen it is bounding, and it is per shared source so
	// a flow mirrored to N nodes does not get N budgets.
	rebuilds map[sourceKey]uint32
}

// sourceKey identifies one shared source. A flow is its id within its
// domain, and the same id in two domains is two flows. The provider is
// part of the key, so a mirror whose provider changes leaves its source
// and is set up again from scratch.
type sourceKey struct {
	domain   string
	flowID   string
	provider fabrics.Provider
}

func (k sourceKey) ref() mxlv1alpha1.FlowRef {
	return mxlv1alpha1.FlowRef{Domain: k.domain, ID: k.flowID}
}

// sharedSource is what the mirrors of one flow on one provider have in
// common: the flow's configuration, read once, and the set of mirrors,
// which a reader rebuild or an origin rotation tears down together.
// Each mirror opens its own reader and initiator, as every libmxl-fabrics
// demo runs one reader and one initiator per transfer loop, so neither
// a target that stops completing nor a reader blocked in a read holds
// up another mirror.
type sharedSource struct {
	key sourceKey

	// openSteps is how long the libmxl calls that opened this source
	// took, logged with the first target added to it.
	openSteps openSteps

	handles *instance.Handles
	cfg     mxl.FlowConfig

	// continuous is set for an audio flow, whose indices count samples
	// rather than grains. xferBatch is the samples one transfer carries
	// and maxBatch the longest run the reader hands out.
	continuous          bool
	xferBatch, maxBatch uint64

	// sliceBatch is the slices of a video grain one transfer adds, the
	// flow's maxSyncBatchSizeHint; zero sends the grain whole.
	sliceBatch uint16

	// interval is the time one transfer covers, a grain or a batch of
	// samples, past which it counts as late. poll is how often a loop
	// with nothing to send probes the head.
	interval, poll time.Duration

	// mirrors is the attached set, written under the reconciler's mutex.
	mirrors map[types.NamespacedName]*sourceEntry

	// openedAt is the wall-clock time this FlowReader was opened, and
	// lastObservedOriginAt the MxlFlow status.locations entry's
	// AppearedAt timestamp for r.NodeName at that moment. A subsequent
	// reconcile that sees a newer timestamp tears the reader down and
	// reopens it so the gateway tails the freshly rebound writer. May be
	// nil (the location carried no AppearedAt at open time); see
	// originRotated for how that case is handled.
	openedAt             time.Time
	lastObservedOriginAt atomic.Pointer[time.Time]

	// rebuilding is set by the reader-stall watchdog before it hands
	// the source to the reopen goroutine, so a second flusher tick --
	// this mirror's or any other attached mirror's -- cannot spawn a
	// competing rebuild for the same reader.
	rebuilding atomic.Bool
}

// sourceLink is one mirror's reader and initiator, the target added to
// it and the loop transferring to it.
type sourceLink struct {
	reader    *mxl.Reader
	initiator *fabrics.Initiator
	info      *fabrics.TargetInfo
	progress  ProgressFunc
	cancel    context.CancelFunc
	done      chan struct{}
	// targetLost is set by the loop before it closes done, when the
	// initiator dropped the target or never connected it.
	targetLost atomic.Bool
}

// linkTracker is the tracker a link's loop reports to: the mirror's
// entry, and the link for a lost target.
type linkTracker struct {
	*sourceEntry
	link *sourceLink
}

func (t linkTracker) recordTargetLost() { t.link.targetLost.Store(true) }

// stop cancels the loop and waits for it to return. Idempotent.
func (l *sourceLink) stop() {
	if l.cancel != nil {
		l.cancel()
	}
	if l.done != nil {
		<-l.done
	}
}

// sourceEntry is one MxlFlowMirror's source half: its link and the
// counters its transfer loop writes and its status flusher and the
// throughput collector read.
type sourceEntry struct {
	// key is this entry's MxlFlowMirror. A teardown that starts from
	// the shared source needs it to find its way back to r.sources.
	key types.NamespacedName

	// shared is the (flow, provider) source this mirror reads from.
	shared *sharedSource

	// link is this mirror's initiator, target and transfer loop. Set
	// once attach returns, before the entry is published.
	link *sourceLink

	// infoStr is the serialized TargetInfo the target was added with.
	// We keep it so a later reconcile can detect that the target has
	// rotated its info (e.g. the target-side gateway pod restarted and
	// re-opened the FlowWriter) and re-add it against the fresh address
	// before the source's writes keep getting refused.
	infoStr string

	// peerNode labels this mirror's byte counter, and is what keeps the
	// label set unique when one node mirrors the same flow to several
	// targets. Immutable for the life of the entry, so the collector
	// reads it without the entry's atomics.
	peerNode string

	// progress counts the transfers MakeProgress reported complete,
	// late the ones of them that took longer than their deadline, and
	// lost the ones outstanding when the target was given up.
	// lastSentAt is when progress last grew and lastCompleted the index
	// that transfer ended at.
	progress      atomic.Uint64
	late          atomic.Uint64
	lost          atomic.Uint64
	lastSentAt    atomic.Pointer[time.Time]
	lastCompleted atomic.Uint64

	// dropped is set once the loop gave the target up. Reconcile
	// replaces an entry that carries it. redialQueued is set once the
	// event asking for that reconcile was accepted.
	dropped      atomic.Bool
	redialQueued atomic.Bool

	// bytes counts payload of completed transfers, read by the
	// throughput collector at scrape time.
	bytes atomic.Uint64

	// skipped counts samples the transfer loop advanced past without
	// sending. It is not the same as loss on the wire: the target's head
	// still moves over them, so each one is a hole its ring fills with
	// whatever it already held. Zero is the only healthy value.
	skipped atomic.Uint64

	// invalidGrains counts grains the producer committed invalid of
	// which the transfer loop sent nothing; invalidAt is when the loop
	// last passed one, offeredAt when it last posted anything else.
	// See idleOnInvalid.
	invalidGrains atomic.Uint64
	invalidAt     atomic.Pointer[time.Time]
	offeredAt     atomic.Pointer[time.Time]

	// resyncGrains and agedOutGrains count the grains the transfer loop
	// passed over without offering, by unsentReason.
	resyncGrains  atomic.Uint64
	agedOutGrains atomic.Uint64

	// agedOutAt records the wall-clock of the most recent
	// reader-aged-out skip the transfer loop has had to perform.
	// Used by the flusher to publish SourceProgress with reason
	// ReaderAgedOut on the transition, and read against lastSentAt to
	// tell a reader that skipped and caught up from one still outside
	// the ring.
	agedOutAt atomic.Pointer[time.Time]

	// lastHead and headAdvancedAt carry the loop's head observations.
	lastHead       atomic.Uint64
	headAdvancedAt atomic.Pointer[time.Time]

	// addTargetAttempts mirrors r.attempts for this key into the
	// flusher's view. lastError records the most recent reconcile
	// error message for the same purpose.
	addTargetAttempts atomic.Uint32
	lastError         atomic.Pointer[string]
	// postError is the last post the initiator rejected for good.
	postError atomic.Pointer[string]

	// openedAt is the wall-clock time this mirror's target was added.
	// Read-only after construction, set before the entry is published
	// into r.sources under r.mu -- safe to read from Reconcile without
	// its own synchronization, same as infoStr.
	openedAt time.Time

	// flusherCancel stops the per-mirror status flusher; flusherDone
	// is closed when it returns.
	flusherCancel context.CancelFunc
	flusherDone   chan struct{}
}

// sourceKey reports the shared source this mirror transfers through.
func (e *sourceEntry) sourceKey() sourceKey {
	if e.shared == nil {
		return sourceKey{}
	}
	return e.shared.key
}

func (e *sourceEntry) flowID() string { return e.sourceKey().flowID }

func (e *sourceEntry) flowRef() mxlv1alpha1.FlowRef { return e.sourceKey().ref() }

func (e *sourceEntry) provider() fabrics.Provider { return e.sourceKey().provider }

// attach adds e to the attached set. Callers hold the reconciler mutex.
func (s *sharedSource) attach(e *sourceEntry) {
	if s.mirrors == nil {
		s.mirrors = make(map[types.NamespacedName]*sourceEntry)
	}
	s.mirrors[e.key] = e
}

// detach drops key from the attached set and reports whether the set
// is now empty, which is the only point at which the reader may be
// closed. Callers hold the reconciler mutex.
func (s *sharedSource) detach(key types.NamespacedName) bool {
	delete(s.mirrors, key)
	return len(s.mirrors) == 0
}

// detachAll empties the attached set and returns what was in it, so a
// caller tearing the whole source down can stop each mirror's flusher
// and link. Callers hold the reconciler mutex.
func (s *sharedSource) detachAll() []*sourceEntry {
	out := make([]*sourceEntry, 0, len(s.mirrors))
	for _, e := range s.mirrors {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b *sourceEntry) int {
		return cmp.Or(
			cmp.Compare(a.key.Namespace, b.key.Namespace),
			cmp.Compare(a.key.Name, b.key.Name),
		)
	})
	s.mirrors = nil
	return out
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch

// reapOrphanedFinalizer drops SourceFinalizerName from a deleting
// mirror whose spec.sourceNode names a Node that no longer exists.
// Every other branch is gated on spec.sourceNode naming this node
// and the gateway is a DaemonSet, so the pod that would run the
// teardown died with the node and nothing else owns the finalizer.
// Node existence is the guard against racing a live owner; once the
// Node is gone so is the reader state the finalizer protected.
func (r *SourceReconciler) reapOrphanedFinalizer(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror) (ctrl.Result, error) {
	if mirror.DeletionTimestamp.IsZero() ||
		!controllerutil.ContainsFinalizer(mirror, SourceFinalizerName) {
		return ctrl.Result{}, nil
	}

	gone, err := nodeGone(ctx, r.nodeReader(), mirror.Spec.SourceNode)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("look up source node %s: %w", mirror.Spec.SourceNode, err)
	}
	if !gone {
		return ctrl.Result{RequeueAfter: orphanRecheckInterval}, nil
	}

	controllerutil.RemoveFinalizer(mirror, SourceFinalizerName)
	if err := r.Update(ctx, mirror); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("remove orphaned source finalizer: %w", err)
	}
	log.FromContext(ctx).Info("reaped orphaned source-side finalizer",
		"mxlflowmirror", client.ObjectKeyFromObject(mirror),
		"sourceNode", mirror.Spec.SourceNode)
	return ctrl.Result{}, nil
}

// nodeReader prefers the uncached APIReader SetupWithManager binds.
func (r *SourceReconciler) nodeReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get

// Reconcile drives one MxlFlowMirror through its source-side
// lifecycle.
func (r *SourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)
	sw := newStopwatch()
	started := sw.last

	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &mirror); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if mirror.Spec.SourceNode != r.NodeName {
		// spec.sourceNode used to name this node and was mutated to
		// another; the in-memory sources map keeps an initiator open
		// against a producer-less .mxl-flow until the pod is bounced.
		// closeEntry is a no-op when the key holds nothing. A re-add run
		// this node counted is no business of the node the source moved
		// to, and would hold a later return here to its backoff.
		r.closeEntry(req.NamespacedName)
		r.clearRedials(req.NamespacedName)
		return r.reapOrphanedFinalizer(ctx, &mirror)
	}

	if !mirror.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&mirror, SourceFinalizerName) {
			return ctrl.Result{}, nil
		}
		r.closeEntry(req.NamespacedName)
		r.clearRedials(req.NamespacedName)
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

	// originAt is the MxlFlow's most recent AppearedAt timestamp
	// for the Origin location on this node. It is read once here so
	// the rotation comparison below and the openShared handoff both
	// observe the same value, and a later rotation is detected
	// even if the MxlFlow watch fires before status propagates.
	originAt := r.observedOriginAt(ctx, mirror.Ref())

	provider, err := providerForSetup(&mirror)
	if err != nil {
		// Never forward auto into libmxl-fabrics: surface the reason on
		// status and stop. The agent or operator patches spec.provider to
		// a concrete value, which wakes this reconciler through its watch.
		if perr := r.publishSourceProgress(ctx, req.NamespacedName, sourceProgressState{
			status:  metav1.ConditionFalse,
			reason:  mxlv1alpha1.ReasonProviderUnresolved,
			message: err.Error(),
		}); perr != nil {
			l.Error(perr, "publish ProviderUnresolved")
		}
		l.Info("refusing source setup: mirror provider is unresolved", "error", err.Error())
		return ctrl.Result{}, nil
	}

	// Resolved before the fast path rather than after it, because the
	// provider is half the identity of the shared source this mirror
	// belongs to.
	skey := sourceKey{domain: mirror.Spec.Domain, flowID: mirror.Spec.FlowID, provider: provider}

	r.mu.Lock()
	existing := r.sources[req.NamespacedName]
	r.mu.Unlock()
	sw.lap("lookup")

	// spec.provider rotated under a running mirror. An initiator is set
	// up against one provider's interface and cannot serve another, so
	// this mirror leaves the source it was sharing and joins the one
	// for its new provider.
	if existing != nil && existing.sourceKey() != skey {
		l.Info("mirror provider rotated, moving to a fresh initiator")
		r.closeEntry(req.NamespacedName)
		existing = nil
	}

	r.mu.Lock()
	shared := r.shared[skey]
	r.mu.Unlock()

	// The MxlFlow Origin location for this node has rotated since the
	// FlowReader was opened (the writer-side agent re-registered the
	// flow, typically after a pod restart). Tear down + reopen so the
	// reader tails the fresh writer instead of holding a handle on a
	// now-invalid ring.
	//
	// Judged against the shared source rather than against this
	// mirror's entry: the other mirrors' readers were opened before the
	// rotation and would otherwise stay on the dead ring.
	if shared != nil && originRotated(shared.lastObservedOriginAt.Load(), originAt, shared.openedAt) {
		l.Info("flow origin rotated, reopening reader")
		for _, k := range r.closeSharedSource(skey) {
			if k == req.NamespacedName {
				continue
			}
			// Every mirror of the source lost its reader. Driving
			// them here rather than waiting for a watch keeps the
			// reopen self-contained, for the same reason
			// rebuildWedgedReader drives its own: the events that
			// would wake them stop precisely when the producer is in
			// trouble.
			r.reopenEvicted(ctx, k)
		}
		existing = nil
	}
	// Its own stage: an origin rotation reconciles every other mirror of
	// the flow inline, which is not this mirror's re-add.
	sw.lap("originReopen")

	if existing != nil && existing.infoStr == mirror.Status.TargetInfo && existing.dropped.Load() {
		// The loop gave this target up; only adding it again resumes
		// the mirror. The same TargetInfo is added again with a backoff
		// of its own, because it may name a target that is gone until
		// the mirror publishes its successor.
		wait, attempt := r.takeRedial(req.NamespacedName)
		if wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
		l.Info("transfer loop gave the target up; adding it again", "attempt", attempt)
		r.closeEntry(req.NamespacedName)
		existing = nil
	}
	if existing != nil {
		if existing.infoStr == mirror.Status.TargetInfo {
			return ctrl.Result{}, nil
		}
		r.clearRedials(req.NamespacedName)
		// TargetInfo rotated under us (typically: target gateway
		// restarted and rebuilt the writer on a fresh ephemeral
		// port). This mirror's link is replaced by one against the new
		// address; the other mirrors of the flow are not touched. The
		// target side also lands here after recoverFromFatalError
		// republishes status.targetInfo via SSA on its Ready
		// transition -- the infoStr comparison above is the source
		// initiator's only wake-up signal for that rotation, so do
		// not add a spec-only predicate to the MxlFlowMirror watch or
		// recovery wakes will be silently dropped.
		l.Info("target info rotated, re-adding target")
		r.closeEntry(req.NamespacedName)
	}
	sw.lap("close")

	// Seed the in-memory AddTarget attempts counter from the persisted
	// status.attemptCount so a gateway pod restart does not reset the
	// backoff for a target that has been unreachable across the bounce.
	// Only the counter is restored: the next attempt fires immediately
	// rather than waiting out a remembered backoff window, which keeps
	// the design's "one free retry on restart" budget intact.
	inputs := sourceAddInputs{targetInfo: mirror.Status.TargetInfo, provider: provider}
	r.mu.Lock()
	r.attempts.seed(req.NamespacedName, uint32(mirror.Status.AttemptCount))
	remaining, gated := r.attempts.wait(req.NamespacedName, inputs, r.now())
	r.mu.Unlock()
	// Publishing SourceProgress is what wakes this reconciler through
	// its own watch, so a gated pass writes nothing.
	if gated {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	// No reader is opened on a flow nothing writes. Without this the
	// release the flusher performs is undone within the second:
	// publishing SourceWriterGone wakes this reconciler through its own
	// watch, which opens a fresh reader and pins the flow directory
	// again. The condition is written once and the requeue carries the
	// retry, because a producer that re-attaches to a directory that
	// was never collected rotates nothing and fires no watch.
	sw.skip()
	if r.writerGone(mirror.Ref()) {
		if !sourceProgressReads(&mirror, mxlv1alpha1.ReasonSourceWriterGone) {
			if err := r.publishSourceProgress(ctx, req.NamespacedName, sourceProgressState{
				status:  metav1.ConditionFalse,
				reason:  mxlv1alpha1.ReasonSourceWriterGone,
				message: "flow has no live writer; leaving the source reader closed so the flow can be reclaimed",
			}); err != nil {
				l.Error(err, "publish SourceWriterGone")
			}
			l.Info("flow has no live writer; not opening a source reader",
				"flowID", mirror.Spec.FlowID)
		}
		return ctrl.Result{RequeueAfter: writerAbsentRetry}, nil
	}

	// Reuses the source the fast path looked up when it is still
	// live, and opens one when this is the flow's first mirror or the
	// origin rotation above tore the previous one down.
	sw.lap("liveCheck")
	shared, opened, err := r.openShared(skey, originAt)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("open reader: %w", err)
	}
	sw.lap("open")
	if opened {
		sw.stages = append(sw.stages, "openReader", shared.openSteps.reader.String())
	}

	entry := &sourceEntry{
		key:      req.NamespacedName,
		shared:   shared,
		infoStr:  mirror.Status.TargetInfo,
		peerNode: mirror.Spec.TargetNode,
		openedAt: time.Now(),
	}
	// Carry the published lastSentAt onto the fresh entry: a rebuilt
	// entry has no transfers of its own, and an omitted field is one
	// SSA releases and the apiserver strips, erasing the timestamp the
	// target's stuck-handshake watchdog discriminates on.
	if mirror.Status.LastSentAt != nil {
		t := mirror.Status.LastSentAt.Time
		entry.lastSentAt.Store(&t)
	}

	link, err := r.opener.attach(shared, mirror.Status.TargetInfo, entry)
	sw.lap("attach")
	if err != nil {
		r.releaseIfUnused(shared)
		if errors.Is(err, errAddTargetFailed) {
			return r.handleAddTargetFailure(ctx, req.NamespacedName, inputs, err)
		}
		return ctrl.Result{}, fmt.Errorf("add target: %w", err)
	}
	entry.link = link

	r.mu.Lock()
	if dup := r.sources[req.NamespacedName]; dup != nil {
		// Another pass attached this mirror first. One that read an
		// older TargetInfo leaves an entry the flusher publishes nothing
		// for, so the mirror is looked at again rather than left on it.
		stale := dup.infoStr != mirror.Status.TargetInfo
		r.mu.Unlock()
		r.opener.detach(link)
		r.releaseIfUnused(shared)
		if stale {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, nil
	}
	r.sources[req.NamespacedName] = entry
	shared.attach(entry)
	targets := len(shared.mirrors)
	delete(r.attempts, req.NamespacedName)
	r.mu.Unlock()

	r.startFlusher(entry)

	l.Info("target added", append([]any{
		"flowID", mirror.Spec.FlowID,
		"targetNode", mirror.Spec.TargetNode,
		"provider", provider.String(),
		"targets", targets,
		"took", time.Since(started).String()}, sw.stages...)...)
	return ctrl.Result{}, nil
}

// openSteps is how long opening a shared source took, step by step.
type openSteps struct {
	reader time.Duration
}

// stopwatch records how long each stage of one Reconcile took, for the
// line that reports the target added. A re-add that takes seconds holds
// its worker for as long, and only the per-stage split says which libmxl
// call it was spent in.
type stopwatch struct {
	last   time.Time
	stages []any
}

func newStopwatch() *stopwatch { return &stopwatch{last: time.Now()} }

// lap records the time since the previous lap under name.
func (w *stopwatch) lap(name string) {
	now := time.Now()
	w.stages = append(w.stages, name, now.Sub(w.last).String())
	w.last = now
}

// skip restarts the clock without recording, for work no stage owns.
func (w *stopwatch) skip() { w.last = time.Now() }

// openShared returns the shared source for skey, opening one if this
// is the first mirror to want it. The libmxl work happens outside the
// reconciler's mutex, so a concurrent opener can win the race; the
// loser closes what it built and takes the winner's source.
func (r *SourceReconciler) openShared(skey sourceKey, originAt *time.Time) (s *sharedSource, opened bool, err error) {
	r.mu.Lock()
	if r.shared == nil {
		r.shared = make(map[sourceKey]*sharedSource)
	}
	s = r.shared[skey]
	r.mu.Unlock()
	if s != nil {
		return s, false, nil
	}

	s, err = r.opener.open(skey)
	if err != nil {
		return nil, false, err
	}
	s.key = skey
	s.openedAt = time.Now()
	if originAt != nil {
		t := *originAt
		s.lastObservedOriginAt.Store(&t)
	}

	r.mu.Lock()
	if dup := r.shared[skey]; dup != nil {
		r.mu.Unlock()
		return dup, false, nil
	}
	r.shared[skey] = s
	r.mu.Unlock()
	return s, true, nil
}

// releaseIfUnused drops a shared source no mirror is attached to, which
// the open path leaves behind whenever the attach that would have been
// its first fails, so the next attach reads the flow's configuration
// again.
func (r *SourceReconciler) releaseIfUnused(s *sharedSource) {
	r.mu.Lock()
	if len(s.mirrors) > 0 || r.shared[s.key] != s {
		r.mu.Unlock()
		return
	}
	delete(r.shared, s.key)
	r.mu.Unlock()
}

// reopenEvicted drives a mirror whose shared source was torn down back
// through Reconcile so it opens or re-attaches against a fresh reader.
func (r *SourceReconciler) reopenEvicted(ctx context.Context, key types.NamespacedName) {
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		ctrl.Log.WithName("source").Error(err, "reopen source mirror", "mirror", key)
	}
}

// handleAddTargetFailure records the failed AddTarget attempt and
// returns a bounded-backoff RequeueAfter so the attach is not retried
// every controller-runtime tick while the target is unreachable.
func (r *SourceReconciler) handleAddTargetFailure(ctx context.Context, key types.NamespacedName, inputs sourceAddInputs, addErr error) (ctrl.Result, error) {
	r.mu.Lock()
	attempts, wait := r.attempts.fail(key, inputs, r.now(), backoffFor)
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
	return ctrl.Result{RequeueAfter: wait}, nil
}

// now reports the current time through the reconciler's clock seam.
func (r *SourceReconciler) now() time.Time {
	if r.nowFn != nil {
		return r.nowFn()
	}
	return time.Now()
}

// sourceAddInputs is what an AddTarget attempt depends on. A rotated
// TargetInfo is a new attempt, not a retry of the one that failed.
type sourceAddInputs struct {
	targetInfo string
	provider   fabrics.Provider
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

// observedOriginAt returns the AppearedAt timestamp of the Origin
// location entry on r.NodeName for the given flow, or nil if the
// flow or location is missing.
//
// AppearedAt, not LastObserved: the latter is refreshed on every
// agent rescan to report liveness, so keying rotation on it would
// read every steady-state flow as a restarted producer once per
// rescan. AppearedAt moves only when a flow directory actually
// appears, which is the event this is trying to detect. Errors are logged but not fatal: a
// missing flow means the reconciler will wait for the MxlFlow watch
// to fire.
func (r *SourceReconciler) observedOriginAt(ctx context.Context, ref mxlv1alpha1.FlowRef) *time.Time {
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name()}, &flow); err != nil {
		return nil
	}
	for _, loc := range flow.Status.Locations {
		if loc.NodeName != r.NodeName || loc.Phase != mxlv1alpha1.MxlFlowLocationOrigin {
			continue
		}
		if loc.AppearedAt == nil {
			return nil
		}
		t := loc.AppearedAt.Time
		return &t
	}
	return nil
}

// originRotated reports whether the freshly observed origin
// timestamp indicates the flow's writer has rotated since this
// reader was opened. A nil observation means "no data yet" and never
// counts as a rotation.
//
// A non-nil baseline is the common case: the reader was opened while
// the location already carried a LastObserved value, and any newer
// observation is a rotation.
//
// A nil baseline means the reader opened while the location carried
// no AppearedAt yet -- Stale, or an appearance whose status write
// had not landed (a status-update conflict, or a fanotify event that
// raced the watcher's startup). Treating that case as "never rotated"
// is wrong: lastObservedOriginAt is set once at open and never
// reassigned except by a rotation-triggered reopen, so a permanently
// false comparison leaves the reader unable to detect any future
// rotation for its entire lifetime -- exactly the failure mode that
// stranded a flow's mirror on a dead ring for two hours during a
// production incident. Falling back to openedAt instead means any
// origin observation landing after the reader opened counts as a
// rotation, which can trigger one avoidable reopen (the writer that
// created the pre-open observation might still be the live one) but
// never silently strands the reader.
func originRotated(baseline, observed *time.Time, openedAt time.Time) bool {
	if observed == nil {
		return false
	}
	if baseline != nil {
		return observed.After(*baseline)
	}
	// observed is sourced from MxlFlow.status.locations[].appearedAt,
	// a metav1.Time -- the Kubernetes API always truncates timestamps
	// to second precision on the round trip through the apiserver,
	// while openedAt is a full-precision local time.Time. Comparing
	// them directly is unsafe: openedAt almost always carries a
	// nonzero sub-second component, so a genuinely later observation
	// that floors to the same second still compares Before it,
	// spuriously reporting "not rotated". Truncate openedAt to the
	// same second-floor grain before comparing so both sides use
	// consistent precision; the cost is, at most, one extra reopen
	// for an observation that turns out to predate the current
	// reader by under a second, which is cheap next to silently
	// stranding the reader for its entire lifetime.
	return !observed.Before(openedAt.Truncate(time.Second))
}

// libmxlOpener is the production initiatorOpener. The fields are the
// subset of SourceReconciler state the open path reads.
type libmxlOpener struct {
	Domains      *instance.Set
	NodeName     string
	Selector     fabric.Selector
	PollInterval time.Duration
}

func (o *libmxlOpener) open(key sourceKey) (*sharedSource, error) {
	handles, err := o.Domains.For(context.Background(), key.domain)
	if err != nil {
		return nil, err
	}
	mxlInst := handles.MXL()
	if mxlInst == nil {
		return nil, fmt.Errorf("mxl instance closed")
	}
	if handles.Fabrics() == nil {
		return nil, fmt.Errorf("fabrics instance closed")
	}

	start := time.Now()
	reader, err := mxlInst.NewReader(key.flowID)
	if err != nil {
		return nil, fmt.Errorf("NewReader: %w", err)
	}
	cfg, err := reader.Config()
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("Reader.Config: %w", err)
	}
	s := &sharedSource{
		key:        key,
		openSteps:  openSteps{reader: time.Since(start)},
		handles:    handles,
		cfg:        cfg,
		continuous: cfg.Common.Format == mxl.FormatAudio,
		interval:   grainInterval(cfg.Common.GrainRate),
		poll:       o.PollInterval,
	}

	if s.continuous {
		// maxBatch is the longest run the reader can hand out; the
		// whole readable window in one TransferSamples is rejected as
		// invalid argument.
		s.maxBatch, err = reader.GetMaxReadLengthSamples()
		if err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("GetMaxReadLengthSamples: %w", err)
		}
		s.xferBatch = sampleBatch(cfg.Common.MaxSyncBatchSizeHint, cfg.Common.GrainRate)
		if s.maxBatch > 0 && s.xferBatch > s.maxBatch {
			s.xferBatch = s.maxBatch
		}
		s.interval *= time.Duration(s.xferBatch)
		if s.poll <= 0 {
			s.poll = defaultSamplePollInterval(cfg.Common.GrainRate, s.xferBatch)
		}
	} else if hint := cfg.Common.MaxSyncBatchSizeHint; hint < uint32(mxl.GrainValidSlicesAll) {
		s.sliceBatch = uint16(hint)
	}
	if s.poll <= 0 {
		s.poll = 2 * time.Millisecond
	}
	// The reader only answered for the flow; each mirror opens its own.
	_ = reader.Close()
	return s, nil
}

// attach opens a reader and an initiator for one mirror, adds its
// target and starts its transfer loop.
func (o *libmxlOpener) attach(s *sharedSource, targetInfoStr string, e *sourceEntry) (*sourceLink, error) {
	fabInst := s.handles.Fabrics()
	if fabInst == nil {
		return nil, fmt.Errorf("fabrics instance closed")
	}
	mxlInst := s.handles.MXL()
	if mxlInst == nil {
		return nil, fmt.Errorf("mxl instance closed")
	}
	iface, err := resolveInterface(fabInst, o.Selector, s.key.provider)
	if err != nil {
		return nil, err
	}
	reader, err := mxlInst.NewReader(s.key.flowID)
	if err != nil {
		return nil, fmt.Errorf("NewReader: %w", err)
	}
	initiator, err := fabInst.NewInitiator()
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("NewInitiator: %w", err)
	}
	if err := initiator.Setup(fabrics.InitiatorConfig{
		Interface: iface,
		Reader:    reader,
		Options:   initiatorOptions(s.cfg),
	}); err != nil {
		_ = initiator.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("Initiator.Setup: %w", err)
	}
	info, err := fabrics.ParseTargetInfo(targetInfoStr)
	if err != nil {
		_ = initiator.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("ParseTargetInfo: %w", err)
	}
	if err := initiator.AddTarget(info); err != nil {
		_ = info.Close()
		_ = initiator.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("%w: %w", errAddTargetFailed, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	progress := progressFuncFor(initiator, iface.Caps.Flags)
	link := &sourceLink{reader: reader, initiator: initiator, info: info, progress: progress,
		cancel: cancel, done: make(chan struct{})}
	tr := linkTracker{sourceEntry: e, link: link}
	probe := func() (uint64, error) {
		rt, err := reader.Runtime()
		if err != nil {
			return 0, err
		}
		return rt.HeadIndex, nil
	}
	if s.continuous {
		rate := s.cfg.Common.GrainRate
		untilIndex := func(index uint64) time.Duration { return time.Duration(mxl.NsUntilIndex(index, rate)) }
		go runSampleTransferLoop(ctx, link.done, s.key.flowID, probe,
			newSampleTransfer(reader, initiator, uint64(s.cfg.Continuous.BufferLength)),
			progress, initiator.MakeProgressNonBlocking, s.xferBatch, s.maxBatch, untilIndex, s.poll, s.interval, tr)
	} else {
		transfer := newGrainTransfer(reader, initiator, s.cfg.Common.Format == mxl.FormatData, sliceBytes(s.cfg), s.sliceBatch)
		go runTransferLoop(ctx, link.done, s.key.flowID, probe, transfer, progress, initiator.MakeProgressNonBlocking, s.poll, s.interval, tr)
	}
	return link, nil
}

// newSampleTransfer is the SampleTransferFunc of a continuous flow. The
// payload of a run is read off the reader's view of it once: libmxl
// publishes no sample width, but a view's channel stride is the width
// times the buffer length.
func newSampleTransfer(r *mxl.Reader, in *fabrics.Initiator, bufferLength uint64) SampleTransferFunc {
	var frameBytes uint64
	return func(end uint64, count int) (uint64, error) {
		if frameBytes == 0 && bufferLength > 0 {
			if v, err := r.GetSamplesNonBlocking(end, count); err == nil {
				frameBytes = v.Stride / bufferLength * v.ChannelCount
			}
		}
		if err := in.TransferSamples(end, count); err != nil {
			return 0, err
		}
		return uint64(count) * frameBytes, nil
	}
}

// sampleBatch is the samples one transfer of a continuous flow carries:
// the flow's maxSyncBatchSizeHint, as in tools/mxl-fabrics-demo/demo.cpp,
// but no more than 10 ms of audio. The target's bounce-buffer entry
// holds the target flow's hint, and the writer the target gateway
// creates (TargetReconciler, NewWriter in target.go) takes libmxl's
// default of 10 ms, whatever hint the producer set.
func sampleBatch(hint uint32, rate mxl.Rational) uint64 {
	tenMs := uint64(1)
	if rate.Den > 0 && rate.Num > 0 {
		tenMs = max(uint64(rate.Num/(100*rate.Den)), 1)
	}
	if hint == 0 {
		return tenMs
	}
	return min(uint64(hint), tenMs)
}

// detachTimeout bounds how long a removed target's disconnect is driven
// before its initiator is destroyed.
const detachTimeout = 500 * time.Millisecond

// drainRemoved drives a removed target's disconnect, which completes
// once progress stops returning not-ready, for up to detachTimeout. An
// interrupt is checked as in a transfer.
func drainRemoved(progress, poll ProgressFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), detachTimeout)
	defer cancel()
	_, _ = awaitTransfer(ctx, progress, poll, time.Now(), 0)
}

// detach stops the mirror's loop, removes its target and closes its
// initiator and reader.
func (o *libmxlOpener) detach(link *sourceLink) {
	if link == nil {
		return
	}
	link.stop()
	// A connection-oriented initiator has already evicted a target it
	// dropped (evictDeadEndpoints in RCInitiator.cpp), and removing it
	// again only fails with "not found".
	if link.initiator != nil && link.info != nil && !link.targetLost.Load() {
		if err := link.initiator.RemoveTarget(link.info); err != nil {
			ctrl.Log.WithName("source").V(1).Info("RemoveTarget", "error", err.Error())
		} else {
			drainRemoved(link.progress, link.initiator.MakeProgressNonBlocking)
		}
	}
	if link.initiator != nil {
		_ = link.initiator.Close()
	}
	if link.info != nil {
		_ = link.info.Close()
	}
	if link.reader != nil {
		_ = link.reader.Close()
	}
}

// maxMirrorLag is how far lastSent may trail the live head before the
// loop stops catching up grain by grain and resyncs to the head. Kept
// below the producer's ring depth, a handful of grains, so a normal
// one- or two-grain catch-up does not resync.
const maxMirrorLag int64 = 4

// defaultReaderStallAfter is the head-index inactivity window behind
// ReasonReaderNotAdvancing. Matches defaultStuckHandshakeAfter so both
// sides of a mirror describe a wedge over the same period.
const defaultReaderStallAfter = 20 * time.Second

// progressTracker is what a transfer loop reports to.
type progressTracker interface {
	recordHead(head uint64, at time.Time)
	recordOffered(at time.Time)
	recordCompleted(idx, bytes uint64, late bool, at time.Time)
	recordDropped(outstanding bool)
	// recordTargetLost marks the target as dropped by the initiator or
	// never connected, so detach does not remove it again.
	recordTargetLost()
	// recordPostFailed keeps a post the initiator rejected for good.
	recordPostFailed(err error)
	recordAgedOut(at time.Time)
	recordSkipped(samples uint64)
	recordInvalid(at time.Time)
	recordPassedOver(reason unsentReason, grains uint64)
}

// unsentReason says why the grain transfer loop did not offer a grain.
type unsentReason string

const (
	// unsentInvalid is a grain its producer committed invalid.
	unsentInvalid unsentReason = "invalid"
	// unsentResync is a grain the loop jumped over when it trailed the
	// head by more than maxMirrorLag.
	unsentResync unsentReason = "resync"
	// unsentAgedOut is a grain whose slot the producer had reused.
	unsentAgedOut unsentReason = "aged_out"
)

// recordCompleted counts a transfer MakeProgress reported complete.
func (e *sourceEntry) recordCompleted(idx, bytes uint64, late bool, at time.Time) {
	e.bytes.Add(bytes)
	e.lastCompleted.Store(idx)
	t := at
	e.lastSentAt.Store(&t)
	e.progress.Add(1)
	if late {
		e.late.Add(1)
	}
}

// recordTargetLost is the link's to act on; see linkTracker.
func (e *sourceEntry) recordTargetLost() {}

// recordPostFailed keeps the text of a post the initiator rejected for
// good, which observedState publishes while the mirror waits to add its
// target again.
func (e *sourceEntry) recordPostFailed(err error) {
	msg := err.Error()
	e.postError.Store(&msg)
}

// recordDropped marks the target given up, counting the transfer that
// was outstanding on it.
func (e *sourceEntry) recordDropped(outstanding bool) {
	if outstanding {
		e.lost.Add(1)
	}
	e.dropped.Store(true)
}

func (e *sourceEntry) recordAgedOut(at time.Time) {
	t := at
	e.agedOutAt.Store(&t)
}

// recordHead stamps headAdvancedAt only on a changed head, so the
// timestamp measures how long the reader has stood still rather than
// how long ago the loop last polled it.
func (e *sourceEntry) recordHead(head uint64, at time.Time) {
	if e.headAdvancedAt.Load() != nil && e.lastHead.Load() == head {
		return
	}
	e.lastHead.Store(head)
	t := at
	e.headAdvancedAt.Store(&t)
}

// recordOffered stamps a post. It says nothing about delivery, but that
// the source had something to send.
func (e *sourceEntry) recordOffered(at time.Time) {
	t := at
	e.offeredAt.Store(&t)
}

// idleOnInvalid reports whether the head has moved over nothing but
// invalid grains for longer than after, as a merge output with every
// member down does. Such a source offers the initiator nothing, so a
// target that completes nothing is not stalled and its transfers are
// not failing to land.
func idleOnInvalid(now time.Time, e *sourceEntry, after time.Duration) bool {
	inv := e.invalidAt.Load()
	if inv == nil || now.Sub(*inv) > after {
		return false
	}
	off := e.offeredAt.Load()
	return off == nil || now.Sub(*off) > after
}

func (e *sourceEntry) recordSkipped(samples uint64) {
	e.skipped.Add(samples)
}

func (e *sourceEntry) recordInvalid(at time.Time) {
	e.invalidGrains.Add(1)
	t := at
	e.invalidAt.Store(&t)
}

func (e *sourceEntry) recordPassedOver(reason unsentReason, grains uint64) {
	switch reason {
	case unsentResync:
		e.resyncGrains.Add(grains)
	case unsentAgedOut:
		e.agedOutGrains.Add(grains)
	}
}

// isReaderAgedOut reports whether the error returned by
// a grain read or TransferGrain is the libmxl "the writer
// has lapped the reader" signal.
func isReaderAgedOut(err error) bool {
	return err != nil && strings.Contains(err.Error(), readerAgedOutMarker)
}

// closeEntry detaches one mirror: its status flusher and link, reader
// included, stopped. The last mirror to leave drops the shared source.
// A no-op when key holds nothing.
func (r *SourceReconciler) closeEntry(key types.NamespacedName) {
	r.mu.Lock()
	entry := r.sources[key]
	delete(r.sources, key)
	if entry != nil && entry.shared != nil {
		s := entry.shared
		if s.detach(key) && r.shared[s.key] == s {
			delete(r.shared, s.key)
		}
	}
	r.mu.Unlock()
	if entry == nil {
		return
	}
	// Both outside r.mu: stopFlusher waits on a goroutine that takes
	// r.mu itself, and detach crosses into libmxl-fabrics.
	stopFlusher(entry)
	r.detachLink(entry)
}

// closeSharedSource tears the whole shared source down, every mirror's
// link and reader with it, and returns the mirrors that were attached so
// the caller can drive them back through Reconcile against fresh
// readers. Used by the paths that invalidate the readers: an origin
// rotation and the reader-stall rebuild.
func (r *SourceReconciler) closeSharedSource(skey sourceKey) []types.NamespacedName {
	r.mu.Lock()
	s := r.shared[skey]
	if s == nil {
		r.mu.Unlock()
		return nil
	}
	delete(r.shared, skey)
	entries := s.detachAll()
	keys := make([]types.NamespacedName, 0, len(entries))
	for _, e := range entries {
		if r.sources[e.key] == e {
			delete(r.sources, e.key)
		}
		keys = append(keys, e.key)
	}
	r.mu.Unlock()

	// Every loop is told to stop first, so their reads, each blocking
	// for up to sourceReadTimeout, end together rather than in turn.
	for _, e := range entries {
		if e.link != nil && e.link.cancel != nil {
			e.link.cancel()
		}
	}
	for _, e := range entries {
		stopFlusher(e)
		r.detachLink(e)
	}
	return keys
}

// closeMirrorSource tears down the shared source key transfers
// through and returns every mirror that was on it, key included.
func (r *SourceReconciler) closeMirrorSource(key types.NamespacedName) []types.NamespacedName {
	r.mu.Lock()
	entry := r.sources[key]
	r.mu.Unlock()
	if entry == nil || entry.shared == nil {
		r.closeEntry(key)
		return []types.NamespacedName{key}
	}
	keys := r.closeSharedSource(entry.shared.key)
	if !slices.Contains(keys, key) {
		keys = append(keys, key)
	}
	return keys
}

// detachLink stops one mirror's link.
func (r *SourceReconciler) detachLink(e *sourceEntry) {
	if e.link == nil || r.opener == nil {
		return
	}
	r.opener.detach(e.link)
}

// stopFlusher stops one mirror's status flusher and waits for it.
func stopFlusher(e *sourceEntry) {
	if e.flusherCancel != nil {
		e.flusherCancel()
	}
	if e.flusherDone != nil {
		<-e.flusherDone
	}
}

// sourceProgressState is the SourceProgress condition the source
// gateway publishes via server-side apply.
type sourceProgressState struct {
	status   metav1.ConditionStatus
	reason   string
	message  string
	attempts uint32
	// rebuildReader marks the states only a fresh FlowReader resolves,
	// so the flusher does not have to re-derive that from the reason:
	// ReaderAgedOut is published both for a reader that skipped once
	// and kept delivering and for one stuck outside the ring, and only
	// the second is a wedge.
	rebuildReader bool
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
	recordProgress(r.Recorder, key, mxlv1alpha1.ConditionTypeSourceProgress,
		state.reason, state.message)
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
func (r *SourceReconciler) startFlusher(entry *sourceEntry) {
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
	go r.runFlusher(ctx, done, entry, interval)
}

// runFlusher is the per-mirror status flusher loop. Tracks the most
// recently published condition so a steady stream of grains does
// not turn into a steady stream of API writes.
func (r *SourceReconciler) runFlusher(ctx context.Context, done chan struct{}, entry *sourceEntry, interval time.Duration) {
	defer close(done)
	key := entry.key
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
		// An entry added against a TargetInfo the target has since
		// replaced is waiting for its reconcile to re-add it, and
		// publishes nothing meanwhile: its transfers go to the replaced
		// fabric side, and a publish here would tell the target that a
		// source is sending into its new one before any source has been
		// added to it.
		if r.attachedToReplacedTarget(ctx, entry) {
			continue
		}
		// A loop that gave its target up has stopped; only adding the
		// target again resumes the mirror. The re-add is Reconcile's,
		// queued rather than run here, so it never races the worker
		// reconciling the same mirror.
		if entry.dropped.Load() && !entry.redialQueued.Load() && r.enqueueRedial(key) {
			entry.redialQueued.Store(true)
		}
		if entry.progress.Load() > 0 {
			r.clearRedials(key)
		}
		state := observedState(entry, r.readerStallAfter())

		// A reader that has stopped feeding the fabric is the one wedge
		// neither side of the mirror resolves on its own. The target's
		// stuck-handshake watchdog is gated on status.lastSentAt
		// postdating its own fabric open, and a reader that transfers
		// nothing never advances lastSentAt, so the target reads the
		// mirror as an idle producer and declines to rebuild.
		// Reporting the wedge and waiting for the other side leaves
		// the mirror Degraded indefinitely; reopening the reader here
		// is what ends it, and it is the same recovery an operator
		// gets by deleting the mirror and letting the requestor
		// recreate it.
		// Asked of every failing state rather than only the ones a
		// reopen resolves: a reader on a flow nothing writes is what
		// keeps that flow alive, however its own failure reads. libmxl
		// reclaims a flow directory only once an exclusive lock on it
		// can be taken, and this reader's handle denies it, so the
		// directory survives, the local agent keeps publishing Origin
		// and renewing the Lease, and the flow is never collected.
		// TransfersNotLanding is the state a source reaches without
		// asking for a reopen, and it pins the flow exactly as the
		// wedge states do.
		if state.status == metav1.ConditionFalse && r.writerGone(entry.flowRef()) {
			state.status = metav1.ConditionFalse
			state.reason = mxlv1alpha1.ReasonSourceWriterGone
			state.message = "flow has no live writer; releasing the source reader so the flow can be reclaimed"
			if err := r.publishSourceProgress(ctx, key, state); err != nil {
				ctrl.Log.WithName("source-flush").Error(err, "publish",
					"mirror", key, "reason", state.reason)
			}
			ctrl.Log.WithName("source-flush").Info(
				"flow has no live writer; releasing source reader",
				"mirror", key, "flowID", entry.flowID())
			// Returning first for the same reason the reopen path does:
			// closeSourceHandles waits on flusherDone, which this
			// goroutine closes.
			go r.closeEntry(key)
			return
		}

		if state.rebuildReader {
			switch {
			case r.readerRebuildsExhausted(entry.sourceKey()):
				state.reason = ReasonReaderRebuildCapReached
				state.message = fmt.Sprintf("%s; unresolved across %d reopens",
					state.message, maxReaderRebuilds)
			case entry.beginRebuild():
				attempt := r.bumpReaderRebuilds(entry.sourceKey())
				if err := r.publishSourceProgress(ctx, key, state); err != nil {
					ctrl.Log.WithName("source-flush").Error(err, "publish",
						"mirror", key, "reason", state.reason)
				}
				ctrl.Log.WithName("source-flush").Info(
					"source reader wedged; reopening reader",
					"mirror", key, "reason", state.reason, "attempt", attempt)
				// Returning first is what makes the reopen safe:
				// closeSourceHandles waits on flusherDone, and this
				// goroutine is the one that closes it.
				go r.dispatchReaderRebuild(key)
				return
			default:
				// A reopen is already in flight for this entry.
				continue
			}
		}
		if state.status == metav1.ConditionTrue &&
			state.reason == mxlv1alpha1.ReasonRecovered {
			r.clearReaderRebuilds(entry.sourceKey())
		}

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

// redial is one mirror's run of re-adds of a target its loop gave up.
type redial struct {
	count     uint32
	notBefore time.Time
}

// takeRedial reports how long key's next re-add has to wait. When it
// may run now, it is counted and the wait after it is armed, and the
// returned attempt is its number.
func (r *SourceReconciler) takeRedial(key types.NamespacedName) (time.Duration, uint32) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.redials[key]
	if wait := d.notBefore.Sub(now); wait > 0 {
		return wait, d.count
	}
	if r.redials == nil {
		r.redials = make(map[types.NamespacedName]redial)
	}
	d.count++
	d.notBefore = now.Add(backoffFor(d.count))
	r.redials[key] = d
	return 0, d.count
}

func (r *SourceReconciler) clearRedials(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.redials, key)
}

// enqueueRedial asks for a reconcile of key. An event the queue has no
// room for is not waited for; the flusher asks again on its next tick.
func (r *SourceReconciler) enqueueRedial(key types.NamespacedName) bool {
	if r.redialEvents == nil {
		return false
	}
	select {
	case r.redialEvents <- event.GenericEvent{Object: &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}}:
		return true
	default:
		return false
	}
}

// attachedToReplacedTarget reports whether the mirror now publishes a
// TargetInfo other than the one entry was added against.
func (r *SourceReconciler) attachedToReplacedTarget(ctx context.Context, entry *sourceEntry) bool {
	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, entry.key, &mirror); err != nil {
		return false
	}
	return mirror.Status.TargetInfo != entry.infoStr
}

// bumpReaderRebuilds records one more consecutive reopen of the shared
// source and returns the new count.
func (r *SourceReconciler) bumpReaderRebuilds(skey sourceKey) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rebuilds == nil {
		r.rebuilds = make(map[sourceKey]uint32)
	}
	r.rebuilds[skey]++
	return r.rebuilds[skey]
}

// beginRebuild claims the reader rebuild for this mirror's shared
// source, so a second flusher tick -- this mirror's or that of any
// other mirror of the source -- cannot spawn a competing reopen.
func (e *sourceEntry) beginRebuild() bool {
	if e.shared == nil {
		return true
	}
	return e.shared.rebuilding.CompareAndSwap(false, true)
}

// writerGone reports whether the flow has no live writer. A nil seam,
// or an error asking, answers false: the check exists to stop a futile
// reopen, and failing to get an answer is not grounds for tearing down
// a mirror that may be healthy. ErrFlowNotFound is the exception,
// because it is an answer.
func (r *SourceReconciler) writerGone(ref mxlv1alpha1.FlowRef) bool {
	if r.writerLiveFn == nil || ref.ID == "" {
		return false
	}
	live, err := r.writerLiveFn(ref)
	if err != nil {
		// The flow is not in this node's domain, so nothing here
		// writes it and no reader can be opened on it. Answering
		// "assume live" sends the mirror into an open that returns
		// this same status, which the reconciler has nowhere to put
		// but a returned error: a failure to retry rather than a state
		// to publish, so the mirror carries no reason and the retry
		// repeats for as long as it exists.
		if errors.Is(err, mxl.ErrFlowNotFound) {
			return true
		}
		ctrl.Log.WithName("source-flush").V(1).Info(
			"writer liveness check failed; assuming the writer is live",
			"flow", ref.Name(), "error", err.Error())
		return false
	}
	return !live
}

// sourceProgressReads reports whether the mirror already carries the
// given reason on its SourceProgress condition. Used to keep a mirror
// parked on an absent writer from rewriting the same condition, and
// emitting the same event, on every retry.
func sourceProgressReads(mirror *mxlv1alpha1.MxlFlowMirror, reason string) bool {
	cond := apimeta.FindStatusCondition(mirror.Status.Conditions,
		mxlv1alpha1.ConditionTypeSourceProgress)
	return cond != nil && cond.Reason == reason
}

// readerRebuildsExhausted reports whether the shared source has used
// its reopen budget.
func (r *SourceReconciler) readerRebuildsExhausted(skey sourceKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rebuilds[skey] >= maxReaderRebuilds
}

// clearReaderRebuilds returns the shared source's full reopen budget.
// Called once grains flow again, so a reader that wedges later is not
// judged on a stall it already recovered from.
func (r *SourceReconciler) clearReaderRebuilds(skey sourceKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.rebuilds, skey)
}

func (r *SourceReconciler) dispatchReaderRebuild(key types.NamespacedName) {
	if r.rebuildFn != nil {
		r.rebuildFn(key)
		return
	}
	r.rebuildWedgedReader(context.Background(), key)
}

// rebuildWedgedReader drops the wedged reader and the links on it and
// runs one reconcile per mirror to open fresh ones.
//
// Reconcile does the reopen rather than this function: it already
// owns provider resolution, the AddTarget attempt counter and the
// flusher handoff, and a second open path here would be a copy of
// all three that could drift. Driving it directly instead of waiting
// for a watch keeps the recovery self-contained -- the events that
// would otherwise wake the reconciler (origin Lease renewals, MxlFlow
// status writes) all stop precisely when the producer is in trouble.
func (r *SourceReconciler) rebuildWedgedReader(ctx context.Context, key types.NamespacedName) {
	// The whole shared source goes, not just this mirror's link: every
	// mirror's reader was opened on the same flow, so one left on a ring
	// the producer replaced is not alone. Each mirror is then driven
	// back through Reconcile, which re-opens the source once and
	// attaches the rest to it.
	for _, k := range r.closeMirrorSource(key) {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: k}); err != nil {
			ctrl.Log.WithName("source-flush").Error(err, "reopen source reader",
				"mirror", k)
		}
	}
}

// readerStallAfter returns the configured reader-stall window,
// falling back to defaultReaderStallAfter when unset.
func (r *SourceReconciler) readerStallAfter() time.Duration {
	if r.ReaderStallAfter > 0 {
		return r.ReaderStallAfter
	}
	return defaultReaderStallAfter
}

// sourceStateEqual reports whether two sourceProgressState values
// would publish a status an observer could tell apart. lastSentAt is
// compared by value rather than pointer identity: observedState
// allocates a fresh *time.Time on every tick (copying the
// atomic-loaded value), so a struct-level == on the two states would
// always disagree once lastSentAt is set, defeating the dedupe and
// turning a single successful transfer into a status write every
// flusher tick.
//
// The comparison is quantised for the same reason the target side
// quantises its commit timestamp: at 50 grains a second the exact
// value is never the same twice, and publishing it turns a healthy
// mirror into a permanent etcd write stream.
func sourceStateEqual(a, b sourceProgressState) bool {
	if a.status != b.status || a.reason != b.reason ||
		a.message != b.message || a.attempts != b.attempts {
		return false
	}
	if (a.lastSentAt == nil) != (b.lastSentAt == nil) {
		return false
	}
	if a.lastSentAt != nil && !sameQuantum(*a.lastSentAt, *b.lastSentAt) {
		return false
	}
	return true
}

// observedState derives the SourceProgress condition the flusher
// should publish from the atomics on the entry. lastSentAt is
// propagated into every state so the target-side watchdog gets a
// freshness signal regardless of which condition is currently being
// published.
//
// Every fault here is judged against the last grain that reached the
// target rather than against the lifetime transfer count. A mirror
// that delivered for hours and then stopped reads identically to one
// that never started, and the lifetime count reports the history
// instead: it turned a source that had gone dead into ConditionTrue /
// Recovered, and no watchdog on either side of the mirror looks at a
// source that calls itself healthy.
func observedState(entry *sourceEntry, stallAfter time.Duration) sourceProgressState {
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
	delivering := sent != nil && time.Since(*sent) <= stallAfter
	// A post the initiator rejects for good fails on every re-add too,
	// so the mirror carries its text rather than only reading as
	// transfers not landing.
	if p := entry.postError.Load(); p != nil && !delivering {
		return sourceProgressState{
			status:     metav1.ConditionFalse,
			reason:     mxlv1alpha1.ReasonTransfersNotLanding,
			message:    "the initiator rejects every transfer: " + *p,
			lastSentAt: sent,
		}
	}
	// An aged-out skip is only evidence about the reader while it is
	// recent. Held for the stall window rather than until the next
	// transfer so a reader skipping every tick stays on one condition
	// instead of flapping it against Recovered, and so a single skip
	// stays visible to an operator for a moment either way.
	agedOut := false
	if at := entry.agedOutAt.Load(); at != nil && time.Since(*at) <= stallAfter {
		agedOut = true
	}

	// The head has not moved: the loop's inner range never runs, so it
	// emits no error and no aged-out skip, and the target reads the
	// mirror as an idle producer and declines to recover. This is what
	// a reader left on a producer's previous flow directory looks like
	// after the producer restarts -- the ring it is mapped onto is
	// gone, so no later grain can ever arrive and only a fresh reader
	// resolves it.
	if at := entry.headAdvancedAt.Load(); at != nil &&
		time.Since(*at) > stallAfter && !delivering {
		return sourceProgressState{
			status: metav1.ConditionFalse,
			reason: mxlv1alpha1.ReasonReaderNotAdvancing,
			message: fmt.Sprintf("reader head stuck at %d for over %s",
				entry.lastHead.Load(), stallAfter),
			rebuildReader: true,
			lastSentAt:    sent,
		}
	}
	// The head is moving and the reader keeps landing outside the
	// producer's ring. Skipping to the head is the loop's own recovery
	// and it works whenever the ring underneath is live, so a reader
	// still aging out a whole stall window after its last delivery is
	// one that cannot reach the live tail at all; only reopening it
	// gets the mirror back.
	if agedOut && !delivering {
		return sourceProgressState{
			status: metav1.ConditionFalse,
			reason: mxlv1alpha1.ReasonReaderAgedOut,
			message: fmt.Sprintf(
				"source reader fell behind writer and has delivered nothing in %s",
				stallAfter),
			rebuildReader: true,
			lastSentAt:    sent,
		}
	}
	// The head is moving, so the producer is alive and the reader is
	// following it, yet nothing has landed at the target. Reporting
	// True here calls that healthy, and it is the state a source sits
	// in when its target publishes an endpoint that never accepts: no
	// transfer completes, so lastSentAt never advances. Left to the
	// target's stuck-handshake watchdog rather than reopened here: the
	// reader is demonstrably reading, and the target gates its own
	// rebuild on seeing this reason.
	if !delivering && !entry.openedAt.IsZero() &&
		time.Since(entry.openedAt) > stallAfter && !idleOnInvalid(time.Now(), entry, stallAfter) {
		return sourceProgressState{
			status: metav1.ConditionFalse,
			reason: mxlv1alpha1.ReasonTransfersNotLanding,
			message: fmt.Sprintf(
				"reader head at %d is advancing but nothing has reached the target in %s",
				entry.lastHead.Load(), stallAfter),
			lastSentAt: sent,
		}
	}
	if agedOut {
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
	if r.shared == nil {
		r.shared = make(map[sourceKey]*sharedSource)
	}
	if r.attempts == nil {
		r.attempts = make(attemptTable[sourceAddInputs])
	}
	if r.rebuilds == nil {
		r.rebuilds = make(map[sourceKey]uint32)
	}
	if r.redialEvents == nil {
		r.redialEvents = make(chan event.GenericEvent, 64)
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.opener == nil {
		r.opener = &libmxlOpener{
			Domains:      r.Domains,
			NodeName:     r.NodeName,
			Selector:     r.Selector,
			PollInterval: r.PollInterval,
		}
	}
	if r.writerLiveFn == nil {
		// libmxl's own answer, not an inference from a stalled head.
		r.writerLiveFn = func(ref mxlv1alpha1.FlowRef) (bool, error) {
			handles, err := r.Domains.For(context.Background(), ref.Domain)
			if err != nil {
				return false, err
			}
			inst := handles.MXL()
			if inst == nil {
				return false, fmt.Errorf("mxl instance closed")
			}
			return inst.IsFlowActive(ref.ID)
		}
	}
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&mxlv1alpha1.MxlFlowMirror{},
		mirrorFlowIDIndex,
		indexMirrorByFlow,
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
		WatchesRawSource(source.Channel(r.redialEvents, &handler.EnqueueRequestForObject{})).
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
	ref, nodeName, ok := mxlv1alpha1.ParseLeaseNameFor(lease.Name)
	if !ok {
		return nil
	}
	if nodeName != r.NodeName {
		return nil
	}
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors, client.MatchingFields{mirrorFlowIDIndex: ref.Name()}); err != nil {
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
		if m.Ref() == flow.Ref() && m.Spec.SourceNode == r.NodeName {
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

// samplePollOversample is how many times per transfer batch the sample
// loop polls the head. Polling once per batch is two free-running
// clocks of the same rate: they drift against each other, so one poll
// finds nothing and the next finds two batches.
const samplePollOversample = 2

// defaultSamplePollInterval is the poll interval of a continuous flow:
// the time one xferBatch spans at the flow's sample rate, divided by
// samplePollOversample. Zero when the rate or batch is unusable.
func defaultSamplePollInterval(rate mxl.Rational, xferBatch uint64) time.Duration {
	if xferBatch == 0 || rate.Den <= 0 || rate.Num <= 0 {
		return 0
	}
	batch := time.Duration(int64(time.Second) * int64(rate.Den) * int64(xferBatch) / int64(rate.Num))
	return batch / samplePollOversample
}
