package mirror

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/fabric"
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

// targetInfoFieldOwner is the server-side-apply field manager owning
// status.targetInfo, and nothing else.
//
// Held apart from targetFieldOwner because the two fields change on
// completely different clocks. TargetInfo is the libmxl-fabrics
// descriptor: it changes only when the fabric side is opened or
// rebuilt, perhaps twice in a mirror's life, and it is large - a
// 12-channel audio flow serialises 402 bounce-buffer regions into
// roughly 23 kB. The progress fields change whenever grains move.
// While one manager owned both, SSA's rule that a manager releases a
// field it omits forced every progress write to re-stamp the whole
// descriptor, so a mirror rewrote 23 kB of unchanged JSON into etcd on
// every flusher tick. Splitting the managers lets the progress payload
// omit targetInfo without the apiserver stripping it.
const targetInfoFieldOwner = "mxl-target-gateway-info"

// statusQuantum is the resolution at which grain-progress timestamps
// are published. Within one quantum a moving timestamp is not worth an
// etcd write and a watch fan-out to every controller in the cluster:
// the fine-grained signal already exists as flow metrics, which is
// where a per-grain view belongs. Coarsening only the timestamp keeps
// every phase and reason transition immediate.
//
// Well inside defaultStuckHandshakeAfter, so the cross-side wedge
// discriminators that compare these timestamps keep their meaning.
const statusQuantum = 5 * time.Second

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

// maxTargetOpenAttempts is the number of consecutive openTarget
// failures after which the mirror stops reading as Materializing.
// Materializing is a transient state a consumer waits through, so a
// mirror parked there is indistinguishable from one a moment away
// from Ready: no phase change to alert on and no counter to
// threshold. From this attempt on the target side publishes
// Phase=Degraded, and a failure that came from the local writer also
// gets the flow directory reclaimed (see reclaimUnusableFlowDir).
const maxTargetOpenAttempts uint32 = 3

// reclaimRetryDelay is how long the retry after a flow-directory
// reclaim waits. Short on purpose: materialising a fresh directory is
// the point of the reclaim, so that attempt does not sit out the
// backoff the failures before it earned.
const reclaimRetryDelay = 100 * time.Millisecond

// flowDirSuffix is the directory-name suffix libmxl gives a per-flow
// directory under a domain (FLOW_DIRECTORY_NAME_SUFFIX). go-mxl keeps
// its own copy unexported, so the gateway carries one; the agent
// module holds the same constant as flowpublisher.FlowDirSuffix.
const flowDirSuffix = ".mxl-flow"

// grainDirName is the subdirectory of a flow directory holding the
// grain segment files (GRAIN_DIRECTORY_NAME). Only ever read, and
// only to report how much of the ring was present when a directory
// is reclaimed.
const grainDirName = "grains"

// errOpenWriterFailed wraps a NewWriter failure so the Reconcile
// failure path can tell "the local flow file could not be opened"
// apart from "the libmxl-fabrics side could not be set up". Only the
// former is answerable by reclaiming the flow directory.
var errOpenWriterFailed = errors.New("open local writer")

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

	// APIReader is an uncached reader for the Node lookup that gates
	// orphaned-finalizer reaping. Nil falls back to Client.
	APIReader client.Reader

	// Recorder publishes mirror-level events for the target side. Nil
	// records nothing.
	Recorder record.EventRecorder

	// NodeName is the Kubernetes node this gateway runs on. Mirrors
	// with spec.targetNode set to a different node are ignored.
	NodeName string

	// BindAddress is the libmxl-fabrics endpoint node passed to each
	// Target Setup. Empty means "bind all interfaces" per
	// libmxl-fabrics semantics.
	BindAddress string

	// Selector narrows the interfaces a setup may bind to the fabric
	// this node is allowed to carry MXL traffic on. The capability
	// publisher applies the same one.
	Selector fabric.Selector

	// Handles owns the long-lived mxl + fabrics instances.
	Handles *instance.Handles

	// DomainPath is the MXL domain directory this gateway operates on,
	// the same path Handles was opened against. Held separately from
	// Handles because reclaimUnusableFlowDir works on the directory
	// with the filesystem rather than through libmxl. Empty disables
	// the reclaim.
	DomainPath string

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

	// openTargetFn is the seam Reconcile opens the target through.
	// Production leaves it nil and falls back to
	// (*TargetReconciler).openTarget, which needs a real libmxl;
	// tests inject failures so the attempt accounting and the
	// escalation out of Materializing can be observed without one.
	openTargetFn func(key types.NamespacedName, flowDef string, provider fabrics.Provider) (*targetEntry, error)

	// nowFn is the clock the open backoff is measured against.
	// Production leaves it nil and the reconciler falls back to
	// time.Now; tests inject one so the gate can be driven without
	// sleeping.
	nowFn func() time.Time

	mu      sync.Mutex
	targets map[types.NamespacedName]*targetEntry

	// attempts counts consecutive openTarget failures per mirror.
	// Cleared the moment a target opens, so the value is always the
	// length of the current failure run. Guarded by mu.
	attempts attemptTable[targetOpenInputs]
}

// targetOpenInputs is what an open attempt depends on. A change to
// either makes the next attempt a new one rather than a retry.
type targetOpenInputs struct {
	flowDef  string
	provider fabrics.Provider
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

	// bytes counts payload committed to the local flow for this
	// mirror, read by the throughput collector at scrape time. A
	// counter rather than a rate: the scrape interval is the
	// collector's business, not the progress loop's.
	bytes atomic.Uint64

	// flowID and peerNode label that counter alongside provider. Set
	// once when the entry is published and never changed, so the
	// collector reads them without the entry's atomics.
	flowID   string
	peerNode string

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

// reapOrphanedFinalizer drops TargetFinalizerName from a deleting
// mirror whose spec.targetNode names a Node that no longer exists.
// Symmetric with the source-side reaper; same DaemonSet gate, same
// orphaned finalizer when the node it named is removed.
func (r *TargetReconciler) reapOrphanedFinalizer(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror) (ctrl.Result, error) {
	if mirror.DeletionTimestamp.IsZero() ||
		!controllerutil.ContainsFinalizer(mirror, TargetFinalizerName) {
		return ctrl.Result{}, nil
	}

	gone, err := nodeGone(ctx, r.nodeReader(), mirror.Spec.TargetNode)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("look up target node %s: %w", mirror.Spec.TargetNode, err)
	}
	if !gone {
		return ctrl.Result{RequeueAfter: orphanRecheckInterval}, nil
	}

	controllerutil.RemoveFinalizer(mirror, TargetFinalizerName)
	if err := r.Update(ctx, mirror); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("remove orphaned target finalizer: %w", err)
	}
	log.FromContext(ctx).Info("reaped orphaned target-side finalizer",
		"mxlflowmirror", client.ObjectKeyFromObject(mirror),
		"targetNode", mirror.Spec.TargetNode)
	return ctrl.Result{}, nil
}

// nodeReader prefers the uncached APIReader SetupWithManager binds.
func (r *TargetReconciler) nodeReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get

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
		return r.reapOrphanedFinalizer(ctx, &mirror)
	}

	// Deletion path: tear down libmxl handles, drop the finalizer.
	if !mirror.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&mirror, TargetFinalizerName) {
			return ctrl.Result{}, nil
		}
		r.closeEntry(req.NamespacedName, r.localFlowDisposition(ctx, mirror.Spec.FlowID))
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
				if err := r.applyTargetStatus(ctx, &mirror, mxlv1alpha1.MxlFlowMirrorMaterializing, nil, nil); err != nil {
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
		r.surfaceTargetFailure(ctx, &mirror, mxlv1alpha1.MxlFlowMirrorMaterializing,
			mxlv1alpha1.ReasonFlowDefinitionEmpty,
			fmt.Sprintf("MxlFlow %s has empty spec.definition", mirror.Spec.FlowID))
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	provider, err := providerForSetup(&mirror)
	if err != nil {
		// Never forward auto into libmxl-fabrics: surface the reason on
		// status and stop. The agent or operator patches spec.provider to
		// a concrete value, which wakes this reconciler through its watch.
		r.surfaceTargetFailure(ctx, &mirror, mxlv1alpha1.MxlFlowMirrorMaterializing,
			mxlv1alpha1.ReasonProviderUnresolved, err.Error())
		l.Info("refusing target setup: mirror provider is unresolved", "error", err.Error())
		return ctrl.Result{}, nil
	}

	// Seed the in-memory attempts counter from the persisted
	// status.targetAttemptCount so a gateway pod restart does not hand
	// a wedged mirror a fresh budget: without this a target that has
	// been unopenable across the bounce reads as Materializing again
	// and the escalation to Degraded restarts from zero. Matches the
	// source side's seeding of status.attemptCount.
	inputs := targetOpenInputs{flowDef: string(flow.Spec.Definition.Raw), provider: provider}
	r.mu.Lock()
	r.attempts.seed(req.NamespacedName, uint32(mirror.Status.TargetAttemptCount))
	remaining, gated := r.attempts.wait(req.NamespacedName, inputs, r.now())
	r.mu.Unlock()
	// Nothing is written on a gated pass: the status write is what
	// wakes this reconciler through its own watch.
	if gated {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	entry, err := r.openTargetDispatch(req.NamespacedName, string(flow.Spec.Definition.Raw), provider)
	if err != nil {
		return r.handleOpenTargetFailure(ctx, &mirror, inputs, err)
	}

	r.mu.Lock()
	if existing := r.targets[req.NamespacedName]; existing != nil {
		// Concurrent reconcile produced a stray entry; close the new
		// one and reuse the existing.
		r.mu.Unlock()
		closeTargetHandles(entry, keepFlow)
		return ctrl.Result{}, nil
	}
	entry.flowID = mirror.Spec.FlowID
	entry.peerNode = mirror.Spec.SourceNode
	r.targets[req.NamespacedName] = entry
	delete(r.attempts, req.NamespacedName)
	r.mu.Unlock()

	// Publish the descriptor before the phase: a Ready mirror whose
	// targetInfo has not landed yet is one the source side cannot dial.
	if err := r.applyTargetInfo(ctx, &mirror, entry.infoStr); err != nil {
		r.closeEntry(req.NamespacedName, r.localFlowDisposition(ctx, mirror.Spec.FlowID))
		return ctrl.Result{}, fmt.Errorf("publish target info: %w", err)
	}
	if err := r.applyTargetStatus(ctx, &mirror, mxlv1alpha1.MxlFlowMirrorReady, nil, nil); err != nil {
		// Status update lost; close the entry so the next pass can
		// retry cleanly.
		r.closeEntry(req.NamespacedName, r.localFlowDisposition(ctx, mirror.Spec.FlowID))
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	r.startFlusher(req.NamespacedName, entry)

	l.Info("target ready",
		"flowID", mirror.Spec.FlowID,
		"sourceNode", mirror.Spec.SourceNode,
		"provider", provider.String())
	return ctrl.Result{}, nil
}

// now reports the current time through the reconciler's clock seam.
func (r *TargetReconciler) now() time.Time {
	if r.nowFn != nil {
		return r.nowFn()
	}
	return time.Now()
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
		return nil, fmt.Errorf("%w: NewWriter: %w", errOpenWriterFailed, err)
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

// openTargetDispatch routes the open through the test seam when set,
// falling back to the cgo openTarget in production.
func (r *TargetReconciler) openTargetDispatch(key types.NamespacedName, flowDef string, provider fabrics.Provider) (*targetEntry, error) {
	if r.openTargetFn != nil {
		return r.openTargetFn(key, flowDef, provider)
	}
	return r.openTarget(key, flowDef, provider)
}

// handleOpenTargetFailure records the failed open, publishes the
// reason on status, and returns a bounded-backoff requeue.
//
// openTarget wraps NewWriter + the libmxl-fabrics Target.Setup, whose
// errors otherwise land only in the gateway log: the producer, the
// consumer and the cluster diagnostics only ever observe the CR.
// Publishing Materializing alone is not enough either - it is the
// state a consumer waits through, so a mirror that can never open
// looks the same as one about to succeed. Past maxTargetOpenAttempts
// consecutive failures the phase escalates to Degraded and, for a
// failure that came from the local writer, the flow directory is
// reclaimed so the retry materialises a fresh one.
func (r *TargetReconciler) handleOpenTargetFailure(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror, inputs targetOpenInputs, openErr error) (ctrl.Result, error) {
	key := client.ObjectKeyFromObject(mirror)
	l := log.FromContext(ctx).WithValues("mxlflowmirror", key)

	r.mu.Lock()
	attempts, wait := r.attempts.fail(key, inputs, r.now(), backoffFor)
	r.mu.Unlock()

	phase := mxlv1alpha1.MxlFlowMirrorMaterializing
	if attempts >= maxTargetOpenAttempts {
		phase = mxlv1alpha1.MxlFlowMirrorDegraded
	}
	r.surfaceTargetFailure(ctx, mirror, phase, mxlv1alpha1.ReasonOpenTargetFailed,
		fmt.Sprintf("%s (attempt %d)", openErr.Error(), attempts))
	l.Error(openErr, "open target", "attempt", attempts, "phase", string(phase))

	// A writer that will not open is the one failure the gateway can
	// act on: it wrote that directory as a mirror target, so a
	// directory it can no longer open is its own torn output. Only
	// reclaim once the failure has repeated, so a single transient
	// error never costs a directory.
	if attempts >= maxTargetOpenAttempts && errors.Is(openErr, errOpenWriterFailed) {
		if reclaimed := r.reclaimUnusableFlowDir(ctx, mirror.Spec.FlowID); reclaimed {
			// Materialising a fresh directory is the point of the
			// reclaim; do not sit out the accumulated backoff first.
			r.mu.Lock()
			r.attempts.rearm(key, r.now(), reclaimRetryDelay)
			r.mu.Unlock()
			return ctrl.Result{RequeueAfter: reclaimRetryDelay}, nil
		}
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// reclaimUnusableFlowDir removes the local flow directory for flowID
// so the next openTarget materialises a complete one, and reports
// whether it removed anything.
//
// A gateway restart part-way through materialising a target leaves a
// flow directory whose grain ring is short of what its flow header
// declares. libmxl walks to a grain segment that was never written
// and every later open of that path fails, so retrying the open
// against the same directory can only keep failing.
//
// The MxlFlow location published for this node is the gate: a flow
// this node is Origin for belongs to a local producer, and removing
// it would take the producer's flow with it. Only a directory this
// gateway owns as a mirror copy is reclaimed, which is the same
// ownership test teardown applies.
func (r *TargetReconciler) reclaimUnusableFlowDir(ctx context.Context, flowID string) bool {
	l := log.FromContext(ctx).WithName("target-reclaim").WithValues("flowID", flowID)
	if r.DomainPath == "" {
		return false
	}
	if r.localFlowDisposition(ctx, flowID) == keepFlow {
		l.Info("leaving flow directory in place: not this node's mirror copy")
		return false
	}

	dir := filepath.Join(r.DomainPath, flowID+flowDirSuffix)
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			l.Error(err, "stat flow directory", "path", dir)
		}
		return false
	}
	// Report how much of the grain ring was there: the shortfall
	// against the flow header is what distinguishes a torn directory
	// from one that failed to open for another reason, and it is not
	// recoverable once the directory is gone.
	grains := -1
	if entries, err := os.ReadDir(filepath.Join(dir, grainDirName)); err == nil {
		grains = len(entries)
	}
	if err := os.RemoveAll(dir); err != nil {
		l.Error(err, "remove flow directory", "path", dir)
		return false
	}
	l.Info("reclaimed unopenable flow directory", "path", dir, "grainFiles", grains)
	return true
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
	iface, err := resolveInterface(fabInst, r.Selector, provider, r.BindAddress)
	if err != nil {
		_ = target.Close()
		return nil, nil, "", err
	}
	info, err := target.Setup(fabrics.TargetConfig{
		Interface: iface,
		Writer:    writer,
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
		readFn := func() (uint64, int, error) { return target.ReadSamples(targetReadTimeout) }
		commitFn := func(head uint64, count int) error {
			n, err := commitArrivedSamples(writer, head, count)
			entry.bytes.Add(n)
			return err
		}
		go runTargetSampleProgressLoop(loopCtx, done, readFn, commitFn, onFatal, entry)
	} else {
		readFn := func() (uint64, error) { return target.ReadGrain(targetReadTimeout) }
		completeFn := func(idx uint64) (bool, error) {
			g, err := writer.GrainInfo(idx)
			if err != nil {
				return false, err
			}
			// TotalSlices == 0 is a grain the source never subdivides
			// and never transfers in ranges, so there is nothing to
			// wait for; Complete() reports false for it.
			return g.TotalSlices == 0 || g.Complete(), nil
		}
		commitFn := func(idx uint64) error {
			n, err := commitArrivedGrain(writer, idx)
			entry.bytes.Add(n)
			return err
		}
		go runTargetProgressLoop(loopCtx, done, readFn, completeFn, commitFn, onFatal, entry)
	}
}

// targetReadTimeout is how long a blocking target read parks before
// reporting not-ready. It bounds how long a cancelled loop takes to
// notice, so it stays short enough for a responsive teardown while
// still being long next to the grain intervals in use.
const targetReadTimeout = 20 * time.Millisecond

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
// The read is the blocking variant. libfabric still treats -EINTR from
// epoll_wait as a fatal "poll failed" in release builds - the filter in
// util_wait.c is behind ENABLE_DEBUG in every release up to and
// including the pinned v2.5.1 - and Go's async preemption sends SIGURG
// to running goroutines ~50/sec, so that interrupt remains routine.
// What changed is libmxl-fabrics: RCTarget and RDMTarget now catch the
// interrupted FabricException per state and return the state unchanged,
// surfacing MXL_ERR_INTERRUPTED instead of tearing the endpoint down.
// classifyFabricError already reads that as fabricTransient, so the
// loop retries and keeps its handles.
//
// idleSleep survives the switch as a spin guard, not as the wait: a
// provider whose blocking read returns not-ready immediately would
// otherwise spin a core. On a provider that does block, the sleep is
// only ever reached after a full timeout, and an arriving grain still
// wakes the read at once.
//
// For every grain ReadGrain reports as received, completeFn decides
// whether the whole grain has landed - a paced source transfers one
// grain as several slice ranges and each range signals separately -
// and commitFn then does the OpenGrain + Commit dance on the local
// FlowWriter so consumer FlowReaders see the arrived grain.
//
// Errors from ReadGrain are classified: idle means nothing arrived,
// transient means the call was disturbed and the handles are still
// good. On anything else the underlying Target
// is no longer safe to poll - libmxl-fabrics has been observed to
// dangle internal state after the remote endpoint drops, and the
// next ReadGrain call segfaults inside cgo. We exit the loop and
// call onFatal so the reconciler can rebuild the fabric side.
//
// The loop takes readFn, completeFn and commitFn as injected closures
// so the state machine - the only piece prone to bugs and the only
// piece worth unit-testing - is isolated from cgo. Production passes a
// closure over fabrics.Target.ReadGrain, one over mxl.Writer.GrainInfo
// and one over commitArrivedGrain(writer, ...).
func runTargetProgressLoop(
	ctx context.Context,
	done chan struct{},
	readGrain ReadGrainFunc,
	grainComplete GrainCompleteFunc,
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
		switch kind := classifyFabricError(err); {
		case err == nil:
			// A grain the source paced arrives as several slice ranges,
			// each signalling separately with a growing valid-slice
			// count. Committing on the first would publish a grain with
			// most of its lines unwritten, and commitArrivedGrain marks
			// every slice valid regardless. Only the arrival that
			// completes the grain may commit it.
			if grainComplete != nil {
				ok, cerr := grainComplete(idx)
				if cerr != nil {
					l.Error(cerr, "grain completeness check", "idx", idx)
					break
				}
				if !ok {
					break
				}
			}
			if err := commit(idx); err != nil {
				l.Error(err, "commit received grain", "idx", idx)
				break
			}
			if tracker != nil {
				tracker.recordCommit(idx, time.Now())
			}
		case kind == fabricIdle:
			select {
			case <-ctx.Done():
				return
			case <-time.After(idleSleep):
			}
		case kind == fabricTransient:
			// Disturbed, not broken: the handles are still usable, so
			// poll again instead of rebuilding the fabric side.
			l.V(1).Info("ReadGrain disturbed, retrying",
				"error", err.Error(), "class", kind.String())
			select {
			case <-ctx.Done():
				return
			case <-time.After(idleSleep):
			}
		default:
			l.Error(err, "ReadGrain - target is no longer safe to poll, exiting loop",
				"class", kind.String())
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
// The returned byte count is the grain payload libmxl declares for the
// flow, which the write access already carries.
func commitArrivedGrain(writer *mxl.Writer, idx uint64) (uint64, error) {
	ga, err := writer.OpenGrain(idx)
	if err != nil {
		return 0, fmt.Errorf("OpenGrain(%d): %w", idx, err)
	}
	if err := ga.Commit(ga.TotalSlices, 0); err != nil {
		return 0, fmt.Errorf("Commit(%d): %w", idx, err)
	}
	return uint64(ga.GrainSize), nil
}

// runTargetSampleProgressLoop drives a libmxl-fabrics Target for a
// continuous (audio) flow until ctx is canceled or ReadSamples reports
// a non-recoverable error. It mirrors runTargetProgressLoop: the
// blocking ReadSamples advances the libfabric event + completion
// queues and parks until a run arrives, with the same idle sleep kept
// as a spin guard for providers that do not block. There is no
// completeness gate here - TransferSamples carries no slice range, so
// a run of samples arrives whole or not at all. For every run of
// samples ReadSamples reports as arrived, commit does the
// OpenSamples + Commit dance on the local FlowWriter so consumer
// FlowReaders see them. Errors are classified as in the grain loop:
// idle and transient are retried, anything else fires onFatal so the
// reconciler rebuilds the fabric side.
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
		switch kind := classifyFabricError(err); {
		case err == nil:
			if err := commit(head, count); err != nil {
				l.Error(err, "commit received samples", "headIndex", head, "count", count)
				break
			}
			if tracker != nil {
				tracker.recordCommit(head, time.Now())
			}
		case kind == fabricIdle:
			select {
			case <-ctx.Done():
				return
			case <-time.After(idleSleep):
			}
		case kind == fabricTransient:
			// Disturbed, not broken: the handles are still usable, so
			// poll again instead of rebuilding the fabric side.
			l.V(1).Info("ReadSamples disturbed, retrying",
				"error", err.Error(), "class", kind.String())
			select {
			case <-ctx.Done():
				return
			case <-time.After(idleSleep):
			}
		default:
			l.Error(err, "ReadSamples - target is no longer safe to poll, exiting loop",
				"class", kind.String())
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
// The returned byte count is the committed run across all channels.
// libmxl publishes no per-sample width, but the write access carries
// the stride between two channels' ring buffers, which is the word
// size times the buffer length the flow config states.
func commitArrivedSamples(writer *mxl.Writer, head uint64, count int) (uint64, error) {
	sa, err := writer.OpenSamples(head, count)
	if err != nil {
		return 0, fmt.Errorf("OpenSamples(%d,%d): %w", head, count, err)
	}
	if err := sa.Commit(); err != nil {
		return 0, fmt.Errorf("CommitSamples(%d,%d): %w", head, count, err)
	}
	bufferLength := uint64(writer.Config().Continuous.BufferLength)
	if bufferLength == 0 {
		return 0, nil
	}
	return uint64(count) * (sa.Stride / bufferLength) * sa.ChannelCount, nil
}

func (r *TargetReconciler) closeEntry(key types.NamespacedName, disp flowDisposition) {
	r.mu.Lock()
	entry := r.targets[key]
	delete(r.targets, key)
	delete(r.attempts, key)
	r.mu.Unlock()
	if entry == nil {
		return
	}
	closeTargetHandles(entry, disp)
}

// localFlowDisposition decides what a teardown should do with the
// local flow. A flow whose published Origin location names this node
// belongs to a local producer: the mirror filled that directory until
// the producer took it over, and offering it for deletion would take
// the producer's flow with it.
//
// A lookup failure answers keepFlow. Reclaiming a mirror copy late
// costs disk on one node until the next teardown; removing a live
// producer's flow costs the flow.
func (r *TargetReconciler) localFlowDisposition(ctx context.Context, flowID string) flowDisposition {
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			return dropFlow
		}
		return keepFlow
	}
	for _, loc := range flow.Status.Locations {
		if loc.NodeName == r.NodeName && loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			return keepFlow
		}
	}
	return dropFlow
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
		if err := r.applyTargetStatus(ctx, mirror, mxlv1alpha1.MxlFlowMirrorMaterializing, nil, nil); err != nil && !apierrors.IsNotFound(err) {
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
	// The rebuilt fabric side has a fresh descriptor; the source side
	// picks the rotation up through its watch on this field.
	if err := r.applyTargetInfo(ctx, mirror, s); err != nil {
		l.Error(err, "publish rebuilt TargetInfo")
		return
	}
	if err := r.applyTargetStatus(ctx, mirror, mxlv1alpha1.MxlFlowMirrorReady, nil, nil); err != nil {
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

// flowDisposition says what should happen to the local flow when a
// target entry's writer is given up.
type flowDisposition bool

const (
	// dropFlow offers the flow for deletion. libmxl removes it when
	// this writer turns out to be its last holder, which is what
	// reclaims a mirror's copy once nothing needs it.
	dropFlow flowDisposition = false
	// keepFlow gives the writer up and leaves the flow in place. Used
	// wherever something other than this entry owns the flow: another
	// entry on this node, or a local producer that has taken over the
	// flow the mirror was filling.
	keepFlow flowDisposition = true
)

func closeTargetHandles(e *targetEntry, disp flowDisposition) {
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
		if disp == keepFlow {
			_ = e.writer.Detach()
		} else {
			_ = e.writer.Close()
		}
	}
}

// applyTargetStatus writes mirror.status via server-side apply with
// FieldOwner=mxl-target-gateway. It is the only path that mutates
// status on this reconciler: routing every write through one field
// manager keeps LastTransitionTime stable across reconciles and lets
// every write stamp observedGeneration off the freshly-cached object.
//
// lastGrainAt and cond are optional; nil pointers omit the
// corresponding key from the SSA payload so the manager does not claim
// ownership of fields it has nothing to say about. status.targetInfo
// is deliberately absent: applyTargetInfo owns it under its own field
// manager, which is what lets this payload stay small.
//
// targetAttemptCount is not optional: it rides on every payload
// because SSA with a single FieldOwner releases ownership of a field
// a later payload omits, and the apiserver then strips it. Sourcing
// it from the live counter also keeps it honest - a write from any
// path (flusher, recovery, teardown) reports the current failure run,
// which is zero for as long as the target is open.
// applyTargetInfo writes status.targetInfo under its own field
// manager. Called only where the descriptor is produced - the initial
// open and each fabric-side rebuild - so an unchanged descriptor costs
// nothing for the life of the mirror.
//
// Owning the field alone is what makes that safe. SSA releases a field
// its manager omits, but only that manager's claim: progress payloads
// written under targetFieldOwner never mention targetInfo, so they
// cannot release a claim they never held.
func (r *TargetReconciler) applyTargetInfo(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror, info string) error {
	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirror"))
	patch.SetNamespace(mirror.Namespace)
	patch.SetName(mirror.Name)
	if err := unstructured.SetNestedField(patch.Object,
		map[string]any{"targetInfo": info}, "status"); err != nil {
		return fmt.Errorf("build targetInfo payload: %w", err)
	}
	return r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(targetInfoFieldOwner),
		client.ForceOwnership,
	)
}

func (r *TargetReconciler) applyTargetStatus(
	ctx context.Context,
	mirror *mxlv1alpha1.MxlFlowMirror,
	phase mxlv1alpha1.MxlFlowMirrorPhase,
	lastGrainAt *time.Time,
	cond *metav1.Condition,
) error {
	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirror"))
	patch.SetNamespace(mirror.Namespace)
	patch.SetName(mirror.Name)
	r.mu.Lock()
	attempts := r.attempts.count(client.ObjectKeyFromObject(mirror))
	r.mu.Unlock()
	status := map[string]any{
		"phase":              string(phase),
		"observedGeneration": mirror.Generation,
		"targetAttemptCount": int64(attempts),
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

// surfaceTargetFailure publishes phase plus a TargetProgress=False
// condition carrying the reason the target side could not be
// established yet. Best-effort: a failed status write must not mask
// the original error the caller returns/requeues on, so the result is
// intentionally ignored. It exists so a target-open failure shows up
// in MxlFlowMirror status (and `kubectl describe`) instead of the
// mirror wedging silently at an empty phase — the producer, the
// consumer, and the cluster diagnostics only observe the CR, never the
// gateway log. Only reached on the pre-Ready path (the Reconcile
// fast-path returns earlier for a live, fresh, Ready mirror), so this
// never demotes a healthy mirror.
//
// The phase is the caller's to choose: a failure that has just started
// is still Materializing, one that has repeated past
// maxTargetOpenAttempts is Degraded.
func (r *TargetReconciler) surfaceTargetFailure(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror, phase mxlv1alpha1.MxlFlowMirrorPhase, reason, message string) {
	_ = r.applyTargetStatus(ctx, mirror, phase, nil, &metav1.Condition{
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
		// Degraded as time goes by. Spawn recovery when no commit has
		// landed since the fabric side opened, the stuck-handshake
		// window has elapsed, *and* mirror.Status.LastSentAt postdates
		// the open: zero commits alone cannot distinguish a wedged
		// handshake from a flow whose producer simply is not sending.
		// Without the source-activity gate an idle flow loops
		// spawn->cap->drop->reopen forever - each reopen resets
		// fabricOpenedAt, the window elapses again, and the cycle
		// closes the writer (and every consumer FlowReader) every few
		// seconds for as long as the producer stays quiet.
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
		neverHandshook := false
		if openedAt != nil &&
			entry.commits.Load() == entry.commitsAtFabricOpen.Load() &&
			time.Since(*openedAt) > r.stuckHandshakeAfter() {
			if mirror, err := r.fetchMirror(ctx, key); err == nil &&
				sourceIsDelivering(mirror, *openedAt) {
				neverHandshook = true
			}
		}
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
					_ = r.applyTargetStatus(ctx, mirror, mxlv1alpha1.MxlFlowMirrorFailed, nil, &metav1.Condition{
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
				neverHandshook2 := false
				if openedAt2 != nil &&
					entry.commits.Load() == entry.commitsAtFabricOpen.Load() &&
					time.Since(*openedAt2) > r.stuckHandshakeAfter() {
					if mirror, err := r.fetchMirror(ctx, key); err == nil &&
						mirror.Status.LastSentAt != nil &&
						mirror.Status.LastSentAt.Time.After(*openedAt2) {
						neverHandshook2 = true
					}
				}
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
		// Reconcile sets Phase=Ready the moment the fabric side opens,
		// before any grain has arrived. Staying silent here leaves that
		// optimistic Ready standing for the lifetime of a target that
		// never receives one, which is what a mirror pointed at a
		// departed source node looks like: Ready, no TargetProgress
		// condition, no lastGrainAt. Once the open is older than the
		// freshness window, report the absence for what it is.
		openedAt := entry.fabricOpenedAt.Load()
		if openedAt == nil || time.Since(*openedAt) < degradedAfter {
			return targetProgressState{}
		}
		return targetProgressState{
			phase:   mxlv1alpha1.MxlFlowMirrorDegraded,
			status:  metav1.ConditionFalse,
			reason:  mxlv1alpha1.ReasonNoGrains,
			message: "no grain commits since the fabric side opened",
		}
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
	if a.lastCommitAt != nil && !sameQuantum(*a.lastCommitAt, *b.lastCommitAt) {
		return false
	}
	return true
}

// sameQuantum reports whether two progress timestamps are close enough
// that republishing the later one would tell an observer nothing it
// could act on. Grains commit far faster than any consumer of this
// status reacts, so comparing at full resolution turns a steady flow
// into one etcd write and one cluster-wide watch fan-out per tick.
func sameQuantum(a, b time.Time) bool {
	d := a.Sub(b)
	if d < 0 {
		d = -d
	}
	return d < statusQuantum
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
	recordProgress(r.Recorder, key, mxlv1alpha1.ConditionTypeTargetProgress,
		state.reason, state.message)
	cond := metav1.Condition{
		Type:               mxlv1alpha1.ConditionTypeTargetProgress,
		Status:             state.status,
		Reason:             state.reason,
		Message:            state.message,
		LastTransitionTime: metav1.Now(),
	}
	return r.applyTargetStatus(ctx, mirror, state.phase, state.lastCommitAt, &cond)
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *TargetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.targets == nil {
		r.targets = make(map[types.NamespacedName]*targetEntry)
	}
	if r.attempts == nil {
		r.attempts = make(attemptTable[targetOpenInputs])
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Named("mxlflowmirror-target").
		Complete(r)
}
