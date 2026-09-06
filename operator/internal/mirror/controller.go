// Package mirror hosts the operator's MxlFlowMirror lifecycle
// controller.
//
// A mirror exists to carry one flow from the node that holds it to a
// node that wants it, so two things justify it and both are required:
// something still asks for it, and the flow still has a live origin to
// pull from. The controller keeps those two answers on the object as
// the Claimed and Sourceable conditions, repoints a mirror whose
// origin has moved, and deletes one whose justification has been gone
// for longer than the grace period.
//
// It replaces a pair of collectors that each owned "their own" mirrors
// and between them owned not all of them. One reaped mirrors carrying
// the agent's intent label; the other reaped mirrors carrying a
// receiver's owner reference. A mirror that fitted neither -- one
// whose labels had been edited off, one left ownerless by a receiver
// dropping its last reference rather than being deleted -- was reached
// by no collector at all and survived for the life of the cluster.
// Neither collector asked whether the flow still had a producer, so a
// mirror whose source had been gone for hours was kept alive by a
// consumer pod that merely happened to still be running, and that
// mirror in turn kept its own flow from being collected.
package mirror

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/mxl-k8s/api/selection"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// FieldOwner is the server-side-apply field manager the operator
// writes the Claimed and Sourceable conditions under. The two gateway
// reconcilers own their own conditions under their own managers, so
// each side's apply carries only its own entries and none of the three
// resets another's.
const FieldOwner = "mxl-operator"

// legacyIntentFinalizer was added to agent-authored mirrors by an
// earlier collector so it could observe the deletion. It never did
// anything on deletion but remove itself, so all it bought was an
// extra round trip and one more way for a mirror to sit in Terminating
// while the operator was down. This controller strips it wherever it
// finds one and adds none of its own; the gateway finalizers are the
// ones that hold a mirror back while the data plane tears down.
const legacyIntentFinalizer = "mxl.qvest-digital.com/intent-gc"

// DefaultGracePeriod is how long a mirror has to read Claimed=False or
// Sourceable=False before it is deleted.
//
// It has to outlast a pod rolling over at either end. A consumer being
// replaced leaves its mirror unclaimed between the old pod going and
// the new one asking again; a producer being replaced leaves it
// unsourceable for as long as the flow has no Origin. Deleting inside
// that window costs the stream a re-materialization for no reason.
const DefaultGracePeriod = 5 * time.Minute

// Reconciler drives one MxlFlowMirror through its lifecycle.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Recorder publishes the retarget and deletion events. Nil records
	// nothing, which keeps the reconciler usable in tests that wire no
	// manager.
	Recorder record.EventRecorder

	// Lease gates the Origin locations this reconciler will point a
	// mirror at. Nil trusts any Origin location, which keeps a
	// reconciler wired without a Lease client resolving the same
	// source the pre-Lease code did.
	Lease LeaseChecker

	// GracePeriod is how long a mirror has to stay unjustified before
	// it is deleted. Zero means DefaultGracePeriod.
	GracePeriod time.Duration
}

// LeaseChecker reports whether the agent on nodeName still holds a
// renewed origin Lease for flowID. Matches the flow and receiver
// packages' interface of the same name so all three consume one
// leasecheck.Checker.
type LeaseChecker interface {
	IsFresh(ctx context.Context, flowID, nodeName string) (fresh bool, deadline time.Time, err error)
}

// Event reasons recorded on an MxlFlowMirror.
const (
	// ReasonSourceRetargeted marks a mirror repointed at a moved
	// origin.
	ReasonSourceRetargeted = "SourceRetargeted"
	// ReasonOriginLocal marks a mirror deleted because the flow's
	// origin arrived on the mirror's own target node, which makes the
	// mirror a transfer from a node to itself. Deleting it is correct
	// and looks identical to a mirror being lost.
	ReasonOriginLocal = "OriginLocal"
	// ReasonMirrorCollected marks a mirror deleted because nothing
	// justifies it any more, carrying which of the two justifications
	// failed. A mirror that simply vanishes leaves the consumer that
	// was reading it nothing to read about why.
	ReasonMirrorCollected = "MirrorCollected"
)

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/finalizers,verbs=update
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlnodecapabilities,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile judges one mirror and acts on the judgement.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)

	var m mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &m); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if controllerutil.ContainsFinalizer(&m, legacyIntentFinalizer) {
		if err := r.stripLegacyFinalizer(ctx, &m); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !m.DeletionTimestamp.IsZero() {
		// The gateway finalizers own the teardown from here. Nothing
		// this controller decides applies to an object already on its
		// way out.
		return ctrl.Result{}, nil
	}

	claimed, err := r.claim(ctx, &m)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("evaluate claim: %w", err)
	}
	src, err := r.source(ctx, &m)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("evaluate source: %w", err)
	}

	// A mirror whose origin has landed on its own target node would be
	// a transfer from a node to itself: the consumer reads the local
	// copy directly. Removing it is safe while the local producer
	// holds the flow, because libmxl reclaims a flow directory only
	// when the departing writer can take an exclusive lock, which the
	// producer's own shared lock denies.
	if src.node != "" && src.node == m.Spec.TargetNode {
		return ctrl.Result{}, r.delete(ctx, &m, ReasonOriginLocal,
			"Deleting: the flow's origin moved onto %s, so this node reads it directly",
			m.Spec.TargetNode)
	}

	if src.node != "" && src.node != m.Spec.SourceNode {
		if err := r.repoint(ctx, &m, src.node); err != nil {
			return ctrl.Result{}, err
		}
	}

	written, err := r.writeConditions(ctx, &m, claimed, src)
	if err != nil {
		return ctrl.Result{}, err
	}

	l.V(1).Info("judged MxlFlowMirror",
		"flowID", m.Spec.FlowID, "claimed", claimed.ok, "sourceable", src.ok)
	return r.collect(ctx, written, claimed, src)
}

// judgement is one of the two answers that justify a mirror, in the
// shape the condition writer and the collector both need.
type judgement struct {
	ok      bool
	reason  string
	message string
}

// sourceJudgement adds the node the mirror should pull from, the
// moment the answer can change on its own, and whether the failure is
// one nothing will undo.
type sourceJudgement struct {
	judgement
	node     string
	deadline time.Time

	// terminal marks a source failure no agent can reverse: the flow
	// is gone, no node claims to hold it, or the target node has left
	// the cluster. Only those make a mirror collectable.
	//
	// A lease that has merely lapsed is deliberately not one. The
	// Origin location still names a node, and an agent that is down
	// while its producer and both gateways are fine is exactly the
	// case that produces it -- collecting there would tear down a
	// transfer that is still delivering grains, on the evidence of a
	// component that is not carrying them. The mirror stays, and the
	// condition says why. The flow collector makes the opposite trade
	// for the opposite reason: an MxlFlow is derived state its agent
	// republishes from flow_def.json within one rescan, so collecting
	// one early costs a rebuild rather than a stream.
	terminal bool
}

// claim reports whether anything still asks for this mirror.
//
// Two things can, and both are statements the requesting side wrote
// onto the object rather than properties read off it. An owner
// reference to a live MxlReceiver is the declarative path's claim; a
// spec.requestor naming a live pod is the on-demand path's. A mirror
// carrying neither is one nothing in the cluster wants.
//
// The creator labels are deliberately not consulted. They record which
// path created the mirror, which is a useful thing to know and a
// terrible thing to gate deletion on: a mirror whose labels had been
// edited off was invisible to both of the collectors this one
// replaces, and sat on a showcase cluster for a day and a half with
// two gateway finalizers and a requestor pod long gone.
func (r *Reconciler) claim(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror) (judgement, error) {
	for _, or := range m.OwnerReferences {
		if or.Kind != "MxlReceiver" || or.APIVersion != mxlv1alpha1.GroupVersion.String() {
			continue
		}
		var recv mxlv1alpha1.MxlReceiver
		err := r.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: or.Name}, &recv)
		switch {
		case apierrors.IsNotFound(err):
			continue
		case err != nil:
			return judgement{}, fmt.Errorf("get owning MxlReceiver %s/%s: %w",
				m.Namespace, or.Name, err)
		}
		if recv.UID != or.UID {
			// The name was reused by a different object; the owner
			// reference names the one that is gone.
			continue
		}
		return judgement{
			ok:      true,
			reason:  mxlv1alpha1.ReasonReceiverOwned,
			message: fmt.Sprintf("MxlReceiver %s/%s owns this mirror", m.Namespace, recv.Name),
		}, nil
	}

	if req := m.Spec.Requestor; req != nil {
		var pod corev1.Pod
		err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, &pod)
		switch {
		case err == nil:
			if req.UID == "" || string(pod.UID) == req.UID {
				return judgement{
					ok:     true,
					reason: mxlv1alpha1.ReasonRequestorLive,
					message: fmt.Sprintf("requestor pod %s/%s is present",
						req.Namespace, req.Name),
				}, nil
			}
		case !apierrors.IsNotFound(err):
			return judgement{}, fmt.Errorf("get requestor pod %s/%s: %w",
				req.Namespace, req.Name, err)
		}
	}

	return judgement{
		reason:  mxlv1alpha1.ReasonUnclaimed,
		message: "no live MxlReceiver owns this mirror and no live requestor pod asks for it",
	}, nil
}

// source reports whether the mirror has somewhere to pull from and
// somewhere to put it, and names the node it should pull from.
//
// The target node is checked first because its absence is the one
// failure no amount of waiting fixes: the gateway that would open the
// writer is a DaemonSet pod that died with the node.
func (r *Reconciler) source(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror) (sourceJudgement, error) {
	gone, err := r.nodeGone(ctx, m.Spec.TargetNode)
	if err != nil {
		return sourceJudgement{}, err
	}
	if gone {
		return sourceJudgement{terminal: true, judgement: judgement{
			reason:  mxlv1alpha1.ReasonTargetNodeGone,
			message: fmt.Sprintf("target node %s has left the cluster", m.Spec.TargetNode),
		}}, nil
	}

	var flow mxlv1alpha1.MxlFlow
	err = r.Get(ctx, types.NamespacedName{Name: m.Spec.FlowID}, &flow)
	if apierrors.IsNotFound(err) {
		return sourceJudgement{terminal: true, judgement: judgement{
			reason:  mxlv1alpha1.ReasonOriginUnresolved,
			message: fmt.Sprintf("MxlFlow %s does not exist", m.Spec.FlowID),
		}}, nil
	}
	if err != nil {
		return sourceJudgement{}, fmt.Errorf("get MxlFlow %s: %w", m.Spec.FlowID, err)
	}

	res, err := mxlv1alpha1.ResolveOrigin(&flow, r.leaseFreshness(ctx))
	if err != nil {
		return sourceJudgement{}, fmt.Errorf("resolve origin for %s: %w", m.Spec.FlowID, err)
	}
	if res.AllStale {
		// Somewhere a node still claims to hold this flow and no agent
		// is saying so. That is a control-plane failure rather than a
		// producer's, and the data plane may well be unaffected.
		return sourceJudgement{judgement: judgement{
			reason:  mxlv1alpha1.ReasonLeaseExpired,
			message: "every Origin location of the flow has an expired lease",
		}}, nil
	}
	if !res.Found {
		return sourceJudgement{terminal: true, judgement: judgement{
			reason:  mxlv1alpha1.ReasonOriginUnresolved,
			message: "the flow names no Origin location",
		}}, nil
	}
	return sourceJudgement{
		judgement: judgement{
			ok:      true,
			reason:  mxlv1alpha1.ReasonOriginResolved,
			message: fmt.Sprintf("origin is on %s", res.Node),
		},
		node:     res.Node,
		deadline: res.Deadline,
	}, nil
}

// leaseFreshness adapts the reconciler's LeaseChecker to the callback
// ResolveOrigin takes. A nil checker yields a nil callback, which is
// what makes ResolveOrigin trust every Origin location.
func (r *Reconciler) leaseFreshness(ctx context.Context) mxlv1alpha1.LeaseFreshness {
	if r.Lease == nil {
		return nil
	}
	return func(flowID, nodeName string) (bool, time.Time, error) {
		return r.Lease.IsFresh(ctx, flowID, nodeName)
	}
}

// repoint patches spec.sourceNode, and the provider derived from it,
// at the node the flow's origin has moved to.
//
// Mirror names do not encode the source node, so without this a mirror
// addresses its create-time node for life and stays Degraded once that
// node stops holding the flow. The source gateway on the stale node
// opens a reader on a local copy nothing writes to, and reopening it
// -- the only recovery the data plane has -- yields another reader on
// the same dead copy.
//
// The provider is re-resolved because the recorded one was resolved
// for a different pair of nodes: an origin that moves onto a node on
// another fabric leaves a mirror asking for a libmxl-fabrics provider
// one of its two ends cannot speak.
func (r *Reconciler) repoint(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, node string) error {
	provider, err := r.resolveProvider(ctx, m, node)
	if err != nil {
		return fmt.Errorf("resolve provider for repointed source %s: %w", node, err)
	}
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"sourceNode": node,
			"provider":   string(provider),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal repoint patch: %w", err)
	}
	previous := m.Spec.SourceNode
	if err := r.Patch(ctx, m, client.RawPatch(types.MergePatchType, patch)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("repoint mirror %s/%s at source %s: %w",
			m.Namespace, m.Name, node, err)
	}

	// Recorded on the mirror itself, not only in the log line below. A
	// mirror that is Degraded right now is read through kubectl rather
	// than through a log a day later, and without the timestamp
	// "Degraded and converging" and "Degraded and stuck" look alike on
	// the object.
	if err := r.recordRetarget(ctx, m, previous); err != nil {
		log.FromContext(ctx).Error(err, "record retarget on mirror",
			"mxlflowmirror", client.ObjectKeyFromObject(m))
	}
	r.event(m, corev1.EventTypeNormal, ReasonSourceRetargeted,
		"Source repointed from %s to %s after the flow's origin moved", previous, node)
	log.FromContext(ctx).Info("repointed mirror at new origin",
		"flowID", m.Spec.FlowID,
		"mxlflowmirror", client.ObjectKeyFromObject(m),
		"previousSourceNode", previous,
		"sourceNode", node,
		"provider", provider)
	return nil
}

// recordRetarget stamps status.sourceRetargetedAt and
// previousSourceNode. Status is a separate subresource from the spec
// patch, so this is a second write; the spec change is the one that
// matters and its failure is what the caller returns, while losing the
// record only costs the timestamp.
func (r *Reconciler) recordRetarget(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, previous string) error {
	if previous == "" {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var live mxlv1alpha1.MxlFlowMirror
		if err := r.Get(ctx, client.ObjectKeyFromObject(m), &live); err != nil {
			return client.IgnoreNotFound(err)
		}
		now := metav1.Now()
		live.Status.PreviousSourceNode = previous
		live.Status.SourceRetargetedAt = &now
		return r.Status().Update(ctx, &live)
	})
}

// resolveProvider picks the libmxl-fabrics provider for a mirror
// between sourceNode and the mirror's target node, from the two nodes'
// MxlNodeCapabilities. The result is always concrete: the operator
// never writes a mirror with provider auto, which libmxl-fabrics has
// not resolved on its own since v1.1.0-beta-1.
func (r *Reconciler) resolveProvider(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, sourceNode string) (mxlv1alpha1.MxlFabricsProvider, error) {
	srcCaps, err := r.nodeCapabilities(ctx, sourceNode)
	if err != nil {
		return "", fmt.Errorf("source node capabilities: %w", err)
	}
	tgtCaps, err := r.nodeCapabilities(ctx, m.Spec.TargetNode)
	if err != nil {
		return "", fmt.Errorf("target node capabilities: %w", err)
	}
	provider, rerr := selection.Resolve(srcCaps, tgtCaps)
	if rerr != nil {
		log.FromContext(ctx).Info("resolved mirror provider with fallback",
			"flowID", m.Spec.FlowID, "sourceNode", sourceNode,
			"targetNode", m.Spec.TargetNode, "provider", provider,
			"reason", rerr.Error())
	}
	return provider, nil
}

// nodeCapabilities reads the cluster-scoped MxlNodeCapabilities the
// gateway publishes for nodeName. A missing resource yields an empty
// status so the resolver falls back rather than failing the reconcile
// on a node whose gateway has not probed yet.
func (r *Reconciler) nodeCapabilities(ctx context.Context, nodeName string) (mxlv1alpha1.MxlNodeCapabilitiesStatus, error) {
	var caps mxlv1alpha1.MxlNodeCapabilities
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &caps); err != nil {
		if apierrors.IsNotFound(err) {
			return mxlv1alpha1.MxlNodeCapabilitiesStatus{}, nil
		}
		return mxlv1alpha1.MxlNodeCapabilitiesStatus{}, err
	}
	return caps.Status, nil
}

// nodeGone reports whether the named Node is absent from the API. An
// empty name reports false: a mirror with no node recorded is
// malformed rather than orphaned, and deleting it on that basis would
// act on a field that was never written.
func (r *Reconciler) nodeGone(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, nil
	}
	var node corev1.Node
	err := r.Get(ctx, types.NamespacedName{Name: name}, &node)
	switch {
	case err == nil:
		return false, nil
	case apierrors.IsNotFound(err):
		return true, nil
	default:
		return false, fmt.Errorf("look up node %s: %w", name, err)
	}
}

// writeConditions publishes Claimed and Sourceable with server-side
// apply under the operator's own field manager, and returns the mirror
// carrying them.
//
// SSA rather than an Update because two gateway reconcilers write
// their own conditions to the same list; each manager applies only its
// own entries and the apiserver merges them by type. The transition
// timestamps are computed here rather than stamped fresh on every
// pass, because they are what the collector measures the grace period
// from -- restamping would reset the clock on every reconcile and no
// mirror would ever be collected.
func (r *Reconciler) writeConditions(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, claimed judgement, src sourceJudgement) (*mxlv1alpha1.MxlFlowMirror, error) {
	conditions := append([]metav1.Condition(nil), m.Status.Conditions...)
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               mxlv1alpha1.ConditionTypeClaimed,
		Status:             conditionStatus(claimed.ok),
		Reason:             claimed.reason,
		Message:            claimed.message,
		ObservedGeneration: m.Generation,
	})
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               mxlv1alpha1.ConditionTypeSourceable,
		Status:             conditionStatus(src.ok),
		Reason:             src.reason,
		Message:            src.message,
		ObservedGeneration: m.Generation,
	})

	ours := make([]any, 0, 2)
	for _, t := range []string{mxlv1alpha1.ConditionTypeClaimed, mxlv1alpha1.ConditionTypeSourceable} {
		c := meta.FindStatusCondition(conditions, t)
		ours = append(ours, map[string]any{
			"type":               c.Type,
			"status":             string(c.Status),
			"reason":             c.Reason,
			"message":            c.Message,
			"observedGeneration": int64(c.ObservedGeneration),
			"lastTransitionTime": c.LastTransitionTime.UTC().Format(time.RFC3339),
		})
	}

	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirror"))
	patch.SetNamespace(m.Namespace)
	patch.SetName(m.Name)
	if err := unstructured.SetNestedField(patch.Object,
		map[string]any{"conditions": ours}, "status"); err != nil {
		return nil, fmt.Errorf("build SSA payload: %w", err)
	}
	if err := r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("apply mirror conditions: %w", err)
	}

	out := m.DeepCopy()
	out.Status.Conditions = conditions
	return out, nil
}

// collect deletes a mirror that has been unjustified for longer than
// the grace period.
//
// The clock is the earlier of the two failing conditions' transition
// times, so a mirror that loses its claim and then its source is
// collected on the first of the two rather than restarting its wait.
// The phase is deliberately not consulted: Degraded says grains are
// not moving, which is a data-plane fault the gateway may still
// recover from, and a mirror that is genuinely wanted and genuinely
// sourceable has to be left alone however long it has been failing.
func (r *Reconciler) collect(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, claimed judgement, src sourceJudgement) (ctrl.Result, error) {
	if m == nil {
		return ctrl.Result{}, nil
	}
	if claimed.ok && (src.ok || !src.terminal) {
		// Only a Lease lapsing can change the answer with no event to
		// carry it. Pods, receivers, nodes, flows and mirrors all
		// arrive on a watch.
		return requeueAt(src.deadline), nil
	}

	grace := r.GracePeriod
	if grace <= 0 {
		grace = DefaultGracePeriod
	}
	since := failingSince(m, claimed.ok, src.ok || !src.terminal)
	if wait := grace - time.Since(since); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	why := src.message
	if !claimed.ok {
		why = claimed.message
	}
	log.FromContext(ctx).Info("collected MxlFlowMirror nothing justifies",
		"flowID", m.Spec.FlowID,
		"mxlflowmirror", client.ObjectKeyFromObject(m),
		"claimed", claimed.ok, "sourceable", src.ok,
		"unjustifiedFor", time.Since(since).Truncate(time.Second))
	return ctrl.Result{}, r.delete(ctx, m, ReasonMirrorCollected, "Collected: %s", why)
}

// failingSince is the earliest transition time among the conditions
// that currently read False. A condition this operator has not written
// yet counts as having just turned, so an upgrade gives every mirror a
// full grace period rather than collecting the collectable ones on
// sight.
func failingSince(m *mxlv1alpha1.MxlFlowMirror, claimed, sourceable bool) time.Time {
	var since time.Time
	consider := func(t string) {
		c := meta.FindStatusCondition(m.Status.Conditions, t)
		if c == nil || c.LastTransitionTime.IsZero() {
			return
		}
		if since.IsZero() || c.LastTransitionTime.Time.Before(since) {
			since = c.LastTransitionTime.Time
		}
	}
	if !claimed {
		consider(mxlv1alpha1.ConditionTypeClaimed)
	}
	if !sourceable {
		consider(mxlv1alpha1.ConditionTypeSourceable)
	}
	if since.IsZero() {
		return time.Now()
	}
	return since
}

// delete removes a mirror, recording why against the object so the
// event outlives it for the event TTL.
func (r *Reconciler) delete(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, reason, format string, args ...any) error {
	if !m.DeletionTimestamp.IsZero() {
		return nil
	}
	r.event(m, corev1.EventTypeNormal, reason, format, args...)
	if err := r.Delete(ctx, m); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete mirror %s/%s: %w", m.Namespace, m.Name, err)
	}
	return nil
}

// stripLegacyFinalizer removes the no-op finalizer an earlier
// collector added. Left in place it blocks deletion until something
// removes it, and nothing in this build ever would.
func (r *Reconciler) stripLegacyFinalizer(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror) error {
	controllerutil.RemoveFinalizer(m, legacyIntentFinalizer)
	if err := r.Update(ctx, m); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("remove legacy intent finalizer: %w", err)
	}
	return nil
}

func (r *Reconciler) event(obj client.Object, kind, reason, format string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, kind, reason, format, args...)
}

func conditionStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// requeueAt schedules the next pass for the moment the answer can
// change on its own. A deadline already past means look again now.
func requeueAt(deadline time.Time) ctrl.Result {
	if deadline.IsZero() {
		return ctrl.Result{}
	}
	if wait := time.Until(deadline); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}
	}
	return ctrl.Result{Requeue: true}
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
//
// Every input to the two judgements arrives on a watch: the pod and
// the receiver that can claim a mirror, the flow and the Lease that
// make it sourceable, and the node that has to exist for either end to
// work. The only thing that changes with no event behind it is a Lease
// passing its window, which the requeue in collect covers.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.podToMirrors),
			builder.WithPredicates(podLifecyclePredicate()),
		).
		Watches(
			&mxlv1alpha1.MxlReceiver{},
			handler.EnqueueRequestsFromMapFunc(r.receiverToMirrors),
		).
		Watches(
			&mxlv1alpha1.MxlFlow{},
			handler.EnqueueRequestsFromMapFunc(r.flowToMirrors),
		).
		Watches(
			&coordinationv1.Lease{},
			handler.EnqueueRequestsFromMapFunc(r.leaseToMirrors),
			builder.WithPredicates(leaseInMxlSystem()),
		).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.nodeToMirrors),
			builder.WithPredicates(nodeDeletedOnly()),
		).
		Named("mxlflowmirror").
		Complete(r)
}

// mirrorsMatching enqueues every mirror the predicate accepts. The
// cluster-wide list is served from the cache the For() watch already
// maintains, so it costs no API traffic; the operator is a single
// Deployment, not a per-node DaemonSet, so that cache exists once.
func (r *Reconciler) mirrorsMatching(ctx context.Context, keep func(*mxlv1alpha1.MxlFlowMirror) bool) []reconcile.Request {
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors); err != nil {
		log.FromContext(ctx).Error(err, "list MxlFlowMirrors for a watch event")
		return nil
	}
	var out []reconcile.Request
	for i := range mirrors.Items {
		if keep(&mirrors.Items[i]) {
			out = append(out, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&mirrors.Items[i]),
			})
		}
	}
	return out
}

func (r *Reconciler) podToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	return r.mirrorsMatching(ctx, func(m *mxlv1alpha1.MxlFlowMirror) bool {
		req := m.Spec.Requestor
		return req != nil && req.Namespace == pod.Namespace && req.Name == pod.Name
	})
}

func (r *Reconciler) receiverToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	recv, ok := obj.(*mxlv1alpha1.MxlReceiver)
	if !ok {
		return nil
	}
	return r.mirrorsMatching(ctx, func(m *mxlv1alpha1.MxlFlowMirror) bool {
		if m.Namespace != recv.Namespace {
			return false
		}
		for _, or := range m.OwnerReferences {
			if or.UID == recv.UID {
				return true
			}
		}
		return false
	})
}

func (r *Reconciler) flowToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	flow, ok := obj.(*mxlv1alpha1.MxlFlow)
	if !ok {
		return nil
	}
	return r.mirrorsMatching(ctx, func(m *mxlv1alpha1.MxlFlowMirror) bool {
		return m.Spec.FlowID == flow.Spec.ID
	})
}

func (r *Reconciler) leaseToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	flowID, _, ok := mxlv1alpha1.ParseLeaseName(obj.GetName())
	if !ok {
		return nil
	}
	return r.mirrorsMatching(ctx, func(m *mxlv1alpha1.MxlFlowMirror) bool {
		return m.Spec.FlowID == flowID
	})
}

func (r *Reconciler) nodeToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetName()
	return r.mirrorsMatching(ctx, func(m *mxlv1alpha1.MxlFlowMirror) bool {
		return m.Spec.TargetNode == name || m.Spec.SourceNode == name
	})
}

// leaseInMxlSystem keeps the Lease watch confined to the namespace the
// agent publishes Origin Leases in. Other Leases (kube-system leader
// election, kube-node-lease) would otherwise wake every mirror of a
// flow on every renew tick in the cluster.
func leaseInMxlSystem() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == mxlv1alpha1.LeaseNamespace
	})
}

// nodeDeletedOnly limits the Node watch to deletions. Node objects are
// among the busiest in a cluster and only a departure changes a
// mirror's judgement.
func nodeDeletedOnly() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
	}
}

// systemNamespacesDenied is the set of cluster-control namespaces
// whose Pod churn this controller must ignore. Static control-plane
// pods, kube-proxy, CoreDNS and similar workloads churn frequently
// enough that letting their events through would dominate the
// reconcile queue with wakeups for namespaces no mirror is ever
// requested from. mxl-system stays accepted so a workload the operator
// co-locates with the agent can still hold a claim.
var systemNamespacesDenied = map[string]struct{}{
	"kube-system":     {},
	"kube-public":     {},
	"kube-node-lease": {},
}

func isSystemNamespace(ns string) bool {
	_, deny := systemNamespacesDenied[ns]
	return deny
}

// podLifecyclePredicate keeps the pod watch off noisy status ticks.
// The claim only turns on a pod appearing, disappearing, or being
// replaced under the same name, and the API server treats a
// replacement as a delete plus a create.
func podLifecyclePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return !isSystemNamespace(e.Object.GetNamespace())
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return !isSystemNamespace(e.Object.GetNamespace())
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if isSystemNamespace(e.ObjectNew.GetNamespace()) {
				return false
			}
			return e.ObjectOld.GetUID() != e.ObjectNew.GetUID()
		},
	}
}
