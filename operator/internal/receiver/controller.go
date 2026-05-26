// Package receiver hosts the operator's MxlReceiver reconciler.
//
// The reconciler resolves the consumer-side pod(s) of each receiver,
// looks up the flow's origin node from MxlFlow.status.locations, and
// ensures one MxlFlowMirror per distinct target node so every node
// hosting a matching pod gets a local copy of the flow.
package receiver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	utilptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// MxlReceiverFinalizer is added to every MxlReceiver the reconciler
// observes. It blocks deletion until the labeled mirrors created on
// the receiver's behalf have been removed, so the gateway never sees
// an orphan mirror with no owning receiver.
const MxlReceiverFinalizer = "mxl.qvest-digital.com/receiver"

// flowIDIndex is the name of the field index registered against
// MxlReceiver on spec.flowID. Reused as both the IndexField key and
// the client.MatchingFields lookup key so a typo in either place
// fails at SetupWithManager rather than silently returning an empty
// list at runtime.
const flowIDIndex = "spec.flowID"

// ownerUIDIndex is the name of the field index registered against
// MxlFlowMirror over every UID in metadata.ownerReferences. The
// string is an arbitrary opaque key -- controller-runtime does NOT
// parse it as JSONPath; the registered extractor is what actually
// produces the index entries. Naming it after the field it covers
// keeps SetupWithManager and the client.MatchingFields call sites
// readable.
const ownerUIDIndex = "ownerReferences.uid"

// Reconciler reconciles MxlReceiver resources.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Lease, when set, gates Origin location picks on a fresh
	// per-(flow, node) Lease. Nil falls back to picking the first
	// Origin without any liveness check -- the pre-Lease behaviour
	// the unit tests built around.
	Lease LeaseChecker
}

// MxlOperatorFieldManager is the field-manager name the receiver
// uses for SSA writes to MxlFlow.Status conditions it owns. Stable
// so any later refactor can identify the entries left in
// managedFields by this controller.
const MxlOperatorFieldManager = "mxl-operator"

// nodeTarget is one (node, namespace) pair derived from a receiver's
// pod selection. Mirrors are created in the pod's namespace, which
// for in-namespace PodSelector matches equals the receiver's
// namespace but for cross-namespace PodRef can differ.
type nodeTarget struct {
	node      string
	namespace string
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers/finalizers,verbs=update
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch

// Reconcile drives one MxlReceiver through its lifecycle: translate
// pod-side intent into one MxlFlowMirror per distinct target node,
// then garbage-collect any labeled mirrors that no longer belong to
// the current desired set.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlreceiver", req.NamespacedName)

	var recv mxlv1alpha1.MxlReceiver
	if err := r.Get(ctx, req.NamespacedName, &recv); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !recv.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &recv)
	}

	if !controllerutil.ContainsFinalizer(&recv, MxlReceiverFinalizer) {
		controllerutil.AddFinalizer(&recv, MxlReceiverFinalizer)
		if err := r.Update(ctx, &recv); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
	}

	targets, err := r.resolveTargets(ctx, &recv)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve targets: %w", err)
	}

	res, err := r.resolveSourceNode(ctx, recv.Spec.FlowID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve source node: %w", err)
	}
	if err := r.applyOriginFreshCondition(ctx, recv.Spec.FlowID, res); err != nil {
		l.Error(err, "apply OriginFresh condition", "flowID", recv.Spec.FlowID)
	}

	// The desired set is the (node, namespace) pairs whose target
	// differs from the source. Same-node consumers read the local
	// flow directly without a mirror. The name derivation must use
	// mirrorNameForReceiver so cross-namespace mirrors carry the
	// per-receiver suffix here; otherwise gcOrphanMirrors would see
	// every cross-ns mirror as unwanted (key mismatch against the
	// suffixed name ensureMirror produces) and Delete-loop it.
	desired := map[mirrorKey]nodeTarget{}
	if res.Found {
		for _, t := range targets {
			if t.node == res.Node {
				continue
			}
			desired[mirrorKey{namespace: t.namespace, name: mirrorNameForReceiver(&recv, t)}] = t
		}
	}

	if err := r.gcOrphanMirrors(ctx, &recv, desired); err != nil {
		return ctrl.Result{}, fmt.Errorf("gc orphan mirrors: %w", err)
	}

	if len(targets) == 0 {
		return r.markPending(ctx, &recv, "no target pods scheduled yet")
	}
	if !res.Found {
		reason := "MxlFlow not yet known or no Origin location"
		if res.AllStale {
			reason = "all Origin locations have an expired Lease"
		}
		return r.markPending(ctx, &recv, reason)
	}

	var primary *mxlv1alpha1.MirrorRef
	for _, t := range targets {
		if t.node == res.Node {
			continue
		}
		mirror, err := r.ensureMirror(ctx, &recv, res.Node, t)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure mirror for %s in %s: %w", t.node, t.namespace, err)
		}
		if primary == nil {
			primary = &mxlv1alpha1.MirrorRef{
				Name:      mirror.Name,
				Namespace: mirror.Namespace,
			}
		}
	}

	l.V(1).Info("reconciled",
		"flowID", recv.Spec.FlowID,
		"sourceNode", res.Node,
		"desired", len(desired))
	result, err := r.markBound(ctx, &recv, primary)
	if err != nil {
		return result, err
	}
	// Schedule a Reconcile just past the Lease deadline so an
	// agent that stops renewing trips OriginFresh=False even though
	// k8s emits no event for a Lease passing its window. Graceful
	// agent shutdown deletes the Lease and the Lease watch covers
	// that fast path; this requeue is the safety net for an
	// ungraceful exit (OOMKill, node loss) where no Lease event
	// ever fires.
	if !res.Deadline.IsZero() {
		wake := time.Until(res.Deadline) + time.Second
		if wake > 0 && (result.RequeueAfter == 0 || wake < result.RequeueAfter) {
			result.RequeueAfter = wake
		}
	}
	return result, nil
}

// handleDeletion releases this receiver's claim on each mirror it
// owns, then strips the finalizer so the API server can complete the
// delete. Same-namespace mirrors are shared between co-resident
// receivers: the receiver drops its OwnerReference and lets the
// apiserver-driven garbage collector remove the mirror once the
// owner list is empty. Cross-namespace mirrors have a per-receiver
// name suffix and no sibling, so this path Deletes them directly.
// Idempotent against partial progress.
func (r *Reconciler) handleDeletion(ctx context.Context, recv *mxlv1alpha1.MxlReceiver) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(recv, MxlReceiverFinalizer) {
		return ctrl.Result{}, nil
	}

	sameNs, err := r.listOwnedSameNsMirrors(ctx, recv)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list same-ns owned mirrors: %w", err)
	}
	for i := range sameNs {
		if err := r.removeOwnerRef(ctx, recv, &sameNs[i]); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove owner ref from %s/%s: %w",
				sameNs[i].Namespace, sameNs[i].Name, err)
		}
	}

	crossNs, err := r.listOwnedCrossNsMirrors(ctx, recv)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list cross-ns owned mirrors: %w", err)
	}
	for i := range crossNs {
		if !crossNs[i].DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, &crossNs[i]); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete cross-ns mirror %s/%s: %w",
				crossNs[i].Namespace, crossNs[i].Name, err)
		}
	}

	// Cross-namespace mirrors can carry a finalizer from the
	// gateway: requeue until the apiserver actually removes the
	// object so the receiver's UID does not get reused under us.
	// Same-namespace mirrors do not block the receiver: dropping
	// the owner ref is the receiver's only obligation; apiserver
	// GC happens out-of-band once the last owner ref is gone.
	remaining, err := r.listOwnedCrossNsMirrors(ctx, recv)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("re-list cross-ns owned mirrors: %w", err)
	}
	if len(remaining) > 0 {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	controllerutil.RemoveFinalizer(recv, MxlReceiverFinalizer)
	if err := r.Update(ctx, recv); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// listOwnedSameNsMirrors returns every MxlFlowMirror in the
// receiver's own namespace that carries the receiver's UID in
// metadata.ownerReferences. The lookup goes through the
// ownerUIDIndex field index when the client has it registered (the
// production controller-runtime cache does); otherwise it falls
// back to a namespace List and a client-side filter on
// OwnerReferences. The fallback covers two cases: an envtest
// direct client that bypasses the cache, and any future cache
// regression where the field-index extractor reports unexpected
// keys -- the plan's mid-flight surprise rule says reach for the
// client-side filter rather than re-introducing label-based GC.
func (r *Reconciler) listOwnedSameNsMirrors(ctx context.Context, recv *mxlv1alpha1.MxlReceiver) ([]mxlv1alpha1.MxlFlowMirror, error) {
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	err := r.List(ctx, &mirrors,
		client.InNamespace(recv.Namespace),
		client.MatchingFields{ownerUIDIndex: string(recv.UID)},
	)
	if err == nil {
		return mirrors.Items, nil
	}
	if !isFieldIndexUnsupported(err) {
		return nil, err
	}
	if err := r.List(ctx, &mirrors, client.InNamespace(recv.Namespace)); err != nil {
		return nil, err
	}
	out := mirrors.Items[:0]
	for i := range mirrors.Items {
		for _, or := range mirrors.Items[i].OwnerReferences {
			if or.UID == recv.UID {
				out = append(out, mirrors.Items[i])
				break
			}
		}
	}
	return out, nil
}

// isFieldIndexUnsupported recognises the error a non-cached client
// returns when asked to honour a MatchingFields selector. The
// apiserver does not implement arbitrary field selectors over CRDs;
// the controller-runtime client surface returns its
// "field label not supported" error verbatim. Matching by message
// keeps the check independent of any wrapping in callers.
func isFieldIndexUnsupported(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "field label not supported")
}

// listOwnedCrossNsMirrors returns every MxlFlowMirror in a namespace
// other than the receiver's own that carries the receiver's name in
// LabelCreatedByReceiver. The label survives as the cross-namespace
// owner-lookup mechanism because controller-runtime's field index
// extractor cannot return entries reaching across the receiver's
// namespace boundary: same-namespace owner-ref lookup is local and
// uses the index, cross-namespace owner-ref lookup falls back to the
// cluster-wide label match.
func (r *Reconciler) listOwnedCrossNsMirrors(ctx context.Context, recv *mxlv1alpha1.MxlReceiver) ([]mxlv1alpha1.MxlFlowMirror, error) {
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors, client.MatchingLabels{
		mxlv1alpha1.LabelCreatedByReceiver: recv.Name,
	}); err != nil {
		return nil, err
	}
	out := mirrors.Items[:0]
	for i := range mirrors.Items {
		if mirrors.Items[i].Namespace == recv.Namespace {
			continue
		}
		out = append(out, mirrors.Items[i])
	}
	return out, nil
}

// gcOrphanMirrors releases this receiver's ownership of any mirror
// not in the desired set. Called on every successful reconcile so a
// pod move, a pod deletion, or a flow origin rotation all converge
// to one mirror per live target node. Same-namespace mirrors lose
// only this receiver's OwnerReference; apiserver GC handles the
// eventual deletion once the owner list is empty. Cross-namespace
// mirrors are unique per-receiver and get Deleted directly.
func (r *Reconciler) gcOrphanMirrors(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, desired map[mirrorKey]nodeTarget) error {
	sameNs, err := r.listOwnedSameNsMirrors(ctx, recv)
	if err != nil {
		return err
	}
	for i := range sameNs {
		m := &sameNs[i]
		if !m.DeletionTimestamp.IsZero() {
			continue
		}
		key := mirrorKey{namespace: m.Namespace, name: m.Name}
		if _, keep := desired[key]; keep {
			continue
		}
		if err := r.removeOwnerRef(ctx, recv, m); err != nil {
			return fmt.Errorf("remove owner ref from orphan mirror %s/%s: %w", m.Namespace, m.Name, err)
		}
	}

	crossNs, err := r.listOwnedCrossNsMirrors(ctx, recv)
	if err != nil {
		return err
	}
	for i := range crossNs {
		m := &crossNs[i]
		if !m.DeletionTimestamp.IsZero() {
			continue
		}
		key := mirrorKey{namespace: m.Namespace, name: m.Name}
		if _, keep := desired[key]; keep {
			continue
		}
		if err := r.Delete(ctx, m); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete orphan cross-ns mirror %s/%s: %w", m.Namespace, m.Name, err)
		}
	}
	return nil
}

// mirrorKey is the namespace+name pair that uniquely identifies a
// mirror in cluster scope. Used to compare the labeled-mirror set
// against the desired set.
type mirrorKey struct {
	namespace string
	name      string
}

// resolveTargets returns the set of distinct (node, namespace)
// pairs hosting the receiver's consumer pods, in deterministic
// order.
func (r *Reconciler) resolveTargets(ctx context.Context, recv *mxlv1alpha1.MxlReceiver) ([]nodeTarget, error) {
	if recv.Spec.PodRef != nil {
		ns := recv.Spec.PodRef.Namespace
		if ns == "" {
			ns = recv.Namespace
		}
		var pod corev1.Pod
		err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: recv.Spec.PodRef.Name}, &pod)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if pod.Spec.NodeName == "" {
			return nil, nil
		}
		return []nodeTarget{{node: pod.Spec.NodeName, namespace: pod.Namespace}}, nil
	}

	if recv.Spec.PodSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(recv.Spec.PodSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid podSelector: %w", err)
		}
		var pods corev1.PodList
		if err := r.List(ctx, &pods,
			client.InNamespace(recv.Namespace),
			client.MatchingLabelsSelector{Selector: sel},
		); err != nil {
			return nil, err
		}
		seen := make(map[string]struct{}, len(pods.Items))
		var out []nodeTarget
		for i := range pods.Items {
			n := pods.Items[i].Spec.NodeName
			if n == "" {
				continue
			}
			if _, dup := seen[n]; dup {
				continue
			}
			seen[n] = struct{}{}
			out = append(out, nodeTarget{node: n, namespace: pods.Items[i].Namespace})
		}
		return out, nil
	}

	return nil, nil
}

// resolveTargetNodes is retained for the unit tests that exercise
// the pod-selection contract without caring about the target
// namespace. It returns just the node names from resolveTargets in
// the same order.
func (r *Reconciler) resolveTargetNodes(ctx context.Context, recv *mxlv1alpha1.MxlReceiver) ([]string, error) {
	ts, err := r.resolveTargets(ctx, recv)
	if err != nil {
		return nil, err
	}
	if len(ts) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.node)
	}
	return out, nil
}

// ensureMirror creates the MxlFlowMirror for (flow, target) if it
// does not already exist, or merge-patches spec.sourceNode and
// spec.provider on the existing mirror when they no longer match
// the desired values. Stamps LabelCreatedByReceiver=<recv.Name> on
// Create as a diagnostic tag and as the cluster-wide index key for
// cross-namespace owner lookup. Same-namespace mirrors carry the
// receiver in metadata.ownerReferences (multiple non-controller
// owners) so apiserver GC removes them once the last receiver
// disappears; cross-namespace mirrors get a per-receiver name
// suffix and stay unique to the receiver.
func (r *Reconciler) ensureMirror(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, sourceNode string, target nodeTarget) (*mxlv1alpha1.MxlFlowMirror, error) {
	name := mirrorNameForReceiver(recv, target)
	provider := recv.Spec.Provider
	if provider == "" {
		provider = mxlv1alpha1.ProviderAuto
	}

	sameNs := target.namespace == recv.Namespace

	var existing mxlv1alpha1.MxlFlowMirror
	err := r.Get(ctx, types.NamespacedName{Namespace: target.namespace, Name: name}, &existing)
	if err == nil {
		if sameNs {
			if err := r.ensureOwnerRef(ctx, recv, &existing); err != nil {
				return nil, fmt.Errorf("ensure owner ref on %s/%s: %w",
					existing.Namespace, existing.Name, err)
			}
		}
		return r.patchMirrorIfDrifted(ctx, recv, &existing, sourceNode, provider)
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	desired := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: target.namespace,
			Name:      name,
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByReceiver: recv.Name,
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     recv.Spec.FlowID,
			SourceNode: sourceNode,
			TargetNode: target.node,
			Provider:   provider,
		},
	}
	if sameNs {
		desired.OwnerReferences = []metav1.OwnerReference{ownerRefFor(recv)}
	}
	if err := r.Create(ctx, desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err := r.Get(ctx, types.NamespacedName{Namespace: target.namespace, Name: name}, &existing); err != nil {
				return nil, err
			}
			if sameNs {
				if err := r.ensureOwnerRef(ctx, recv, &existing); err != nil {
					return nil, fmt.Errorf("ensure owner ref on %s/%s: %w",
						existing.Namespace, existing.Name, err)
				}
			}
			return r.patchMirrorIfDrifted(ctx, recv, &existing, sourceNode, provider)
		}
		return nil, err
	}
	return desired, nil
}

// patchMirrorIfDrifted sends a merge-patch updating spec.sourceNode
// and spec.provider when they have drifted from the desired values.
// A merge-patch (not Update) is used so the agent-owned Requestor
// field on intent mirrors is not clobbered if the two ownership
// domains ever target the same mirror by accident; merge-patch
// touches only the keys the patch document lists. Label drift is
// not handled here: the receiver label is stamped once on Create
// and never rewritten on Patch, so two co-resident receivers do not
// pingpong it on every reconcile.
func (r *Reconciler) patchMirrorIfDrifted(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, mirror *mxlv1alpha1.MxlFlowMirror, sourceNode string, provider mxlv1alpha1.MxlFabricsProvider) (*mxlv1alpha1.MxlFlowMirror, error) {
	_ = recv
	if mirror.Spec.SourceNode == sourceNode && mirror.Spec.Provider == provider {
		return mirror, nil
	}

	patch := map[string]any{
		"spec": map[string]any{
			"sourceNode": sourceNode,
			"provider":   string(provider),
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("marshal merge patch: %w", err)
	}

	if err := r.Patch(ctx, mirror, client.RawPatch(types.MergePatchType, raw)); err != nil {
		return nil, fmt.Errorf("patch mirror %s/%s: %w", mirror.Namespace, mirror.Name, err)
	}
	return mirror, nil
}

// mirrorName produces a deterministic, DNS-subdomain-safe name from
// (flowID, targetNode). FlowIDs are UUIDs; node names are
// DNS-compliant; the result is lowercased and any non
// [a-z0-9-] runes are replaced with '-'.
func mirrorName(flowID, targetNode string) string {
	joined := strings.ToLower(flowID + "--" + targetNode)
	var b strings.Builder
	b.Grow(len(joined))
	for _, c := range joined {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b.WriteRune(c)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// mirrorNameForReceiver returns the mirror name the given receiver
// should use for a target. Same-namespace receivers share one
// mirror per (flowID, targetNode); cross-namespace receivers carry
// a per-(receiver namespace, receiver name) suffix so the cluster
// can hold multiple PodRef mirrors for the same target without
// fighting for one DNS name. The discriminator is the resolved
// target.namespace, not recv.Spec.PodRef.Namespace -- a PodRef whose
// Namespace defaults to recv.Namespace is same-namespace and must
// not gain a suffix.
func mirrorNameForReceiver(recv *mxlv1alpha1.MxlReceiver, target nodeTarget) string {
	base := mirrorName(recv.Spec.FlowID, target.node)
	if target.namespace == recv.Namespace {
		return base
	}
	return base + "-" + shortHash(recv.Namespace+"/"+recv.Name)
}

// shortHash returns the lowercase hex of the first 4 bytes of
// sha256(s). 8 characters is DNS-safe and the birthday-collision
// budget covers ~65k entries per (flow, target node) -- well past
// any plausible per-tuple receiver count.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:4])
}

// ownerRefFor builds a non-controller, non-blocking OwnerReference
// pointing at the receiver. Mirrors carry one of these per
// co-resident receiver; apiserver GC removes the mirror when the
// owner-reference list empties. Controller=false because multiple
// receivers co-own the mirror; BlockOwnerDeletion=false because
// receiver deletion must not wait on the mirror finalising.
func ownerRefFor(recv *mxlv1alpha1.MxlReceiver) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         mxlv1alpha1.GroupVersion.String(),
		Kind:               "MxlReceiver",
		Name:               recv.Name,
		UID:                recv.UID,
		Controller:         utilptr.To(false),
		BlockOwnerDeletion: utilptr.To(false),
	}
}

// ensureOwnerRef appends an OwnerReference pointing at recv onto
// mirror if one is not already present. Idempotent. Must be called
// only when recv and mirror share a namespace -- the apiserver
// rejects cross-namespace OwnerReferences. Uses Get+Update inside
// retry.RetryOnConflict instead of SSA so all receivers write
// through one field manager (no managedFields fragmentation) and
// the ownerReferences slice mutation is atomic per attempt; a JSON
// merge-patch would replace the array wholesale per RFC 7396 and
// silently strip sibling owners.
func (r *Reconciler) ensureOwnerRef(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, mirror *mxlv1alpha1.MxlFlowMirror) error {
	key := types.NamespacedName{Namespace: mirror.Namespace, Name: mirror.Name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var live mxlv1alpha1.MxlFlowMirror
		if err := r.Get(ctx, key, &live); err != nil {
			return err
		}
		for _, or := range live.OwnerReferences {
			if or.UID == recv.UID {
				return nil
			}
		}
		live.OwnerReferences = append(live.OwnerReferences, ownerRefFor(recv))
		return r.Update(ctx, &live)
	})
}

// removeOwnerRef removes the OwnerReference whose UID matches recv
// from mirror. No-op when absent. Same retry shape as ensureOwnerRef
// so a stale resourceVersion under contention does not surface as a
// reconcile error. Logs the resulting owner count at V(1) so a
// running operator can observe refcount activity.
func (r *Reconciler) removeOwnerRef(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, mirror *mxlv1alpha1.MxlFlowMirror) error {
	key := types.NamespacedName{Namespace: mirror.Namespace, Name: mirror.Name}
	var remaining int
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var live mxlv1alpha1.MxlFlowMirror
		if err := r.Get(ctx, key, &live); err != nil {
			if apierrors.IsNotFound(err) {
				remaining = 0
				return nil
			}
			return err
		}
		kept := live.OwnerReferences[:0]
		removed := false
		for _, or := range live.OwnerReferences {
			if or.UID == recv.UID {
				removed = true
				continue
			}
			kept = append(kept, or)
		}
		if !removed {
			remaining = len(live.OwnerReferences)
			return nil
		}
		live.OwnerReferences = kept
		if err := r.Update(ctx, &live); err != nil {
			return err
		}
		remaining = len(kept)
		return nil
	})
	if err != nil {
		return err
	}
	log.FromContext(ctx).V(1).Info("released mirror owner ref",
		"mirror", key.String(), "owners", remaining)
	return nil
}

func (r *Reconciler) markPending(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, reason string) (ctrl.Result, error) {
	log.FromContext(ctx).V(1).Info("pending", "reason", reason)
	if recv.Status.Phase != mxlv1alpha1.MxlReceiverPending {
		recv.Status.Phase = mxlv1alpha1.MxlReceiverPending
		recv.Status.ObservedGeneration = recv.Generation
		if err := r.Status().Update(ctx, recv); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *Reconciler) markBound(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, primary *mxlv1alpha1.MirrorRef) (ctrl.Result, error) {
	recv.Status.BoundMirror = primary
	recv.Status.Phase = mxlv1alpha1.MxlReceiverBound
	recv.Status.ObservedGeneration = recv.Generation
	if err := r.Status().Update(ctx, recv); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
//
// The Watches lets the reconciler notice in real time the events
// that change the desired-mirror set: a previously-bound mirror
// disappears (manual cleanup, gateway crash, peer-side GC); a
// consumer pod's node changes; the flow's Origin location rotates
// to a different node; an Origin Lease in mxl-system is created,
// renewed, or expires. The field index on spec.flowID lets the
// flow-to-receivers and lease-to-receivers map functions avoid a
// cluster-wide receiver scan on each event.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.setupWithManagerAgainst(mgr, r)
}

// setupWithManagerAgainst is the wiring helper SetupWithManager
// dispatches through. The target argument is the reconcile.Reconciler
// the controller hands work to; production always passes r so the
// receiver's own Reconcile observes the dispatches. Tests can pass a
// recording wrapper so the same Watches and predicates fire against
// an observable target without forking the wiring.
func (r *Reconciler) setupWithManagerAgainst(mgr ctrl.Manager, target reconcile.Reconciler) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&mxlv1alpha1.MxlReceiver{},
		flowIDIndex,
		func(o client.Object) []string {
			recv, ok := o.(*mxlv1alpha1.MxlReceiver)
			if !ok || recv.Spec.FlowID == "" {
				return nil
			}
			return []string{recv.Spec.FlowID}
		},
	); err != nil {
		return fmt.Errorf("index MxlReceiver by %s: %w", flowIDIndex, err)
	}

	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&mxlv1alpha1.MxlFlowMirror{},
		ownerUIDIndex,
		func(o client.Object) []string {
			mirror, ok := o.(*mxlv1alpha1.MxlFlowMirror)
			if !ok {
				return nil
			}
			ors := mirror.GetOwnerReferences()
			if len(ors) == 0 {
				return nil
			}
			out := make([]string, 0, len(ors))
			for _, or := range ors {
				out = append(out, string(or.UID))
			}
			return out
		},
	); err != nil {
		return fmt.Errorf("index MxlFlowMirror by %s: %w", ownerUIDIndex, err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlReceiver{}).
		Watches(
			&mxlv1alpha1.MxlFlowMirror{},
			handler.EnqueueRequestsFromMapFunc(r.mirrorToReceivers),
		).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.podToReceivers),
			builder.WithPredicates(podNodeChangePredicate()),
		).
		Watches(
			&mxlv1alpha1.MxlFlow{},
			handler.EnqueueRequestsFromMapFunc(r.flowToReceivers),
		).
		Watches(
			&coordinationv1.Lease{},
			handler.EnqueueRequestsFromMapFunc(r.leaseToReceivers),
			builder.WithPredicates(leaseInMxlSystem()),
		).
		Named("mxlreceiver").
		Complete(target)
}

// mirrorToReceivers maps an MxlFlowMirror event to reconcile
// requests for any MxlReceiver in the mirror's namespace whose
// spec.flowID matches. The receiver reconciler is idempotent
// against the resulting requeues.
func (r *Reconciler) mirrorToReceivers(ctx context.Context, obj client.Object) []reconcile.Request {
	mirror, ok := obj.(*mxlv1alpha1.MxlFlowMirror)
	if !ok {
		return nil
	}
	var receivers mxlv1alpha1.MxlReceiverList
	if err := r.List(ctx, &receivers, client.InNamespace(mirror.Namespace)); err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range receivers.Items {
		if receivers.Items[i].Spec.FlowID == mirror.Spec.FlowID {
			out = append(out, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: receivers.Items[i].Namespace,
					Name:      receivers.Items[i].Name,
				},
			})
		}
	}
	return out
}

// podToReceivers maps a Pod event to reconcile requests for every
// MxlReceiver in the pod's namespace. The reconciler then re-runs
// its label-selector / podRef match; doing the filtering inside
// reconcile keeps the map function simple and means a selector
// change does not need a separate cache invalidation path.
func (r *Reconciler) podToReceivers(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	var receivers mxlv1alpha1.MxlReceiverList
	if err := r.List(ctx, &receivers, client.InNamespace(pod.Namespace)); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(receivers.Items))
	for i := range receivers.Items {
		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: receivers.Items[i].Namespace,
				Name:      receivers.Items[i].Name,
			},
		})
	}
	return out
}

// flowToReceivers maps an MxlFlow event to reconcile requests for
// every MxlReceiver whose spec.flowID equals the flow's name (=
// flow ID). Uses the spec.flowID field index registered in
// SetupWithManager so the lookup is O(matches) rather than a
// cluster-wide scan.
func (r *Reconciler) flowToReceivers(ctx context.Context, obj client.Object) []reconcile.Request {
	flow, ok := obj.(*mxlv1alpha1.MxlFlow)
	if !ok {
		return nil
	}
	var receivers mxlv1alpha1.MxlReceiverList
	if err := r.List(ctx, &receivers, client.MatchingFields{flowIDIndex: flow.Spec.ID}); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(receivers.Items))
	for i := range receivers.Items {
		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: receivers.Items[i].Namespace,
				Name:      receivers.Items[i].Name,
			},
		})
	}
	return out
}

// leaseToReceivers maps a coordination.k8s.io Lease event in the
// mxl-system namespace to reconcile requests for every MxlReceiver
// whose spec.flowID matches the flow ID encoded in the Lease name.
// The Lease is the authoritative liveness signal for an Origin
// location; without this map the receiver only reconverged on the
// next Pod or MxlFlow event, so demote and promote on Lease
// expiry lagged arbitrarily.
func (r *Reconciler) leaseToReceivers(ctx context.Context, obj client.Object) []reconcile.Request {
	lease, ok := obj.(*coordinationv1.Lease)
	if !ok {
		return nil
	}
	flowID, _, ok := mxlv1alpha1.ParseLeaseName(lease.Name)
	if !ok {
		return nil
	}
	var receivers mxlv1alpha1.MxlReceiverList
	if err := r.List(ctx, &receivers, client.MatchingFields{flowIDIndex: flowID}); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(receivers.Items))
	for i := range receivers.Items {
		out = append(out, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&receivers.Items[i]),
		})
	}
	return out
}

// leaseInMxlSystem keeps the Lease watch confined to the namespace
// the agent publishes Origin Leases in. Other Leases (kube-system
// leader election, kube-node-lease) would otherwise wake every
// receiver on every renew tick.
func leaseInMxlSystem() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == mxlv1alpha1.LeaseNamespace
	})
}

// systemNamespacesDenied is the set of cluster-control namespaces
// whose Pod churn the receiver and mirror watches must ignore.
// Static control-plane pods, kube-proxy, CoreDNS, and similar
// kubelet- or DaemonSet-driven workloads churn frequently enough
// that letting their events through would dominate the reconcile
// queue with wakeups for namespaces the operator never schedules
// flow consumers into. mxl-system stays accepted: Origin Leases
// live there (caught by a separate Watches) and the receiver may
// legitimately bind pods that the operator co-locates with the
// agent.
var systemNamespacesDenied = map[string]struct{}{
	"kube-system":     {},
	"kube-public":     {},
	"kube-node-lease": {},
}

// isSystemNamespace reports whether the named namespace is one the
// pod watches must drop events from.
func isSystemNamespace(ns string) bool {
	_, deny := systemNamespacesDenied[ns]
	return deny
}

// podNodeChangePredicate keeps the pod watch from firing on every
// pod status tick. The receiver only cares about pod placement, so
// Create and Delete always pass; Update passes only when
// spec.nodeName changed. Events from kube-system, kube-public, and
// kube-node-lease are dropped before the placement check: the
// receiver never schedules consumer pods into those namespaces, so
// every wakeup they would cause is waste.
func podNodeChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return !isSystemNamespace(e.Object.GetNamespace())
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return !isSystemNamespace(e.Object.GetNamespace())
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, oldOK := e.ObjectOld.(*corev1.Pod)
			newPod, newOK := e.ObjectNew.(*corev1.Pod)
			if !oldOK || !newOK {
				return false
			}
			if isSystemNamespace(newPod.Namespace) {
				return false
			}
			return oldPod.Spec.NodeName != newPod.Spec.NodeName
		},
	}
}
