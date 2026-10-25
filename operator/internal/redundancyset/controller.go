// Package redundancyset reconciles MxlFlowRedundancySets: it checks that a
// set's members can be merged, and mirrors every member to each node a
// reader wants the output on. The gateway there merges the output from
// those copies; agents add a node to status.nodes when a reader there
// waits for the output.
package redundancyset

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/mxl-k8s/api/selection"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Conditions and reasons on a set.
const (
	ConditionValid        = mxlv1alpha1.ConditionTypeValid
	ConditionMembersReady = mxlv1alpha1.ConditionTypeMembersReady
	ReasonMembersAligned  = "MembersAligned"
	ReasonMembersReady    = "MembersReady"
	ReasonMemberMissing   = "MemberMissing"
	ReasonOtherDomain     = "MemberInOtherDomain"
	ReasonMisaligned      = "MembersMisaligned"
	ReasonOutputTaken     = "OutputIsAFlow"
)

// LeaseChecker reports whether an Origin's Lease is fresh.
type LeaseChecker interface {
	IsFresh(ctx context.Context, flowID, nodeName string) (fresh bool, deadline time.Time, err error)
}

// Reconciler reconciles MxlFlowRedundancySets.
type Reconciler struct {
	client.Client
	// Namespace holds the member mirrors a set creates.
	Namespace string
	Lease     LeaseChecker
}

// alignedKeys are the flow definition keys that must agree for two flows
// to be merged slot for slot.
var alignedKeys = []string{"format", "media_type", "grain_rate", "sample_rate", "frame_width", "frame_height",
	"interlace_mode", "components", "channel_count", "bit_depth"}

// Reconcile validates one set and ensures its member mirrors.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var set mxlv1alpha1.MxlFlowRedundancySet
	if err := r.Get(ctx, req.NamespacedName, &set); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !set.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	nodes, err := r.existingNodes(ctx, set.Status.Nodes)
	if err != nil {
		return ctrl.Result{}, err
	}
	pruned := len(nodes) != len(set.Status.Nodes)
	set.Status.Nodes = nodes
	ms, err := r.members(ctx, &set)
	if err != nil {
		return ctrl.Result{}, err
	}
	valid := r.validate(ctx, &set, ms)
	ready := membersReady(ms)
	res := ctrl.Result{}
	if ready.Status != metav1.ConditionTrue {
		res = ctrl.Result{RequeueAfter: 10 * time.Second} // a member or its producer is not up
	}
	if valid.Status == metav1.ConditionTrue {
		if err := r.ensureMirrors(ctx, &set, ms); err != nil {
			return ctrl.Result{}, err
		}
	}
	valid.ObservedGeneration, ready.ObservedGeneration = set.Generation, set.Generation
	changed := meta.SetStatusCondition(&set.Status.Conditions, valid)
	changed = meta.SetStatusCondition(&set.Status.Conditions, ready) || changed
	if changed || set.Status.ObservedGeneration != set.Generation || pruned {
		set.Status.ObservedGeneration = set.Generation
		if err := r.Status().Update(ctx, &set); err != nil {
			return ctrl.Result{}, err
		}
	}
	return res, nil
}

// existingNodes drops the nodes that are gone from the cluster: no agent
// is left there to remove its own, and the member mirrors onto it would
// be kept claimed.
func (r *Reconciler) existingNodes(ctx context.Context, nodes []string) ([]string, error) {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		var node corev1.Node
		err := r.Get(ctx, types.NamespacedName{Name: n}, &node)
		switch {
		case apierrors.IsNotFound(err):
			log.FromContext(ctx).Info("dropping a node that is gone from the redundancy set", "node", n)
		case err != nil:
			return nil, err
		default:
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		out = nil
	}
	return out, nil
}

// member is what the operator knows of one member of a set.
type member struct {
	ref    mxlv1alpha1.FlowRef
	flow   *mxlv1alpha1.MxlFlow // nil while it has no MxlFlow
	origin mxlv1alpha1.OriginResolution
	def    map[string]any // nil while it has no readable definition
	// unreadable says why the member's definition cannot be read.
	unreadable string
}

// members reads every member's MxlFlow, Origin and definition.
func (r *Reconciler) members(ctx context.Context, set *mxlv1alpha1.MxlFlowRedundancySet) ([]member, error) {
	ms := make([]member, len(set.Spec.Members))
	for i, m := range set.Spec.Members {
		ms[i].ref = mxlv1alpha1.FlowRef{Domain: m.Domain, ID: m.FlowID}
		var f mxlv1alpha1.MxlFlow
		if err := r.Get(ctx, types.NamespacedName{Name: ms[i].ref.Name()}, &f); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		ms[i].flow = &f
		origin, err := mxlv1alpha1.ResolveOrigin(&f, r.freshness(ctx))
		if err != nil {
			return nil, err
		}
		ms[i].origin = origin
		if err := json.Unmarshal(f.Spec.Definition.Raw, &ms[i].def); err != nil {
			ms[i].def, ms[i].unreadable = nil, fmt.Sprintf("member %s: %v", ms[i].ref.Name(), err)
		}
	}
	return ms, nil
}

// disagreement lists, for every aligned key the members' definitions
// differ in, each member's value; empty when they agree. No member is
// the reference: the gateways keep whichever geometry their output was
// built in.
func disagreement(ms []member) string {
	var keys []string
	for _, k := range alignedKeys {
		var first any
		var vals []string
		differ := false
		for _, m := range ms {
			if m.def == nil {
				continue
			}
			if vals == nil {
				first = m.def[k]
			} else if !reflect.DeepEqual(first, m.def[k]) {
				differ = true
			}
			v, _ := json.Marshal(m.def[k])
			vals = append(vals, fmt.Sprintf("%s %s", m.ref.Name(), v))
		}
		if differ {
			keys = append(keys, k+": "+strings.Join(vals, ", "))
		}
	}
	if keys == nil {
		return ""
	}
	return "members differ in " + strings.Join(keys, "; ")
}

// validate reports whether the set can be served. The spec is checked
// every time; the members only until they first agree at a generation.
// From then on a member that goes away, or comes back in another
// geometry, leaves the output served from the others, and MembersReady
// reports it.
func (r *Reconciler) validate(ctx context.Context, set *mxlv1alpha1.MxlFlowRedundancySet, ms []member) metav1.Condition {
	fail := func(reason, msg string) metav1.Condition {
		return metav1.Condition{Type: ConditionValid, Status: metav1.ConditionFalse, Reason: reason, Message: msg}
	}
	out := mxlv1alpha1.FlowRef{Domain: set.Spec.Output.Domain, ID: set.Spec.Output.FlowID}
	var existing mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: out.Name()}, &existing); err == nil {
		return fail(ReasonOutputTaken, fmt.Sprintf("output %s is a flow of its own", out.Name()))
	}
	for _, m := range set.Spec.Members {
		if m.Domain != set.Spec.Output.Domain {
			return fail(ReasonOtherDomain, fmt.Sprintf("member %s is not in the output's domain; one output selects among pages of one domain", m.FlowID))
		}
	}
	differ := disagreement(ms)
	published := 0
	for _, m := range ms {
		if m.def != nil {
			published++
		}
	}
	ok := metav1.Condition{Type: ConditionValid, Status: metav1.ConditionTrue, Reason: ReasonMembersAligned,
		Message: "members agree in format, rate and geometry"}
	switch {
	case differ != "":
		ok.Message = "members agreed when the set was checked; MembersReady names those apart now"
	case published < len(ms):
		ok.Message = "members agree in format, rate and geometry as far as published"
	}
	if c := meta.FindStatusCondition(set.Status.Conditions, ConditionValid); c != nil &&
		c.Status == metav1.ConditionTrue && c.ObservedGeneration == set.Generation {
		return ok
	}
	for _, m := range ms {
		if m.unreadable != "" {
			return fail(ReasonMisaligned, m.unreadable)
		}
	}
	if differ != "" {
		return fail(ReasonMisaligned, differ)
	}
	if published == 0 {
		return fail(ReasonMemberMissing, "no member has an MxlFlow yet")
	}
	return ok
}

// membersReady reports the members the output is served without.
func membersReady(ms []member) metav1.Condition {
	var missing, apart []string
	for _, m := range ms {
		switch {
		case m.flow == nil:
			missing = append(missing, fmt.Sprintf("member %s has no MxlFlow", m.ref.Name()))
		case m.unreadable != "":
			apart = append(apart, m.unreadable)
		case !m.origin.Found:
			missing = append(missing, fmt.Sprintf("member %s has no producer", m.ref.Name()))
		}
	}
	if d := disagreement(ms); d != "" {
		apart = append(apart, d)
	}
	switch {
	case len(missing) > 0:
		return metav1.Condition{Type: ConditionMembersReady, Status: metav1.ConditionFalse, Reason: ReasonMemberMissing,
			Message: strings.Join(append(missing, apart...), "; ")}
	case len(apart) > 0:
		return metav1.Condition{Type: ConditionMembersReady, Status: metav1.ConditionFalse, Reason: ReasonMisaligned,
			Message: strings.Join(apart, "; ")}
	}
	return metav1.Condition{Type: ConditionMembersReady, Status: metav1.ConditionTrue, Reason: ReasonMembersReady,
		Message: "every member is produced in the output's geometry"}
}

// ensureMirrors mirrors every member that has an Origin to each node
// the output is wanted on. A member whose producer is gone has its
// mirrors collected by the mirror controller once they have no source;
// they are created again here when it comes back.
func (r *Reconciler) ensureMirrors(ctx context.Context, set *mxlv1alpha1.MxlFlowRedundancySet, ms []member) error {
	for _, node := range set.Status.Nodes {
		for i, m := range ms {
			if !m.origin.Found || m.origin.Node == node {
				continue // the producer is not up, or produces on that node
			}
			if err := r.ensureMirror(ctx, set, set.Spec.Members[i], m.origin.Node, node); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Reconciler) freshness(ctx context.Context) mxlv1alpha1.LeaseFreshness {
	if r.Lease == nil {
		return nil
	}
	return func(flowID, node string) (bool, time.Time, error) { return r.Lease.IsFresh(ctx, flowID, node) }
}

func (r *Reconciler) ensureMirror(ctx context.Context, set *mxlv1alpha1.MxlFlowRedundancySet, m mxlv1alpha1.RedundancyFlowRef, source, target string) error {
	ref := mxlv1alpha1.FlowRef{Domain: m.Domain, ID: m.FlowID}
	provider, err := r.provider(ctx, source, target)
	if err != nil {
		return err
	}
	name := mxlv1alpha1.MirrorNameFor(ref, target)
	var existing mxlv1alpha1.MxlFlowMirror
	err = r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: name}, &existing)
	if err == nil {
		if existing.Spec.SourceNode == source && existing.Spec.Provider == provider {
			return nil
		}
		patch := client.MergeFrom(existing.DeepCopy())
		existing.Spec.SourceNode, existing.Spec.Provider = source, provider
		return r.Patch(ctx, &existing, patch)
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Namespace,
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: mxlv1alpha1.GroupVersion.String(), Kind: "MxlFlowRedundancySet",
				Name: set.Name, UID: set.UID,
			}},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: m.FlowID, Domain: m.Domain, SourceNode: source, TargetNode: target, Provider: provider},
	}
	if err := r.Create(ctx, mirror); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	log.FromContext(ctx).Info("mirroring redundancy set member", "member", ref.Name(), "source", source, "target", target)
	return nil
}

// provider resolves a concrete provider from the two nodes'
// MxlNodeCapabilities, as a receiver's mirror does.
func (r *Reconciler) provider(ctx context.Context, source, target string) (mxlv1alpha1.MxlFabricsProvider, error) {
	caps := func(node string) (mxlv1alpha1.MxlNodeCapabilitiesStatus, error) {
		var c mxlv1alpha1.MxlNodeCapabilities
		if err := r.Get(ctx, types.NamespacedName{Name: node}, &c); err != nil {
			if apierrors.IsNotFound(err) {
				return mxlv1alpha1.MxlNodeCapabilitiesStatus{}, nil
			}
			return mxlv1alpha1.MxlNodeCapabilitiesStatus{}, err
		}
		return c.Status, nil
	}
	src, err := caps(source)
	if err != nil {
		return "", err
	}
	tgt, err := caps(target)
	if err != nil {
		return "", err
	}
	p, _ := selection.Resolve(src, tgt)
	return p, nil
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowredundancysets,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowredundancysets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// SetupWithManager registers the controller. A set waiting for a member
// or its producer requeues itself; a change to a member's or the output's
// MxlFlow checks the set again, since members a producer restarted in
// another geometry agree again only once both definitions have followed.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowRedundancySet{}).
		Owns(&mxlv1alpha1.MxlFlowMirror{}).
		Watches(&mxlv1alpha1.MxlFlow{}, handler.EnqueueRequestsFromMapFunc(r.setsOf)).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.setsOn),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return false },
				UpdateFunc:  func(event.UpdateEvent) bool { return false },
				GenericFunc: func(event.GenericEvent) bool { return false },
			})).
		Complete(r)
}

// setsOn maps a deleted Node to the sets that list it in status.nodes.
func (r *Reconciler) setsOn(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	var sets mxlv1alpha1.MxlFlowRedundancySetList
	if err := r.List(ctx, &sets); err != nil {
		log.FromContext(ctx).Error(err, "list redundancy sets for a node", "node", obj.GetName())
		return nil
	}
	var reqs []ctrlreconcile.Request
	for _, s := range sets.Items {
		if slices.Contains(s.Status.Nodes, obj.GetName()) {
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: types.NamespacedName{Name: s.Name}})
		}
	}
	return reqs
}

// setsOf maps an MxlFlow to the sets that name it as a member or output.
func (r *Reconciler) setsOf(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	var sets mxlv1alpha1.MxlFlowRedundancySetList
	if err := r.List(ctx, &sets); err != nil {
		log.FromContext(ctx).Error(err, "list redundancy sets for a flow", "flow", obj.GetName())
		return nil
	}
	var reqs []ctrlreconcile.Request
	for _, s := range sets.Items {
		refs := append([]mxlv1alpha1.RedundancyFlowRef{s.Spec.Output}, s.Spec.Members...)
		for _, ref := range refs {
			if (mxlv1alpha1.FlowRef{Domain: ref.Domain, ID: ref.FlowID}).Name() == obj.GetName() {
				reqs = append(reqs, ctrlreconcile.Request{NamespacedName: types.NamespacedName{Name: s.Name}})
				break
			}
		}
	}
	return reqs
}
