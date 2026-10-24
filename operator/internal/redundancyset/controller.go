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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/mxl-k8s/api/selection"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Condition and reasons on a set.
const (
	ConditionValid       = "Valid"
	ReasonMembersAligned = "MembersAligned"
	ReasonMemberMissing  = "MemberMissing"
	ReasonOtherDomain    = "MemberInOtherDomain"
	ReasonMisaligned     = "MembersMisaligned"
	ReasonOutputTaken    = "OutputIsAFlow"
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
	flows, cond := r.validate(ctx, &set)
	res := ctrl.Result{}
	if cond.Status == metav1.ConditionTrue {
		var err error
		res, err = r.ensureMirrors(ctx, &set, flows)
		if err != nil {
			return ctrl.Result{}, err
		}
	} else if cond.Reason == ReasonMemberMissing {
		res = ctrl.Result{RequeueAfter: 10 * time.Second}
	}
	cond.ObservedGeneration = set.Generation
	if meta.SetStatusCondition(&set.Status.Conditions, cond) || set.Status.ObservedGeneration != set.Generation {
		set.Status.ObservedGeneration = set.Generation
		if err := r.Status().Update(ctx, &set); err != nil {
			return ctrl.Result{}, err
		}
	}
	return res, nil
}

func (r *Reconciler) validate(ctx context.Context, set *mxlv1alpha1.MxlFlowRedundancySet) ([]*mxlv1alpha1.MxlFlow, metav1.Condition) {
	fail := func(reason, msg string) ([]*mxlv1alpha1.MxlFlow, metav1.Condition) {
		return nil, metav1.Condition{Type: ConditionValid, Status: metav1.ConditionFalse, Reason: reason, Message: msg}
	}
	out := mxlv1alpha1.FlowRef{Domain: set.Spec.Output.Domain, ID: set.Spec.Output.FlowID}
	var existing mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: out.Name()}, &existing); err == nil {
		return fail(ReasonOutputTaken, fmt.Sprintf("output %s is a flow of its own", out.Name()))
	}
	var flows []*mxlv1alpha1.MxlFlow
	var first map[string]any
	for _, m := range set.Spec.Members {
		if m.Domain != set.Spec.Output.Domain {
			return fail(ReasonOtherDomain, fmt.Sprintf("member %s is not in the output's domain; one output selects among pages of one domain", m.FlowID))
		}
		ref := mxlv1alpha1.FlowRef{Domain: m.Domain, ID: m.FlowID}
		var f mxlv1alpha1.MxlFlow
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name()}, &f); err != nil {
			if apierrors.IsNotFound(err) {
				return fail(ReasonMemberMissing, fmt.Sprintf("member %s has no MxlFlow yet", ref.Name()))
			}
			return fail(ReasonMemberMissing, err.Error())
		}
		var def map[string]any
		if err := json.Unmarshal(f.Spec.Definition.Raw, &def); err != nil {
			return fail(ReasonMisaligned, fmt.Sprintf("member %s: %v", ref.Name(), err))
		}
		if first == nil {
			first = def
		} else {
			for _, k := range alignedKeys {
				if !reflect.DeepEqual(first[k], def[k]) {
					return fail(ReasonMisaligned, fmt.Sprintf("member %s differs from %s in %s", ref.Name(), set.Spec.Members[0].FlowID, k))
				}
			}
		}
		flows = append(flows, &f)
	}
	return flows, metav1.Condition{Type: ConditionValid, Status: metav1.ConditionTrue, Reason: ReasonMembersAligned,
		Message: "members agree in format, rate and geometry"}
}

func (r *Reconciler) ensureMirrors(ctx context.Context, set *mxlv1alpha1.MxlFlowRedundancySet, flows []*mxlv1alpha1.MxlFlow) (ctrl.Result, error) {
	res := ctrl.Result{}
	for _, node := range set.Status.Nodes {
		for i, m := range set.Spec.Members {
			origin, err := mxlv1alpha1.ResolveOrigin(flows[i], r.freshness(ctx))
			if err != nil {
				return res, err
			}
			if !origin.Found {
				res = ctrl.Result{RequeueAfter: 10 * time.Second} // the member's producer is not up
				continue
			}
			if origin.Node == node {
				continue // produced on that node
			}
			if err := r.ensureMirror(ctx, set, m, origin.Node, node); err != nil {
				return res, err
			}
		}
	}
	return res, nil
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

// SetupWithManager registers the controller. A set waiting for a member
// or its producer requeues itself.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowRedundancySet{}).
		Owns(&mxlv1alpha1.MxlFlowMirror{}).
		Complete(r)
}
