// Package receiver hosts the operator's MxlReceiver reconciler.
//
// The reconciler resolves the consumer-side pod(s) of each receiver,
// looks up the flow's origin node from MxlFlow.status.locations, and
// ensures one MxlFlowMirror per distinct target node so every node
// hosting a matching pod gets a local copy of the flow.
package receiver

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Reconciler reconciles MxlReceiver resources.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlreceivers/finalizers,verbs=update
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile drives one MxlReceiver through its lifecycle: translate
// pod-side intent into one MxlFlowMirror per distinct target node.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlreceiver", req.NamespacedName)

	var recv mxlv1alpha1.MxlReceiver
	if err := r.Get(ctx, req.NamespacedName, &recv); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !recv.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	targetNodes, err := r.resolveTargetNodes(ctx, &recv)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve target nodes: %w", err)
	}
	if len(targetNodes) == 0 {
		return r.markPending(ctx, &recv, "no target pods scheduled yet")
	}

	sourceNode, ok, err := r.resolveSourceNode(ctx, recv.Spec.FlowID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve source node: %w", err)
	}
	if !ok {
		return r.markPending(ctx, &recv, "MxlFlow not yet known or no Origin location")
	}

	var primary *mxlv1alpha1.MirrorRef
	for _, target := range targetNodes {
		if target == sourceNode {
			continue
		}
		mirror, err := r.ensureMirror(ctx, &recv, sourceNode, target)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure mirror for %s: %w", target, err)
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
		"sourceNode", sourceNode,
		"targetNodes", targetNodes)
	return r.markBound(ctx, &recv, primary)
}

// resolveTargetNodes returns the set of distinct node names hosting
// the receiver's consumer pods, in deterministic order.
func (r *Reconciler) resolveTargetNodes(ctx context.Context, recv *mxlv1alpha1.MxlReceiver) ([]string, error) {
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
		return []string{pod.Spec.NodeName}, nil
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
		var out []string
		for i := range pods.Items {
			n := pods.Items[i].Spec.NodeName
			if n == "" {
				continue
			}
			if _, dup := seen[n]; dup {
				continue
			}
			seen[n] = struct{}{}
			out = append(out, n)
		}
		return out, nil
	}

	return nil, nil
}

// resolveSourceNode finds the node hosting the flow's authoritative
// copy (status.locations[?].phase == Origin).
func (r *Reconciler) resolveSourceNode(ctx context.Context, flowID string) (string, bool, error) {
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	for _, loc := range flow.Status.Locations {
		if loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			return loc.NodeName, true, nil
		}
	}
	return "", false, nil
}

// ensureMirror creates the MxlFlowMirror for (flow, target node) if
// it does not already exist. The name is deterministic so concurrent
// receivers targeting the same (flow, node) converge on one mirror.
func (r *Reconciler) ensureMirror(ctx context.Context, recv *mxlv1alpha1.MxlReceiver, sourceNode, targetNode string) (*mxlv1alpha1.MxlFlowMirror, error) {
	name := mirrorName(recv.Spec.FlowID, targetNode)

	var existing mxlv1alpha1.MxlFlowMirror
	err := r.Get(ctx, types.NamespacedName{Namespace: recv.Namespace, Name: name}, &existing)
	if err == nil {
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	provider := recv.Spec.Provider
	if provider == "" {
		provider = mxlv1alpha1.ProviderAuto
	}

	desired := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: recv.Namespace,
			Name:      name,
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     recv.Spec.FlowID,
			SourceNode: sourceNode,
			TargetNode: targetNode,
			Provider:   provider,
		},
	}
	if err := r.Create(ctx, desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err := r.Get(ctx, types.NamespacedName{Namespace: recv.Namespace, Name: name}, &existing); err != nil {
				return nil, err
			}
			return &existing, nil
		}
		return nil, err
	}
	return desired, nil
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
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlReceiver{}).
		Named("mxlreceiver").
		Complete(r)
}
