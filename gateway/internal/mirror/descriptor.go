package mirror

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// The target descriptor is published as an MxlFlowMirrorTarget owned
// by its mirror, and the mirror's status names it by hash. A video
// flow's descriptor runs to hundreds of kilobytes and changes only
// when the fabric side is opened or rebuilt, while the mirror's status
// changes with every phase transition, and etcd stores the whole
// object again on each.
//
// The descriptor is also still written to status.targetInfo, for
// source gateways that predate the object.

// applyMirrorTarget writes info into the MxlFlowMirrorTarget of
// mirror. It reports false without an error when the API server does
// not serve the kind, which is a cluster whose CRDs predate it, or
// denies this gateway access to it, which is RBAC that predates it:
// the mirror then carries the descriptor in status.targetInfo alone.
func applyMirrorTarget(ctx context.Context, c client.Client, mirror *mxlv1alpha1.MxlFlowMirror, info string) (bool, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirrorTarget"))
	obj.SetNamespace(mirror.Namespace)
	obj.SetName(mirror.Name)
	controller, block := true, false
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion:         mxlv1alpha1.GroupVersion.String(),
		Kind:               "MxlFlowMirror",
		Name:               mirror.Name,
		UID:                mirror.UID,
		Controller:         &controller,
		BlockOwnerDeletion: &block,
	}})
	if err := unstructured.SetNestedField(obj.Object, map[string]any{"targetInfo": info}, "spec"); err != nil {
		return false, fmt.Errorf("build MxlFlowMirrorTarget payload: %w", err)
	}
	err := c.Patch(ctx, obj, client.Apply, client.FieldOwner(targetInfoFieldOwner), client.ForceOwnership)
	if meta.IsNoMatchError(err) || apierrors.IsForbidden(err) {
		ctrl.Log.WithName("target").Info("MxlFlowMirrorTarget not writable; publishing the descriptor in status.targetInfo only",
			"mirror", client.ObjectKeyFromObject(mirror), "reason", err.Error())
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("apply MxlFlowMirrorTarget: %w", err)
	}
	return true, nil
}

// targetDescriptor returns the descriptor the source should dial for
// mirror, and false while there is none yet.
//
// A mirror naming a descriptor by hash is served by the object whose
// content has that hash, or by status.targetInfo when that has it; an
// object that does not match yet is one the cache has not caught up
// with, and the watch on the object brings the mirror back. A mirror
// without a hash comes from a target gateway that predates the
// object, and status.targetInfo is its descriptor.
func (r *SourceReconciler) targetDescriptor(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror) (string, bool, error) {
	want := mirror.Status.TargetInfoHash
	if want == "" {
		return mirror.Status.TargetInfo, mirror.Status.TargetInfo != "", nil
	}
	var obj mxlv1alpha1.MxlFlowMirrorTarget
	err := r.Get(ctx, types.NamespacedName{Namespace: mirror.Namespace, Name: mirror.Name}, &obj)
	switch {
	case err == nil:
		if mxlv1alpha1.TargetInfoHash(obj.Spec.TargetInfo) == want {
			return obj.Spec.TargetInfo, true, nil
		}
	case apierrors.IsNotFound(err), meta.IsNoMatchError(err):
	default:
		return "", false, fmt.Errorf("get MxlFlowMirrorTarget: %w", err)
	}
	if mirror.Status.TargetInfo != "" && mxlv1alpha1.TargetInfoHash(mirror.Status.TargetInfo) == want {
		return mirror.Status.TargetInfo, true, nil
	}
	return "", false, nil
}

// servesDescriptorOf reports whether e was added against the
// descriptor mirror currently publishes: by hash where the mirror
// names one, and by content for a target gateway that writes none.
func (e *sourceEntry) servesDescriptorOf(mirror *mxlv1alpha1.MxlFlowMirror) bool {
	if h := mirror.Status.TargetInfoHash; h != "" {
		return e.infoHash == h
	}
	return e.infoStr == mirror.Status.TargetInfo
}
