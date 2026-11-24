package mirror

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// The target descriptor is published as an MxlFlowMirrorTarget owned
// by its mirror, and the mirror's status names it by hash. A video
// flow's descriptor runs to hundreds of kilobytes and changes only
// when the fabric side is opened or rebuilt, while the mirror's status
// changes with every phase transition, and etcd stores the whole
// object again on each.

// applyMirrorTarget writes info into the MxlFlowMirrorTarget of
// mirror. The kind ships with the chart's CRDs and the gateway's RBAC,
// so a cluster that does not serve it or a gateway denied it is an
// error like any other failed write.
func applyMirrorTarget(ctx context.Context, c client.Client, mirror *mxlv1alpha1.MxlFlowMirror, info string) error {
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
		return fmt.Errorf("build MxlFlowMirrorTarget payload: %w", err)
	}
	if err := c.Patch(ctx, obj, client.Apply, client.FieldOwner(targetInfoFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply MxlFlowMirrorTarget: %w", err)
	}
	return nil
}

// targetDescriptor returns the descriptor the source should dial for
// mirror, and false while there is none yet.
//
// A mirror names its descriptor by hash, and the object whose content
// has that hash serves it. No hash is a target that has not published
// one yet; an object that does not match yet is one the cache has not
// caught up with, and the watch on the object brings the mirror back.
func (r *SourceReconciler) targetDescriptor(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror) (string, bool, error) {
	want := mirror.Status.TargetInfoHash
	if want == "" {
		return "", false, nil
	}
	var obj mxlv1alpha1.MxlFlowMirrorTarget
	err := r.Get(ctx, types.NamespacedName{Namespace: mirror.Namespace, Name: mirror.Name}, &obj)
	switch {
	case err == nil:
		if mxlv1alpha1.TargetInfoHash(obj.Spec.TargetInfo) == want {
			return obj.Spec.TargetInfo, true, nil
		}
	case apierrors.IsNotFound(err):
	default:
		return "", false, fmt.Errorf("get MxlFlowMirrorTarget: %w", err)
	}
	return "", false, nil
}

// servesDescriptorOf reports whether e was added against the
// descriptor mirror currently names.
func (e *sourceEntry) servesDescriptorOf(mirror *mxlv1alpha1.MxlFlowMirror) bool {
	return e.infoHash == mirror.Status.TargetInfoHash
}
