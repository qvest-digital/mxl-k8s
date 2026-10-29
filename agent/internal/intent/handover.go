package intent

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/mxl-k8s/agent/internal/observed"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// HandOver names a pod still reading a mirrored flow as the requestor
// of each mirror this dispatcher created onto the node whose requestor
// no longer reads it. scan returns, per pod UID, the flows the pod's
// processes map, as observed.Scanner reports them; it is called only
// when the node has such a mirror, so a node with none costs one
// metadata list and no walk of /proc.
//
// A requestor is recorded only when a pod's open of a missing flow
// asks for the mirror. A pod that opens the flow while the mirror is
// already there, as a replacement on the same node does, finds its
// directory in place and never asks, so the mirror stays claimed by
// the pod it replaced and is collected when that pod goes. The
// operator waits RequestorHandover before collecting a mirror whose
// requestor is gone; this is what names a reader inside that window.
//
// A reader in another namespace is named when none in the mirror's
// own namespace reads it. The mirror then counts against that pod's
// Limits.PerPod, as one it asked for would.
//
// Only mirrors whose requestor UID label says it does not map the flow
// are read in full.
func (d *Dispatcher) HandOver(ctx context.Context, scan func() (map[string][]observed.Use, error)) error {
	var mirrors metav1.PartialObjectMetadataList
	mirrors.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirrorList"))
	if err := d.Client.List(ctx, &mirrors,
		client.MatchingLabels{mxlv1alpha1.LabelCreatedByIntent: d.NodeName}); err != nil {
		return fmt.Errorf("list on-demand mirrors: %w", err)
	}
	if !slices.ContainsFunc(mirrors.Items, func(m metav1.PartialObjectMetadata) bool {
		return m.DeletionTimestamp.IsZero()
	}) {
		return nil
	}

	uses, err := scan()
	if err != nil {
		return fmt.Errorf("scan flow use: %w", err)
	}
	readers := map[string][]string{} // mirror name -> pod UIDs mapping its flow
	for uid, us := range uses {
		for _, u := range us {
			if u.Direction != observed.Input {
				continue
			}
			name := mxlv1alpha1.MirrorNameFor(mxlv1alpha1.FlowRef{Domain: u.Domain, ID: u.FlowID}, d.NodeName)
			readers[name] = append(readers[name], uid)
		}
	}

	var pods []corev1.Pod
	var errs []error
	for _, meta := range mirrors.Items {
		uids := readers[meta.Name]
		if len(uids) == 0 || !meta.DeletionTimestamp.IsZero() ||
			slices.Contains(uids, meta.Labels[mxlv1alpha1.LabelRequestorPodUID]) {
			continue
		}
		var m mxlv1alpha1.MxlFlowMirror
		if err := d.Client.Get(ctx, client.ObjectKeyFromObject(&meta), &m); err != nil {
			if !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("get mirror %s/%s: %w", meta.Namespace, meta.Name, err))
			}
			continue
		}
		if m.Spec.TargetNode != d.NodeName || m.Spec.Requestor == nil ||
			slices.Contains(uids, m.Spec.Requestor.UID) {
			continue
		}
		if pods == nil {
			var list corev1.PodList
			if err := d.Client.List(ctx, &list, client.MatchingFields{"spec.nodeName": d.NodeName}); err != nil {
				return errors.Join(append(errs, fmt.Errorf("list pods: %w", err))...)
			}
			pods = list.Items
		}
		next := d.reader(m.Namespace, uids, pods)
		if next == nil {
			continue
		}
		if err := d.handOver(ctx, &m, next); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// reader picks the pod a mirror is handed to among the pods mapping
// its flow: one still running and not mxl-k8s's own, in the mirror's
// namespace first, then the longest running, so successive sweeps
// agree.
func (d *Dispatcher) reader(namespace string, uids []string, pods []corev1.Pod) *corev1.Pod {
	var best *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if !slices.Contains(uids, string(p.UID)) || !p.DeletionTimestamp.IsZero() ||
			p.Spec.NodeName != d.NodeName || (d.Own != nil && d.Own(p)) {
			continue
		}
		if best == nil || before(namespace, p, best) {
			best = p
		}
	}
	return best
}

func before(namespace string, a, b *corev1.Pod) bool {
	if (a.Namespace == namespace) != (b.Namespace == namespace) {
		return a.Namespace == namespace
	}
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name)) < 0
}

// handOver records pod as m's requestor, in spec and in the label the
// limits count by. The patch carries the version m was read at, so a
// mirror the operator has collected or another sweep has handed over
// since is left as it is.
func (d *Dispatcher) handOver(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, pod *corev1.Pod) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"resourceVersion": m.ResourceVersion,
			"labels":          map[string]string{mxlv1alpha1.LabelRequestorPodUID: string(pod.UID)},
		},
		"spec": map[string]any{
			"requestor": mxlv1alpha1.PodRef{Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID)},
		},
	})
	if err != nil {
		return err
	}
	target := &mxlv1alpha1.MxlFlowMirror{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: m.Name}}
	if err := d.Client.Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return nil
		}
		return fmt.Errorf("hand mirror %s/%s to %s/%s: %w", m.Namespace, m.Name, pod.Namespace, pod.Name, err)
	}
	log.FromContext(ctx).WithName("intent").Info("handed mirror to a pod still reading it",
		"mirror", m.Namespace+"/"+m.Name,
		"from", m.Spec.Requestor.Namespace+"/"+m.Spec.Requestor.Name,
		"to", pod.Namespace+"/"+pod.Name)
	return nil
}
