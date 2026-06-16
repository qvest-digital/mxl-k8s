package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func newIntentMirror(name, ns, podName, podNS, podUID string) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByIntent: "n-target",
				mxlv1alpha1.LabelRequestorPodUID: podUID,
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     "11111111-2222-3333-4444-555555555555",
			SourceNode: "n-src",
			TargetNode: "n-target",
			Provider:   mxlv1alpha1.ProviderAuto,
			Requestor: &mxlv1alpha1.PodRef{
				Name:      podName,
				Namespace: podNS,
				UID:       podUID,
			},
		},
	}
}

func reconcileOnce(t *testing.T, r *Reconciler, ns, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	require.NoError(t, err)
	return res
}

// TestReconcile_IntentMirror_PodGone_IsDeleted is the load-bearing
// case for this reconciler: when the agent-created mirror's
// requestor pod has disappeared, the mirror gets deleted so the
// gateway tears down the libmxl-fabrics resources behind it.
// Without this, an evicted or finished pod would leave the mirror
// (and the bandwidth it costs) live forever.
func TestReconcile_IntentMirror_PodGone_IsDeleted(t *testing.T) {
	scheme := newScheme(t)
	mirror := newIntentMirror("m", "ns", "consumer", "ns", "uid-1")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()

	r := &Reconciler{Client: c, Scheme: scheme}

	// First pass stamps the finalizer.
	reconcileOnce(t, r, "ns", "m")
	var stamped mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "m"}, &stamped))
	assert.Contains(t, stamped.Finalizers, MxlFlowMirrorIntentFinalizer,
		"the GC must claim the mirror with a finalizer before deciding to "+
			"delete it; otherwise a Delete call would race the API server's "+
			"foreground GC and the cache might not converge")

	// Second pass observes the missing pod and deletes the mirror.
	reconcileOnce(t, r, "ns", "m")

	var after mxlv1alpha1.MxlFlowMirror
	err := c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "m"}, &after)
	if err == nil {
		require.False(t, after.DeletionTimestamp.IsZero(),
			"a mirror whose requestor pod is gone must be deleted (or at "+
				"least carry a non-zero DeletionTimestamp under the fake "+
				"client's finalizer semantics)")
	} else {
		require.True(t, apierrors.IsNotFound(err),
			"unexpected error reading mirror after GC: %v", err)
	}
}

// TestReconcile_IntentMirror_PodUIDMismatch_IsDeleted covers the
// pod-replacement case: a pod with the same name but a fresh UID is
// not the pod that asked for the mirror. The mirror is bound to the
// original UID and must be torn down so the agent on the next probe
// can materialize a fresh one for the new pod.
func TestReconcile_IntentMirror_PodUIDMismatch_IsDeleted(t *testing.T) {
	scheme := newScheme(t)
	mirror := newIntentMirror("m", "ns", "consumer", "ns", "uid-old")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "consumer",
			UID:       "uid-new",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror, pod).
		Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	reconcileOnce(t, r, "ns", "m")
	reconcileOnce(t, r, "ns", "m")

	var after mxlv1alpha1.MxlFlowMirror
	err := c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "m"}, &after)
	if err == nil {
		assert.False(t, after.DeletionTimestamp.IsZero(),
			"a pod with the same name but a different UID is a fresh pod; "+
				"the mirror tied to the old UID must be reaped")
	} else {
		require.True(t, apierrors.IsNotFound(err))
	}
}

// TestReconcile_IntentMirror_PodAlive_IsUntouched guards the
// happy-path: an intent mirror whose requestor pod still exists with
// the matching UID stays in place. The finalizer is stamped but the
// mirror is not deleted.
func TestReconcile_IntentMirror_PodAlive_IsUntouched(t *testing.T) {
	scheme := newScheme(t)
	mirror := newIntentMirror("m", "ns", "consumer", "ns", "uid-1")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "consumer",
			UID:       "uid-1",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror, pod).
		Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	reconcileOnce(t, r, "ns", "m")
	reconcileOnce(t, r, "ns", "m")

	var after mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "m"}, &after))
	assert.True(t, after.DeletionTimestamp.IsZero(),
		"a mirror whose requestor pod is alive must not be deleted")
	assert.Contains(t, after.Finalizers, MxlFlowMirrorIntentFinalizer)
}

// TestReconcile_ReceiverOnlyMirror_IsUntouched is the contract that
// separates the two ownership domains: a mirror stamped only with
// LabelCreatedByReceiver belongs to the receiver reconciler. The
// intent GC must not delete it and must not add the intent
// finalizer to it, even when no pod with the (non-existent) name
// in spec.requestor exists -- the field is irrelevant for
// receiver-owned mirrors.
func TestReconcile_ReceiverOnlyMirror_IsUntouched(t *testing.T) {
	scheme := newScheme(t)
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "m",
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByReceiver: "rcv",
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     "11111111-2222-3333-4444-555555555555",
			SourceNode: "n-src",
			TargetNode: "n-target",
			Provider:   mxlv1alpha1.ProviderTCP,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror.DeepCopy()).
		Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	reconcileOnce(t, r, "ns", "m")

	var after mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "m"}, &after))
	assert.True(t, after.DeletionTimestamp.IsZero(),
		"a receiver-owned mirror must not be touched by the intent GC; "+
			"deleting it here would let the intent reconciler reap a "+
			"mirror the receiver still needs")
	assert.NotContains(t, after.Finalizers, MxlFlowMirrorIntentFinalizer,
		"the intent finalizer must not be stamped on a receiver-owned "+
			"mirror; that would block deletion the receiver reconciler "+
			"initiated")
	assert.Equal(t, mirror.Spec, after.Spec,
		"the intent GC must not mutate spec on a receiver-owned mirror")
}

// TestReconcile_MissingMirror_NoError covers the case where the
// mirror was deleted between event enqueue and reconcile. The
// reconciler returns cleanly without requeue.
func TestReconcile_MissingMirror_NoError(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "missing"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
}

// TestReconcile_IntentMirror_AlreadyDeleting_DropsFinalizer verifies
// that once the mirror enters deletion (DeletionTimestamp set), the
// intent finalizer comes off so the API server can complete the
// delete. This is the path the GC itself walks on its second pass
// after Delete() but it also handles externally-initiated deletes
// (kubectl delete, gateway-driven cleanup).
func TestReconcile_IntentMirror_AlreadyDeleting_DropsFinalizer(t *testing.T) {
	scheme := newScheme(t)
	now := metav1.Now()
	mirror := newIntentMirror("m", "ns", "consumer", "ns", "uid-1")
	mirror.DeletionTimestamp = &now
	mirror.Finalizers = []string{
		MxlFlowMirrorIntentFinalizer,
		"test.mxl.qvest-digital.com/keepalive",
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(mirror).
		Build()
	r := &Reconciler{Client: c, Scheme: scheme}

	reconcileOnce(t, r, "ns", "m")

	var after mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "m"}, &after))
	assert.NotContains(t, after.Finalizers, MxlFlowMirrorIntentFinalizer,
		"once the mirror is deleting, the intent reconciler's job is to "+
			"release its finalizer so the API server can finish; the "+
			"gateway's own finalizer is what guards the data-plane teardown")
	assert.Contains(t, after.Finalizers, "test.mxl.qvest-digital.com/keepalive",
		"the intent reconciler must remove only its own finalizer; other "+
			"finalizers belong to peers (the gateway, the receiver) and "+
			"removing them would race their teardown")
}

// TestPodPredicate_DenyKubeSystem locks in the namespace deny-list
// on the intent GC's pod watch. Without it, churn on
// kube-system (kube-proxy, CoreDNS, kubelet-managed static pods)
// dominates the reconcile queue with wakeups for namespaces the
// agent never authors mirrors in.
func TestPodPredicate_DenyKubeSystem(t *testing.T) {
	pred := podLifecyclePredicate()

	pod := func(ns string, uid string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "p", UID: types.UID(uid)},
		}
	}

	denied := []string{"kube-system", "kube-public", "kube-node-lease"}
	for _, ns := range denied {
		t.Run("deny/"+ns, func(t *testing.T) {
			obj := pod(ns, "uid-1")
			assert.False(t, pred.Create(event.CreateEvent{Object: obj}),
				"Create event from %s must be dropped: the agent never "+
					"authors intent mirrors there, so wakeups from those "+
					"namespaces are pure overhead", ns)
			assert.False(t, pred.Delete(event.DeleteEvent{Object: obj}),
				"Delete event from %s must be dropped", ns)
			assert.False(t, pred.Update(event.UpdateEvent{
				ObjectOld: pod(ns, "uid-old"),
				ObjectNew: pod(ns, "uid-new"),
			}), "Update event from %s must be dropped even when UID changes", ns)
		})
	}

	allowed := []string{"mxl-system", "default", "app"}
	for _, ns := range allowed {
		t.Run("allow/"+ns, func(t *testing.T) {
			obj := pod(ns, "uid-1")
			assert.True(t, pred.Create(event.CreateEvent{Object: obj}),
				"Create event from %s must pass", ns)
			assert.True(t, pred.Delete(event.DeleteEvent{Object: obj}),
				"Delete event from %s must pass", ns)
			assert.True(t, pred.Update(event.UpdateEvent{
				ObjectOld: pod(ns, "uid-old"),
				ObjectNew: pod(ns, "uid-new"),
			}), "Update event from %s with UID change must pass", ns)
		})
	}
}

var _ = client.IgnoreNotFound
