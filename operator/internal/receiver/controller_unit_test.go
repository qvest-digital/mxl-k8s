package receiver

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// scheme is the per-test scheme passed to the fake client. The
// envtest path uses its own scheme registered in testutil; keep this
// one minimal so the unit tests stay cheap.
func unitScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func TestMirrorName_DeterministicAndDNSSafe(t *testing.T) {
	cases := []struct {
		name   string
		flowID string
		node   string
		want   string
	}{
		{
			name:   "lowercase canonical",
			flowID: "11111111-2222-3333-4444-555555555555",
			node:   "worker-1",
			want:   "11111111-2222-3333-4444-555555555555--worker-1",
		},
		{
			name:   "uppercase chars are lowered",
			flowID: "ABCDABCD-1234-5678-9ABC-DEF012345678",
			node:   "Worker-A",
			want:   "abcdabcd-1234-5678-9abc-def012345678--worker-a",
		},
		{
			name:   "dots in a node name are replaced with -",
			flowID: "11111111-2222-3333-4444-555555555555",
			node:   "ip-10.0.0.1.eu-central-1.compute",
			want:   "11111111-2222-3333-4444-555555555555--ip-10-0-0-1-eu-central-1-compute",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mirrorName(tc.flowID, tc.node)
			assert.Equal(t, tc.want, got)
			// The result must satisfy the kubernetes DNS-1123 subdomain
			// rules; if it does not, the reconciler would silently fail
			// to Create the mirror at runtime.
			errs := validation.IsDNS1123Subdomain(got)
			assert.Emptyf(t, errs, "mirrorName output %q is not a valid DNS-1123 subdomain: %v", got, errs)
		})
	}

	t.Run("is deterministic", func(t *testing.T) {
		a := mirrorName("11111111-2222-3333-4444-555555555555", "n1")
		b := mirrorName("11111111-2222-3333-4444-555555555555", "n1")
		assert.Equal(t, a, b)
	})

	t.Run("differs for distinct nodes", func(t *testing.T) {
		assert.NotEqual(t,
			mirrorName("11111111-2222-3333-4444-555555555555", "n1"),
			mirrorName("11111111-2222-3333-4444-555555555555", "n2"))
	})
}

func TestResolveTargetNodes_PodRef(t *testing.T) {
	ctx := context.Background()

	t.Run("found and scheduled", func(t *testing.T) {
		recv := &mxlv1alpha1.MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
			Spec: mxlv1alpha1.MxlReceiverSpec{
				PodRef: &mxlv1alpha1.PodRef{Namespace: "ns", Name: "pod-a"},
			},
		}
		c := fake.NewClientBuilder().
			WithScheme(unitScheme(t)).
			WithObjects(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-a"},
				Spec:       corev1.PodSpec{NodeName: "worker-1"},
			}).
			Build()
		r := &Reconciler{Client: c}

		got, err := r.resolveTargetNodes(ctx, recv)
		require.NoError(t, err)
		assert.Equal(t, []string{"worker-1"}, got)
	})

	t.Run("not found returns empty without error", func(t *testing.T) {
		recv := &mxlv1alpha1.MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
			Spec: mxlv1alpha1.MxlReceiverSpec{
				PodRef: &mxlv1alpha1.PodRef{Namespace: "ns", Name: "missing"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
		r := &Reconciler{Client: c}

		got, err := r.resolveTargetNodes(ctx, recv)
		require.NoError(t, err)
		assert.Empty(t, got, "missing pod must not bubble up as an error; it is a normal pending state")
	})

	t.Run("unscheduled pod is treated as empty", func(t *testing.T) {
		recv := &mxlv1alpha1.MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
			Spec: mxlv1alpha1.MxlReceiverSpec{
				PodRef: &mxlv1alpha1.PodRef{Namespace: "ns", Name: "pod-a"},
			},
		}
		c := fake.NewClientBuilder().
			WithScheme(unitScheme(t)).
			WithObjects(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-a"},
				Spec:       corev1.PodSpec{}, // NodeName is empty
			}).
			Build()
		r := &Reconciler{Client: c}

		got, err := r.resolveTargetNodes(ctx, recv)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("podRef.Namespace empty falls back to receiver namespace", func(t *testing.T) {
		recv := &mxlv1alpha1.MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
			Spec: mxlv1alpha1.MxlReceiverSpec{
				PodRef: &mxlv1alpha1.PodRef{Name: "pod-a"},
			},
		}
		c := fake.NewClientBuilder().
			WithScheme(unitScheme(t)).
			WithObjects(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-a"},
				Spec:       corev1.PodSpec{NodeName: "worker-1"},
			}).
			Build()
		r := &Reconciler{Client: c}

		got, err := r.resolveTargetNodes(ctx, recv)
		require.NoError(t, err)
		assert.Equal(t, []string{"worker-1"}, got)
	})
}

func TestResolveTargetNodes_PodSelector(t *testing.T) {
	ctx := context.Background()

	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "consumer"}},
		},
	}

	t.Run("returns distinct nodes only", func(t *testing.T) {
		c := fake.NewClientBuilder().
			WithScheme(unitScheme(t)).
			WithObjects(
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a", Labels: map[string]string{"app": "consumer"}},
					Spec:       corev1.PodSpec{NodeName: "n1"},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "b", Labels: map[string]string{"app": "consumer"}},
					Spec:       corev1.PodSpec{NodeName: "n1"},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c", Labels: map[string]string{"app": "consumer"}},
					Spec:       corev1.PodSpec{NodeName: "n2"},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "skipme", Labels: map[string]string{"app": "other"}},
					Spec:       corev1.PodSpec{NodeName: "n3"},
				},
			).
			Build()
		r := &Reconciler{Client: c}

		got, err := r.resolveTargetNodes(ctx, recv)
		require.NoError(t, err)
		sort.Strings(got)
		assert.Equal(t, []string{"n1", "n2"}, got,
			"deduplication must collapse the two pods on n1; non-matching labels must be filtered")
	})

	t.Run("invalid selector surfaces an error", func(t *testing.T) {
		bad := &mxlv1alpha1.MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
			Spec: mxlv1alpha1.MxlReceiverSpec{
				PodSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{Key: "app", Operator: "NotARealOperator"},
					},
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
		r := &Reconciler{Client: c}

		_, err := r.resolveTargetNodes(ctx, bad)
		require.Error(t, err)
	})

	t.Run("neither podRef nor selector returns empty without error", func(t *testing.T) {
		// The CRD's XValidation rule forbids this state, but the
		// reconciler must not panic if it ever sees it (a manual edit
		// of an old CR could produce one).
		empty := &mxlv1alpha1.MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
			Spec:       mxlv1alpha1.MxlReceiverSpec{},
		}
		c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
		r := &Reconciler{Client: c}

		got, err := r.resolveTargetNodes(ctx, empty)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestResolveSourceNode(t *testing.T) {
	ctx := context.Background()

	t.Run("returns the Origin location's node", func(t *testing.T) {
		c := fake.NewClientBuilder().
			WithScheme(unitScheme(t)).
			WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
			WithObjects(&mxlv1alpha1.MxlFlow{
				ObjectMeta: metav1.ObjectMeta{Name: "flow-a"},
				Spec:       mxlv1alpha1.MxlFlowSpec{ID: "flow-a"},
				Status: mxlv1alpha1.MxlFlowStatus{
					Locations: []mxlv1alpha1.MxlFlowLocation{
						{NodeName: "n-mirror", Phase: mxlv1alpha1.MxlFlowLocationMirroring},
						{NodeName: "n-origin", Phase: mxlv1alpha1.MxlFlowLocationOrigin},
						{NodeName: "n-stale", Phase: mxlv1alpha1.MxlFlowLocationStale},
					},
				},
			}).
			Build()
		r := &Reconciler{Client: c}

		node, ok, err := r.resolveSourceNode(ctx, "flow-a")
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "n-origin", node)
	})

	t.Run("flow not found returns false without error", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
		r := &Reconciler{Client: c}
		_, ok, err := r.resolveSourceNode(ctx, "missing")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("no Origin location returns false without error", func(t *testing.T) {
		c := fake.NewClientBuilder().
			WithScheme(unitScheme(t)).
			WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).
			WithObjects(&mxlv1alpha1.MxlFlow{
				ObjectMeta: metav1.ObjectMeta{Name: "flow-a"},
				Spec:       mxlv1alpha1.MxlFlowSpec{ID: "flow-a"},
				Status: mxlv1alpha1.MxlFlowStatus{
					Locations: []mxlv1alpha1.MxlFlowLocation{
						{NodeName: "n-stale", Phase: mxlv1alpha1.MxlFlowLocationStale},
					},
				},
			}).
			Build()
		r := &Reconciler{Client: c}
		_, ok, err := r.resolveSourceNode(ctx, "flow-a")
		require.NoError(t, err)
		assert.False(t, ok,
			"a flow without an Origin location is not yet a usable source; "+
				"the reconciler must mark its receivers Pending rather than "+
				"misroute the mirror to a Mirroring/Stale node")
	})
}

func TestMirrorToReceivers_FiltersByFlowID(t *testing.T) {
	ctx := context.Background()
	scheme := unitScheme(t)

	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
		Spec:       mxlv1alpha1.MxlFlowMirrorSpec{FlowID: "flow-a"},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r-match"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "flow-a"},
			},
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r-skip"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "flow-b"},
			},
			&mxlv1alpha1.MxlReceiver{
				// Different namespace: must not be enqueued even if
				// flowID matches; the mirror lives in "ns" and only
				// receivers in "ns" can own it.
				ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "r-other-ns"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "flow-a"},
			},
		).
		Build()
	r := &Reconciler{Client: c}

	out := r.mirrorToReceivers(ctx, mirror)
	require.Len(t, out, 1)
	assert.Equal(t, reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: "ns", Name: "r-match"},
	}, out[0])
}

func TestMirrorToReceivers_NonMirrorObjectReturnsNil(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
	r := &Reconciler{Client: c}

	out := r.mirrorToReceivers(ctx, &corev1.Pod{})
	assert.Nil(t, out,
		"the mapper guards against a misconfigured Watches by ignoring "+
			"non-MxlFlowMirror events; if this regresses, every Pod event "+
			"would enqueue every receiver in the cluster")
}

func Test_ensureMirror_UpdatesSourceNodeOnRescheduling(t *testing.T) {
	ctx := context.Background()
	const (
		flowID = "11111111-2222-3333-4444-555555555555"
		recvNS = "ns"
		recvN  = "r"
		oldSrc = "n-old"
		newSrc = "n-new"
		tgt    = "n-target"
	)

	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: recvNS, Name: recvN},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			FlowID:   flowID,
			Provider: mxlv1alpha1.ProviderTCP,
		},
	}

	// A pre-existing mirror with the old source node and an
	// agent-stamped Requestor. The patch must update SourceNode and
	// stamp the receiver label, but must not clobber Requestor.
	existing := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: recvNS,
			Name:      mirrorName(flowID, tgt),
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: oldSrc,
			TargetNode: tgt,
			Provider:   mxlv1alpha1.ProviderTCP,
			Requestor: &mxlv1alpha1.PodRef{
				Namespace: recvNS,
				Name:      "consumer-pod",
				UID:       "pod-uid",
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(existing).
		Build()
	r := &Reconciler{Client: c}

	got, err := r.ensureMirror(ctx, recv, newSrc, nodeTarget{node: tgt, namespace: recvNS})
	require.NoError(t, err)
	require.NotNil(t, got)

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: recvNS, Name: existing.Name}, &live))
	assert.Equal(t, newSrc, live.Spec.SourceNode,
		"a producer rescheduling onto a different node must rewrite the "+
			"existing mirror's SourceNode; without that the source-side "+
			"gateway on the old node would keep trying to read a flow that "+
			"no longer has a writer there")
	assert.Equal(t, tgt, live.Spec.TargetNode, "TargetNode is the mirror's identity key and must not be rewritten")
	assert.Equal(t, mxlv1alpha1.ProviderTCP, live.Spec.Provider)
	require.NotNil(t, live.Spec.Requestor, "merge-patch must preserve agent-owned Requestor")
	assert.Equal(t, "consumer-pod", live.Spec.Requestor.Name)
	assert.Equal(t, recvN, live.Labels[mxlv1alpha1.LabelCreatedByReceiver],
		"the receiver label is what the GC pass uses to recognise its own mirrors")
}

func Test_ensureMirror_StampsLabelOnCreate(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			FlowID:   "11111111-2222-3333-4444-555555555555",
			Provider: mxlv1alpha1.ProviderTCP,
		},
	}
	c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
	r := &Reconciler{Client: c}

	m, err := r.ensureMirror(ctx, recv, "n-src", nodeTarget{node: "n-tgt", namespace: "ns"})
	require.NoError(t, err)
	assert.Equal(t, "r", m.Labels[mxlv1alpha1.LabelCreatedByReceiver])
}

func Test_PodWatch_EnqueuesReceiversOnNodeChange(t *testing.T) {
	ctx := context.Background()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "consumer"},
	}

	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r-a"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-a"},
			},
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r-b"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-b"},
			},
			&mxlv1alpha1.MxlReceiver{
				// Different namespace - must not be enqueued by an event
				// in "ns"; the pod and the receiver have to share a
				// namespace for the selector match to apply.
				ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "r-other"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-a"},
			},
		).
		Build()
	r := &Reconciler{Client: c}

	out := r.podToReceivers(ctx, pod)
	names := make([]string, 0, len(out))
	for _, req := range out {
		assert.Equal(t, "ns", req.Namespace)
		names = append(names, req.Name)
	}
	sort.Strings(names)
	assert.Equal(t, []string{"r-a", "r-b"}, names,
		"a pod event must enqueue every receiver in the pod's namespace; "+
			"the reconciler then re-evaluates the selector. Cross-namespace "+
			"receivers stay out so each namespace's intent stays its own.")

	// Predicate gates: Update is suppressed when spec.nodeName has not
	// changed. Without this the receiver would re-reconcile on every
	// pod status tick (container ready, IP assignment, conditions).
	pred := podNodeChangePredicate()
	assert.False(t, pred.Update(event.UpdateEvent{
		ObjectOld: &corev1.Pod{Spec: corev1.PodSpec{NodeName: "n1"}},
		ObjectNew: &corev1.Pod{Spec: corev1.PodSpec{NodeName: "n1"}},
	}), "pod status churn must not enqueue the receiver")
	assert.True(t, pred.Update(event.UpdateEvent{
		ObjectOld: &corev1.Pod{Spec: corev1.PodSpec{NodeName: ""}},
		ObjectNew: &corev1.Pod{Spec: corev1.PodSpec{NodeName: "n1"}},
	}), "initial scheduling decision must enqueue the receiver")
	assert.True(t, pred.Update(event.UpdateEvent{
		ObjectOld: &corev1.Pod{Spec: corev1.PodSpec{NodeName: "n1"}},
		ObjectNew: &corev1.Pod{Spec: corev1.PodSpec{NodeName: "n2"}},
	}), "rescheduling onto a different node must enqueue the receiver")
	assert.True(t, pred.Create(event.CreateEvent{Object: pod}))
	assert.True(t, pred.Delete(event.DeleteEvent{Object: pod}))
}

func Test_FlowWatch_EnqueuesReceiversOnOriginRotation(t *testing.T) {
	ctx := context.Background()

	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "f-a"},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: "f-a"},
	}

	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithIndex(&mxlv1alpha1.MxlReceiver{}, flowIDIndex, func(o client.Object) []string {
			recv, ok := o.(*mxlv1alpha1.MxlReceiver)
			if !ok || recv.Spec.FlowID == "" {
				return nil
			}
			return []string{recv.Spec.FlowID}
		}).
		WithObjects(
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "r-match-1"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-a"},
			},
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns-2", Name: "r-match-2"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-a"},
			},
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "r-skip"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-other"},
			},
		).
		Build()
	r := &Reconciler{Client: c}

	out := r.flowToReceivers(ctx, flow)
	gotKeys := make(map[client.ObjectKey]struct{}, len(out))
	for _, req := range out {
		gotKeys[req.NamespacedName] = struct{}{}
	}
	assert.Equal(t, map[client.ObjectKey]struct{}{
		{Namespace: "ns-1", Name: "r-match-1"}: {},
		{Namespace: "ns-2", Name: "r-match-2"}: {},
	}, gotKeys,
		"every receiver whose spec.flowID matches the flow ID must be "+
			"enqueued, across every namespace; the operator owns receivers "+
			"cluster-wide and an origin rotation has to wake them all.")
}

func Test_FlowWatch_NonFlowObjectReturnsNil(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
	r := &Reconciler{Client: c}
	assert.Nil(t, r.flowToReceivers(ctx, &corev1.Pod{}))
}

func Test_PodWatch_NonPodObjectReturnsNil(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
	r := &Reconciler{Client: c}
	assert.Nil(t, r.podToReceivers(ctx, &mxlv1alpha1.MxlFlow{}))
}
