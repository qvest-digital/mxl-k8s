package receiver

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

		res, err := r.resolveSourceNode(ctx, "flow-a")
		require.NoError(t, err)
		assert.True(t, res.Found)
		assert.Equal(t, "n-origin", res.Node)
	})

	t.Run("flow not found returns false without error", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
		r := &Reconciler{Client: c}
		res, err := r.resolveSourceNode(ctx, "missing")
		require.NoError(t, err)
		assert.False(t, res.Found)
		assert.False(t, res.AllStale)
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
		res, err := r.resolveSourceNode(ctx, "flow-a")
		require.NoError(t, err)
		assert.False(t, res.Found,
			"a flow without an Origin location is not yet a usable source; "+
				"the reconciler must mark its receivers Pending rather than "+
				"misroute the mirror to a Mirroring/Stale node")
		assert.False(t, res.AllStale,
			"AllStale is the 'origin existed but lease expired' signal; "+
				"a flow that never had an Origin should not raise it")
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
	// agent-stamped Requestor. The patch must update SourceNode but
	// must not clobber Requestor and must not rewrite the receiver
	// label -- label drift is no longer corrected on Patch (it would
	// pingpong between co-resident receivers); label is only stamped
	// on Create.
	existing := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: recvNS,
			Name:      mirrorName(flowID, tgt),
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByReceiver: "first-creator",
			},
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
	assert.Equal(t, "first-creator", live.Labels[mxlv1alpha1.LabelCreatedByReceiver],
		"label drift must not be corrected on Patch; the label is a "+
			"first-creator diagnostic tag and rewriting it on every reconcile "+
			"is the pingpong PR #79 removed")
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

func Test_LeaseWatch_EnqueuesReceiversByFlowID(t *testing.T) {
	ctx := context.Background()
	const flowID = "11111111-2222-3333-4444-555555555555"

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mxlv1alpha1.LeaseNamespace,
			Name:      mxlv1alpha1.LeaseName(flowID, "nodea"),
		},
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
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: flowID},
			},
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns-2", Name: "r-match-2"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: flowID},
			},
			&mxlv1alpha1.MxlReceiver{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "r-skip"},
				Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-other"},
			},
		).
		Build()
	r := &Reconciler{Client: c}

	out := r.leaseToReceivers(ctx, lease)
	got := make(map[client.ObjectKey]struct{}, len(out))
	for _, req := range out {
		got[req.NamespacedName] = struct{}{}
	}
	assert.Equal(t, map[client.ObjectKey]struct{}{
		{Namespace: "ns-1", Name: "r-match-1"}: {},
		{Namespace: "ns-2", Name: "r-match-2"}: {},
	}, got,
		"every receiver whose spec.flowID matches the flow ID encoded in "+
			"the Lease name must be enqueued, across every namespace; the "+
			"Lease is the canonical liveness signal so demote and promote "+
			"on Lease expiry must reach every receiver bound to that flow.")
}

func Test_LeaseWatch_NonLeaseObjectReturnsNil(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
	r := &Reconciler{Client: c}
	assert.Nil(t, r.leaseToReceivers(ctx, &corev1.Pod{}),
		"the mapper guards against a misconfigured Watches by ignoring "+
			"non-Lease events; without this every Pod event would feed "+
			"ParseLeaseName a pod name and either spuriously enqueue or "+
			"silently drop.")
}

func TestReceiver_LeaseToReceivers_MalformedNameDropped(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(&mxlv1alpha1.MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
			Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f-a"},
		}).
		Build()
	r := &Reconciler{Client: c}

	cases := []string{
		"unrelated-lease",         // no mxl-flow- prefix
		"mxl-flow-",               // empty remainder
		"mxl-flow-only-prefix",    // no trailing -node
		"mxl-flow-flow-id-",       // empty node segment
		"kube-controller-manager", // leader election lease
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			lease := &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: mxlv1alpha1.LeaseNamespace,
					Name:      name,
				},
			}
			assert.Empty(t, r.leaseToReceivers(ctx, lease),
				"a Lease whose name does not match the LeaseName format must "+
					"not enqueue anything; otherwise an unrelated coordination "+
					"Lease (leader election, kube-node-lease) that leaked into "+
					"mxl-system would wake every receiver in the cluster")
		})
	}
}

func TestPodPredicate_DenyKubeSystem(t *testing.T) {
	pred := podNodeChangePredicate()

	pod := func(ns string, node string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "p"},
			Spec:       corev1.PodSpec{NodeName: node},
		}
	}

	denied := []string{"kube-system", "kube-public", "kube-node-lease"}
	for _, ns := range denied {
		t.Run("deny/"+ns, func(t *testing.T) {
			obj := pod(ns, "n1")
			assert.False(t, pred.Create(event.CreateEvent{Object: obj}),
				"Create event from %s must be dropped before the reconcile "+
					"queue: the receiver never schedules consumer pods there, "+
					"so every wakeup it would cause is waste", ns)
			assert.False(t, pred.Delete(event.DeleteEvent{Object: obj}),
				"Delete event from %s must be dropped", ns)
			assert.False(t, pred.Update(event.UpdateEvent{
				ObjectOld: pod(ns, "n1"),
				ObjectNew: pod(ns, "n2"),
			}), "Update event from %s must be dropped even when node changes", ns)
		})
	}

	allowed := []string{"mxl-system", "default", "app"}
	for _, ns := range allowed {
		t.Run("allow/"+ns, func(t *testing.T) {
			obj := pod(ns, "n1")
			assert.True(t, pred.Create(event.CreateEvent{Object: obj}),
				"Create event from %s must pass; the operator schedules "+
					"consumer pods into application namespaces and may also "+
					"co-locate with the agent in mxl-system", ns)
			assert.True(t, pred.Delete(event.DeleteEvent{Object: obj}),
				"Delete event from %s must pass", ns)
			assert.True(t, pred.Update(event.UpdateEvent{
				ObjectOld: pod(ns, ""),
				ObjectNew: pod(ns, "n1"),
			}), "Update event from %s with node change must pass", ns)
		})
	}
}

func TestReceiver_LeaseInMxlSystemPredicate(t *testing.T) {
	pred := leaseInMxlSystem()

	cases := []struct {
		ns   string
		want bool
	}{
		{ns: mxlv1alpha1.LeaseNamespace, want: true},
		{ns: "kube-system", want: false},
		{ns: "kube-node-lease", want: false},
		{ns: "default", want: false},
		{ns: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.ns, func(t *testing.T) {
			obj := &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Namespace: tc.ns, Name: "x"},
			}
			assert.Equal(t, tc.want, pred.Create(event.CreateEvent{Object: obj}))
			assert.Equal(t, tc.want, pred.Delete(event.DeleteEvent{Object: obj}))
			assert.Equal(t, tc.want, pred.Update(event.UpdateEvent{ObjectOld: obj, ObjectNew: obj}))
			assert.Equal(t, tc.want, pred.Generic(event.GenericEvent{Object: obj}))
		})
	}
}

// TestSetupWithManager_RegistersOwnerUIDIndex asserts the fake
// client honours a MatchingFields lookup on ownerUIDIndex after
// the same extractor SetupWithManager uses gets registered via
// WithIndex. A passing test means the extractor shape (returning
// every owner UID as its own string) and the index name are
// internally consistent: any later refactor that splits the name
// from the extractor will surface as an empty result here rather
// than a silent miss at runtime.
func TestSetupWithManager_RegistersOwnerUIDIndex(t *testing.T) {
	ctx := context.Background()

	mirrorWithOwners := func(name string, uids ...string) *mxlv1alpha1.MxlFlowMirror {
		m := &mxlv1alpha1.MxlFlowMirror{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
			Spec:       mxlv1alpha1.MxlFlowMirrorSpec{FlowID: "f"},
		}
		for _, uid := range uids {
			m.OwnerReferences = append(m.OwnerReferences, metav1.OwnerReference{
				APIVersion: mxlv1alpha1.GroupVersion.String(),
				Kind:       "MxlReceiver",
				Name:       "r-" + uid,
				UID:        types.UID(uid),
			})
		}
		return m
	}

	extractor := func(o client.Object) []string {
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
	}

	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithIndex(&mxlv1alpha1.MxlFlowMirror{}, ownerUIDIndex, extractor).
		WithObjects(
			mirrorWithOwners("solo", "uid-a"),
			mirrorWithOwners("shared", "uid-a", "uid-b"),
			mirrorWithOwners("other", "uid-c"),
			mirrorWithOwners("orphan"),
		).
		Build()

	var matchA mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(ctx, &matchA, client.MatchingFields{ownerUIDIndex: "uid-a"}))
	gotA := map[string]struct{}{}
	for _, m := range matchA.Items {
		gotA[m.Name] = struct{}{}
	}
	assert.Equal(t, map[string]struct{}{"solo": {}, "shared": {}}, gotA,
		"the extractor must register every owner UID on the object so a "+
			"mirror with two co-owners is reachable via either UID; "+
			"otherwise the second receiver's lookup would silently miss it")

	var matchB mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(ctx, &matchB, client.MatchingFields{ownerUIDIndex: "uid-b"}))
	require.Len(t, matchB.Items, 1)
	assert.Equal(t, "shared", matchB.Items[0].Name)

	var matchNone mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(ctx, &matchNone, client.MatchingFields{ownerUIDIndex: "uid-missing"}))
	assert.Empty(t, matchNone.Items)
}

func TestMirrorNameForReceiver_SameNs_NoSuffix(t *testing.T) {
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
		Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "11111111-2222-3333-4444-555555555555"},
	}
	got := mirrorNameForReceiver(recv, nodeTarget{node: "node-a", namespace: "ns"})
	assert.Equal(t, mirrorName(recv.Spec.FlowID, "node-a"), got,
		"same-namespace receivers share the base mirror name; the "+
			"OwnerReferences refcount is what scopes the mirror to its "+
			"co-resident receivers")
}

func TestMirrorNameForReceiver_PodSelectorIsAlwaysSameNs(t *testing.T) {
	// A PodSelector receiver lists pods in its own namespace, so the
	// resolved target is always same-ns regardless of where the
	// selector matches; the mirror name must never gain a suffix.
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r"},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			FlowID:      "f",
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
		},
	}
	got := mirrorNameForReceiver(recv, nodeTarget{node: "n", namespace: "ns"})
	assert.Equal(t, mirrorName("f", "n"), got)
}

func TestMirrorNameForReceiver_CrossNs_HasSuffix(t *testing.T) {
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "recv-ns", Name: "r"},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			FlowID: "f",
			PodRef: &mxlv1alpha1.PodRef{Namespace: "pod-ns", Name: "p"},
		},
	}
	got := mirrorNameForReceiver(recv, nodeTarget{node: "n", namespace: "pod-ns"})
	base := mirrorName("f", "n")
	require.NotEqual(t, base, got,
		"a cross-namespace PodRef must carry a per-receiver suffix; "+
			"without it two cross-ns receivers would collide on the bare "+
			"mirror name and the apiserver would reject the second Create")
	assert.True(t, len(got) == len(base)+1+8,
		"suffix is one dash plus 8 hex chars; the DNS-1123 budget for "+
			"a UUID-based mirror name absorbs that comfortably")
}

func TestMirrorNameForReceiver_DistinctReceiversHashDifferently(t *testing.T) {
	a := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ra-ns", Name: "ra"},
		Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f"},
	}
	b := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rb-ns", Name: "rb"},
		Spec:       mxlv1alpha1.MxlReceiverSpec{FlowID: "f"},
	}
	tgt := nodeTarget{node: "n", namespace: "pod-ns"}
	assert.NotEqual(t, mirrorNameForReceiver(a, tgt), mirrorNameForReceiver(b, tgt),
		"two cross-ns receivers must produce two distinct mirror names so "+
			"both can coexist in the same target namespace")
}

func TestShortHash_DNSSafe(t *testing.T) {
	cases := []string{
		"ns/r",
		"a-very-long-name-with-many-dashes/receiver",
		"unicode-ns/Receiver",
	}
	for _, s := range cases {
		got := shortHash(s)
		assert.Len(t, got, 8, "shortHash must be 8 chars; %q produced %q", s, got)
		for _, c := range got {
			ok := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
			assert.True(t, ok,
				"shortHash must be lowercase hex so the result keeps the "+
					"composed mirror name DNS-1123-safe; %q produced %q", s, got)
		}
	}

	// Determinism is the contract OwnerReferences refcount depends on:
	// two reconciles of the same receiver must address the same mirror.
	assert.Equal(t, shortHash("ns/r"), shortHash("ns/r"))
}

func TestEnsureOwnerRef_AppendsAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r", UID: "recv-uid"},
	}
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
	}
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(mirror).
		Build()
	r := &Reconciler{Client: c}

	require.NoError(t, r.ensureOwnerRef(ctx, recv, mirror))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, &live))
	require.Len(t, live.OwnerReferences, 1)
	assert.Equal(t, types.UID("recv-uid"), live.OwnerReferences[0].UID)
	require.NotNil(t, live.OwnerReferences[0].Controller)
	assert.False(t, *live.OwnerReferences[0].Controller,
		"Controller must be false; multiple co-owners is the whole point")
	require.NotNil(t, live.OwnerReferences[0].BlockOwnerDeletion)
	assert.False(t, *live.OwnerReferences[0].BlockOwnerDeletion,
		"BlockOwnerDeletion must be false; receiver delete must not wait "+
			"on the co-owned mirror finalising")

	// Idempotent: a second call must not duplicate the entry.
	require.NoError(t, r.ensureOwnerRef(ctx, recv, mirror))
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, &live))
	assert.Len(t, live.OwnerReferences, 1,
		"ensureOwnerRef must be a no-op when the receiver is already an owner; "+
			"a second Update on every reconcile would feed the pingpong")
}

func TestEnsureOwnerRef_PreservesSiblingOwners(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r", UID: "recv-uid"},
	}
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "m",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: mxlv1alpha1.GroupVersion.String(),
					Kind:       "MxlReceiver",
					Name:       "sibling",
					UID:        "sibling-uid",
				},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(mirror).
		Build()
	r := &Reconciler{Client: c}

	require.NoError(t, r.ensureOwnerRef(ctx, recv, mirror))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, &live))
	require.Len(t, live.OwnerReferences, 2,
		"the sibling owner ref must survive; a JSON merge-patch would replace "+
			"the entire ownerReferences array per RFC 7396 and silently strip it")
	uids := map[types.UID]struct{}{}
	for _, or := range live.OwnerReferences {
		uids[or.UID] = struct{}{}
	}
	assert.Contains(t, uids, types.UID("sibling-uid"))
	assert.Contains(t, uids, types.UID("recv-uid"))
}

func TestRemoveOwnerRef_RemovesOnlyMatchingUID(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r", UID: "recv-uid"},
	}
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "m",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: mxlv1alpha1.GroupVersion.String(), Kind: "MxlReceiver", Name: "r", UID: "recv-uid"},
				{APIVersion: mxlv1alpha1.GroupVersion.String(), Kind: "MxlReceiver", Name: "sibling", UID: "sibling-uid"},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(mirror).
		Build()
	r := &Reconciler{Client: c}

	require.NoError(t, r.removeOwnerRef(ctx, recv, mirror))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, &live))
	require.Len(t, live.OwnerReferences, 1,
		"only the matching UID must be removed; without that the sibling "+
			"loses its claim and the mirror is GC'd out from under it")
	assert.Equal(t, types.UID("sibling-uid"), live.OwnerReferences[0].UID)
}

func TestRemoveOwnerRef_NoopWhenAbsent(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r", UID: "recv-uid"},
	}
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "m",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: mxlv1alpha1.GroupVersion.String(), Kind: "MxlReceiver", Name: "sibling", UID: "sibling-uid"},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(mirror).
		Build()
	r := &Reconciler{Client: c}

	require.NoError(t, r.removeOwnerRef(ctx, recv, mirror))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, &live))
	assert.Len(t, live.OwnerReferences, 1,
		"removeOwnerRef must be a no-op when the receiver is not an owner; "+
			"writing anyway would burn API budget on every gcOrphan pass")
}

func TestRemoveOwnerRef_ToleratesNotFound(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r", UID: "recv-uid"},
	}
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
	}
	c := fake.NewClientBuilder().WithScheme(unitScheme(t)).Build()
	r := &Reconciler{Client: c}

	require.NoError(t, r.removeOwnerRef(ctx, recv, mirror),
		"a mirror deleted out from under the controller is a normal race; "+
			"the reconciler must treat it as a successful no-op rather than "+
			"surface a NotFound error and requeue forever")
}

// mxlFlowMirrorGR names the resource group/resource pair the API
// server's conflict error embeds. Built once so the synthetic
// conflicts the tests inject look identical to a real apiserver
// rejection.
var mxlFlowMirrorGR = schema.GroupResource{
	Group:    mxlv1alpha1.GroupVersion.Group,
	Resource: "mxlflowmirrors",
}

func TestRemoveOwnerRef_RejectsDeleteOnConcurrentAdd(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r", UID: "recv-uid"},
	}
	// Sole-owner mirror: removeOwnerRef will empty the slice, set
	// deleteRV, and issue the resourceVersion-guarded Delete. The
	// interceptor mimics the race window where a sibling ensureOwnerRef
	// has appended its own owner ref between this loop's Update and the
	// guarded Delete - the apiserver bumps the RV and the precondition
	// fails with IsConflict.
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "m",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: mxlv1alpha1.GroupVersion.String(), Kind: "MxlReceiver", Name: "r", UID: "recv-uid"},
			},
		},
	}
	base := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(mirror).
		Build()

	c := interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(ctx context.Context, _ client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return apierrors.NewConflict(mxlFlowMirrorGR, obj.GetName(),
				errors.New("simulated concurrent owner add"))
		},
	})
	r := &Reconciler{Client: c}

	require.NoError(t, r.removeOwnerRef(ctx, recv, mirror),
		"a Delete precondition that fires on a concurrent ensureOwnerRef must "+
			"be swallowed: the conflict means the mirror correctly has owners "+
			"again, not that the receiver failed to release its claim. Surfacing "+
			"the error would feed back into Reconcile and the next pass would "+
			"redo the work it just bailed on.")

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, base.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, &live),
		"the precondition aborted the Delete, so the mirror must still exist; "+
			"otherwise a sibling receiver's just-added owner ref would have lost "+
			"its mirror out from under it")
	assert.Empty(t, live.OwnerReferences,
		"the receiver's own Update inside the retry loop emptied the owner ref "+
			"list before the guarded Delete was attempted; the conflict on Delete "+
			"is the only thing that kept the (now-ownerless) object alive, which is "+
			"the contract a concurrent ensureOwnerRef relies on for its append to land")
}

func TestEnsureOwnerRef_RetryOnConflict(t *testing.T) {
	ctx := context.Background()
	recv := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "r", UID: "recv-uid"},
	}
	mirror := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
	}
	base := fake.NewClientBuilder().
		WithScheme(unitScheme(t)).
		WithObjects(mirror).
		Build()

	// One-shot conflict: the first Update returns IsConflict, every
	// subsequent Update falls through to the real fake client so the
	// retry's second attempt actually mutates the mirror.
	var updates int
	c := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updates++
			if updates == 1 {
				return apierrors.NewConflict(mxlFlowMirrorGR, obj.GetName(),
					errors.New("simulated stale resourceVersion"))
			}
			return inner.Update(ctx, obj, opts...)
		},
	})
	r := &Reconciler{Client: c}

	require.NoError(t, r.ensureOwnerRef(ctx, recv, mirror),
		"ensureOwnerRef must swallow a one-shot conflict and retry against the "+
			"fresh apiserver state; surfacing the error would push the conflict "+
			"into Reconcile and the next pass would re-Get, see the stale view "+
			"the cache still holds, and loop indefinitely")
	assert.GreaterOrEqual(t, updates, 2,
		"the first Update returned IsConflict; the retry must issue a second "+
			"Update or the receiver never actually adopts the mirror")

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, base.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, &live))
	require.Len(t, live.OwnerReferences, 1,
		"the retry-after-conflict path must end with the owner ref actually "+
			"stamped; without RetryOnConflict the receiver would walk away from "+
			"a mirror it owns and apiserver-GC refcounting would silently miss it")
	assert.Equal(t, types.UID("recv-uid"), live.OwnerReferences[0].UID)
}
