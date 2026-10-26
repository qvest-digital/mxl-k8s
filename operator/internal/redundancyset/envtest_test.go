package redundancyset

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

// Against a real API server: the schema, and a reconcile whose status
// write and owned mirrors must be accepted by it, which the fake client
// does not check.
func TestRedundancySetAgainstAnAPIServer(t *testing.T) {
	env, err := testutil.Start()
	if err != nil {
		t.Skipf("envtest unavailable: %v", err)
	}
	t.Cleanup(env.Stop)
	ctx := context.Background()

	t.Run("schema", func(t *testing.T) {
		obj := func(name string, spec map[string]any) *unstructured.Unstructured {
			return &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "mxl.qvest-digital.com/v1alpha1",
				"kind":       "MxlFlowRedundancySet",
				"metadata":   map[string]any{"name": name},
				"spec":       spec,
			}}
		}
		ref := func(id string) map[string]any { return map[string]any{"flowID": id} }
		base := func() map[string]any {
			return map[string]any{"members": []any{ref(memberA), ref(memberB)}, "output": ref(output)}
		}
		cases := []struct {
			name    string
			mutate  func(map[string]any)
			wantErr string
		}{
			{"valid", func(map[string]any) {}, ""},
			{"one member", func(s map[string]any) { s["members"] = []any{ref(memberA)} }, "members"},
			{"eight members", func(s map[string]any) {
				m := []any{}
				for range 8 {
					m = append(m, ref(memberA))
				}
				s["members"] = m
			}, "members"},
			{"unknown granularity", func(s map[string]any) { s["granularity"] = "Line" }, "granularity"},
			{"slice granularity, never implemented", func(s map[string]any) { s["granularity"] = "Slice" }, "granularity"},
			{"unknown preference", func(s map[string]any) { s["preference"] = "Random" }, "preference"},
			{"node name as flow id", func(s map[string]any) { s["output"] = ref("n1") }, "flowID"},
			{"missing output", func(s map[string]any) { delete(s, "output") }, "output"},
			{"output is a member", func(s map[string]any) { s["output"] = ref(memberB) }, "output must not be one of the members"},
			{"output is a member, domain given", func(s map[string]any) {
				s["members"] = []any{ref(memberA), map[string]any{"flowID": output, "domain": "studio"}}
				s["output"] = map[string]any{"flowID": output, "domain": "studio"}
			}, "output must not be one of the members"},
			{"output id in another domain than the member", func(s map[string]any) {
				s["members"] = []any{ref(memberA), map[string]any{"flowID": output, "domain": "studio"}}
			}, ""},
		}
		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				spec := base()
				tc.mutate(spec)
				err := env.Client.Create(ctx, obj("schema-"+string(rune('a'+i)), spec))
				if tc.wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			})
		}

		var got mxlv1alpha1.MxlFlowRedundancySet
		require.NoError(t, env.Client.Get(ctx, types.NamespacedName{Name: "schema-a"}, &got))
		assert.Equal(t, mxlv1alpha1.RedundancyGrain, got.Spec.Granularity)
		assert.Equal(t, mxlv1alpha1.RedundancyOrdered, got.Spec.Preference)
		assert.Equal(t, 40*time.Millisecond, got.Spec.SkewBudget.Duration)
	})

	t.Run("reconcile", func(t *testing.T) {
		mirrorNS := env.NewNamespace(t)
		require.NoError(t, env.Client.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "reader"}}))
		for _, f := range []*mxlv1alpha1.MxlFlow{flow(memberA, video1080, ""), flow(memberB, video1080, "")} {
			require.NoError(t, env.Client.Create(ctx, f))
			origin := map[string]string{memberA: "node-a", memberB: "node-b"}[f.Name]
			f.Status.Locations = []mxlv1alpha1.MxlFlowLocation{{NodeName: origin, Phase: mxlv1alpha1.MxlFlowLocationOrigin}}
			require.NoError(t, env.Client.Status().Update(ctx, f))
		}
		set := newSet()
		set.UID = ""
		require.NoError(t, env.Client.Create(ctx, set))
		set.Status.Nodes = []string{"reader"}
		require.NoError(t, env.Client.Status().Update(ctx, set))

		r := &Reconciler{Client: env.Client, Namespace: mirrorNS}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: set.Name}})
		require.NoError(t, err)

		var got mxlv1alpha1.MxlFlowRedundancySet
		require.NoError(t, env.Client.Get(ctx, types.NamespacedName{Name: set.Name}, &got))
		cond := meta.FindStatusCondition(got.Status.Conditions, ConditionValid)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		ready := meta.FindStatusCondition(got.Status.Conditions, ConditionMembersReady)
		require.NotNil(t, ready)
		assert.Equal(t, metav1.ConditionTrue, ready.Status)
		assert.Equal(t, []string{"reader"}, got.Status.Nodes, "the operator's status write keeps the agents' nodes")

		var list mxlv1alpha1.MxlFlowMirrorList
		require.NoError(t, env.Client.List(ctx, &list))
		n := 0
		for _, m := range list.Items {
			if m.Namespace != mirrorNS {
				continue
			}
			n++
			require.Len(t, m.OwnerReferences, 1)
			assert.Equal(t, got.UID, m.OwnerReferences[0].UID)
			assert.Equal(t, "reader", m.Spec.TargetNode)
		}
		assert.Equal(t, 2, n)
	})

	// Each merging gateway applies its own node's entry under a field
	// manager of its own node, with the payload merge.reportStalled
	// sends. The entries must merge by node, survive the operator's
	// status update, and go when their gateway applies none.
	t.Run("stalled members", func(t *testing.T) {
		require.NoError(t, env.Client.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-c"}}))
		var set mxlv1alpha1.MxlFlowRedundancySet
		require.NoError(t, env.Client.Get(ctx, types.NamespacedName{Name: "programme"}, &set))
		set.Status.Nodes = []string{"reader", "node-c"}
		require.NoError(t, env.Client.Status().Update(ctx, &set))
		apply := func(node string, ids ...any) {
			t.Helper()
			status := map[string]any{}
			if len(ids) > 0 {
				status["stalledMembers"] = []any{map[string]any{"node": node, "flowIDs": ids}}
			}
			patch := &unstructured.Unstructured{Object: map[string]any{"status": status}}
			patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowRedundancySet"))
			patch.SetName("programme")
			require.NoError(t, env.Client.Status().Patch(ctx, patch, client.Apply,
				client.FieldOwner("mxl-merge-gateway-"+node), client.ForceOwnership))
		}
		r := &Reconciler{Client: env.Client, Namespace: env.NewNamespace(t)}
		ready := func() *metav1.Condition {
			t.Helper()
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "programme"}})
			require.NoError(t, err)
			require.NoError(t, env.Client.Get(ctx, types.NamespacedName{Name: "programme"}, &set))
			return meta.FindStatusCondition(set.Status.Conditions, ConditionMembersReady)
		}

		apply("reader", memberB)
		apply("node-c", memberA, memberB)
		cond := ready()
		assert.Equal(t, ReasonMemberStalled, cond.Reason)
		assert.Equal(t, "member "+memberA+" has not advanced on node-c; member "+memberB+
			" has not advanced on reader, node-c", cond.Message)
		assert.Len(t, set.Status.StalledMembers, 2, "the operator's status write dropped a gateway's entry")

		apply("reader")
		require.NoError(t, env.Client.Get(ctx, types.NamespacedName{Name: "programme"}, &set))
		require.Equal(t, []mxlv1alpha1.StalledMembers{{Node: "node-c", FlowIDs: []string{memberA, memberB}}},
			set.Status.StalledMembers, "an apply without its entry left it, or took another node's")
		apply("node-c")
		cond = ready()
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Empty(t, set.Status.StalledMembers)
	})
}
