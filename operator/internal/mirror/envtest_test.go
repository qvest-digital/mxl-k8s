package mirror

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

// A reader replaced on the same node, against a real API server and the
// controller running in a manager: the old pod's delete event reaches
// the controller through its pod watch, and the agent's handover is the
// merge patch it sends, preconditioned on the version it read.
func TestReplacedReaderKeepsItsMirrorAgainstAnAPIServer(t *testing.T) {
	env, err := testutil.Start()
	if err != nil {
		t.Skipf("envtest unavailable: %v", err)
	}
	t.Cleanup(env.Stop)
	ctx := context.Background()
	c := env.Client
	ns := env.NewNamespace(t)

	for _, n := range []string{srcNode, tgtNode} {
		require.NoError(t, c.Create(ctx, node(n)))
	}
	flow := flowOriginAt(srcNode)
	flow.Spec.Definition.Raw = []byte(`{"id":"` + flowID + `"}`)
	status := flow.Status
	require.NoError(t, c.Create(ctx, flow))
	flow.Status = status
	require.NoError(t, c.Status().Update(ctx, flow))

	reader := func(name string) *corev1.Pod {
		p := pod(name, "")
		p.Namespace, p.UID = ns, ""
		require.NoError(t, c.Create(ctx, p))
		return p
	}
	old, repl := reader("engine-old"), reader("engine-new")

	m := newMirror(withIntentLabel())
	m.Namespace = ns
	m.Labels[mxlv1alpha1.LabelRequestorPodUID] = string(old.UID)
	m.Spec.Requestor = &mxlv1alpha1.PodRef{Namespace: ns, Name: old.Name, UID: string(old.UID)}
	require.NoError(t, c.Create(ctx, m))

	skip := true
	mgr, err := ctrl.NewManager(env.Cfg, manager.Options{
		Scheme:                 env.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: &skip},
	})
	require.NoError(t, err)
	require.NoError(t, (&Reconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		APIReader: mgr.GetAPIReader(),
	}).SetupWithManager(mgr))
	mctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Start(mctx)
	}()
	t.Cleanup(func() { cancel(); <-done })

	key := client.ObjectKeyFromObject(m)
	claimed := func() *metav1.Condition {
		var got mxlv1alpha1.MxlFlowMirror
		if err := c.Get(ctx, key, &got); err != nil {
			return nil
		}
		return meta.FindStatusCondition(got.Status.Conditions, mxlv1alpha1.ConditionTypeClaimed)
	}
	require.Eventually(t, func() bool {
		cond := claimed()
		return cond != nil && cond.Reason == mxlv1alpha1.ReasonRequestorLive
	}, 10*time.Second, 50*time.Millisecond, "the controller never judged the mirror claimed by its live requestor")

	gone := func() bool {
		var got mxlv1alpha1.MxlFlowMirror
		return apierrors.IsNotFound(c.Get(ctx, key, &got))
	}
	require.NoError(t, c.Delete(ctx, old, client.GracePeriodSeconds(0)))
	require.Eventually(t, func() bool {
		cond := claimed()
		return gone() || cond != nil && cond.Status == metav1.ConditionFalse
	}, 10*time.Second, 50*time.Millisecond, "the requestor's delete never reached the controller")

	// The agent sweeps every ten seconds; the old reader going must not
	// have taken the mirror before it could.
	require.Never(t, gone, 3*time.Second, 100*time.Millisecond,
		"collected the moment its requestor went, leaving the pod still reading it a ring nothing writes")

	var cur mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, key, &cur))
	handOver := func(rv string) error {
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{
				"resourceVersion": rv,
				"labels":          map[string]string{mxlv1alpha1.LabelRequestorPodUID: string(repl.UID)},
			},
			"spec": map[string]any{
				"requestor": mxlv1alpha1.PodRef{Namespace: ns, Name: repl.Name, UID: string(repl.UID)},
			},
		})
		require.NoError(t, err)
		return c.Patch(ctx, &mxlv1alpha1.MxlFlowMirror{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: m.Name}},
			client.RawPatch(types.MergePatchType, patch))
	}
	stale := m.ResourceVersion
	require.NotEqual(t, stale, cur.ResourceVersion)
	assert.True(t, apierrors.IsConflict(handOver(stale)),
		"a handover read before the controller last wrote must not land on what it wrote since")
	require.NoError(t, handOver(cur.ResourceVersion))

	require.Eventually(t, func() bool {
		cond := claimed()
		return cond != nil && cond.Reason == mxlv1alpha1.ReasonRequestorLive &&
			cond.Message == "requestor pod "+ns+"/"+repl.Name+" is present"
	}, 10*time.Second, 50*time.Millisecond, "the controller did not take the handover as a claim")
}
