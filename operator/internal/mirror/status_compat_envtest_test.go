package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

// Gateways are upgraded node by node, so a gateway of the previous
// release keeps writing a mirror's status against the CRD this release
// installs. The API server rejects a server-side apply that names a
// field the schema does not declare, so a schema without the fields
// that release writes would fail every one of its descriptor, progress
// and phase writes until its node is upgraded: its sources never dial,
// its targets never report. These are the payloads 1.1.0-rc.57 applies,
// under the field managers it applies them with.
func TestStatusSchemaAcceptsThePreviousGatewaysWrites(t *testing.T) {
	env, err := testutil.Start()
	if err != nil {
		t.Skipf("envtest unavailable: %v", err)
	}
	t.Cleanup(env.Stop)
	ctx := context.Background()
	c := env.Client

	m := newMirror()
	m.Namespace = env.NewNamespace(t)
	require.NoError(t, c.Create(ctx, m))

	now := time.Now().UTC().Format(time.RFC3339)
	apply := func(owner string, status map[string]any) error {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirror"))
		u.SetNamespace(m.Namespace)
		u.SetName(m.Name)
		if err := unstructured.SetNestedField(u.Object, status, "status"); err != nil {
			return err
		}
		return c.Status().Patch(ctx, u, client.Apply, client.FieldOwner(owner), client.ForceOwnership)
	}

	require.NoError(t, apply("mxl-target-gateway-info", map[string]any{
		"targetInfo":                 "descriptor",
		"targetInfoHash":             mxlv1alpha1.TargetInfoHash("descriptor"),
		"targetMaxSyncBatchSizeHint": int64(480),
		"targetInterface":            "10.0.0.2",
	}), "the previous target gateway's descriptor publish")
	require.NoError(t, apply("mxl-target-gateway", map[string]any{
		"phase":              string(mxlv1alpha1.MxlFlowMirrorReady),
		"observedGeneration": m.Generation,
		"targetAttemptCount": int64(0),
		"lastGrainAt":        now,
		"conditions": []any{map[string]any{
			"type": mxlv1alpha1.ConditionTypeTargetProgress, "status": "True",
			"reason": mxlv1alpha1.ReasonHandshakeComplete, "message": "grain commits observed",
			"lastTransitionTime": now,
		}},
	}), "the previous target gateway's progress write")
	require.NoError(t, apply("mxl-source-gateway", map[string]any{
		"attemptCount":    int64(0),
		"lastError":       "",
		"lastSentAt":      now,
		"sourceInterface": "10.0.0.1",
		"conditions": []any{map[string]any{
			"type": mxlv1alpha1.ConditionTypeSourceProgress, "status": "True",
			"reason": mxlv1alpha1.ReasonHandshakeComplete, "message": "initiator running",
			"lastTransitionTime": now,
		}},
	}), "the previous source gateway's progress write")

	var got mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(m), &got))
	require.NotEmpty(t, got.Status.TargetInfo)
	require.NotNil(t, got.Status.LastGrainAt)
	require.NotNil(t, got.Status.LastSentAt)

	// This release applies the same payloads under the same managers,
	// without the deprecated fields. A manager that omits a field it
	// owned releases it, and with no other owner the apiserver drops
	// it, so an upgraded gateway clears what the previous one left.
	require.NoError(t, apply("mxl-target-gateway-info", map[string]any{
		"targetInfoHash":             mxlv1alpha1.TargetInfoHash("descriptor"),
		"targetMaxSyncBatchSizeHint": int64(480),
		"targetInterface":            "10.0.0.2",
	}))
	require.NoError(t, apply("mxl-target-gateway", map[string]any{
		"phase":              string(mxlv1alpha1.MxlFlowMirrorReady),
		"observedGeneration": m.Generation,
		"targetAttemptCount": int64(0),
	}))
	require.NoError(t, apply("mxl-source-gateway", map[string]any{
		"attemptCount": int64(0),
		"lastError":    "",
	}))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(m), &got))
	require.Empty(t, got.Status.TargetInfo)
	require.Nil(t, got.Status.LastGrainAt)
	require.Nil(t, got.Status.LastSentAt)
	require.Equal(t, mxlv1alpha1.TargetInfoHash("descriptor"), got.Status.TargetInfoHash)
}
