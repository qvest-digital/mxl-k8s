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
// installs. The previous release writes the same payloads as this one,
// under the same field managers, and none of them names a field this
// schema dropped, so all of them are accepted. A gateway older than
// that still names status.targetInfo, status.lastGrainAt and
// status.lastSentAt, and the API server rejects a server-side apply
// naming a field the schema does not declare: that is why every
// gateway has to run the previous release before this one.
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
	progress := func(condType string) []any {
		return []any{map[string]any{
			"type": condType, "status": "True",
			"reason": mxlv1alpha1.ReasonHandshakeComplete, "message": "m",
			"lastTransitionTime": now,
		}}
	}

	require.NoError(t, apply("mxl-target-gateway-info", map[string]any{
		"targetInfoHash":             mxlv1alpha1.TargetInfoHash("descriptor"),
		"targetMaxSyncBatchSizeHint": int64(480),
		"targetInterface":            "10.0.0.2",
	}), "the previous target gateway's descriptor publish")
	require.NoError(t, apply("mxl-target-gateway", map[string]any{
		"phase":              string(mxlv1alpha1.MxlFlowMirrorReady),
		"observedGeneration": m.Generation,
		"targetAttemptCount": int64(0),
		"conditions":         progress(mxlv1alpha1.ConditionTypeTargetProgress),
	}), "the previous target gateway's progress write")
	require.NoError(t, apply("mxl-source-gateway", map[string]any{
		"attemptCount":    int64(0),
		"lastError":       "",
		"sourceInterface": "10.0.0.1",
		"conditions":      progress(mxlv1alpha1.ConditionTypeSourceProgress),
	}), "the previous source gateway's progress write")

	for _, w := range []struct {
		owner, field string
		value        any
	}{
		{"mxl-target-gateway-info", "targetInfo", "descriptor"},
		{"mxl-target-gateway", "lastGrainAt", now},
		{"mxl-source-gateway", "lastSentAt", now},
	} {
		err := apply(w.owner, map[string]any{w.field: w.value})
		require.ErrorContains(t, err, ".status."+w.field+": field not declared in schema",
			"a gateway that still writes %s is rejected, hence the upgrade order", w.field)
	}
}
