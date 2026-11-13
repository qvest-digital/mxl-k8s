package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

// The MxlDomain schema is enforced by the API server, so its rules are
// tested against one. A value the schema admits but the typed client
// cannot decode is worse than a rejected create: every List of
// MxlDomains -- each agent's sync and the operator's informer -- then
// fails on the one bad object and no domain is served anywhere.
func TestMxlDomainSchema(t *testing.T) {
	env, err := testutil.Start()
	if err != nil {
		t.Skipf("envtest unavailable: %v", err)
	}
	t.Cleanup(env.Stop)
	ctx := context.Background()

	obj := func(name string, spec map[string]any) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "mxl.qvest-digital.com/v1alpha1",
			"kind":       "MxlDomain",
			"metadata":   map[string]any{"name": name},
			"spec":       spec,
		}}
		return u
	}
	base := func() map[string]any {
		return map[string]any{"id": "1ac254d9-a5eb-475f-a2b6-3d02a5cfbc82", "directory": "domain"}
	}

	cases := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{"valid", func(map[string]any) {}, ""},
		{"history duration", func(s map[string]any) { s["historyDuration"] = "2s" }, ""},
		{"compound history duration", func(s map[string]any) { s["historyDuration"] = "1m30s" }, ""},
		{"zero history duration", func(s map[string]any) { s["historyDuration"] = "0s" }, "historyDuration"},
		{"negative history duration", func(s map[string]any) { s["historyDuration"] = "-1s" }, "historyDuration"},
		{"history duration that is not a duration", func(s map[string]any) { s["historyDuration"] = "two seconds" }, "historyDuration"},
		{"uppercase id", func(s map[string]any) { s["id"] = "1AC254D9-A5EB-475F-A2B6-3D02A5CFBC82" }, "id"},
		{"node name as id", func(s map[string]any) { s["id"] = "n1" }, "id"},
		{"path as directory", func(s map[string]any) { s["directory"] = "a/b" }, "directory"},
		{"missing id", func(s map[string]any) { delete(s, "id") }, "id"},
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

	// id and directory cannot change once set.
	u := obj("schema-immutable", base())
	require.NoError(t, env.Client.Create(ctx, u))
	require.NoError(t, unstructured.SetNestedField(u.Object, "other", "spec", "directory"))
	err = env.Client.Update(ctx, u)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable")
}
