package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

// The payload must decode into the typed status the operator reads, and
// one with nothing stalled must carry no entry at all: an empty entry
// would fail the schema's minimum, and only an absent one is removed.
func TestStalledMembersApply(t *testing.T) {
	var set MxlFlowRedundancySet
	u := StalledMembersApply("programme", "n1", []string{"a", "b"})
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &set))
	require.Equal(t, "programme", set.Name)
	require.Equal(t, "MxlFlowRedundancySet", set.Kind)
	require.Equal(t, []NodeStalledMembers{{Node: "n1", FlowIDs: []string{"a", "b"}}}, set.Status.StalledMembers)

	require.Equal(t, map[string]any{}, StalledMembersApply("programme", "n1", nil).Object["status"])
}
