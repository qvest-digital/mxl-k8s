package v1alpha1

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// StalledMembersFieldOwner is the field manager node's gateway applies its
// entry in status.stalledMembers under. It is one per node: an apply under
// a shared manager would remove every other node's entry it leaves out.
func StalledMembersFieldOwner(node string) string { return "mxl-merge-gateway-" + node }

// StalledMembersApply is the server-side apply payload for node's entry in
// set's status.stalledMembers. With no flowIDs it carries no entry, which
// removes the one the node's field manager applied before.
func StalledMembersApply(set, node string, flowIDs []string) *unstructured.Unstructured {
	status := map[string]any{}
	if len(flowIDs) > 0 {
		ids := make([]any, len(flowIDs))
		for i, id := range flowIDs {
			ids[i] = id
		}
		status["stalledMembers"] = []any{map[string]any{"node": node, "flowIDs": ids}}
	}
	u := &unstructured.Unstructured{Object: map[string]any{"status": status}}
	u.SetGroupVersionKind(GroupVersion.WithKind("MxlFlowRedundancySet"))
	u.SetName(set)
	return u
}
