package mirror

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

func TestFlowIDFromDef(t *testing.T) {
	id, ok := flowIDFromDef(`{"id":"5FBEC3B1-1B0F-417D-9059-8B94A47197ED","format":"urn:x-nmos:format:video"}`)
	require.True(t, ok)
	require.Equal(t, "5fbec3b1-1b0f-417d-9059-8b94a47197ed", id)

	_, ok = flowIDFromDef(`{"format":"urn:x-nmos:format:video"}`)
	require.False(t, ok)
	_, ok = flowIDFromDef(`not json`)
	require.False(t, ok)
}

// StopFlow is what a local writer taking over a placeholder calls: it
// must stop exactly the targets writing that flow in that domain.
func TestStopFlowStopsOnlyTargetsOfThatFlow(t *testing.T) {
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skip("/dev/shm not present")
	}
	open := func() *instance.Handles {
		dir, err := os.MkdirTemp("/dev/shm", "stopflow-test-*")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		h, err := instance.Open(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	a, b := open(), open()
	const flow, other = "5fbec3b1-1b0f-417d-9059-8b94a47197ed", "11111111-1111-4111-8111-111111111111"

	r := &TargetReconciler{targets: map[types.NamespacedName]*targetEntry{
		{Namespace: "ns", Name: "same-flow-same-domain"}:  {handles: a, flowID: flow},
		{Namespace: "ns2", Name: "same-flow-other-ns"}:    {handles: a, flowID: flow},
		{Namespace: "ns", Name: "same-flow-other-domain"}: {handles: b, flowID: flow},
		{Namespace: "ns", Name: "other-flow-same-domain"}: {handles: a, flowID: other},
	}}

	r.StopFlow(a.DomainPath(), flow)

	var left []string
	for k := range r.targets {
		left = append(left, k.Name)
	}
	require.ElementsMatch(t, []string{"same-flow-other-domain", "other-flow-same-domain"}, left)
}
