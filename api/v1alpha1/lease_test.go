package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeaseName_Format(t *testing.T) {
	// Pin the wire format. A change here would silently desync the
	// agent renewer from the operator freshness checker and demote
	// every Origin location in the cluster.
	assert.Equal(t,
		"mxl-flow-11111111-2222-3333-4444-555555555555-node-a",
		LeaseName("11111111-2222-3333-4444-555555555555", "node-a"),
	)
}

func TestParseLeaseName_RoundTrip(t *testing.T) {
	const flowID = "11111111-2222-3333-4444-555555555555"
	cases := []struct {
		name     string
		nodeName string
	}{
		{name: "bare-metal node name", nodeName: "n07"},
		{name: "node name with dashes", nodeName: "node-a"},
		// The case the last-dash split got wrong, and the one every
		// AWS cluster is made of.
		{name: "ec2 private-dns node name", nodeName: "ip-10-66-1-235"},
		{name: "fqdn node name", nodeName: "worker-1.eu-central-1.compute.internal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotFlow, gotNode, ok := ParseLeaseName(LeaseName(flowID, tc.nodeName))
			require.True(t, ok,
				"LeaseName output must round-trip through ParseLeaseName; "+
					"the Lease watches enqueue whatever comes back, so a "+
					"wrong split wakes nothing and every consumer waits out "+
					"the renewal window instead")
			assert.Equal(t, flowID, gotFlow)
			assert.Equal(t, tc.nodeName, gotNode)
		})
	}
}

func TestParseLeaseName_Rejects(t *testing.T) {
	// The first field is a canonical 8-4-4-4-12 flow id and the rest
	// is the node name. Anything else is not an Origin Lease name and
	// has to be rejected rather than split somewhere arbitrary, so a
	// caller never passes an invented flow id downstream.
	cases := []struct {
		name  string
		input string
	}{
		{name: "empty input", input: ""},
		{name: "missing prefix", input: "lease-flow-11111111-2222-3333-4444-555555555555-n1"},
		{name: "prefix only", input: "mxl-flow-"},
		{name: "no node name", input: "mxl-flow-11111111-2222-3333-4444-555555555555"},
		{name: "empty node name", input: "mxl-flow-11111111-2222-3333-4444-555555555555-"},
		{name: "first field is not a flow id", input: "mxl-flow-not-a-uuid-at-all-really-nope-n1"},
		{name: "another component's lease", input: "mxl-flow-controller-leader-election"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flow, node, ok := ParseLeaseName(tc.input)
			assert.False(t, ok)
			assert.Empty(t, flow)
			assert.Empty(t, node)
		})
	}
}
