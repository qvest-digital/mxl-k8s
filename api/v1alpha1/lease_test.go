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
	cases := []struct {
		name     string
		flowID   string
		nodeName string
	}{
		{
			name:     "canonical UUID flow id",
			flowID:   "11111111-2222-3333-4444-555555555555",
			nodeName: "node1",
		},
		{
			name:     "flow id with embedded dashes",
			flowID:   "flow-abc-123",
			nodeName: "node1",
		},
		{
			name:     "single segment flow id",
			flowID:   "abc",
			nodeName: "node1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leaseName := LeaseName(tc.flowID, tc.nodeName)
			gotFlow, gotNode, ok := ParseLeaseName(leaseName)
			require.True(t, ok,
				"LeaseName output must round-trip through ParseLeaseName; "+
					"otherwise a renamed Lease would orphan its owner")
			assert.Equal(t, tc.flowID, gotFlow)
			assert.Equal(t, tc.nodeName, gotNode)
		})
	}
}

func TestParseLeaseName_Rejects(t *testing.T) {
	// nodeName is the trailing segment after the last "-"; a name
	// without that segment is not a valid Origin Lease name and must
	// be rejected so callers do not silently pass an empty nodeName
	// downstream.
	cases := []struct {
		name  string
		input string
	}{
		{name: "empty input", input: ""},
		{name: "missing prefix", input: "lease-flow-abc-node1"},
		{name: "prefix only", input: "mxl-flow-"},
		{name: "no trailing dash after prefix", input: "mxl-flow-onlyone"},
		{name: "trailing dash with empty node", input: "mxl-flow-abc-"},
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
