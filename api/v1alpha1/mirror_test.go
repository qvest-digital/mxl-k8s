package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMirrorName_Format(t *testing.T) {
	// Pin the wire format. The agent's intent dispatcher and the
	// operator's receiver reconciler both address mirrors by this
	// name; a change here would stop them meeting on one object and
	// let both create their own for the same (flow, target node).
	assert.Equal(t,
		"11111111-2222-3333-4444-555555555555--node-a",
		MirrorName("11111111-2222-3333-4444-555555555555", "node-a"),
	)
}

func TestMirrorName_SanitisesToDNSSubdomain(t *testing.T) {
	cases := []struct {
		name       string
		flowID     string
		targetNode string
		want       string
	}{
		{
			name:       "uppercase is lowered",
			flowID:     "ABC",
			targetNode: "Node-B",
			want:       "abc--node-b",
		},
		{
			name:       "dots and underscores become dashes",
			flowID:     "a.b_c",
			targetNode: "ip-10.0.0.1",
			want:       "a-b-c--ip-10-0-0-1",
		},
		{
			name:       "empty target still yields the separator",
			flowID:     "abc",
			targetNode: "",
			want:       "abc--",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, MirrorName(tc.flowID, tc.targetNode))
		})
	}
}

func TestDefaultLeaseDuration_MatchesRenewerAndCheckerExpectation(t *testing.T) {
	// The renewer stamps this on every Lease and both freshness
	// checkers fall back to it. Writer and checker disagreeing on
	// the window would make the two sides disagree about where a
	// flow's Origin is.
	assert.Equal(t, "30s", DefaultLeaseDuration.String())
}
