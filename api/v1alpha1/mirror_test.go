package v1alpha1

import (
	"strings"
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

// Each side renews its own heartbeat, so the two names differ, and
// two mirrors that share a name in different namespaces must not share
// a Lease.
func TestMirrorLeaseName(t *testing.T) {
	src := MirrorLeaseName("ns-a", "m", MirrorSideSource)
	assert.NotEqual(t, src, MirrorLeaseName("ns-a", "m", MirrorSideTarget))
	assert.NotEqual(t, src, MirrorLeaseName("ns-b", "m", MirrorSideSource))
	assert.Equal(t, src, MirrorLeaseName("ns-a", "m", MirrorSideSource), "the name is deterministic")
	assert.True(t, strings.HasPrefix(src, "mxl-mirror-"))
	_, _, isOrigin := ParseLeaseName(src)
	assert.False(t, isOrigin, "a heartbeat Lease never parses as an Origin Lease")

	long := strings.Repeat("a", 63)
	assert.LessOrEqual(t, len(MirrorLeaseName(long, strings.Repeat("b", 253), MirrorSideTarget)), 63,
		"the name fits the longest namespace and mirror name")
}

func TestTargetInfoHash(t *testing.T) {
	assert.Len(t, TargetInfoHash("x"), 64)
	assert.Equal(t, TargetInfoHash("x"), TargetInfoHash("x"))
	assert.NotEqual(t, TargetInfoHash("x"), TargetInfoHash("y"))
}

func TestIsMirrorLease(t *testing.T) {
	assert.True(t, IsMirrorLease(map[string]string{LabelMirrorSide: string(MirrorSideSource)}))
	assert.False(t, IsMirrorLease(nil), "an Origin Lease carries no side")
}
