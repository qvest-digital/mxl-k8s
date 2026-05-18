package v1alpha1

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allProviders is the closed set of MxlFabricsProvider values accepted by
// the CRD validation (kubebuilder:validation:Enum on the type). When
// libmxl-fabrics gains a provider, both the enum and this slice grow
// together; the tests below pin that invariant.
var allProviders = []MxlFabricsProvider{
	ProviderAuto,
	ProviderTCP,
	ProviderVerbs,
	ProviderEFA,
	ProviderSHM,
}

func TestFlowIDPattern_Matches(t *testing.T) {
	re := regexp.MustCompile(FlowIDPattern)

	cases := []struct {
		name  string
		input string
		match bool
	}{
		{"canonical lower", "1bcad0e1-3d2d-4d3a-8f1d-9c8c2a2f4f01", true},
		{"all zeros", "00000000-0000-0000-0000-000000000000", true},
		{"all f", "ffffffff-ffff-ffff-ffff-ffffffffffff", true},
		{"uppercase rejected", "1BCAD0E1-3D2D-4D3A-8F1D-9C8C2A2F4F01", false},
		{"mixed case rejected", "1BCAD0e1-3d2d-4d3a-8f1d-9c8c2a2f4f01", false},
		{"missing hyphens", "1bcad0e13d2d4d3a8f1d9c8c2a2f4f01", false},
		{"wrong group sizes", "1bcad0e-13d2d-4d3a-8f1d-9c8c2a2f4f01", false},
		{"extra group", "1bcad0e1-3d2d-4d3a-8f1d-9c8c2a2f4f01-extra", false},
		{"trailing whitespace", "1bcad0e1-3d2d-4d3a-8f1d-9c8c2a2f4f01 ", false},
		{"leading whitespace", " 1bcad0e1-3d2d-4d3a-8f1d-9c8c2a2f4f01", false},
		{"non-hex chars", "1bcad0e1-3d2d-4d3a-8f1d-9c8c2a2f4f0g", false},
		{"empty", "", false},
		{"too short", "1bcad0e1-3d2d-4d3a-8f1d", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.match, re.MatchString(tc.input))
		})
	}
}

func TestMxlFabricsProvider_KnownValues(t *testing.T) {
	t.Run("each provider stringifies to its const", func(t *testing.T) {
		expected := map[MxlFabricsProvider]string{
			ProviderAuto:  "auto",
			ProviderTCP:   "tcp",
			ProviderVerbs: "verbs",
			ProviderEFA:   "efa",
			ProviderSHM:   "shm",
		}
		for p, want := range expected {
			assert.Equal(t, want, string(p), "provider %q", p)
		}
	})

	t.Run("allProviders is unique", func(t *testing.T) {
		seen := map[MxlFabricsProvider]struct{}{}
		for _, p := range allProviders {
			_, dup := seen[p]
			require.Falsef(t, dup, "duplicate provider %q in allProviders", p)
			seen[p] = struct{}{}
		}
	})

	t.Run("zero value is not a valid provider", func(t *testing.T) {
		var zero MxlFabricsProvider
		assert.NotContains(t, allProviders, zero,
			"empty provider string must not be a valid CRD enum value; "+
				"the operator relies on the kubebuilder default kicking in "+
				"when the field is omitted")
	})
}

func TestFlowPhaseConstants(t *testing.T) {
	// Pin the wire format of every CRD phase. A typo in a const that
	// flips the casing or drops a letter would cascade into stale CRs
	// that the controllers no longer match against.
	t.Run("MxlFlowLocationPhase", func(t *testing.T) {
		assert.Equal(t, MxlFlowLocationPhase("Origin"), MxlFlowLocationOrigin)
		assert.Equal(t, MxlFlowLocationPhase("Mirroring"), MxlFlowLocationMirroring)
		assert.Equal(t, MxlFlowLocationPhase("Ready"), MxlFlowLocationReady)
		assert.Equal(t, MxlFlowLocationPhase("Stale"), MxlFlowLocationStale)
	})

	t.Run("MxlReceiverPhase", func(t *testing.T) {
		assert.Equal(t, MxlReceiverPhase("Pending"), MxlReceiverPending)
		assert.Equal(t, MxlReceiverPhase("Bound"), MxlReceiverBound)
		assert.Equal(t, MxlReceiverPhase("Failed"), MxlReceiverFailed)
	})

	t.Run("MxlFlowMirrorPhase", func(t *testing.T) {
		assert.Equal(t, MxlFlowMirrorPhase("Pending"), MxlFlowMirrorPending)
		assert.Equal(t, MxlFlowMirrorPhase("Materializing"), MxlFlowMirrorMaterializing)
		assert.Equal(t, MxlFlowMirrorPhase("Ready"), MxlFlowMirrorReady)
		assert.Equal(t, MxlFlowMirrorPhase("Failed"), MxlFlowMirrorFailed)
	})
}
