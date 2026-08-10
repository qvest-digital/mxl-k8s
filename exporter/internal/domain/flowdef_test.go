package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The group hint is what pairs a video flow with the audio flow
// published beside it, so both halves have to survive the split.
func TestGroupHintSplitsNameAndRole(t *testing.T) {
	def, err := ParseFlowDef(`{
	  "label": "MXL Test Flow, 1080p29",
	  "tags": {"urn:x-nmos:tag:grouphint/v1.0": ["Media Function XYZ:Video"]}
	}`)
	require.NoError(t, err)

	name, role := def.GroupHint()
	require.Equal(t, "Media Function XYZ", name)
	require.Equal(t, "Video", role)
}

// A flow published without the tag still yields labels, empty ones,
// rather than failing the whole metadata series.
func TestGroupHintEmptyWithoutTag(t *testing.T) {
	def, err := ParseFlowDef(`{"label": "no tags here"}`)
	require.NoError(t, err)

	name, role := def.GroupHint()
	require.Empty(t, name)
	require.Empty(t, role)
}

// Only id is guaranteed in a flow_def.json. Absent fields have to stay
// empty instead of failing the parse and dropping the flow's metadata.
func TestParseFlowDefToleratesMissingFields(t *testing.T) {
	def, err := ParseFlowDef(`{"id": "5fbec3b1-1b0f-417d-9059-8b94a47197ed"}`)
	require.NoError(t, err)
	require.Empty(t, def.Label)
	require.Empty(t, def.MediaType)
	require.Empty(t, def.Colorspace)
}

func TestParseFlowDefRejectsNonJSON(t *testing.T) {
	_, err := ParseFlowDef("not json")
	require.Error(t, err)
}
