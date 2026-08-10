package domain

import (
	"encoding/json"
	"strings"
)

// groupHintTag is the NMOS tag carrying a flow's group membership. Its
// value is "<group>:<role>", so the two halves are what relate a video
// flow to the audio flow published beside it.
const groupHintTag = "urn:x-nmos:tag:grouphint/v1.0"

// FlowDef is the part of a flow's flow_def.json this exporter turns
// into labels. Fields absent from a definition stay empty rather than
// failing the parse: the file is written by whichever media function
// created the flow, and only id is guaranteed.
type FlowDef struct {
	Label       string              `json:"label"`
	Description string              `json:"description"`
	Format      string              `json:"format"`
	MediaType   string              `json:"media_type"`
	Colorspace  string              `json:"colorspace"`
	Version     string              `json:"version"`
	Tags        map[string][]string `json:"tags"`
}

// ParseFlowDef decodes a flow_def.json document.
func ParseFlowDef(raw string) (*FlowDef, error) {
	def := &FlowDef{}
	if err := json.Unmarshal([]byte(raw), def); err != nil {
		return nil, err
	}
	return def, nil
}

// GroupHint splits the NMOS group hint into its group name and role.
// Both are empty when the tag is absent, which is the case for a flow
// published without one.
func (d *FlowDef) GroupHint() (name, role string) {
	values := d.Tags[groupHintTag]
	if len(values) == 0 {
		return "", ""
	}
	name, role, _ = strings.Cut(values[0], ":")
	return name, role
}
