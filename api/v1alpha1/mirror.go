package v1alpha1

import "strings"

// MirrorName produces the MxlFlowMirror object name for a (flowID,
// targetNode) pair, lowercased with every rune outside [a-z0-9-]
// replaced by "-" so the result is a DNS subdomain. FlowIDs are
// UUIDs and node names are DNS-compliant, so the substitution is a
// guard rather than a routine rewrite.
//
// The name is the key on which the two ownership domains meet: the
// agent's intent dispatcher and the operator's receiver reconciler
// both derive it from the same inputs, so a mirror one of them
// created is the mirror the other finds. Computing it here keeps
// that agreement from depending on two copies staying in step.
func MirrorName(flowID, targetNode string) string {
	joined := strings.ToLower(flowID + "--" + targetNode)
	var b strings.Builder
	b.Grow(len(joined))
	for _, c := range joined {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b.WriteRune(c)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}
