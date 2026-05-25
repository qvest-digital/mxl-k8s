package v1alpha1

import "strings"

// LeaseNamespace is the namespace every per-flow Origin Lease lives
// in. Pinned so the agent's Role can be scoped to a single namespace
// instead of cluster-scoped, and so the operator's freshness checker
// knows where to look without a list call.
const LeaseNamespace = "mxl-system"

// leaseNamePrefix is the literal prefix LeaseName stamps on every
// Origin Lease object name. Kept private so ParseLeaseName and
// LeaseName share a single source of truth.
const leaseNamePrefix = "mxl-flow-"

// LeaseName produces the Lease object name for the per-(flowID,
// nodeName) Origin Lease. The agent renewer and the operator's
// freshness checker compute it from the same inputs so the dispatcher
// and the receiver never disagree on which Lease backs a given Origin
// location.
func LeaseName(flowID, nodeName string) string {
	return leaseNamePrefix + flowID + "-" + nodeName
}

// ParseLeaseName reverses LeaseName: it strips the literal
// "mxl-flow-" prefix and splits the remainder at the last "-" so a
// flowID that contains dashes (the canonical 8-4-4-4-12 UUID form
// always does) still parses back to the original two segments. ok is
// false when the prefix is missing, when the remainder has no
// trailing "-", or when either segment would be empty.
func ParseLeaseName(name string) (flowID, nodeName string, ok bool) {
	rest, found := strings.CutPrefix(name, leaseNamePrefix)
	if !found {
		return "", "", false
	}
	idx := strings.LastIndex(rest, "-")
	if idx <= 0 || idx == len(rest)-1 {
		return "", "", false
	}
	return rest[:idx], rest[idx+1:], true
}
