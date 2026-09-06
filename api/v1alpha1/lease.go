package v1alpha1

import (
	"regexp"
	"strings"
	"time"
)

// DefaultLeaseDuration is the validity window stamped on an Origin
// Lease that carries no explicit spec.leaseDurationSeconds. The
// renewer and both freshness checkers read it from here: a renewer
// writing on one window while a checker expires on another would
// make the two sides disagree about where a flow's Origin is.
const DefaultLeaseDuration = 30 * time.Second

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

// flowIDLength is the length of the canonical 8-4-4-4-12 UUID a flow
// id always is. It is what makes a Lease name decomposable: both
// segments may contain dashes, so only a fixed-width first field
// separates them unambiguously.
const flowIDLength = 36

// flowIDRE is FlowIDPattern compiled once, so ParseLeaseName rejects a
// name whose first field is not a flow id rather than splitting it
// somewhere arbitrary.
var flowIDRE = regexp.MustCompile(FlowIDPattern)

// ParseLeaseName reverses LeaseName: it strips the literal
// "mxl-flow-" prefix, takes the canonical UUID that follows as the
// flow id, and treats the rest as the node name. ok is false when the
// prefix is missing, when the first field is not a flow id, or when
// no node name follows it.
//
// Splitting at the last dash instead is what this replaces. It reads
// the right answer only when the node name has no dash in it, which
// is true of a bare-metal "n07" and false of every node an AWS
// cluster names -- "ip-10-66-1-235" parsed as node "235" belonging to
// a flow id with "-ip-10-66-1" glued on the end. Nothing failed
// loudly: the Lease watches simply enqueued a flow id no object had,
// so a Lease appearing or being released woke nothing, and every
// mirror and flow on those clusters waited out the renewal window
// instead.
func ParseLeaseName(name string) (flowID, nodeName string, ok bool) {
	rest, found := strings.CutPrefix(name, leaseNamePrefix)
	if !found || len(rest) < flowIDLength+2 || rest[flowIDLength] != '-' {
		return "", "", false
	}
	flowID, nodeName = rest[:flowIDLength], rest[flowIDLength+1:]
	if !flowIDRE.MatchString(flowID) {
		return "", "", false
	}
	return flowID, nodeName, true
}
