package v1alpha1

import "time"

// LeaseFreshness reports whether the Origin Lease for (flowID,
// nodeName) is inside its renewal window, and when it lapses.
//
// A nil LeaseFreshness means the caller has no way to check and every
// Origin location is trusted. That keeps a component wired without a
// Lease client resolving the same origin the pre-Lease code did,
// rather than resolving none at all.
//
// +kubebuilder:object:generate=false
type LeaseFreshness func(flowID, nodeName string) (fresh bool, deadline time.Time, err error)

// OriginResolution answers which node a consumer of a flow should
// source it from, and separates the two ways the answer can be empty
// so a caller can react to them differently.
//
// Found with a Node is the only usable outcome. AllStale means the
// flow does name one or more Origin locations but no agent is
// renewing their Lease, which is a producer the cluster has lost
// rather than one it has not seen yet; Found and AllStale both false
// means no node claims to hold the flow at all.
//
// Deadline is the latest moment the answer still holds, taken from
// the Lease that sustains it. Time passing raises no event, so a
// caller that acts on Found has to schedule its own re-check.
//
// +kubebuilder:object:generate=false
type OriginResolution struct {
	Node     string
	Found    bool
	AllStale bool
	Deadline time.Time
}

// ResolveOrigin picks the flow's live Origin location.
//
// The operator's flow collector, its mirror lifecycle controller, its
// receiver reconciler and the agent's intent dispatcher all have to
// agree on which node holds a flow. They used to each walk
// status.locations themselves, and the copies drifted: one treated a
// missing Lease as fresh, another skipped the AllStale distinction,
// and a flow could be routable from one component's view and not from
// another's at the same instant. One implementation makes that
// disagreement impossible rather than merely unlikely.
//
// A nil flow resolves to nothing, which is the same answer a flow
// with no locations gives: neither is a node a consumer can be sent
// to.
func ResolveOrigin(flow *MxlFlow, fresh LeaseFreshness) (OriginResolution, error) {
	if flow == nil {
		return OriginResolution{}, nil
	}
	sawOrigin := false
	for _, loc := range flow.Status.Locations {
		if loc.Phase != MxlFlowLocationOrigin {
			continue
		}
		sawOrigin = true
		if fresh == nil {
			return OriginResolution{Node: loc.NodeName, Found: true}, nil
		}
		live, deadline, err := fresh(flow.Spec.ID, loc.NodeName)
		if err != nil {
			return OriginResolution{}, err
		}
		if live {
			return OriginResolution{Node: loc.NodeName, Found: true, Deadline: deadline}, nil
		}
	}
	return OriginResolution{AllStale: sawOrigin}, nil
}

// OriginNode returns the node whose location claims Origin, or the
// empty string when none does. It is the raw claim, without the Lease
// check ResolveOrigin applies, and exists so the operator can keep
// status.originNode equal to the locations list rather than to a
// remembered value that outlives the entry it was derived from.
func OriginNode(flow *MxlFlow) string {
	if flow == nil {
		return ""
	}
	for _, loc := range flow.Status.Locations {
		if loc.Phase == MxlFlowLocationOrigin {
			return loc.NodeName
		}
	}
	return ""
}
