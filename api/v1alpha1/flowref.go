package v1alpha1

import "strings"

// FlowRef names one flow: its id within one MxlDomain.
//
// Flow ids are unique within a domain, not across domains: the same
// id in two domains is two flows. Everything keyed by a flow -- the
// MxlFlow object, its mirrors, its Origin Lease -- is keyed by the
// pair.
//
// Domain is the MxlDomain name, and empty means the primary domain,
// the one the agent's --domain-path names. That is what every object
// written before domains existed means, so those objects keep their
// names and nothing has to be migrated.
//
// +kubebuilder:object:generate=false
type FlowRef struct {
	Domain string
	ID     string
}

// domainSeparator joins a domain name to a flow id in an object name.
// A dot, because an MxlDomain name is a DNS subdomain and a flow id
// holds no dot, so the last dot always separates the two.
const domainSeparator = "."

// Name is the MxlFlow object name for the flow: the bare id in the
// primary domain, "<domain>.<id>" in any other.
func (r FlowRef) Name() string {
	if r.Domain == "" {
		return r.ID
	}
	return r.Domain + domainSeparator + r.ID
}

// Normalize folds the primary domain's name into the empty form.
//
// The primary domain is a named MxlDomain too, so a flow in it can be
// spelled either way, and two spellings would give one flow two object
// names and two mirrors. Every ref is compared and named in the empty
// form; primary is the primary domain's MxlDomain name, and empty folds
// nothing.
func (r FlowRef) Normalize(primary string) FlowRef {
	if primary != "" && r.Domain == primary {
		r.Domain = ""
	}
	return r
}

// ParseFlowName reverses FlowRef.Name.
func ParseFlowName(name string) FlowRef {
	if i := strings.LastIndex(name, domainSeparator); i >= 0 {
		return FlowRef{Domain: name[:i], ID: name[i+1:]}
	}
	return FlowRef{ID: name}
}

// Ref is the flow an MxlFlow describes.
func (f *MxlFlow) Ref() FlowRef { return FlowRef{Domain: f.Spec.Domain, ID: f.Spec.ID} }

// Ref is the flow an MxlFlowMirror copies.
func (m *MxlFlowMirror) Ref() FlowRef { return FlowRef{Domain: m.Spec.Domain, ID: m.Spec.FlowID} }

// Ref is the flow an MxlReceiver asks for.
func (r *MxlReceiver) Ref() FlowRef { return FlowRef{Domain: r.Spec.Domain, ID: r.Spec.FlowID} }
