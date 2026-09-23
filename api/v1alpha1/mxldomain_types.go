package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DomainIDPattern matches an MXL UUID as BCP-007-03 defines it for a
// domain or flow identifier: lowercase canonical form, RFC 9562 version
// 1-15 and the RFC 4122/9562 variant.
const DomainIDPattern = `^[0-9a-f]{8}-[0-9a-f]{4}-[1-9a-f][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`

// DomainDefFile is the file an MXL domain carries its identity in, at
// the root of the domain directory.
const DomainDefFile = "domain_def.json"

// DomainOptionsFile is libmxl's optional per-domain configuration, at
// the root of the domain directory.
const DomainOptionsFile = "options.json"

// MxlDomainSpec defines one MXL domain.
//
// A domain is one store of flows with one identity. The agent
// materialises it as a directory on every node it selects, and mxl-k8s
// mirrors flows between those directories on demand, so a function on
// any selected node opens any flow of the domain by id. That is why the
// identity is the domain's and not the node's: it is the same on every
// node and known before any pod is placed.
type MxlDomainSpec struct {
	// ID is the domain identity written into domain_def.json and
	// published as mxl_domain_id by NMOS nodes. Immutable: every
	// reference a controller holds is to this value.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-[0-9a-f]{4}-[1-9a-f][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="id is immutable"
	ID string `json:"id"`

	// Directory is the domain directory's name below the agent's
	// runtime root, one path segment.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="directory is immutable; flows already written live under the old one"
	Directory string `json:"directory"`

	// Label is the domain_def.json label.
	// +optional
	Label string `json:"label,omitempty"`

	// Description is the domain_def.json description.
	// +optional
	Description string `json:"description,omitempty"`

	// Tags are the domain_def.json tags.
	// +optional
	Tags map[string][]string `json:"tags,omitempty"`

	// HistoryDuration is the ring buffer depth libmxl gives every flow
	// created in the domain, written to options.json. Unset leaves
	// options.json alone, whoever wrote it. libmxl reads it when a flow
	// is created, so a change reaches only flows created after it.
	// +optional
	HistoryDuration *metav1.Duration `json:"historyDuration,omitempty"`

	// NodeSelector limits the nodes the domain is materialised on.
	// Empty selects every node an agent runs on.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// MxlDomainNodeStatus is one node's account of a domain.
type MxlDomainNodeStatus struct {
	// NodeName is the node the entry describes.
	NodeName string `json:"nodeName"`

	// Ready is true when the directory and domain_def.json on the node
	// match the spec.
	Ready bool `json:"ready"`

	// Mirrored is true when the node's agent tracks this domain's
	// flows, which is what lets mxl-k8s mirror them to other nodes. A
	// domain that is materialised but not mirrored is readable only on
	// the node a flow was written on.
	Mirrored bool `json:"mirrored"`

	// FanotifyReady reports whether the agent watches the directory
	// for flow creation. Only meaningful where Mirrored is true.
	// +optional
	FanotifyReady bool `json:"fanotifyReady,omitempty"`

	// CapacityBytes is the size of the filesystem backing the
	// directory.
	// +optional
	CapacityBytes int64 `json:"capacityBytes,omitempty"`

	// FreeBytes is the unused size of that filesystem.
	// +optional
	FreeBytes int64 `json:"freeBytes,omitempty"`

	// Message says why Ready is false.
	// +optional
	Message string `json:"message,omitempty"`

	// LastSeen is when the node's agent last reported.
	// +optional
	LastSeen *metav1.Time `json:"lastSeen,omitempty"`
}

// MxlDomainStatus reports where a domain is materialised.
type MxlDomainStatus struct {
	// Nodes has one entry per node the domain is selected on, each
	// written by that node's agent.
	// +optional
	// +listType=map
	// +listMapKey=nodeName
	Nodes []MxlDomainNodeStatus `json:"nodes,omitempty"`

	// Conditions describes the current state of the domain.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Node returns the entry for a node, or nil.
func (s *MxlDomainStatus) Node(name string) *MxlDomainNodeStatus {
	for i := range s.Nodes {
		if s.Nodes[i].NodeName == name {
			return &s.Nodes[i]
		}
	}
	return nil
}

// Selects reports whether the domain is materialised on a node with the
// given labels.
func (s *MxlDomainSpec) Selects(nodeLabels map[string]string) bool {
	for k, v := range s.NodeSelector {
		if got, ok := nodeLabels[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=mxldom
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=ID,type=string,JSONPath=`.spec.id`
// +kubebuilder:printcolumn:name=Directory,type=string,JSONPath=`.spec.directory`
// +kubebuilder:printcolumn:name=Label,type=string,JSONPath=`.spec.label`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlDomain is one MXL domain: one identity, materialised as a
// directory carrying domain_def.json on every selected node.
type MxlDomain struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MxlDomainSpec   `json:"spec,omitempty"`
	Status MxlDomainStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MxlDomainList is a list of MxlDomain resources.
type MxlDomainList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MxlDomain `json:"items"`
}
