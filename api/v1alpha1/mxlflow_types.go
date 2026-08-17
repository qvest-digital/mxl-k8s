package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// MxlFlowLocationPhase is the state of a flow's materialization on
// one node.
// +kubebuilder:validation:Enum=Origin;Mirroring;Ready;Stale
type MxlFlowLocationPhase string

const (
	// MxlFlowLocationOrigin marks the node hosting the authoritative
	// copy (the node where the writer lives).
	MxlFlowLocationOrigin MxlFlowLocationPhase = "Origin"
	// MxlFlowLocationMirroring marks a node where the gateway is
	// actively materializing the flow.
	MxlFlowLocationMirroring MxlFlowLocationPhase = "Mirroring"
	// MxlFlowLocationReady marks a node where a complete mirror is
	// available to local readers.
	MxlFlowLocationReady MxlFlowLocationPhase = "Ready"
	// MxlFlowLocationStale marks a node where a mirror exists but the
	// agent has not confirmed it recently.
	MxlFlowLocationStale MxlFlowLocationPhase = "Stale"
)

// MxlFlowLocation reports a flow's state on one node.
type MxlFlowLocation struct {
	// NodeName is the Kubernetes node this entry describes.
	// +kubebuilder:validation:Required
	NodeName string `json:"nodeName"`

	// Phase is the materialization state on this node.
	// +kubebuilder:validation:Required
	Phase MxlFlowLocationPhase `json:"phase"`

	// LastObserved is when the local agent last confirmed the flow's
	// presence on this node. It is refreshed on every agent rescan
	// while the flow is present, so a recent value means "seen just
	// now" and a value drifting into the past means the agent has
	// stopped confirming it. It says nothing about when this copy
	// arrived, which is AppearedAt.
	// +optional
	LastObserved *metav1.Time `json:"lastObserved,omitempty"`

	// AppearedAt is when the agent last saw this copy of the flow
	// come into existence, and changes only when it does: a fresh
	// directory, or one recreated after the previous writer released
	// it. A steady rescan leaves it alone.
	//
	// The source gateway keys writer-rotation detection on this
	// rather than on LastObserved. The two were one field, which made
	// a periodic refresh indistinguishable from a producer restart
	// and so forced LastObserved to stand still, leaving nothing that
	// answered "is this copy still being confirmed".
	// +optional
	AppearedAt *metav1.Time `json:"appearedAt,omitempty"`
}

// MxlFlowSpec defines a logical MXL flow.
type MxlFlowSpec struct {
	// ID is the MXL flow UUID, matching the "id" field in the flow's
	// flow_def.json.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	ID string `json:"id"`

	// Definition is the verbatim NMOS-shaped flow definition document
	// (the contents of flow_def.json). It is stored opaquely; mxl-k8s
	// does not validate its inner structure.
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Definition runtime.RawExtension `json:"definition"`
}

// MxlFlowStatus reports where the flow is materialized.
type MxlFlowStatus struct {
	// Locations lists the nodes where the flow's data is currently
	// available, including the origin and any active mirrors.
	// +optional
	// +listType=map
	// +listMapKey=nodeName
	Locations []MxlFlowLocation `json:"locations,omitempty"`

	// OriginNode is the node currently holding the authoritative copy,
	// mirroring the Locations entry whose phase is Origin. Duplicated
	// here so the move can be recorded as a transition rather than
	// inferred by diffing a list.
	// +optional
	OriginNode string `json:"originNode,omitempty"`

	// PreviousOriginNode is where the authoritative copy sat before
	// the most recent move. Empty until the flow's origin has moved
	// at least once.
	// +optional
	PreviousOriginNode string `json:"previousOriginNode,omitempty"`

	// OriginChangedAt is when the authoritative copy last moved to a
	// different node.
	//
	// Nothing else records this. A location's LastObserved says the
	// copy is alive now and AppearedAt says when that copy arrived,
	// but neither distinguishes a mirror still converging on a moved
	// origin from one stranded on a node that no longer holds it.
	// Diagnosing that difference from the objects alone was not
	// possible before this field existed.
	// +optional
	OriginChangedAt *metav1.Time `json:"originChangedAt,omitempty"`

	// Conditions describes the current state of the flow.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=mxlf
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=ID,type=string,JSONPath=`.spec.id`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlFlow represents a logical MXL flow registered with the control
// plane.
type MxlFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MxlFlowSpec   `json:"spec,omitempty"`
	Status MxlFlowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MxlFlowList is a list of MxlFlow resources.
type MxlFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MxlFlow `json:"items"`
}
