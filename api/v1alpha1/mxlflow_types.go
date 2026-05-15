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
	// presence.
	// +optional
	LastObserved *metav1.Time `json:"lastObserved,omitempty"`
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
