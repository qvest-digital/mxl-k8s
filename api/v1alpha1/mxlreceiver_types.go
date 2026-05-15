package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MxlReceiverPhase is the lifecycle state of a receiver.
// +kubebuilder:validation:Enum=Pending;Bound;Failed
type MxlReceiverPhase string

const (
	// MxlReceiverPending means the operator has not yet produced an
	// MxlFlowMirror.
	MxlReceiverPending MxlReceiverPhase = "Pending"
	// MxlReceiverBound means an MxlFlowMirror exists for the
	// receiver. The mirror's own status reports materialization
	// progress.
	MxlReceiverBound MxlReceiverPhase = "Bound"
	// MxlReceiverFailed means the operator could not produce a
	// mirror. Inspect status.conditions for the cause.
	MxlReceiverFailed MxlReceiverPhase = "Failed"
)

// MxlReceiverSpec declares that one or more Pods want a flow
// available locally on their node.
//
// +kubebuilder:validation:XValidation:rule="(has(self.podSelector)?1:0)+(has(self.podRef)?1:0)==1",message="exactly one of podSelector or podRef must be set"
type MxlReceiverSpec struct {
	// FlowID is the MXL flow the consumer wants to read.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	FlowID string `json:"flowID"`

	// PodSelector matches the consumer Pods in this namespace whose
	// nodes should have the flow materialized. Exactly one of
	// podSelector / podRef must be set.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`

	// PodRef pins this receiver to a single Pod in this namespace.
	// Exactly one of podSelector / podRef must be set.
	// +optional
	PodRef *PodRef `json:"podRef,omitempty"`

	// Provider selects the libmxl-fabrics provider used to move
	// grains. Defaults to auto.
	// +kubebuilder:default=auto
	// +optional
	Provider MxlFabricsProvider `json:"provider,omitempty"`
}

// MxlReceiverStatus reports the runtime state of a receiver.
type MxlReceiverStatus struct {
	// BoundMirror references the MxlFlowMirror created to satisfy
	// this receiver.
	// +optional
	BoundMirror *MirrorRef `json:"boundMirror,omitempty"`

	// Phase tracks the receiver lifecycle.
	// +optional
	Phase MxlReceiverPhase `json:"phase,omitempty"`

	// ObservedGeneration is the spec generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describes the current state of the receiver.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=mxlr
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Flow,type=string,JSONPath=`.spec.flowID`
// +kubebuilder:printcolumn:name=Phase,type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlReceiver expresses a consumer Pod's intent to read an MXL flow.
// The operator translates each MxlReceiver into one MxlFlowMirror per
// distinct target node.
type MxlReceiver struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MxlReceiverSpec   `json:"spec,omitempty"`
	Status MxlReceiverStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MxlReceiverList is a list of MxlReceiver resources.
type MxlReceiverList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MxlReceiver `json:"items"`
}
