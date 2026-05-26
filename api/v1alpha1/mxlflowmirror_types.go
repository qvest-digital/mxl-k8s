package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MxlFlowMirrorPhase is the lifecycle state of a mirror.
// +kubebuilder:validation:Enum=Pending;Materializing;Ready;Degraded;Failed
type MxlFlowMirrorPhase string

const (
	// MxlFlowMirrorPending means the operator has not picked the
	// mirror up yet.
	MxlFlowMirrorPending MxlFlowMirrorPhase = "Pending"
	// MxlFlowMirrorMaterializing means the gateway is establishing
	// the libmxl-fabrics connection.
	MxlFlowMirrorMaterializing MxlFlowMirrorPhase = "Materializing"
	// MxlFlowMirrorReady means the handshake is complete and grain
	// activity has been observed within the freshness window.
	MxlFlowMirrorReady MxlFlowMirrorPhase = "Ready"
	// MxlFlowMirrorDegraded means the handshake is complete but no
	// grain progress has been observed for longer than the target
	// gateway's freshness window. The mirror may recover without
	// operator intervention; inspect status.conditions for the
	// reason.
	MxlFlowMirrorDegraded MxlFlowMirrorPhase = "Degraded"
	// MxlFlowMirrorFailed means the mirror failed permanently.
	// Inspect status.conditions for the cause.
	MxlFlowMirrorFailed MxlFlowMirrorPhase = "Failed"
)

// MxlFlowMirrorSpec defines a desired mirror of a flow onto a
// specific node.
type MxlFlowMirrorSpec struct {
	// FlowID is the MXL flow being mirrored.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	FlowID string `json:"flowID"`

	// SourceNode is the Kubernetes node hosting the origin flow.
	// +kubebuilder:validation:Required
	SourceNode string `json:"sourceNode"`

	// TargetNode is the Kubernetes node where the flow should be
	// materialized.
	// +kubebuilder:validation:Required
	TargetNode string `json:"targetNode"`

	// Provider selects the libmxl-fabrics provider used to move
	// grains. Defaults to auto.
	// +kubebuilder:default=auto
	// +optional
	Provider MxlFabricsProvider `json:"provider,omitempty"`

	// Requestor identifies the Pod whose intent triggered this
	// mirror. Set by the agent for fanotify-driven mirrors; unset for
	// MxlReceiver-driven ones.
	// +optional
	Requestor *PodRef `json:"requestor,omitempty"`
}

// MxlFlowMirrorStatus reports the runtime state of a mirror.
type MxlFlowMirrorStatus struct {
	// Phase tracks the mirror lifecycle.
	// +optional
	Phase MxlFlowMirrorPhase `json:"phase,omitempty"`

	// TargetInfo is the libmxl-fabrics target descriptor produced by
	// mxlFabricsTargetSetup and serialized via
	// mxlFabricsTargetInfoToString. The initiator-side gateway parses
	// it with mxlFabricsTargetInfoFromString.
	// +optional
	TargetInfo string `json:"targetInfo,omitempty"`

	// ObservedGeneration is the spec generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describes the current state of the mirror.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastGrainAt is the wall-clock time of the most recent grain
	// commit observed by the target gateway. Used to demote Ready
	// to Degraded when no grains have flowed for the freshness
	// window.
	// +optional
	LastGrainAt *metav1.Time `json:"lastGrainAt,omitempty"`

	// LastSentAt is the wall-clock time of the most recent grain
	// the source-side gateway successfully handed to libmxl-fabrics
	// for transfer. Set on every source-side flusher tick once the
	// initiator is live. Target-side recovery uses the delta between
	// LastSentAt and LastGrainAt to distinguish "source is sending
	// but target is wedged" (rebuild) from "source is idle"
	// (leave alone). Unset before the first transfer.
	// +optional
	// +nullable
	LastSentAt *metav1.Time `json:"lastSentAt,omitempty"`

	// LastError is the most recent reconcile error message recorded
	// by the source gateway when bounded backoff is engaged. Empty
	// after a successful reconcile.
	// +optional
	LastError string `json:"lastError,omitempty"`

	// AttemptCount is the number of consecutive failed AddTarget
	// attempts since the last successful reconcile. Reset to zero
	// when the source gateway succeeds.
	// +optional
	AttemptCount int32 `json:"attemptCount,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=mxlfm
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Flow,type=string,JSONPath=`.spec.flowID`
// +kubebuilder:printcolumn:name=Source,type=string,JSONPath=`.spec.sourceNode`
// +kubebuilder:printcolumn:name=Target,type=string,JSONPath=`.spec.targetNode`
// +kubebuilder:printcolumn:name=Provider,type=string,JSONPath=`.spec.provider`
// +kubebuilder:printcolumn:name=Phase,type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlFlowMirror represents the desired and observed state of one
// (flow, target node) mirror.
type MxlFlowMirror struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MxlFlowMirrorSpec   `json:"spec,omitempty"`
	Status MxlFlowMirrorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MxlFlowMirrorList is a list of MxlFlowMirror resources.
type MxlFlowMirrorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MxlFlowMirror `json:"items"`
}
