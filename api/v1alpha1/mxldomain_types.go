package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MxlDomainSpec defines an MXL domain on a node.
type MxlDomainSpec struct {
	// NodeName is the Kubernetes node hosting this domain.
	// +kubebuilder:validation:Required
	NodeName string `json:"nodeName"`

	// HostPath is the absolute path on the node where the MXL domain
	// directory is mounted, typically a tmpfs.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^/.+`
	HostPath string `json:"hostPath"`
}

// MxlDomainStatus reports the runtime state of an MxlDomain as
// observed by the per-node agent.
type MxlDomainStatus struct {
	// CapacityBytes is the total size of the backing filesystem.
	// +optional
	CapacityBytes int64 `json:"capacityBytes,omitempty"`

	// FreeBytes is the unused size of the backing filesystem.
	// +optional
	FreeBytes int64 `json:"freeBytes,omitempty"`

	// FanotifyReady reports whether the agent has placed a fanotify
	// mark on the domain mount. When false, on-demand materialization
	// is unavailable on this node.
	// +optional
	FanotifyReady bool `json:"fanotifyReady,omitempty"`

	// LastSeen is the last time the agent reported on this domain.
	// +optional
	LastSeen *metav1.Time `json:"lastSeen,omitempty"`

	// Conditions describes the current state of the domain.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=mxldom
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Node,type=string,JSONPath=`.spec.nodeName`
// +kubebuilder:printcolumn:name=Path,type=string,JSONPath=`.spec.hostPath`
// +kubebuilder:printcolumn:name=Fanotify,type=boolean,JSONPath=`.status.fanotifyReady`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlDomain represents one MXL domain directory on one node.
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
