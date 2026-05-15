package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MxlFabricsProviderCapability reports what one libmxl-fabrics
// provider is able to do on a node.
type MxlFabricsProviderCapability struct {
	// Name is the libmxl-fabrics provider name.
	// +kubebuilder:validation:Required
	Name MxlFabricsProvider `json:"name"`

	// Version is the underlying libfabric provider version string,
	// as reported by the provider when libmxl-fabrics initializes it.
	// +optional
	Version string `json:"version,omitempty"`

	// DeviceCount is the number of devices the provider can use on
	// this node (NICs, EFA adapters, etc.). Zero means the provider
	// is supported by libmxl-fabrics but has nothing usable here.
	// +optional
	DeviceCount int32 `json:"deviceCount,omitempty"`
}

// MxlNodeCapabilitiesSpec identifies the node these capabilities
// describe.
type MxlNodeCapabilitiesSpec struct {
	// NodeName is the Kubernetes node these capabilities describe.
	// +kubebuilder:validation:Required
	NodeName string `json:"nodeName"`
}

// MxlNodeCapabilitiesStatus reports what the gateway found at
// startup.
type MxlNodeCapabilitiesStatus struct {
	// Providers lists libmxl-fabrics providers the gateway probed
	// successfully on this node.
	// +optional
	// +listType=map
	// +listMapKey=name
	Providers []MxlFabricsProviderCapability `json:"providers,omitempty"`

	// LastSeen is the last time the gateway updated this resource.
	// +optional
	LastSeen *metav1.Time `json:"lastSeen,omitempty"`

	// Conditions describes the current state of the gateway probe.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=mxlnc
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Node,type=string,JSONPath=`.spec.nodeName`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlNodeCapabilities reports a node's libmxl-fabrics capabilities as
// probed by the local gateway at startup.
type MxlNodeCapabilities struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MxlNodeCapabilitiesSpec   `json:"spec,omitempty"`
	Status MxlNodeCapabilitiesStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MxlNodeCapabilitiesList is a list of MxlNodeCapabilities resources.
type MxlNodeCapabilitiesList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MxlNodeCapabilities `json:"items"`
}
