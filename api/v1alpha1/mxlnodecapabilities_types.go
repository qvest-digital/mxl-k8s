package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MxlFabricInterface reports one local fabric interface a provider
// offers, as returned by the libmxl-fabrics interface enumeration.
type MxlFabricInterface struct {
	// Address is the node address the interface binds to, as the
	// provider reports it. Unique within a provider's interface list.
	// +kubebuilder:validation:Required
	Address string `json:"address"`

	// Device names the underlying device. For the tcp provider this
	// is the kernel netdev name; other providers report whatever the
	// libfabric NIC descriptor carries. Empty when the provider
	// reports no device.
	// +optional
	Device string `json:"device,omitempty"`

	// LinkState is the interface link state the provider reports:
	// up, down, or unknown. Empty when the provider reports no link
	// attributes, which is the common case for interfaces backed by
	// no physical NIC.
	// +optional
	// +kubebuilder:validation:Enum=up;down;unknown
	LinkState string `json:"linkState,omitempty"`

	// LinkSpeedBitsPerSecond is the link speed the provider reports.
	// Zero means it reported none.
	// +optional
	LinkSpeedBitsPerSecond int64 `json:"linkSpeedBitsPerSecond,omitempty"`

	// MaxMessageSize is the largest message the interface accepts.
	// +optional
	MaxMessageSize int64 `json:"maxMessageSize,omitempty"`

	// PCIAddress is the interface's PCI address in
	// domain:bus:device.function form. Empty when the provider
	// reports no PCI bus attributes.
	// +optional
	PCIAddress string `json:"pciAddress,omitempty"`
}

// MxlFabricsProviderCapability reports what one libmxl-fabrics
// provider is able to do on a node.
type MxlFabricsProviderCapability struct {
	// Name is the libmxl-fabrics provider name.
	// +kubebuilder:validation:Required
	Name MxlFabricsProvider `json:"name"`

	// Version is the underlying libfabric provider version string,
	// as reported by the provider when libmxl-fabrics initializes it.
	// Unset: libmxl-fabrics exposes no per-provider version through
	// its C API, so the gateway has nothing to fill it from.
	// +optional
	Version string `json:"version,omitempty"`

	// DeviceCount is the number of devices the provider can use on
	// this node (NICs, EFA adapters, etc.). Zero means the provider
	// is supported by libmxl-fabrics but has nothing usable here.
	//
	// Devices are counted by the name the provider reports, falling
	// back to the address where it reports none, so one device that
	// enumerates once per address family or endpoint type counts
	// once. Only interfaces the gateway is willing to bind are
	// counted; see Interfaces.
	// +optional
	DeviceCount int32 `json:"deviceCount,omitempty"`

	// Interfaces lists the local fabric interfaces this provider
	// offers that pass the gateway's fabric selection, which is the
	// same set a mirror on this node can be set up on. Empty on a
	// gateway that reports no probe.
	// +optional
	// +listType=map
	// +listMapKey=address
	Interfaces []MxlFabricInterface `json:"interfaces,omitempty"`
}

// MxlNodeCapabilitiesSpec identifies the node these capabilities
// describe.
type MxlNodeCapabilitiesSpec struct {
	// NodeName is the Kubernetes node these capabilities describe.
	// +kubebuilder:validation:Required
	NodeName string `json:"nodeName"`
}

// MxlNodeCapabilitiesStatus reports what the gateway found on the
// node it runs on.
type MxlNodeCapabilitiesStatus struct {
	// Providers lists libmxl-fabrics providers the gateway probed on
	// this node, restricted to the providers it is configured to
	// consider. A provider appears here with deviceCount zero when
	// libmxl-fabrics knows it but found nothing usable, so absence
	// means "the gateway was told not to consider it" and zero means
	// "the hardware is not there".
	// +optional
	// +listType=map
	// +listMapKey=name
	Providers []MxlFabricsProviderCapability `json:"providers,omitempty"`

	// LastSeen is the last time the gateway updated this resource.
	// +optional
	LastSeen *metav1.Time `json:"lastSeen,omitempty"`

	// Conditions describes the current state of the gateway probe.
	// See ConditionTypeProbed.
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
// probed by the local gateway, refreshed for as long as it runs.
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
