package v1alpha1

// FlowIDPattern matches the canonical 8-4-4-4-12 hexadecimal MXL flow
// identifier (the value of the "id" field in flow_def.json).
const FlowIDPattern = `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`

// MxlFabricsProvider identifies a libmxl-fabrics provider. The string
// values match the names accepted by mxlFabricsProviderFromString.
// +kubebuilder:validation:Enum=auto;tcp;verbs;efa;shm
type MxlFabricsProvider string

const (
	// ProviderAuto lets libmxl-fabrics pick a provider at runtime.
	ProviderAuto MxlFabricsProvider = "auto"
	// ProviderTCP uses Linux TCP sockets (no RDMA hardware required).
	ProviderTCP MxlFabricsProvider = "tcp"
	// ProviderVerbs uses libibverbs + librdmacm (RoCE/RoCEv2/IB).
	ProviderVerbs MxlFabricsProvider = "verbs"
	// ProviderEFA uses AWS Elastic Fabric Adapter.
	ProviderEFA MxlFabricsProvider = "efa"
	// ProviderSHM uses host-local shared memory.
	ProviderSHM MxlFabricsProvider = "shm"
)

// PodRef identifies a Pod by name, namespace, and optionally UID.
type PodRef struct {
	// Name is the Pod name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace is the Pod namespace.
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`

	// UID is the Pod's UID, captured at the time the reference was
	// recorded. Empty when the producer cannot determine it.
	// +optional
	UID string `json:"uid,omitempty"`
}

// MirrorRef identifies an MxlFlowMirror by name and namespace.
type MirrorRef struct {
	// Name is the MxlFlowMirror name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace is the MxlFlowMirror namespace.
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`
}
