package v1alpha1

// Label keys used to attribute MxlFlowMirror objects to the
// controller that created them. The agent-intent path
// garbage-collects only mirrors carrying its own label so the two
// ownership domains never reap each other's objects. The
// receiver-driven path expresses ownership through
// metadata.ownerReferences on the mirror instead; its label
// remains as a first-creator diagnostic tag and as the index key
// for cross-namespace owner lookup, where the OwnerReferences
// field index does not apply.
const (
	// LabelCreatedByReceiver is set on mirrors created by the
	// operator's MxlReceiver reconciler. Its value is the
	// MxlReceiver name. First-creator diagnostic tag; not used
	// for refcounting. Receivers express ownership via
	// metadata.ownerReferences on the mirror. The value is also
	// the cluster-wide index key used to look up cross-namespace
	// mirrors owned by a given receiver, because controller-runtime
	// field indices on ownerReferences are scoped per cache and the
	// cross-namespace lookup must reach mirrors in namespaces other
	// than the receiver's.
	LabelCreatedByReceiver = "mxl.qvest-digital.com/created-by-receiver"

	// LabelCreatedByIntent is set on mirrors created by the agent
	// in response to a local consumer's fanotify intent. Its value
	// is the node name where the consumer is scheduled.
	LabelCreatedByIntent = "mxl.qvest-digital.com/created-by-intent"

	// LabelRequestorPodUID is set on intent-created mirrors to
	// record the UID of the consumer pod that triggered creation.
	// Used by the intent-mirror garbage collector to detect pod
	// replacement.
	LabelRequestorPodUID = "mxl.qvest-digital.com/requestor-pod-uid"
)
