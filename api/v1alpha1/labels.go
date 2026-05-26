package v1alpha1

// Label keys used to attribute MxlFlowMirror objects to the
// controller that created them. The receiver-driven path and the
// agent-intent path each garbage-collect only the mirrors carrying
// their own label, so the two ownership domains never reap each
// other's objects.
const (
	// LabelCreatedByReceiver is set on mirrors created by the
	// operator's MxlReceiver reconciler. Its value is the
	// MxlReceiver name.
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
