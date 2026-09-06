package v1alpha1

// Label keys used to attribute MxlFlowMirror objects to the
// controller that created them.
//
// Diagnostic, and deliberately not part of any lifecycle decision. A
// mirror is claimed by an owner reference to a live MxlReceiver or by
// a spec.requestor naming a live pod, both of which the requesting
// side writes onto the object; the collector reads those and nothing
// else. Two collectors used to key on these labels instead, one per
// creation path, and a mirror whose labels had been edited off
// belonged to neither -- it sat on a showcase cluster for a day and a
// half with two gateway finalizers and a requestor pod long gone.
//
// LabelCreatedByReceiver keeps one non-diagnostic use: it is the
// cluster-wide index key for finding a receiver's cross-namespace
// mirrors, which carry no owner reference because the apiserver
// rejects one. Losing it there costs the receiver its own eager
// cleanup, not the mirror's collectability.
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
	// than the receiver's. Receiver namespace disambiguation lives
	// on the separate LabelCreatedByReceiverNamespace key so the two
	// values stay independently within the 63-char label-value limit.
	LabelCreatedByReceiver = "mxl.qvest-digital.com/created-by-receiver"

	// LabelCreatedByReceiverNamespace is set alongside
	// LabelCreatedByReceiver on cross-namespace mirrors so a
	// cluster-wide lookup can distinguish two receivers that share
	// a name across different namespaces. Composing namespace and
	// name into one label value would exceed the 63-char k8s label
	// value limit when either segment is long.
	LabelCreatedByReceiverNamespace = "mxl.qvest-digital.com/created-by-receiver-namespace"

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
