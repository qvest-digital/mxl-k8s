package v1alpha1

// Condition type constants for MxlFlowMirror and MxlFlow status.
// Each type names a single field manager that owns writes to its
// entry in status.conditions; the operator and the two gateway
// reconcilers must not overlap on the same type.
const (
	// ConditionTypeSourceProgress reports whether the source-side
	// gateway is transferring grains for a mirror. Owned by the
	// source gateway reconciler.
	ConditionTypeSourceProgress = "SourceProgress"

	// ConditionTypeTargetProgress reports whether the target-side
	// gateway is committing grains for a mirror. Owned by the
	// target gateway reconciler.
	ConditionTypeTargetProgress = "TargetProgress"

	// ConditionTypeOriginFresh reports whether the origin Lease for
	// an MxlFlow is within its renewal window. Owned by the
	// operator and written to MxlFlow status.
	ConditionTypeOriginFresh = "OriginFresh"

	// ConditionTypeProbed reports whether status.providers on an
	// MxlNodeCapabilities came from a libmxl-fabrics interface
	// enumeration. Owned by the gateway capability publisher.
	//
	// It is what makes deviceCount readable. A gateway that only
	// echoed its configured provider list left the field at zero for
	// providers that worked, so a consumer cannot tell "measured
	// none" from "never measured" without this.
	ConditionTypeProbed = "Probed"

	// ConditionTypeRDMADevicesEnumerated reports whether the RDMA
	// devices the host kernel exposes are accounted for by
	// status.providers. Owned by the gateway capability publisher.
	//
	// libfabric builds a provider's device list once per process, on
	// the first enumeration, and rebuilds it only for a caller that
	// asks to rescan. libmxl-fabrics takes no flags on its
	// enumeration entry point, so nothing here can ask. A gateway
	// that first enumerated while the host's RDMA devices were
	// unusable therefore reports none of them for the rest of its
	// life, and every mirror it takes part in resolves to the tcp
	// fallback while still reaching Ready.
	//
	// A False condition is a discrepancy, not a proven fault. An
	// active port does not by itself make a device one the verbs
	// provider will build an endpoint on, so the gateway reports the
	// difference rather than acting on it.
	ConditionTypeRDMADevicesEnumerated = "RDMADevicesEnumerated"
)

// Condition reason constants for MxlFlowMirror and MxlFlow status.
const (
	// ReasonHandshakeComplete marks a mirror whose libmxl-fabrics
	// initiator and target have exchanged setup information.
	ReasonHandshakeComplete = "HandshakeComplete"

	// ReasonNoGrains marks a mirror where the handshake succeeded
	// but no grain progress has been observed within the freshness
	// window.
	ReasonNoGrains = "NoGrains"

	// ReasonAddTargetFailed marks a mirror whose source gateway
	// could not register the target descriptor with the initiator.
	ReasonAddTargetFailed = "AddTargetFailed"

	// ReasonOpenTargetFailed marks a mirror whose target gateway could
	// not open the local writer or the libmxl-fabrics target endpoint
	// (Target.Setup). Without it the failure surfaced only in the
	// gateway log while the mirror sat silently at an empty phase.
	ReasonOpenTargetFailed = "OpenTargetFailed"

	// ReasonFlowDefinitionEmpty marks a mirror whose MxlFlow exists but
	// carries no spec.definition yet, so the target side cannot open the
	// local writer. Transient while the producer publishes the flow.
	ReasonFlowDefinitionEmpty = "FlowDefinitionEmpty"

	// ReasonReaderAgedOut marks a mirror whose source-side flow
	// reader fell behind the writer and advanced past the missed
	// grains. Benign on its own: a reader that skips once and reaches
	// the live tail again keeps the mirror delivering. The message
	// separates that from a reader still outside the ring a whole
	// stall window after its last delivered grain, which is the one
	// the gateway reopens.
	ReasonReaderAgedOut = "ReaderAgedOut"

	// ReasonReaderNotAdvancing marks a mirror whose source-side flow
	// reader has delivered no grain within the stall window and whose
	// head index has not moved either.
	ReasonReaderNotAdvancing = "ReaderNotAdvancing"

	// ReasonTransfersNotLanding marks a mirror whose source-side reader
	// head is advancing, so the producer is alive, while nothing has
	// reached the target since the initiator opened.
	//
	// It exists because status.lastSentAt cannot express this: the
	// timestamp only moves on a successful transfer, so a source that
	// cannot reach its target suppresses the very evidence that it is
	// trying. The target's stuck-handshake watchdog reads this reason
	// as the source-activity signal it would otherwise take from
	// lastSentAt.
	ReasonTransfersNotLanding = "TransfersNotLanding"

	// ReasonSourceWriterGone marks a source-side mirror whose flow has
	// no live writer, as reported by libmxl rather than inferred from a
	// stalled head.
	//
	// It is distinct from ReaderNotAdvancing and TransfersNotLanding,
	// which describe a reader that might yet recover: reopening a
	// reader on a flow nobody writes cannot help, and holding one open
	// is actively harmful. libmxl only reclaims a flow directory when
	// the departing writer can take an exclusive lock, so a reader
	// kept on a dead flow prevents the reclaim, which keeps the local
	// agent claiming Origin and renewing the flow's Lease, which in
	// turn keeps the flow from being collected -- a cycle in which the
	// mirror's own reader is what preserves the flow it is failing to
	// mirror.
	ReasonSourceWriterGone = "SourceWriterGone"

	// ReasonProviderUnresolved marks a mirror the gateway refused to
	// set up because spec.provider is still auto. libmxl-fabrics no
	// longer resolves auto itself (v1.1.0-beta-1 dropped it), so the
	// agent or operator must stamp a concrete provider before the
	// gateway sees the mirror; forwarding auto makes fi_getinfo fail
	// on an RDMA fabric.
	ReasonProviderUnresolved = "ProviderUnresolved"

	// ReasonLeaseExpired marks an MxlFlow whose origin Lease has
	// passed its renewal deadline.
	ReasonLeaseExpired = "LeaseExpired"

	// ReasonRecovered marks a condition that previously reported a
	// fault and has since returned to a healthy state.
	ReasonRecovered = "Recovered"

	// ReasonProbeComplete marks an MxlNodeCapabilities whose
	// providers were enumerated from libmxl-fabrics.
	ReasonProbeComplete = "ProbeComplete"

	// ReasonProbeFailed marks an MxlNodeCapabilities whose provider
	// enumeration returned an error. The previously published
	// providers are left in place rather than cleared, so a
	// transient failure does not strand every mirror on the node.
	ReasonProbeFailed = "ProbeFailed"

	// ReasonHostDevicesRepresented marks an MxlNodeCapabilities whose
	// enumerated providers account for the host's RDMA devices. A
	// host exposing none is represented by definition.
	ReasonHostDevicesRepresented = "HostDevicesRepresented"

	// ReasonHostDevicesUnenumerated marks an MxlNodeCapabilities
	// where the host exposes an RDMA device with an active port and
	// no RDMA-capable provider was enumerated. Restarting the gateway
	// is what runs the enumeration again; nothing reachable from a
	// running process rebuilds it.
	ReasonHostDevicesUnenumerated = "HostDevicesUnenumerated"

	// ReasonHostDevicesUnreadable marks an MxlNodeCapabilities whose
	// host RDMA device list could not be read. The enumerated
	// providers stand; only the cross-check is missing.
	ReasonHostDevicesUnreadable = "HostDevicesUnreadable"
)
