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
	// grains.
	ReasonReaderAgedOut = "ReaderAgedOut"

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
)
