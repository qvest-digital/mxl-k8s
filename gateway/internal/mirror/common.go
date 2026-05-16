// Package mirror contains the gateway's MxlFlowMirror reconcilers.
//
// Two reconcilers are registered against the same kind, filtered by
// spec.targetNode and spec.sourceNode respectively:
//
//   - TargetReconciler is the receiving half. For mirrors whose
//     targetNode is this gateway's node, it opens a libmxl FlowWriter
//     on the flow, registers its memory regions with libmxl-fabrics,
//     sets up a fabrics.Target, and writes the serialized TargetInfo
//     back to status.targetInfo with phase=Ready.
//   - SourceReconciler is the sending half. For mirrors whose
//     sourceNode is this gateway's node and that already carry a
//     status.targetInfo, it opens a FlowReader on the local flow,
//     builds a fabrics.Initiator + AddTarget(targetInfo), and runs a
//     per-flow goroutine that calls TransferGrain on every grain the
//     reader sees and MakeProgress on a tick.
//
// The two reconcilers operate on disjoint mirror sets (one mirror has
// a single targetNode and a single sourceNode, only one of which can
// match) and keep their own state and finalizers, so they can be
// enabled or torn down independently.
package mirror

import (
	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// mapProvider translates the API enum into the fabrics package enum.
// Unknown / empty values map to ProviderAuto.
func mapProvider(p mxlv1alpha1.MxlFabricsProvider) fabrics.Provider {
	switch p {
	case mxlv1alpha1.ProviderTCP:
		return fabrics.ProviderTCP
	case mxlv1alpha1.ProviderVerbs:
		return fabrics.ProviderVerbs
	case mxlv1alpha1.ProviderEFA:
		return fabrics.ProviderEFA
	case mxlv1alpha1.ProviderSHM:
		return fabrics.ProviderSHM
	}
	return fabrics.ProviderAuto
}
