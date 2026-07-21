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
	"errors"
	"fmt"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// errProviderUnresolved is returned by providerForSetup when a mirror
// still carries the auto provider. libmxl-fabrics v1.1.0-beta-1 dropped
// automatic provider resolution, so the agent and operator resolve auto
// to a concrete provider before the gateway sets the mirror up. A mirror
// that still says auto here would make fi_getinfo fail (-22) on an RDMA
// fabric, so the gateway fails fast with a legible error instead of
// forwarding auto into setup.
var errProviderUnresolved = errors.New(
	"provider is auto; the agent or operator must resolve it to a concrete provider before setup")

// mapProvider translates the API enum into the fabrics package enum.
// The CRD "auto" value and unknown / empty values map to ProviderAny,
// which lets libmxl-fabrics pick a provider at runtime.
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
	return fabrics.ProviderAny
}

// providerForSetup maps a mirror's provider to the fabrics enum for
// source/target setup, refusing the auto sentinel. Concrete providers
// pass through mapProvider unchanged; auto (or an unset provider) yields
// errProviderUnresolved wrapped with the mirror's namespace/name so the
// reconciler can surface which mirror is misconfigured without
// forwarding auto to libmxl-fabrics.
func providerForSetup(m *mxlv1alpha1.MxlFlowMirror) (fabrics.Provider, error) {
	if m.Spec.Provider == "" || m.Spec.Provider == mxlv1alpha1.ProviderAuto {
		return fabrics.ProviderAny, fmt.Errorf("mirror %s/%s: %w", m.Namespace, m.Name, errProviderUnresolved)
	}
	return mapProvider(m.Spec.Provider), nil
}
