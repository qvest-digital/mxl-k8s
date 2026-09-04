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
//     builds a fabrics.Initiator over it, and runs a per-flow
//     goroutine that calls TransferGrain on every grain the reader
//     sees and MakeProgress on a tick. The reader, the initiator and
//     the goroutine are shared by every mirror of one flow on one
//     provider: libmxl-fabrics enqueues a transfer to every target an
//     initiator holds, so a mirror costs one AddTarget rather than a
//     second copy of all three.
//
// The two reconcilers operate on disjoint mirror sets (one mirror has
// a single targetNode and a single sourceNode, only one of which can
// match) and keep their own state and finalizers, so they can be
// enabled or torn down independently.
package mirror

import (
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/fabric"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

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

// errNoInterface reports that no local fabric interface can carry a
// transfer for the requested provider.
var errNoInterface = errors.New("no usable fabric interface")

// resolveInterface asks libmxl-fabrics what this host offers and
// returns the entry a setup should be given.
//
// A setup handed a config with no address leaves interface resolution
// to fi_getinfo, which answers ENODATA on a fabric that needs a
// concrete source address, and the setup fails with nothing naming the
// cause. Enumerating first is how libmxl-fabrics expects the question
// to be answered: the entry it returns carries the provider's own
// address and capabilities, including the maxMessageSize a caller
// cannot otherwise learn.
//
// bindAddress narrows the search to one address when set, and is left
// out of the query otherwise so every address of the provider is
// considered. Service is not taken from the entry: the endpoint's port
// is the caller's to choose, and an empty one means ephemeral.
//
// The selector then drops what this node may not carry MXL traffic on.
// It is the same selector the capability publisher applies, so a
// mirror is only ever set up on an interface the node advertised, and
// libfabric's own preference order chooses among what survives. Left
// unfiltered, a host whose fabric is one of several NICs would hand a
// setup whichever address the enumeration happened to return first,
// including loopback, an ST 2110 essence NIC, or the management port.
func resolveInterface(fi interfaceLister, sel fabric.Selector, provider fabrics.Provider, bindAddress string) (fabrics.InterfaceConfig, error) {
	query := &fabrics.InterfaceConfig{Provider: provider}
	if bindAddress != "" {
		query.Address.Node = bindAddress
	}

	found, err := fi.Interfaces(query)
	if err != nil {
		return fabrics.InterfaceConfig{}, fmt.Errorf("query fabric interfaces: %w", err)
	}
	ifaces, rejected := sel.Select(found)

	// Remote write is the only transfer mode libmxl-fabrics implements,
	// and a setup that cannot do it is refused further down anyway.
	iface, ok := fabrics.SelectInterface(ifaces, fabrics.InterfaceCapRemoteWrite)
	if !ok {
		return fabrics.InterfaceConfig{}, fmt.Errorf("%w for provider %s on %q: %d of %d candidate(s) excluded%s",
			errNoInterface, provider, bindAddress, len(rejected), len(found), firstRejection(rejected))
	}
	iface.Address.Service = ""
	return iface, nil
}

// firstRejection renders one excluded interface for the error a failed
// resolve returns, so the reason reaches the mirror's condition rather
// than only the gateway log.
func firstRejection(rejected []fabric.Rejection) string {
	if len(rejected) == 0 {
		return ""
	}
	return ", first: " + rejected[0].String()
}

// interfaceLister is the slice of fabrics.Instance resolveInterface
// needs, so tests can drive it without a libmxl-fabrics instance.
type interfaceLister interface {
	Interfaces(query *fabrics.InterfaceConfig) ([]fabrics.InterfaceConfig, error)
}

// fabricFailure is what a libmxl-fabrics error means for the loop that
// received it. libmxl reports both halves of its API through one
// mxlStatus enum, and until go-mxl named the fabrics half every one of
// these arrived as an opaque integer: the loops logged
// "unrecognized status 1025" at error level once per iteration and
// carried on, so a signal-interrupted poll and a dead endpoint were
// indistinguishable in the log and identical in behaviour.
type fabricFailure int

const (
	// fabricIdle is the not-ready answer every non-blocking call gives
	// when it has nothing to report. Not a failure.
	fabricIdle fabricFailure = iota

	// fabricTransient is a call that was disturbed rather than broken.
	// MXL_ERR_INTERRUPTED is the common one: libfabric returns EINTR
	// from its poll when a signal lands, and Go's async preemption
	// makes that routine. The handles are still good; call again.
	fabricTransient

	// fabricEndpointLost means the libmxl-fabrics side is no longer
	// usable and has to be rebuilt. Retrying the same handles cannot
	// recover it.
	fabricEndpointLost

	// fabricPermanent is a call the gateway got wrong, or an operation
	// this build of libmxl-fabrics does not offer. Rebuilding changes
	// nothing; the mirror needs a human or a different configuration.
	fabricPermanent
)

func (f fabricFailure) String() string {
	switch f {
	case fabricIdle:
		return "idle"
	case fabricTransient:
		return "transient"
	case fabricEndpointLost:
		return "endpoint-lost"
	case fabricPermanent:
		return "permanent"
	}
	return "unclassified"
}

// classifyFabricError maps an error from a libmxl-fabrics call onto
// the action its caller should take.
//
// An error this function does not recognise is reported as
// fabricEndpointLost rather than assumed harmless: an unknown failure
// that is actually fatal, treated as transient, is the wedge that
// leaves a mirror logging into the void forever, whereas an unknown
// failure that is actually harmless, treated as fatal, costs one
// rebuild. The asymmetry decides the default.
func classifyFabricError(err error) fabricFailure {
	switch {
	case err == nil:
		return fabricIdle
	case errors.Is(err, fabrics.ErrNotReady):
		return fabricIdle
	case errors.Is(err, mxl.ErrInterrupted), errors.Is(err, mxl.ErrTimeout):
		return fabricTransient
	case errors.Is(err, mxl.ErrInvalidArg),
		errors.Is(err, mxl.ErrUnsupportedOperation),
		errors.Is(err, mxl.ErrStrLen),
		errors.Is(err, mxl.ErrPermissionDenied):
		return fabricPermanent
	}
	return fabricEndpointLost
}

// attempt is one mirror's failure run: consecutive failures, the
// earliest the next try may run, and the inputs that wait was earned
// against.
type attempt[I comparable] struct {
	count     uint32
	notBefore time.Time
	inputs    I
}

// attemptTable holds the failure runs of a reconciler that publishes
// status on the object it watches. Such a write wakes the reconciler
// through its own watch long before the RequeueAfter a failure
// returned is due, so the wait has to be enforced on the way in
// rather than returned as advice. Callers hold their own lock.
type attemptTable[I comparable] map[types.NamespacedName]attempt[I]

// wait reports how long remains before key may be retried against
// inputs. Different inputs are a new attempt rather than a retry of
// the one that failed, and serve no wait.
func (t attemptTable[I]) wait(key types.NamespacedName, inputs I, now time.Time) (time.Duration, bool) {
	a, ok := t[key]
	if !ok || a.inputs != inputs {
		return 0, false
	}
	if d := a.notBefore.Sub(now); d > 0 {
		return d, true
	}
	return 0, false
}

// fail records a failure against inputs, arms the wait and returns the
// new consecutive count.
func (t attemptTable[I]) fail(key types.NamespacedName, inputs I, now time.Time, backoff func(uint32) time.Duration) (uint32, time.Duration) {
	a := t[key]
	a.count++
	a.inputs = inputs
	d := backoff(a.count)
	a.notBefore = now.Add(d)
	t[key] = a
	return a.count, d
}

// rearm shortens or extends the wait already recorded for key.
func (t attemptTable[I]) rearm(key types.NamespacedName, now time.Time, d time.Duration) {
	if a, ok := t[key]; ok {
		a.notBefore = now.Add(d)
		t[key] = a
	}
}

// seed restores a count persisted on status without arming a wait, so
// a restart does not hand a wedged mirror a fresh budget.
func (t attemptTable[I]) seed(key types.NamespacedName, count uint32) {
	if _, ok := t[key]; !ok && count > 0 {
		t[key] = attempt[I]{count: count}
	}
}

func (t attemptTable[I]) count(key types.NamespacedName) uint32 { return t[key].count }

// sourceIsDelivering reports whether the source half of the mirror is
// putting grains on the wire, judged against the moment the target's
// fabric side opened.
//
// The target's stuck-handshake watchdog needs this to tell a wedge
// from an idle producer, and status.lastSentAt alone cannot answer it:
// the timestamp only moves on a successful transfer, so a source that
// cannot reach the target never advances it and reads exactly like a
// producer with nothing to send. The source publishes
// ReasonTransfersNotLanding for that case, which is the same claim
// lastSentAt would have made had a transfer been possible.
func sourceIsDelivering(mirror *mxlv1alpha1.MxlFlowMirror, openedAt time.Time) bool {
	if mirror == nil {
		return false
	}
	if mirror.Status.LastSentAt != nil && mirror.Status.LastSentAt.Time.After(openedAt) {
		return true
	}
	for _, c := range mirror.Status.Conditions {
		if c.Type == mxlv1alpha1.ConditionTypeSourceProgress {
			return c.Reason == mxlv1alpha1.ReasonTransfersNotLanding
		}
	}
	return false
}
