package capabilities

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/fabric"
)

// InterfaceLister enumerates the fabric interfaces libmxl-fabrics
// offers on this host. Production binds it to *fabrics.Instance; tests
// bind it to a stub, which is what keeps this package exercisable
// without a libmxl domain.
type InterfaceLister interface {
	Interfaces(query *fabrics.InterfaceConfig) ([]fabrics.InterfaceConfig, error)
}

// HostDeviceLister reports the RDMA devices the host kernel exposes
// with at least one active port. Production binds it to an
// rdma.Inventory; tests bind it to a stub.
type HostDeviceLister interface {
	ActiveDevices() ([]string, error)
}

// rdmaProviders are the providers that drive RDMA hardware, so the
// ones whose absence the host device list can contradict. tcp and shm
// need no device and never appear here.
var rdmaProviders = []fabrics.Provider{
	fabrics.ProviderEFA,
	fabrics.ProviderVerbs,
}

// Publisher creates and refreshes the MxlNodeCapabilities CR for this
// gateway, reporting the providers libmxl-fabrics can drive on the
// node rather than the ones the gateway was configured with.
type Publisher struct {
	Client client.Client

	// Recorder publishes probe transitions. Nil records nothing.
	Recorder record.EventRecorder

	// NodeName is the Kubernetes node this gateway runs on.
	NodeName string

	// Providers is the upper bound on what may be advertised. A
	// provider outside it is never published, even where the hardware
	// supports it. A list holding fabrics.ProviderAny imposes no
	// bound.
	Providers []fabrics.Provider

	// Lister is the libmxl-fabrics interface enumeration. A nil
	// Lister reports the probe as failed rather than falling back to
	// the configured list, which would put an unmeasured claim behind
	// a Probed condition and make deviceCount unreadable again.
	Lister InterfaceLister

	// Selector narrows the enumeration to the interfaces this node
	// may carry MXL traffic on. The mirror setup path applies the
	// same one, so nothing is advertised that a setup would refuse.
	Selector fabric.Selector

	// HostDevices reports the host's RDMA devices, which is the only
	// thing that separates a provider that measured no hardware from
	// one that enumerated before the hardware was usable. Both
	// publish the same empty entry. Nil skips the cross-check and
	// leaves the condition unset.
	HostDevices HostDeviceLister

	// BindAddress narrows the enumeration query the way a mirror
	// setup narrows it, so a gateway pinned to one address does not
	// advertise capacity it would never bind.
	BindAddress string

	// APIReader reads the owner Node. A cached read would need list
	// and watch on nodes, which the gateway's ClusterRole does not
	// grant.
	APIReader client.Reader

	// ProbePeriod is the shortest interval between two enumerations.
	// Zero enumerates on every Refresh. libfabric sweeps every
	// provider it was built with per call and warns about each one it
	// finds no device for, so the sweep is rate-limited separately
	// from the status refresh.
	ProbePeriod time.Duration

	// Refresh runs from a single goroutine, so these need no lock.
	lastProbe time.Time
	cached    []mxlv1alpha1.MxlFabricsProviderCapability
}

// Name is the metadata.name used for the gateway's
// MxlNodeCapabilities resource (one per node, keyed by node name).
func (p *Publisher) Name() string { return p.NodeName }

// EnsureExists creates the MxlNodeCapabilities resource if it isn't
// present, owned by the Node it describes. Status is left to be
// populated by Refresh.
//
// The owner reference is the only thing that deletes one: nothing
// else does, so without it a cluster accumulates a resource per node
// that ever existed. Both kinds are cluster-scoped, so garbage
// collection acts on the reference without a controller here.
func (p *Publisher) EnsureExists(ctx context.Context) error {
	l := log.FromContext(ctx)

	owner, err := p.nodeOwnerRef(ctx)
	if err != nil {
		return err
	}

	obj := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{
			Name:            p.Name(),
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: p.NodeName},
	}
	if err := p.Client.Create(ctx, obj); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create MxlNodeCapabilities: %w", err)
		}
		l.V(1).Info("MxlNodeCapabilities already exists", "name", p.Name())
		return p.adoptExisting(ctx, owner)
	}
	l.Info("created MxlNodeCapabilities", "name", p.Name())
	return nil
}

// nodeOwnerRef builds the owner reference to this gateway's Node. The
// UID comes from the live object because garbage collection treats a
// reference whose UID does not match as dangling and deletes the
// dependent at once.
func (p *Publisher) nodeOwnerRef(ctx context.Context) (metav1.OwnerReference, error) {
	var node corev1.Node
	if err := p.nodeReader().Get(ctx, types.NamespacedName{Name: p.NodeName}, &node); err != nil {
		return metav1.OwnerReference{}, fmt.Errorf("get Node %q for owner reference: %w", p.NodeName, err)
	}
	return metav1.OwnerReference{
		APIVersion: corev1.SchemeGroupVersion.String(),
		Kind:       "Node",
		Name:       node.Name,
		UID:        node.UID,
		// A gateway that cannot reach the API must not hold a node
		// in Terminating.
		BlockOwnerDeletion: ptr.To(false),
	}, nil
}

// nodeReader falls back to the cached client when no APIReader is
// bound, which is the case in tests.
func (p *Publisher) nodeReader() client.Reader {
	if p.APIReader != nil {
		return p.APIReader
	}
	return p.Client
}

// adoptExisting sets the owner reference on a resource created
// without one, and replaces a reference left by an earlier node of the
// same name: a stale UID would have the resource collected out from
// under the running gateway.
func (p *Publisher) adoptExisting(ctx context.Context, owner metav1.OwnerReference) error {
	var obj mxlv1alpha1.MxlNodeCapabilities
	if err := p.Client.Get(ctx, types.NamespacedName{Name: p.Name()}, &obj); err != nil {
		return fmt.Errorf("get MxlNodeCapabilities for adoption: %w", err)
	}
	for _, ref := range obj.OwnerReferences {
		if ref.UID == owner.UID {
			return nil
		}
	}
	patch := client.MergeFrom(obj.DeepCopy())
	obj.OwnerReferences = []metav1.OwnerReference{owner}
	if err := p.Client.Patch(ctx, &obj, patch); err != nil {
		return fmt.Errorf("adopt MxlNodeCapabilities onto Node %q: %w", p.NodeName, err)
	}
	log.FromContext(ctx).Info("adopted MxlNodeCapabilities onto its node", "name", p.Name())
	return nil
}

// Refresh probes libmxl-fabrics and writes what it found to
// MxlNodeCapabilities status.
//
// A probe that fails leaves the previously published providers in
// place and records the failure on the Probed condition. Clearing them
// would drop every mirror on the node onto the tcp fallback over what
// is usually a transient enumeration error.
func (p *Publisher) Refresh(ctx context.Context) error {
	l := log.FromContext(ctx).WithName("capabilities")

	var obj mxlv1alpha1.MxlNodeCapabilities
	if err := p.Client.Get(ctx, types.NamespacedName{Name: p.Name()}, &obj); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get MxlNodeCapabilities: %w", err)
		}
		// EnsureExists runs once at startup, so without this a
		// resource removed under a running gateway is never rebuilt
		// and the node stops advertising until the pod restarts.
		if err := p.EnsureExists(ctx); err != nil {
			return err
		}
		if err := p.Client.Get(ctx, types.NamespacedName{Name: p.Name()}, &obj); err != nil {
			return fmt.Errorf("get MxlNodeCapabilities after recreate: %w", err)
		}
	}

	now := metav1.Now()
	obj.Status.LastSeen = &now

	probed, perr := p.probe(ctx)
	if perr != nil {
		// SetStatusCondition reports whether it changed anything, which
		// is the only way to event this without one line per resync:
		// the publisher runs on a timer and re-asserts the condition
		// every pass whether or not the node's fabric moved.
		changed := meta.SetStatusCondition(&obj.Status.Conditions, metav1.Condition{
			Type:    mxlv1alpha1.ConditionTypeProbed,
			Status:  metav1.ConditionFalse,
			Reason:  mxlv1alpha1.ReasonProbeFailed,
			Message: perr.Error(),
		})
		if changed && p.Recorder != nil {
			p.Recorder.Eventf(&obj, corev1.EventTypeWarning, mxlv1alpha1.ReasonProbeFailed,
				"Fabric probe failed on %s: %v", p.NodeName, perr)
		}
		if err := p.Client.Status().Update(ctx, &obj); err != nil {
			return fmt.Errorf("update MxlNodeCapabilities status after a failed probe (%v): %w", perr, err)
		}
		return fmt.Errorf("probe fabric interfaces: %w", perr)
	}

	obj.Status.Providers = probed
	recovered := meta.SetStatusCondition(&obj.Status.Conditions, metav1.Condition{
		Type:    mxlv1alpha1.ConditionTypeProbed,
		Status:  metav1.ConditionTrue,
		Reason:  mxlv1alpha1.ReasonProbeComplete,
		Message: fmt.Sprintf("libmxl-fabrics reports %d provider(s) on this node", len(probed)),
	})
	if recovered && p.Recorder != nil {
		p.Recorder.Eventf(&obj, corev1.EventTypeNormal, mxlv1alpha1.ReasonProbeComplete,
			"Fabric probe succeeded on %s: %d provider(s)", p.NodeName, len(probed))
	}

	// Log and event are transition-scoped; the state they announce
	// lasts until the process restarts. The condition is what carries
	// it for as long as it holds, which is why it is set even where
	// nothing is emitted.
	if cond, ok := p.hostDeviceCondition(probed); ok {
		if meta.SetStatusCondition(&obj.Status.Conditions, cond) {
			if cond.Status == metav1.ConditionTrue {
				l.V(1).Info("host RDMA devices are represented in the probed providers",
					"reason", cond.Reason, "detail", cond.Message)
			} else {
				l.Info("host RDMA devices are not represented in the probed providers",
					"reason", cond.Reason, "detail", cond.Message)
			}
			if p.Recorder != nil {
				eventType := corev1.EventTypeNormal
				if cond.Status != metav1.ConditionTrue {
					eventType = corev1.EventTypeWarning
				}
				p.Recorder.Eventf(&obj, eventType, cond.Reason,
					"Host RDMA devices on %s: %s", p.NodeName, cond.Message)
			}
		}
	}

	if err := p.Client.Status().Update(ctx, &obj); err != nil {
		return fmt.Errorf("update MxlNodeCapabilities status: %w", err)
	}
	l.V(1).Info("published node capabilities", "providers", providerSummary(probed))
	return nil
}

// hostDeviceCondition compares the RDMA devices the host exposes
// against the providers the probe reported, and returns the condition
// to publish. The second return reports whether the comparison
// applies at all.
//
// It applies only when a host device list is configured and the
// provider bound admits a provider that drives RDMA hardware. A
// gateway configured for tcp alone advertises no RDMA provider by
// instruction rather than by measurement, and has nothing to
// contradict.
func (p *Publisher) hostDeviceCondition(probed []mxlv1alpha1.MxlFabricsProviderCapability) (metav1.Condition, bool) {
	if p.HostDevices == nil {
		return metav1.Condition{}, false
	}
	admitsRDMA := false
	for _, provider := range rdmaProviders {
		if p.allows(provider) {
			admitsRDMA = true
			break
		}
	}
	if !admitsRDMA {
		return metav1.Condition{}, false
	}

	devices, err := p.HostDevices.ActiveDevices()
	if err != nil {
		// Unknown rather than False: the providers stand on their own
		// measurement, and only the cross-check went missing.
		return metav1.Condition{
			Type:    mxlv1alpha1.ConditionTypeRDMADevicesEnumerated,
			Status:  metav1.ConditionUnknown,
			Reason:  mxlv1alpha1.ReasonHostDevicesUnreadable,
			Message: fmt.Sprintf("host RDMA device list unreadable: %v", err),
		}, true
	}

	if enumerated := rdmaDeviceCount(probed); enumerated > 0 || len(devices) == 0 {
		msg := fmt.Sprintf("%d host device(s) active, %d enumerated across %s",
			len(devices), enumerated, providerNames(rdmaProviders))
		return metav1.Condition{
			Type:    mxlv1alpha1.ConditionTypeRDMADevicesEnumerated,
			Status:  metav1.ConditionTrue,
			Reason:  mxlv1alpha1.ReasonHostDevicesRepresented,
			Message: msg,
		}, true
	}

	return metav1.Condition{
		Type:   mxlv1alpha1.ConditionTypeRDMADevicesEnumerated,
		Status: metav1.ConditionFalse,
		Reason: mxlv1alpha1.ReasonHostDevicesUnenumerated,
		Message: fmt.Sprintf(
			"host exposes %s with an active port and no RDMA provider enumerated a device; "+
				"mirrors on this node resolve to a non-RDMA provider until the gateway restarts",
			strings.Join(devices, ", ")),
	}, true
}

// rdmaDeviceCount totals the devices reported across the providers
// that drive RDMA hardware.
func rdmaDeviceCount(probed []mxlv1alpha1.MxlFabricsProviderCapability) int {
	total := 0
	for _, capability := range probed {
		for _, provider := range rdmaProviders {
			if string(capability.Name) == provider.String() {
				total += int(capability.DeviceCount)
				break
			}
		}
	}
	return total
}

// providerNames renders providers for a condition message.
func providerNames(providers []fabrics.Provider) string {
	names := make([]string, 0, len(providers))
	for _, provider := range providers {
		names = append(names, provider.String())
	}
	return strings.Join(names, "/")
}

// probe enumerates the host's fabric interfaces and folds them into
// one capability entry per provider, reusing the previous result
// until ProbePeriod has elapsed.
func (p *Publisher) probe(ctx context.Context) ([]mxlv1alpha1.MxlFabricsProviderCapability, error) {
	l := log.FromContext(ctx).WithName("capabilities")
	if p.Lister == nil {
		return nil, fmt.Errorf("no libmxl-fabrics interface lister configured")
	}
	if p.cached != nil && time.Since(p.lastProbe) < p.ProbePeriod {
		return p.cached, nil
	}

	// ProviderAny is the enumeration's "do not filter by provider",
	// so one sweep answers for every provider at once.
	query := &fabrics.InterfaceConfig{Provider: fabrics.ProviderAny}
	if p.BindAddress != "" {
		query.Address.Node = p.BindAddress
	}
	ifaces, err := p.Lister.Interfaces(query)
	if err != nil {
		return nil, fmt.Errorf("query fabric interfaces: %w", err)
	}

	kept, rejected := p.Selector.Select(ifaces)
	for _, r := range rejected {
		l.V(1).Info("interface excluded from the node fabric", "interface", r.String())
	}
	if len(kept) == 0 && len(rejected) > 0 {
		// A node that advertises nothing takes every mirror it hosts
		// down to the tcp fallback, and the reason is otherwise only
		// visible at raised verbosity.
		l.Info("no fabric interface passed selection",
			"rejected", len(rejected), "first", rejected[0].String())
	}

	byProvider := make(map[fabrics.Provider][]fabrics.InterfaceConfig)
	for _, iface := range kept {
		byProvider[iface.Provider] = append(byProvider[iface.Provider], iface)
	}

	// One entry per provider the upper bound admits, fastest first,
	// including providers the sweep found nothing for: a published
	// zero is what tells a consumer the hardware is absent rather
	// than unconsidered.
	out := make([]mxlv1alpha1.MxlFabricsProviderCapability, 0, len(probeOrder))
	for _, provider := range probeOrder {
		if !p.allows(provider) {
			continue
		}
		name := provider.String()
		if name == "" {
			continue
		}
		group := byProvider[provider]
		out = append(out, mxlv1alpha1.MxlFabricsProviderCapability{
			Name:        mxlv1alpha1.MxlFabricsProvider(name),
			DeviceCount: int32(fabric.DeviceCount(group)),
			Interfaces:  describe(fabric.Dedupe(group)),
		})
	}

	p.cached = out
	p.lastProbe = time.Now()
	return out, nil
}

// probeOrder lists the concrete providers a probe can report, fastest
// first. ProviderAny is absent: it names no provider, and an
// enumeration answers with concrete ones.
var probeOrder = []fabrics.Provider{
	fabrics.ProviderEFA,
	fabrics.ProviderVerbs,
	fabrics.ProviderTCP,
	fabrics.ProviderSHM,
}

// allows reports whether the configured upper bound admits provider.
// A bound naming ProviderAny admits every provider.
func (p *Publisher) allows(provider fabrics.Provider) bool {
	if slices.Contains(p.Providers, fabrics.ProviderAny) {
		return true
	}
	return slices.Contains(p.Providers, provider)
}

// describe renders enumerated interfaces into their API form.
func describe(ifaces []fabrics.InterfaceConfig) []mxlv1alpha1.MxlFabricInterface {
	if len(ifaces) == 0 {
		return nil
	}
	out := make([]mxlv1alpha1.MxlFabricInterface, 0, len(ifaces))
	for _, iface := range ifaces {
		// A parse failure costs the detail, not the entry: the
		// interface is one a mirror can be set up on either way.
		attrs, _ := fabric.ParseAttributes(iface.Attr)
		out = append(out, mxlv1alpha1.MxlFabricInterface{
			Address:                iface.Address.Node,
			Device:                 attrs.Device,
			LinkState:              attrs.LinkState,
			LinkSpeedBitsPerSecond: int64(attrs.LinkSpeed),
			MaxMessageSize:         int64(iface.Caps.MaxMessageSize),
			PCIAddress:             attrs.PCIAddress,
		})
	}
	return out
}

// providerSummary renders the published providers for a log line.
func providerSummary(caps []mxlv1alpha1.MxlFabricsProviderCapability) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		out = append(out, fmt.Sprintf("%s=%d", c.Name, c.DeviceCount))
	}
	return out
}

// RunRefreshLoop calls Refresh on every tick until ctx is canceled.
func (p *Publisher) RunRefreshLoop(ctx context.Context, period time.Duration) {
	l := log.FromContext(ctx).WithName("capabilities")
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		if err := p.Refresh(ctx); err != nil {
			l.Error(err, "refresh failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
