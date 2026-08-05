package capabilities

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

// Publisher creates and refreshes the MxlNodeCapabilities CR for this
// gateway, reporting the providers libmxl-fabrics can drive on the
// node rather than the ones the gateway was configured with.
type Publisher struct {
	Client client.Client

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

	// BindAddress narrows the enumeration query the way a mirror
	// setup narrows it, so a gateway pinned to one address does not
	// advertise capacity it would never bind.
	BindAddress string
}

// Name is the metadata.name used for the gateway's
// MxlNodeCapabilities resource (one per node, keyed by node name).
func (p *Publisher) Name() string { return p.NodeName }

// EnsureExists creates the MxlNodeCapabilities resource if it isn't
// present. Status is left to be populated by Refresh.
func (p *Publisher) EnsureExists(ctx context.Context) error {
	l := log.FromContext(ctx)
	obj := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: p.Name()},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: p.NodeName},
	}
	if err := p.Client.Create(ctx, obj); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create MxlNodeCapabilities: %w", err)
		}
		l.V(1).Info("MxlNodeCapabilities already exists", "name", p.Name())
		return nil
	}
	l.Info("created MxlNodeCapabilities", "name", p.Name())
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
		return fmt.Errorf("get MxlNodeCapabilities: %w", err)
	}

	now := metav1.Now()
	obj.Status.LastSeen = &now

	probed, perr := p.probe(ctx)
	if perr != nil {
		meta.SetStatusCondition(&obj.Status.Conditions, metav1.Condition{
			Type:    mxlv1alpha1.ConditionTypeProbed,
			Status:  metav1.ConditionFalse,
			Reason:  mxlv1alpha1.ReasonProbeFailed,
			Message: perr.Error(),
		})
		if err := p.Client.Status().Update(ctx, &obj); err != nil {
			return fmt.Errorf("update MxlNodeCapabilities status after a failed probe (%v): %w", perr, err)
		}
		return fmt.Errorf("probe fabric interfaces: %w", perr)
	}

	obj.Status.Providers = probed
	meta.SetStatusCondition(&obj.Status.Conditions, metav1.Condition{
		Type:    mxlv1alpha1.ConditionTypeProbed,
		Status:  metav1.ConditionTrue,
		Reason:  mxlv1alpha1.ReasonProbeComplete,
		Message: fmt.Sprintf("libmxl-fabrics reports %d provider(s) on this node", len(probed)),
	})

	if err := p.Client.Status().Update(ctx, &obj); err != nil {
		return fmt.Errorf("update MxlNodeCapabilities status: %w", err)
	}
	l.V(1).Info("published node capabilities", "providers", providerSummary(probed))
	return nil
}

// probe enumerates the host's fabric interfaces and folds them into
// one capability entry per provider.
func (p *Publisher) probe(ctx context.Context) ([]mxlv1alpha1.MxlFabricsProviderCapability, error) {
	l := log.FromContext(ctx).WithName("capabilities")
	if p.Lister == nil {
		return nil, fmt.Errorf("no libmxl-fabrics interface lister configured")
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
