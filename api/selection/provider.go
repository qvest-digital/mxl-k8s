// Package selection resolves the libmxl-fabrics provider a cross-node
// MxlFlowMirror should use from the capabilities the gateway probed on
// its two nodes.
//
// libmxl-fabrics v1.1.0-beta-1 dropped automatic provider resolution:
// provider selection is a downstream concern. This package is the seam
// that owns that decision so the agent and operator can stamp a
// concrete provider onto every mirror before the gateway sets it up.
package selection

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"

	"github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// crossNodePreference lists the providers usable for a cross-node
// mirror, most preferred first. shm is deliberately absent: it moves
// grains through host-local shared memory and cannot bridge two nodes,
// and every mirror this resolver selects for is cross-node. auto is
// absent because it is a request for resolution, not a concrete
// provider, so Resolve never returns it.
var crossNodePreference = []v1alpha1.MxlFabricsProvider{
	v1alpha1.ProviderEFA,
	v1alpha1.ProviderVerbs,
	v1alpha1.ProviderTCP,
}

// ErrCapabilitiesUnknown reports that at least one node advertises no
// usable cross-node provider: its gateway has not published an
// MxlNodeCapabilities yet, or it probed and found no device for any
// provider that can bridge two nodes. Resolve returns ProviderTCP
// alongside it so the mirror still comes up on the provider every host
// supports.
var ErrCapabilitiesUnknown = errors.New("node capabilities not recorded")

// ErrNoCommonProvider reports that both nodes advertised cross-node
// providers but share none. Resolve returns ProviderTCP alongside it so
// a caller can proceed on the lowest common denominator while logging
// why the richer providers were skipped.
var ErrNoCommonProvider = errors.New("source and target share no cross-node provider")

// Resolve picks the libmxl-fabrics provider a cross-node mirror should
// use, given the capabilities the gateway probed on the source and
// target nodes. It returns a concrete provider - never auto - by
// intersecting the two nodes' advertised providers and taking the most
// preferred entry (efa > verbs > tcp). shm is never selected because
// the mirror path always spans two nodes.
//
// The returned provider is always usable. A non-nil error is advisory:
// it names why Resolve had to fall back to tcp rather than pick from a
// real intersection - ErrCapabilitiesUnknown when a side advertises
// nothing, ErrNoCommonProvider when the two sides are disjoint - so
// callers can log the fallback while still stamping the returned
// provider onto the mirror.
//
// A provider a node probed and found no device for is excluded rather
// than preferred, so the intersection is drawn from what the two nodes
// can actually do. See providerSet for why that reading is conditional
// on the node reporting a probe at all.
func Resolve(source, target v1alpha1.MxlNodeCapabilitiesStatus) (v1alpha1.MxlFabricsProvider, error) {
	src := providerSet(source)
	tgt := providerSet(target)
	if len(src) == 0 || len(tgt) == 0 {
		return v1alpha1.ProviderTCP, fmt.Errorf("%w: source advertises %d, target %d cross-node providers",
			ErrCapabilitiesUnknown, len(src), len(tgt))
	}
	for _, p := range crossNodePreference {
		_, inSrc := src[p]
		_, inTgt := tgt[p]
		if inSrc && inTgt {
			return p, nil
		}
	}
	return v1alpha1.ProviderTCP, ErrNoCommonProvider
}

// providerSet collects the cross-node-relevant providers a node
// advertises. Entries outside crossNodePreference (shm, auto, or an
// unknown future name) are dropped so they never influence selection.
//
// DeviceCount decides membership only when the node reports a probe.
// A gateway that enumerated its providers reports zero for one whose
// hardware is absent, and selecting it would fail at setup on that
// node. A gateway that reports no probe published its configured
// list, where zero is the value every entry carries whether or not
// the provider works, so reading it there would strand every mirror
// in the cluster on the tcp fallback for as long as one un-upgraded
// gateway remains.
func providerSet(status v1alpha1.MxlNodeCapabilitiesStatus) map[v1alpha1.MxlFabricsProvider]struct{} {
	probed := meta.IsStatusConditionTrue(status.Conditions, v1alpha1.ConditionTypeProbed)
	out := make(map[v1alpha1.MxlFabricsProvider]struct{}, len(status.Providers))
	for _, pc := range status.Providers {
		if probed && pc.DeviceCount <= 0 {
			continue
		}
		for _, known := range crossNodePreference {
			if pc.Name == known {
				out[pc.Name] = struct{}{}
			}
		}
	}
	return out
}
