package selection

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// caps builds the status of a node that reports no probe: providers
// listed, every DeviceCount zero. That is what a gateway published
// before it enumerated libmxl-fabrics, and Resolve must keep reading
// those entries as available.
func caps(names ...v1alpha1.MxlFabricsProvider) v1alpha1.MxlNodeCapabilitiesStatus {
	s := v1alpha1.MxlNodeCapabilitiesStatus{}
	for _, n := range names {
		s.Providers = append(s.Providers, v1alpha1.MxlFabricsProviderCapability{Name: n})
	}
	return s
}

// probedCaps builds the status of a node whose gateway enumerated
// libmxl-fabrics, giving each named provider one device.
func probedCaps(names ...v1alpha1.MxlFabricsProvider) v1alpha1.MxlNodeCapabilitiesStatus {
	s := v1alpha1.MxlNodeCapabilitiesStatus{
		Conditions: []metav1.Condition{{
			Type:   v1alpha1.ConditionTypeProbed,
			Status: metav1.ConditionTrue,
			Reason: v1alpha1.ReasonProbeComplete,
		}},
	}
	for _, n := range names {
		s.Providers = append(s.Providers, v1alpha1.MxlFabricsProviderCapability{Name: n, DeviceCount: 1})
	}
	return s
}

// withAbsent adds a provider the node probed and found no device for.
func withAbsent(s v1alpha1.MxlNodeCapabilitiesStatus, names ...v1alpha1.MxlFabricsProvider) v1alpha1.MxlNodeCapabilitiesStatus {
	for _, n := range names {
		s.Providers = append(s.Providers, v1alpha1.MxlFabricsProviderCapability{Name: n, DeviceCount: 0})
	}
	return s
}

func TestResolve(t *testing.T) {
	cases := []struct {
		name    string
		source  v1alpha1.MxlNodeCapabilitiesStatus
		target  v1alpha1.MxlNodeCapabilitiesStatus
		want    v1alpha1.MxlFabricsProvider
		wantErr error
	}{
		{
			name:   "plain intersection picks the shared provider",
			source: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs),
			target: caps(v1alpha1.ProviderVerbs),
			want:   v1alpha1.ProviderVerbs,
		},
		{
			name:   "preference order prefers efa over verbs and tcp",
			source: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs, v1alpha1.ProviderEFA),
			target: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs, v1alpha1.ProviderEFA),
			want:   v1alpha1.ProviderEFA,
		},
		{
			name:   "preference order prefers verbs over tcp",
			source: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs),
			target: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs),
			want:   v1alpha1.ProviderVerbs,
		},
		{
			name:   "tcp on both sides is a clean pick, not a fallback",
			source: caps(v1alpha1.ProviderTCP),
			target: caps(v1alpha1.ProviderTCP),
			want:   v1alpha1.ProviderTCP,
		},
		{
			name:   "efa selected despite zero DeviceCount on an unprobed node",
			source: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderEFA),
			target: caps(v1alpha1.ProviderEFA),
			want:   v1alpha1.ProviderEFA,
		},
		{
			// The mixed-hardware case: efa is listed on both nodes,
			// but only one has the adapter. Preferring it would fail
			// the setup on the node that cannot honour it.
			name:    "a probed provider with no device is excluded",
			source:  withAbsent(probedCaps(v1alpha1.ProviderTCP), v1alpha1.ProviderEFA),
			target:  probedCaps(v1alpha1.ProviderTCP, v1alpha1.ProviderEFA),
			want:    v1alpha1.ProviderTCP,
			wantErr: nil,
		},
		{
			name:   "a probed provider with a device is selected",
			source: probedCaps(v1alpha1.ProviderTCP, v1alpha1.ProviderEFA),
			target: probedCaps(v1alpha1.ProviderTCP, v1alpha1.ProviderEFA),
			want:   v1alpha1.ProviderEFA,
		},
		{
			// A gateway that has not rolled yet publishes its
			// configured list with every count at zero. Reading that
			// as absent hardware would strand the whole cluster on
			// tcp for the length of a DaemonSet rollout.
			name:   "a zero count on an unprobed node stays available",
			source: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs),
			target: probedCaps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs),
			want:   v1alpha1.ProviderVerbs,
		},
		{
			name: "a probe that failed does not gate on device counts",
			source: v1alpha1.MxlNodeCapabilitiesStatus{
				Conditions: []metav1.Condition{{
					Type:   v1alpha1.ConditionTypeProbed,
					Status: metav1.ConditionFalse,
					Reason: v1alpha1.ReasonProbeFailed,
				}},
				Providers: []v1alpha1.MxlFabricsProviderCapability{
					{Name: v1alpha1.ProviderVerbs, DeviceCount: 0},
				},
			},
			target: probedCaps(v1alpha1.ProviderVerbs),
			want:   v1alpha1.ProviderVerbs,
		},
		{
			name:    "a node whose probe found nothing falls back to tcp",
			source:  withAbsent(probedCaps(), v1alpha1.ProviderTCP, v1alpha1.ProviderEFA),
			target:  probedCaps(v1alpha1.ProviderTCP),
			want:    v1alpha1.ProviderTCP,
			wantErr: ErrCapabilitiesUnknown,
		},
		{
			name:    "target capabilities absent falls back to tcp",
			source:  caps(v1alpha1.ProviderTCP, v1alpha1.ProviderVerbs),
			target:  caps(),
			want:    v1alpha1.ProviderTCP,
			wantErr: ErrCapabilitiesUnknown,
		},
		{
			name:    "both sides absent falls back to tcp",
			source:  caps(),
			target:  caps(),
			want:    v1alpha1.ProviderTCP,
			wantErr: ErrCapabilitiesUnknown,
		},
		{
			name:    "shm-only node has no cross-node provider, falls back to tcp",
			source:  caps(v1alpha1.ProviderSHM),
			target:  caps(v1alpha1.ProviderSHM),
			want:    v1alpha1.ProviderTCP,
			wantErr: ErrCapabilitiesUnknown,
		},
		{
			name:    "disjoint provider sets fall back to tcp",
			source:  caps(v1alpha1.ProviderVerbs),
			target:  caps(v1alpha1.ProviderEFA),
			want:    v1alpha1.ProviderTCP,
			wantErr: ErrNoCommonProvider,
		},
		{
			name:   "shm is ignored when a real cross-node provider is shared",
			source: caps(v1alpha1.ProviderSHM, v1alpha1.ProviderTCP),
			target: caps(v1alpha1.ProviderSHM, v1alpha1.ProviderTCP),
			want:   v1alpha1.ProviderTCP,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Resolve(tc.source, tc.target)
			assert.Equal(t, tc.want, got)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.NotEqual(t, v1alpha1.ProviderAuto, got,
				"Resolve must never return auto; libmxl-fabrics no longer "+
					"resolves it, so a concrete provider is the whole point")
		})
	}
}
