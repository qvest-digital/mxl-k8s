package selection

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// caps builds an MxlNodeCapabilitiesStatus advertising the named
// providers. DeviceCount is left at zero on purpose: the beta gateway
// probe reports zero for working providers, and Resolve must not gate
// on it.
func caps(names ...v1alpha1.MxlFabricsProvider) v1alpha1.MxlNodeCapabilitiesStatus {
	s := v1alpha1.MxlNodeCapabilitiesStatus{}
	for _, n := range names {
		s.Providers = append(s.Providers, v1alpha1.MxlFabricsProviderCapability{Name: n})
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
			name:   "efa selected despite zero DeviceCount on both sides",
			source: caps(v1alpha1.ProviderTCP, v1alpha1.ProviderEFA),
			target: caps(v1alpha1.ProviderEFA),
			want:   v1alpha1.ProviderEFA,
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
