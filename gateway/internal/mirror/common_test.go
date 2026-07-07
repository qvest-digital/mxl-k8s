package mirror

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestMapProvider(t *testing.T) {
	cases := []struct {
		in   mxlv1alpha1.MxlFabricsProvider
		want fabrics.Provider
	}{
		{mxlv1alpha1.ProviderTCP, fabrics.ProviderTCP},
		{mxlv1alpha1.ProviderVerbs, fabrics.ProviderVerbs},
		{mxlv1alpha1.ProviderEFA, fabrics.ProviderEFA},
		{mxlv1alpha1.ProviderSHM, fabrics.ProviderSHM},
		{mxlv1alpha1.ProviderAuto, fabrics.ProviderAny},
		{"", fabrics.ProviderAny},
		{"made-up", fabrics.ProviderAny},
	}
	for _, tc := range cases {
		t.Run(string(tc.in), func(t *testing.T) {
			assert.Equal(t, tc.want, mapProvider(tc.in),
				"every CRD enum value must map to a known fabrics provider; "+
					"silent fallthrough to Any for unknown inputs preserves "+
					"upgrade compatibility for receivers that pin a new provider "+
					"the gateway version does not understand yet")
		})
	}
}
