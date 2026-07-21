package mirror

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestProviderForSetup(t *testing.T) {
	mirror := func(p mxlv1alpha1.MxlFabricsProvider) *mxlv1alpha1.MxlFlowMirror {
		return &mxlv1alpha1.MxlFlowMirror{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m"},
			Spec:       mxlv1alpha1.MxlFlowMirrorSpec{Provider: p},
		}
	}

	t.Run("concrete providers pass through", func(t *testing.T) {
		cases := []struct {
			in   mxlv1alpha1.MxlFabricsProvider
			want fabrics.Provider
		}{
			{mxlv1alpha1.ProviderTCP, fabrics.ProviderTCP},
			{mxlv1alpha1.ProviderVerbs, fabrics.ProviderVerbs},
			{mxlv1alpha1.ProviderEFA, fabrics.ProviderEFA},
			{mxlv1alpha1.ProviderSHM, fabrics.ProviderSHM},
		}
		for _, tc := range cases {
			got, err := providerForSetup(mirror(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		}
	})

	t.Run("auto and empty are refused, naming the mirror", func(t *testing.T) {
		for _, p := range []mxlv1alpha1.MxlFabricsProvider{mxlv1alpha1.ProviderAuto, ""} {
			_, err := providerForSetup(mirror(p))
			require.ErrorIs(t, err, errProviderUnresolved,
				"auto must never reach libmxl-fabrics setup")
			assert.Contains(t, err.Error(), "ns/m",
				"the error must name the offending mirror for diagnostics")
		}
	})
}
