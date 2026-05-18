package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qvest-digital/go-mxl/fabrics"
)

func TestProviderNames(t *testing.T) {
	cases := []struct {
		name string
		in   []fabrics.Provider
		want []string
	}{
		{"empty", nil, []string{}},
		{"single", []fabrics.Provider{fabrics.ProviderTCP}, []string{"tcp"}},
		{
			"multiple preserves order",
			[]fabrics.Provider{fabrics.ProviderTCP, fabrics.ProviderVerbs, fabrics.ProviderSHM},
			[]string{"tcp", "verbs", "shm"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := providerNames(tc.in)
			assert.Equal(t, tc.want, got)
		})
	}
}
