package mirror

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// progressBlocking partitions the fabric providers by whether their
// completion queue needs a blocking MakeProgress call. EFA drains its
// CQ on the provider's own event queue, so a non-blocking poll is
// correct. The verbs provider does not, and a non-blocking poll returns
// before the NIC finishes the DMA -- so the send queue fills and the
// next transfer call returns EAGAIN. TCP and SHM are loopback, where
// blocking is harmless but buys nothing; the selection keeps them on
// the blocking path because the cost is one short sleep and the
// benefit is a drained CQ on every provider that is not EFA.
func TestProgressBlocking_PartitionsEFAFromEverythingElse(t *testing.T) {
	assert.False(t, progressBlocking(fabrics.ProviderEFA),
		"EFA drains its completion queue on the provider's event queue; a blocking call adds latency without improving throughput")
	for _, p := range []fabrics.Provider{
		fabrics.ProviderVerbs,
		fabrics.ProviderTCP,
		fabrics.ProviderSHM,
		fabrics.ProviderAny,
	} {
		assert.True(t, progressBlocking(p),
			"provider %s needs a blocking MakeProgress so the CQ drains before the next transfer", p)
	}
}

func TestDefaultSampleProgressInterval_48kHz(t *testing.T) {
	// 48000/1 with a 480-sample batch: 480/48000 s = 10 ms.
	got := defaultSampleProgressInterval(mxl.Rational{Num: 48000, Den: 1}, 480)
	assert.Equal(t, 10*time.Millisecond, got,
		"48 kHz with a 480-sample batch should yield a 10 ms interval")
}

func TestDefaultSampleProgressInterval_ZeroBatchFallsBack(t *testing.T) {
	assert.Zero(t, defaultSampleProgressInterval(mxl.Rational{Num: 48000, Den: 1}, 0),
		"a zero batch leaves the interval to the caller's fallback")
}

func TestDefaultSampleProgressInterval_ZeroRateFallsBack(t *testing.T) {
	assert.Zero(t, defaultSampleProgressInterval(mxl.Rational{}, 480),
		"a zero rate leaves the interval to the caller's fallback")
}
