package mirror

import (
	"testing"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every target is set up with the completion queue depth that holds what
// a lagging read loop and a source with batches in flight can deliver
// between two reads; without the option libmxl-fabrics uses its small
// default.
func TestTargetConfig_CarriesTheCQDepth(t *testing.T) {
	_, inst := hintsDomain(t)
	w, _, err := inst.NewWriter(sharedFlowAudioDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	iface := fabrics.InterfaceConfig{Provider: fabrics.ProviderTCP}

	cfg := targetConfig(iface, w)

	assert.JSONEq(t, `{"cqDepth": 64}`, cfg.Options)
	assert.Same(t, w, cfg.Writer)
	assert.Equal(t, iface, cfg.Interface)
}
