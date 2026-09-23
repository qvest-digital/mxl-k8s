package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const refID = "11111111-2222-4333-8444-555555555555"

// The primary domain keeps every name it had, which is what lets
// objects written before domains existed carry on without migration.
func TestFlowRef_PrimaryDomainKeepsExistingNames(t *testing.T) {
	r := FlowRef{ID: refID}
	assert.Equal(t, refID, r.Name())
	assert.Equal(t, MirrorName(refID, "n1"), MirrorNameFor(r, "n1"))
	assert.Equal(t, LeaseName(refID, "n1"), LeaseNameFor(r, "n1"))
}

// The same id in two domains is two flows, so every name differs.
func TestFlowRef_OtherDomainsAreDistinct(t *testing.T) {
	a, b := FlowRef{ID: refID}, FlowRef{Domain: "studio-b", ID: refID}
	assert.NotEqual(t, a.Name(), b.Name())
	assert.NotEqual(t, MirrorNameFor(a, "n1"), MirrorNameFor(b, "n1"))
	assert.NotEqual(t, LeaseNameFor(a, "n1"), LeaseNameFor(b, "n1"))
	assert.Equal(t, b, ParseFlowName(b.Name()))
	assert.Equal(t, a, ParseFlowName(a.Name()))
}

func TestParseLeaseNameFor_RoundTrip(t *testing.T) {
	for _, ref := range []FlowRef{{ID: refID}, {Domain: "studio-b", ID: refID}, {Domain: "a.b", ID: refID}} {
		for _, node := range []string{"n07", "ip-10-66-1-235", "worker-1.eu-central-1.compute.internal"} {
			got, gotNode, ok := ParseLeaseNameFor(LeaseNameFor(ref, node))
			require.True(t, ok, "%v %s", ref, node)
			assert.Equal(t, ref, got)
			assert.Equal(t, node, gotNode)
		}
	}
	_, _, ok := ParseLeaseNameFor("mxl-flow-studio.not-an-id-n1")
	assert.False(t, ok)
}
