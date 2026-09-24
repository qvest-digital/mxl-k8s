package v1alpha1

import (
	"testing"
	"time"

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

// A flow's Origin Lease is keyed by the flow's own name. For a flow in
// another domain the bare id would name the primary domain's flow of the
// same id, so its Lease -- live or not -- would decide the other's origin.
func TestResolveOrigin_ChecksTheLeaseOfTheFlowItsDomainNames(t *testing.T) {
	flow := &MxlFlow{Spec: MxlFlowSpec{ID: refID, Domain: "studio-b"}}
	flow.Status.Locations = []MxlFlowLocation{{NodeName: "n2", Phase: MxlFlowLocationOrigin}}

	var asked string
	res, err := ResolveOrigin(flow, func(name, node string) (bool, time.Time, error) {
		asked = name
		return true, time.Time{}, nil
	})
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.Equal(t, "studio-b."+refID, asked)

	primary := &MxlFlow{Spec: MxlFlowSpec{ID: refID}}
	primary.Status.Locations = flow.Status.Locations
	_, err = ResolveOrigin(primary, func(name, node string) (bool, time.Time, error) {
		asked = name
		return true, time.Time{}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, refID, asked, "the primary domain keeps the bare id")
}

// A domain created without a directory lives at domains/<id>, so its path
// is unique by construction and does not change while the id cannot; one
// with a directory keeps it, which is how the primary domain keeps the
// path every existing flow was written under.
func TestMxlDomainSpecPath(t *testing.T) {
	const id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	if got := (&MxlDomainSpec{ID: id}).Path(); got != "domains/"+id {
		t.Errorf("no directory: got %q", got)
	}
	if got := (&MxlDomainSpec{ID: id, Directory: "domain"}).Path(); got != "domain" {
		t.Errorf("directory: got %q", got)
	}
}
