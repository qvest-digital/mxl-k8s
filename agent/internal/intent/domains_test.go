package intent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A path names a flow in whichever domain directory it is under, and the
// primary domain -- the one --domain-path names -- is spelled with no name, so
// every object that existed before domains keeps its key.
func TestFlowRefFromPath(t *testing.T) {
	const id = "11111111-2222-4333-8444-555555555555"
	domains := Domains{
		Root:    "/run/mxl",
		Primary: "domain",
		ByDir: map[string]string{
			"domain":   "default",
			"studio-b": "studio-b",
			"domains/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee": "studio-c",
		},
	}
	for _, tc := range []struct {
		name string
		path string
		want mxlv1alpha1.FlowRef
		ok   bool
	}{
		{"primary", "/run/mxl/domain/" + id + ".mxl-flow/flow_def.json",
			mxlv1alpha1.FlowRef{ID: id}, true},
		{"primary, directory itself", "/run/mxl/domain/" + id + ".mxl-flow",
			mxlv1alpha1.FlowRef{ID: id}, true},
		{"second domain", "/run/mxl/studio-b/" + id + ".mxl-flow/data",
			mxlv1alpha1.FlowRef{Domain: "studio-b", ID: id}, true},
		{"domain at domains/<id>", "/run/mxl/domains/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee/" + id + ".mxl-flow/data",
			mxlv1alpha1.FlowRef{Domain: "studio-c", ID: id}, true},
		{"domains/<id> no MxlDomain materialises here", "/run/mxl/domains/ffffffff-bbbb-4ccc-8ddd-eeeeeeeeeeee/" + id + ".mxl-flow",
			mxlv1alpha1.FlowRef{}, false},
		{"a directory no MxlDomain materialises here", "/run/mxl/other/" + id + ".mxl-flow/data",
			mxlv1alpha1.FlowRef{}, false},
		{"outside the runtime root", "/tmp/domain/" + id + ".mxl-flow/data",
			mxlv1alpha1.FlowRef{}, false},
		{"escapes the root", "/run/mxl/domain/../../etc/" + id + ".mxl-flow",
			mxlv1alpha1.FlowRef{}, false},
		{"not a flow directory", "/run/mxl/domain/domain_def.json",
			mxlv1alpha1.FlowRef{}, false},
		{"nested below a flow", "/run/mxl/studio-b/x/" + id + ".mxl-flow",
			mxlv1alpha1.FlowRef{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := domains.FlowRefFromPath(tc.path)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A domain in its own directory other than the primary's is not mounted in
// the gateway, so a mirror in it could never be written; it is left out of
// what the agent tracks and dispatches rather than failing on the gateway.
func TestMirrored(t *testing.T) {
	got := Mirrored("domain", map[string]string{
		"default":  "domain",
		"studio-b": "studio-b",
		"studio-c": "domains/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
	})
	assert.Equal(t, map[string]string{
		"default":  "domain",
		"studio-c": "domains/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
	}, got)
}
