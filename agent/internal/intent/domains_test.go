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
		ByDir:   map[string]string{"domain": "default", "studio-b": "studio-b"},
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
