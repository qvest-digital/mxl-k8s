package intent

import (
	"path/filepath"
	"strings"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Domains is the MXL domains materialised on this node: the runtime root
// every domain directory sits in, the primary domain's directory, and the
// MxlDomain name of each directory.
//
// The primary domain is the one --domain-path names. Its flows are spelled
// with no domain name, which is how every object written before domains
// existed names them.
type Domains struct {
	Root    string
	Primary string
	ByDir   map[string]string
}

// FlowRefFromPath is the flow a path names: <root>/<dir>/<id>.mxl-flow, the
// directory itself or anything in it, in a directory an MxlDomain
// materialises here. libmxl probes the flow directory and the files in it
// before flow_def.json, so the shim can report any of them.
func (d Domains) FlowRefFromPath(path string) (mxlv1alpha1.FlowRef, bool) {
	rel, err := filepath.Rel(filepath.Clean(d.Root), filepath.Clean(path))
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return mxlv1alpha1.FlowRef{}, false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) < 2 {
		return mxlv1alpha1.FlowRef{}, false
	}
	name, ok := d.ByDir[parts[0]]
	if !ok {
		return mxlv1alpha1.FlowRef{}, false
	}
	id, ok := strings.CutSuffix(parts[1], flowDirSuffix)
	if !ok || id == "" {
		return mxlv1alpha1.FlowRef{}, false
	}
	if parts[0] == d.Primary {
		name = ""
	}
	return mxlv1alpha1.FlowRef{Domain: name, ID: id}, true
}

// flowDirSuffix marks a flow directory inside a domain.
const flowDirSuffix = ".mxl-flow"
