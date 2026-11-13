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
// materialises here. <dir> is one segment or domains/<id>. libmxl probes
// the flow directory and the files in it before flow_def.json, so the shim
// can report any of them.
func (d Domains) FlowRefFromPath(path string) (mxlv1alpha1.FlowRef, bool) {
	rel, err := filepath.Rel(filepath.Clean(d.Root), filepath.Clean(path))
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return mxlv1alpha1.FlowRef{}, false
	}
	for dir, name := range d.ByDir {
		rest, ok := strings.CutPrefix(rel, dir+string(filepath.Separator))
		if !ok {
			continue
		}
		flowDir, _, _ := strings.Cut(rest, string(filepath.Separator))
		id, ok := strings.CutSuffix(flowDir, flowDirSuffix)
		if !ok || id == "" {
			return mxlv1alpha1.FlowRef{}, false
		}
		if dir == d.Primary {
			name = ""
		}
		return mxlv1alpha1.FlowRef{Domain: name, ID: id}, true
	}
	return mxlv1alpha1.FlowRef{}, false
}

// Mirrored narrows the domains materialised on the node, name to directory,
// to those mirrored between nodes: the primary domain and every domain below
// domains/. The gateway mounts those two and nothing else of the runtime
// root, so a domain in any other directory stays node-local.
func Mirrored(primary string, materialised map[string]string) map[string]string {
	out := make(map[string]string, len(materialised))
	for name, dir := range materialised {
		if dir == primary || filepath.Dir(dir) == mxlv1alpha1.DomainsDir {
			out[name] = dir
		}
	}
	return out
}

// flowDirSuffix marks a flow directory inside a domain.
const flowDirSuffix = ".mxl-flow"
