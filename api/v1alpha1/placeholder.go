package v1alpha1

import (
	"os"
	"path/filepath"
)

// PlaceholderMarkerDir is the directory in an mxlfs domain that holds one
// empty file, named after the flow id, per placeholder: a flow directory
// the gateway holds so the flow is listed on a node that has no copy of
// it. A flow with a marker is not a location of the flow.
const PlaceholderMarkerDir = ".mxlfs-placeholders"

// PlaceholderMarker is the marker of flow id in the domain at domainDir.
func PlaceholderMarker(domainDir, id string) string {
	return filepath.Join(domainDir, PlaceholderMarkerDir, id)
}

// IsPlaceholder reports whether flow id in the domain at domainDir is a
// placeholder.
func IsPlaceholder(domainDir, id string) bool {
	_, err := os.Stat(PlaceholderMarker(domainDir, id))
	return err == nil
}
