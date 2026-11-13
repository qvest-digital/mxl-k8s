package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsPlaceholder(t *testing.T) {
	dir := t.TempDir()
	const id = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
	if IsPlaceholder(dir, id) {
		t.Fatal("placeholder without a marker")
	}
	if err := os.MkdirAll(filepath.Join(dir, PlaceholderMarkerDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(PlaceholderMarker(dir, id), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if !IsPlaceholder(dir, id) {
		t.Fatal("marker not recognised")
	}
}
