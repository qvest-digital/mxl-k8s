// Package shiminstall drops the LD_PRELOAD intent shim the agent
// image carries onto the node, next to the intent socket, so a
// consumer pod can preload it from the same hostPath it already
// mounts for the socket.
//
// It is the alternative to the carrier image
// (docker/shim.Dockerfile), which a consumer copies out of an
// initContainer. Consumers that cannot template an initContainer
// into their own pod spec have no way to reach the carrier image;
// the node-delivered copy needs nothing but the mount.
package shiminstall

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ImagePath is where the agent image carries the compiled shim. The
// carrier image uses the same path, so both deliver the same file
// from the same place.
const ImagePath = "/opt/mxl-intent/libmxl-intent.so"

// Install copies src to dst, replacing whatever is there.
//
// Run on every agent start: the shim and the agent ship in one
// image, so a node that has been upgraded must not keep serving the
// previous version's .so.
func Install(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".libmxl-intent-*.so")
	if err != nil {
		return fmt.Errorf("create temp in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name())

	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		return fmt.Errorf("copy %s: %w", src, err)
	}
	// CreateTemp opens 0600 and consumer pods run as their own uids.
	// Chmod on the handle rather than the path so the process umask
	// cannot narrow the result.
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp.Name(), err)
	}
	// Rename rather than write dst in place: no consumer ever opens a
	// half-written .so, and a pod that already mapped the old file
	// keeps that inode instead of faulting on changed contents.
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return fmt.Errorf("rename onto %s: %w", dst, err)
	}
	return nil
}
