//go:build linux

package statfs

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Stats returns (capacityBytes, freeBytes) for the filesystem holding
// path. Uses statfs(2).
func Stats(path string) (int64, int64, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return 0, 0, fmt.Errorf("statfs %q: %w", path, err)
	}
	capacity := int64(s.Blocks) * int64(s.Bsize)
	free := int64(s.Bavail) * int64(s.Bsize)
	return capacity, free, nil
}
