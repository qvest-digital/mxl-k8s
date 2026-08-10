package domain

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Stat reports the filesystem backing the domain directory. On the
// clusters that is the tmpfs the flows' shared memory lives in, so
// available bytes is the headroom the next flow has to allocate from.
func (d *Domain) Stat() (FSStat, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(d.path, &st); err != nil {
		return FSStat{}, fmt.Errorf("statfs %q: %w", d.path, err)
	}
	bsize := uint64(st.Bsize)
	total := st.Blocks * bsize
	avail := st.Bavail * bsize
	return FSStat{
		TotalBytes:     total,
		AvailableBytes: avail,
		UsedBytes:      (st.Blocks - st.Bfree) * bsize,
	}, nil
}
