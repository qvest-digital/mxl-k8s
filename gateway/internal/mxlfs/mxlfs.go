// Package mxlfs drives the ioctls of mxlfs, a memory filesystem for MXL
// domains (github.com/danielclasen/mxlfs), from the gateway.
//
// libmxl builds a complete temporary flow, fallocate of every grain
// included, before it finds that a flow it is asked to write already
// exists. The gateway opens existing flows that way whenever a mirror
// targets a flow that is already in the domain. On an mxlfs domain the
// gateway therefore allocates lazily, so a temporary flow costs its
// headers only, and allocates the flows it fills explicitly.
package mxlfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Magic is the f_type statfs reports for an mxlfs.
const Magic = 0x6d786c66

// Ioctl numbers from mxlfs_uapi.h: _IOW('x', 7, __u32) and _IO('x', 8).
const (
	iocLazyAlloc = 0x40047807
	iocAllocate  = 0x7808
)

// IsMxlfs reports whether path is on an mxlfs.
func IsMxlfs(path string) (bool, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false, fmt.Errorf("statfs %q: %w", path, err)
	}
	return st.Type == Magic, nil
}

// SetLazy turns lazy allocation on or off for the calling process on the
// mxlfs holding dir: with it on, the process's fallocate calls there only
// set file sizes. It needs no privilege, as it changes only how the
// caller's own fallocate calls allocate.
func SetLazy(dir string, on bool) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	v := 0
	if on {
		v = 1
	}
	if err := unix.IoctlSetPointerInt(int(f.Fd()), iocLazyAlloc, v); err != nil {
		return fmt.Errorf("MXLFS_IOC_LAZY_ALLOC on %q: %w", dir, err)
	}
	return nil
}

// Allocate allocates all of the file at path in large folios, whatever
// the caller's allocation mode.
func Allocate(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := unix.IoctlSetInt(int(f.Fd()), iocAllocate, 0); err != nil {
		return fmt.Errorf("MXLFS_IOC_ALLOCATE on %q: %w", path, err)
	}
	return nil
}

// FlowDir returns the directory libmxl keeps flow id in, in domain.
func FlowDir(domain string, id [16]byte) string {
	return filepath.Join(domain, fmt.Sprintf("%x-%x-%x-%x-%x.mxl-flow", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16]))
}

// AllocateFlow allocates the ring of the flow directory flowDir: every
// grain file of a discrete flow, or the channels file of a continuous
// one.
func AllocateFlow(flowDir string) error {
	grains, err := filepath.Glob(filepath.Join(flowDir, "grains", "data.*"))
	if err != nil {
		return err
	}
	if len(grains) == 0 {
		channels := filepath.Join(flowDir, "channels")
		if _, err := os.Stat(channels); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%s holds neither grains nor channels", flowDir)
			}
			return err
		}
		grains = []string{channels}
	}
	for _, g := range grains {
		if err := Allocate(g); err != nil {
			return err
		}
	}
	return nil
}
