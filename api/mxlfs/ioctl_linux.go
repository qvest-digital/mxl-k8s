//go:build linux

package mxlfs

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

func ioctl(f *os.File, name string, req uintptr, arg unsafe.Pointer) error {
	_, _, e := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), req, uintptr(arg))
	runtime.KeepAlive(f)
	if e != 0 {
		return fmt.Errorf("%s on %s: %w", name, f.Name(), e)
	}
	return nil
}

// SetNode sets the node later allocations of f's file prefer, or clears
// it with NodeNone. Needs CAP_SYS_NICE; EBUSY while another open file
// holds the file.
func SetNode(f *os.File, node int32) error {
	p := nodePref{Node: node}
	return ioctl(f, "MXLFS_IOC_SET_NODE", iocSetNode, unsafe.Pointer(&p))
}

// NodePages reports where the pages of f's file are, from the module's
// counters. Needs no privilege.
func NodePages(f *os.File) (Pages, error) {
	n := uint32(maxNodes)
	for {
		buf := make([]uint64, n)
		p := nodePages{NrNodes: n}
		p.PagesPtr = uint64(uintptr(unsafe.Pointer(&buf[0])))
		err := ioctl(f, "MXLFS_IOC_NODE_PAGES", iocNodePages, unsafe.Pointer(&p))
		runtime.KeepAlive(buf)
		if err != nil {
			return Pages{}, err
		}
		if p.NrNodes > n {
			n = p.NrNodes
			continue
		}
		return Pages{PerNode: buf[:p.NrNodes], Pref: p.Pref, Effective: p.Effective, Holds: p.Holds}, nil
	}
}

// MoveTo moves every block of f's file that is not on node to node. The
// returned Move is filled as far as the module got, also when err is set.
// Needs CAP_SYS_NICE unless flags carries MoveDryRun.
func MoveTo(f *os.File, node int32, flags uint32) (Move, error) {
	m := Move{Node: node, Flags: flags}
	err := ioctl(f, "MXLFS_IOC_MOVE", iocMove, unsafe.Pointer(&m))
	return m, err
}

// Hold takes (on) or drops this open file's hold on its file: while it
// holds, MOVE and SET_NODE through any other open file fail with EBUSY.
// The hold ends with the last close of f. Needs CAP_SYS_NICE.
func Hold(f *os.File, on bool) error {
	var v uint32
	if on {
		v = 1
	}
	return ioctl(f, "MXLFS_IOC_HOLD", iocHold, unsafe.Pointer(&v))
}

// MountNodes returns the per-node counters of the mount holding f, one
// entry per node id. Needs no privilege.
func MountNodes(f *os.File) ([]NodeStat, error) {
	n := uint32(maxNodes)
	for {
		buf := make([]NodeStat, n)
		m := mountNodes{Nr: n}
		m.StatsPtr = uint64(uintptr(unsafe.Pointer(&buf[0])))
		err := ioctl(f, "MXLFS_IOC_MOUNT_NODES", iocMountNodes, unsafe.Pointer(&m))
		runtime.KeepAlive(buf)
		if err != nil {
			return nil, err
		}
		if m.Nr > n {
			n = m.Nr
			continue
		}
		return buf[:m.Nr], nil
	}
}

// ReadStats returns the counters of the mount holding f.
func ReadStats(f *os.File) (Stats, error) {
	var s Stats
	err := ioctl(f, "MXLFS_IOC_STATS", iocStats, unsafe.Pointer(&s))
	return s, err
}
