// Package mxlfs mirrors the NUMA part of the mxlfs userspace interface,
// kernel/mxlfs_uapi.h in github.com/danielclasen/mxlfs: the ioctl
// numbers, the structs they take, and one call per ioctl. A module that
// predates these ioctls answers ENOTTY; Unsupported tells that apart.
// HostNICNode finds the NUMA node a placement moves rings to.
package mxlfs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// NodeNone is MXLFS_NODE_NONE: no node.
const NodeNone = -1

// Ioctl numbers: _IOC(dir, 'x', nr, size) with the struct sizes below.
const (
	iocStats      = 0x81107804 // _IOR('x', 4, struct mxlfs_stats), 272 bytes
	iocSetNode    = 0x4008780b // _IOW('x', 11, struct mxlfs_node_pref)
	iocNodePages  = 0xc020780c // _IOWR('x', 12, struct mxlfs_node_pages)
	iocMove       = 0xc050780d // _IOWR('x', 13, struct mxlfs_move)
	iocHold       = 0x4004780e // _IOW('x', 14, __u32)
	iocMountNodes = 0xc010780f // _IOWR('x', 15, struct mxlfs_mount_nodes)
)

// MOVE flags.
const (
	MoveDryRun = 1 << 0 // count and preflight only
	MoveStrict = 1 << 1 // any block left fails the call
)

// NodePagesRecount walks the page cache instead of reading the counters.
const NodePagesRecount = 1 << 0

// Result is what a MOVE did, MXLFS_MOVED_*.
type Result uint32

const (
	MovedAll Result = iota + 1
	MovedPartial
	MovedNone
)

// Reason is why a MOVE left a block where it was, MXLFS_REFUSE_*.
type Reason uint32

const (
	RefusePinned Reason = iota + 1 // folio_maybe_dma_pinned
	RefuseView                     // a view maps the block
	RefuseBusy                     // extra references after retries
	RefuseNomem                    // destination reservation failed
	RefuseHeld                     // another open file holds the file
	RefuseSignal                   // fatal signal between blocks
)

// RefuseMax is MXLFS_REFUSE_MAX, the length of Stats.MoveRefused.
const RefuseMax = 8

// String is the reason as a metric label: none for 0, error for one this
// package does not know.
func (r Reason) String() string {
	switch r {
	case 0:
		return "none"
	case RefusePinned:
		return "pinned"
	case RefuseView:
		return "view"
	case RefuseBusy:
		return "busy"
	case RefuseNomem:
		return "nomem"
	case RefuseHeld:
		return "held"
	case RefuseSignal:
		return "signal"
	default:
		return "error"
	}
}

// nodePref is struct mxlfs_node_pref.
type nodePref struct {
	Node  int32
	Flags uint32
}

// nodePages is struct mxlfs_node_pages.
type nodePages struct {
	PagesPtr  uint64
	NrNodes   uint32
	Flags     uint32
	Pref      int32
	Effective int32
	Holds     uint32
	Pad       uint32
}

// Move is struct mxlfs_move: Node and Flags in, the rest out.
type Move struct {
	Node          int32
	Flags         uint32
	Result        Result
	Reason        Reason // of the first block left, 0 when none was
	Blocks        uint64
	BlocksOnNode  uint64 // already on Node before the call
	BlocksMoved   uint64
	BlocksRefused uint64
	BytesMoved    uint64
	RefusedOffset uint64
	DurationNs    uint64
	MaxWindowNs   uint64
}

// NodeStat is struct mxlfs_node_stat, one NUMA node of a mount.
type NodeStat struct {
	Pages          uint64 // the mount's pages on this node now
	FallbackFolios uint64 // folios that preferred it and got another node
	FallbackPages  uint64
	MovedInPages   uint64
	MovedOutPages  uint64
}

// mountNodes is struct mxlfs_mount_nodes.
type mountNodes struct {
	StatsPtr uint64
	Nr       uint32
	Flags    uint32
}

// WindowBuckets are the upper bounds, in seconds, of the first eight
// buckets of Stats.MoveWindowHist; the ninth counts the rest.
var WindowBuckets = []float64{64e-6, 128e-6, 256e-6, 512e-6, 1e-3, 2e-3, 4e-3, 8e-3}

// Stats is struct mxlfs_stats. A module older than a field leaves it
// zero.
type Stats struct {
	BytesAllocated    uint64
	BytesLimit        uint64
	ViewSelects       uint64
	ViewFaults        uint64
	ViewReplaces      uint64
	NumaFallbackFolio uint64
	NumaFallbackBytes uint64
	MoveCalls         uint64 // dry runs not counted
	MoveCallsPartial  uint64
	MoveCallsNone     uint64 // blocks were off node, none moved
	MoveBlocks        uint64
	MoveBytes         uint64
	MoveNs            uint64 // sum of call durations
	MoveRefused       [RefuseMax]uint64
	MoveWindowNsSum   uint64
	MoveWindowNsMax   uint64
	MoveWindowHist    [9]uint64
	Holds             uint64 // open files holding now
	DryRuns           uint64
}

// Pages is where a file's pages are.
type Pages struct {
	// PerNode is the file's pages on each NUMA node, by node.
	PerNode []uint64
	// Pref is the file's node preference, Effective the node its next
	// allocation prefers; NodeNone for none.
	Pref, Effective int32
	// Holds is how many open files hold the file.
	Holds uint32
}

// Off returns the pages not on node.
func (p Pages) Off(node int32) uint64 {
	var n uint64
	for i, v := range p.PerNode {
		if int32(i) != node {
			n += v
		}
	}
	return n
}

// maxNodes is how many nodes a call asks for first; a host with more is
// asked again with its count.
const maxNodes = 8

// Unsupported reports whether err says the module has no such ioctl.
func Unsupported(err error) bool {
	return errors.Is(err, syscall.ENOTTY) || errors.Is(err, errors.ErrUnsupported)
}

// RingFiles returns the files of the flow directory flowDir that hold its
// ring: every grain file of a discrete flow, or the channels file of a
// continuous one. The flow's data, access and flow_def.json files are
// not among them.
func RingFiles(flowDir string) ([]string, error) {
	grains, err := filepath.Glob(filepath.Join(flowDir, "grains", "data.*"))
	if err != nil || len(grains) > 0 {
		return grains, err
	}
	channels := filepath.Join(flowDir, "channels")
	if _, err := os.Stat(channels); err != nil {
		return nil, err
	}
	return []string{channels}, nil
}
