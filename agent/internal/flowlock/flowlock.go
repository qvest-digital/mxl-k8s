// Package flowlock answers whether a flow directory in the node's MXL
// domain still has a writer attached to it.
//
// The agent publishes what it can see on disk, and a directory on its
// own says nothing about whether anything is still writing to it.
// libmxl removes a flow directory only when the departing writer can
// take an exclusive lock, so a directory outlives its producer by up
// to one domain sweep, and a directory a mirror was filling outlives
// the mirror's teardown the same way. Publishing a location for one of
// those claims a copy that has no producer behind it and, with the
// mirror already gone, claims it as an Origin -- which is how a flow
// that had just been collected came straight back with a mirror target
// node named as its producer.
package flowlock

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/qvest-digital/mxl-k8s/api/mxlfs"
)

// flowDirSuffix is the directory-name suffix libmxl gives a per-flow
// directory under a domain.
const flowDirSuffix = ".mxl-flow"

// dataFileName is the file inside a flow directory libmxl holds its
// lock on. It is created and locked at construction, before the ring
// is allocated, and held for as long as the writer is attached. Video,
// audio and data flows all carry one: the entries around it differ per
// media type (grains and access for discrete flows, channels for
// continuous ones) and this file does not.
const dataFileName = "data"

// WriterAttached reports whether a writer is attached to flowID's
// directory under domainPath.
//
// The test is libmxl's own: flow.cpp decides a flow is active by
// whether an exclusive lock can be taken on its data file, because an
// attached writer holds a shared one. Using the same test means the
// agent and the library never disagree about which flows are alive,
// and no threshold has to be guessed.
//
// It reports that a writer is attached, not that a producer is: the
// gateway filling a mirror target holds the same shared lock as a
// media function does. Separating those is the caller's job and it has
// the evidence for it -- a mirror naming this node as its target.
//
// A directory with no data file has no writer: libmxl creates that
// file before it allocates anything, so its absence means nothing ever
// got as far as attaching. A flow the domain does not list has none
// either, and is not looked up (see listed).
//
// Any other error, a domain that cannot be listed included, reports a
// writer attached, and returns the error for the caller to log. Being
// unable to tell must not read as "abandoned": demoting a live
// producer's location costs every consumer of that flow its source,
// while leaving a corpse published costs one more pass.
//
// Deliberately not O_NOATIME. libmxl opens flow files with it, and
// without CAP_FOWNER that only succeeds on files the caller owns, so a
// flow created by another uid would report a writer that is not there.
func WriterAttached(domainPath, flowID string) (bool, error) {
	if listed, err := listed(domainPath, flowID); err != nil || !listed {
		return err != nil, err
	}
	path := filepath.Join(domainPath, flowID+flowDirSuffix, dataFileName)

	fd, err := openData(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return true, err
	}
	defer func() { _ = syscall.Close(fd) }()

	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return true, nil
		}
		return true, err
	}
	// Released by the close above in any case; unlocking first keeps
	// the window this process holds an exclusive lock as short as it
	// can be, so a writer attaching right now is not held off.
	_ = syscall.Flock(fd, syscall.LOCK_UN)
	return false, nil
}

// listed reports whether the domain at domainPath lists flowID's
// directory (see mxlfs.Listed). On mxlfs a lookup of a flow directory
// that is not there waits for it to appear, so a flow is looked up by
// name only once the listing has returned it. A domain that does not
// exist lists no flow.
func listed(domainPath, flowID string) (bool, error) {
	ok, err := mxlfs.Listed(domainPath, flowID+flowDirSuffix)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return ok, err
}

// openData and statData look a flow's data file up by name. Seams for
// tests, which have no mxlfs to show that a lookup of a missing flow
// waits.
var (
	openData = syscall.Open
	statData = unix.Stat
)

// ProcLocks is where the kernel lists every file lock.
var ProcLocks = "/proc/locks"

// Locks is one read of ProcLocks: the pids holding a writer's lock, by
// the file they hold it on.
type Locks map[string][]int32

// ReadLocks reads every writer's lock on the node once, so a pass over
// many flows reads the table once rather than once per flow.
//
// It says who the writers are, which WriterAttached cannot: the
// gateway filling a mirror target and a producer that took the copy
// over hold the same lock. The kernel lists only holders visible in
// the reader's PID namespace, so the answer is complete only in the
// host's.
func ReadLocks() (Locks, error) {
	f, err := os.Open(ProcLocks)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return parseLocks(f)
}

// WriterPIDs lists, once each, the processes holding a writer's lock on
// flowID's data file under domainPath. A missing file has none, and a
// flow the domain does not list is not looked up.
func (l Locks) WriterPIDs(domainPath, flowID string) ([]int32, error) {
	if listed, err := listed(domainPath, flowID); err != nil || !listed {
		return nil, err
	}
	var st unix.Stat_t
	if err := statData(filepath.Join(domainPath, flowID+flowDirSuffix, dataFileName), &st); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return l[fmt.Sprintf("%02x:%02x:%d", unix.Major(st.Dev), unix.Minor(st.Dev), st.Ino)], nil
}

// parseLocks reads /proc/locks lines such as
//
//	1: FLOCK  ADVISORY  READ 514830 08:01:11011342 0 EOF
//
// keeping each shared FLOCK under its major:minor:ino, which the kernel
// prints with the device in hex and the inode in decimal. Waiters,
// marked "->", hold nothing. Only shared locks are writers: an
// exclusive one is libmxl testing whether a flow is active, which its
// garbage collection, mxlIsFlowActive and WriterAttached here all do
// and which any reader may run.
//
// Waiters and holders listed with pid 0 are dropped, unlike in the
// gateway's parser: here a doubtful writer must not become a producer,
// since an Origin claimed for a mirror's copy is not demoted.
func parseLocks(r io.Reader) (Locks, error) {
	locks := Locks{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 6 || fields[1] != "FLOCK" || fields[3] != "READ" {
			continue
		}
		pid, err := strconv.ParseInt(fields[4], 10, 32)
		if err != nil || pid <= 0 || slices.Contains(locks[fields[5]], int32(pid)) {
			continue
		}
		locks[fields[5]] = append(locks[fields[5]], int32(pid))
	}
	return locks, sc.Err()
}
