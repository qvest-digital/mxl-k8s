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
	"errors"
	"io/fs"
	"path/filepath"
	"syscall"
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
// got as far as attaching.
//
// Any other error reports a writer attached, and returns the error for
// the caller to log. Being unable to tell must not read as "abandoned":
// demoting a live producer's location costs every consumer of that flow
// its source, while leaving a corpse published costs one more pass.
//
// Deliberately not O_NOATIME. libmxl opens flow files with it, and
// without CAP_FOWNER that only succeeds on files the caller owns, so a
// flow created by another uid would report a writer that is not there.
func WriterAttached(domainPath, flowID string) (bool, error) {
	path := filepath.Join(domainPath, flowID+flowDirSuffix, dataFileName)

	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
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
