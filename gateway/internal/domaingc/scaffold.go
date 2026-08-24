package domaingc

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// scaffoldPrefix is the prefix libmxl gives the directory it builds a flow
// in before renaming it into place. From FlowManager.cpp:
//
//	auto pathBuffer = (base / ".mxl-tmp-XXXXXXXXXXXXXXXX").string();
//	::mkdtemp(pathBuffer.data());
//
// A directory under this name has never been a flow. libmxl's own collector
// walks published flow directories, so it does not see one at all, and
// nothing else in the platform does either.
const scaffoldPrefix = ".mxl-tmp-"

// flowDataFileName is the file inside a flow directory that libmxl holds a
// lock on. It takes a shared lock at construction, before the ring is
// allocated, and holds it for as long as it is attached - so it is held for
// the whole of the window a scaffold exists in.
const flowDataFileName = "data"

// DefaultScaffoldGrace is how old a scaffold must be before it is considered
// at all.
//
// The lock below already decides whether a scaffold is abandoned, and the age
// is not a second opinion on that. It closes the window between libmxl
// creating the data file and taking its lock: in that window the file exists
// and is unlocked, and a sweep acting on the lock alone would delete a flow
// that is being built. The window is microseconds and the cost of waiting is
// tmpfs nobody is using, so the grace is generous rather than tuned.
const DefaultScaffoldGrace = 10 * time.Minute

// sweepScaffolds removes flow directories libmxl never finished building.
//
// libmxl builds a flow in a temporary directory and renames it into place on
// success, with the removal on every unwinding exit. That cleanup is correct
// and there is no bug in it; it is defeated only by a death that does not
// unwind. A SIGKILLed producer - an OOM kill, an eviction - therefore leaves
// the whole partially allocated ring behind, and because the ring is sized
// from the domain rather than from the writer it is not small: a crashlooping
// 1080p60 producer put 26.7 GiB into one node's domain in 27 restarts, at
// which point no other producer on that node could create a flow.
//
// Nothing reclaims these. The domain is shared, so the damage is to the node
// rather than to the booking that caused it, which is why this belongs here
// and not in any one media function.
func (s *Sweeper) sweepScaffolds(grace time.Duration) (removed int, reclaimed int64) {
	if s.DomainPath == "" {
		return 0, 0
	}
	entries, err := os.ReadDir(s.DomainPath)
	if err != nil {
		s.Log.Error(err, "list domain for abandoned scaffolds", "domain", s.DomainPath)
		return 0, 0
	}

	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), scaffoldPrefix) {
			continue
		}
		path := filepath.Join(s.DomainPath, e.Name())

		age, err := scaffoldAge(path)
		if err != nil {
			// Gone between the listing and the stat is the normal race with
			// a writer that has just succeeded and renamed it into place.
			if !errors.Is(err, fs.ErrNotExist) {
				s.Log.Error(err, "stat scaffold", "path", path)
			}
			continue
		}
		if age < grace {
			continue
		}

		held, err := scaffoldHeld(path)
		if err != nil {
			// Unable to tell reads as held. Removing a flow being built
			// costs a producer its flow; leaving one costs tmpfs until the
			// next sweep.
			s.Log.Error(err, "test scaffold for a writer", "path", path)
			continue
		}
		if held {
			continue
		}

		size := dirSize(path)
		if err := os.RemoveAll(path); err != nil {
			s.Log.Error(err, "remove abandoned scaffold", "path", path)
			continue
		}
		removed++
		reclaimed += size
		s.Log.Info("reclaimed abandoned flow scaffold",
			"path", path, "bytes", size, "age", age.Truncate(time.Second))
	}
	return removed, reclaimed
}

// scaffoldHeld reports whether a writer is still building this scaffold.
//
// The test is libmxl's own: flow.cpp decides a flow is active by whether an
// exclusive lock can be taken on its data file, because an attached writer
// holds a shared one. Applying the same test to a scaffold gives the same
// answer for the same reason, and needs no threshold.
//
// A scaffold with no data file is not held: libmxl creates that file before
// it allocates anything, so its absence means the writer died in the window
// between mkdtemp and the first write. The caller's grace covers that window.
//
// Deliberately not O_NOATIME. libmxl opens flow files with it, and outside
// CAP_FOWNER that only succeeds on files the caller owns, so a flow created
// by another uid reads as active when it is not. Nothing here needs the flag.
func scaffoldHeld(dir string) (bool, error) {
	fd, err := syscall.Open(filepath.Join(dir, flowDataFileName),
		syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
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
	// Released by the close above in any case; unlocking first keeps the
	// window this process holds an exclusive lock to the shortest it can be.
	_ = syscall.Flock(fd, syscall.LOCK_UN)
	return false, nil
}

// scaffoldAge is how long ago the scaffold was last written to, taken as the
// newest modification time in it. The directory's own mtime only moves when
// an entry is added to it, so it stops advancing early while the ring is
// still being allocated.
func scaffoldAge(dir string) (time.Duration, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return 0, err
	}
	newest := info.ModTime()

	// One level is enough: libmxl writes the descriptor, the access file and
	// the data file directly in the scaffold, and the grain files below it.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		ei, err := e.Info()
		if err != nil {
			continue
		}
		if ei.ModTime().After(newest) {
			newest = ei.ModTime()
		}
	}
	return time.Since(newest), nil
}

// dirSize is what removing the scaffold gives back, for the log line. A
// failure to walk costs the number, not the removal.
func dirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // a partial total is better than none
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
