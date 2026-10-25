//go:build linux

package fanotify

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// Open is one file opened on a watched filesystem.
type Open struct {
	// PID is the process that opened the file; 0 if the kernel could not
	// attribute it to one in this pid namespace.
	PID int32
	// Path is the file as readlink of the event's fd reports it. An open
	// through a mount this process does not have, a pod's bind mount,
	// reads as the opener sees it, so only its name below the mount root
	// is shared with this process's view.
	Path string
}

// OpenWatcher reports every file opened on a filesystem, through any
// mount of it: a filesystem mark sees the opens of pods that mount the
// domain through their own bind mounts.
type OpenWatcher struct {
	fd int
}

// NewOpenWatcher marks the filesystem holding path for FAN_OPEN. It needs
// CAP_SYS_ADMIN.
func NewOpenWatcher(path string) (*OpenWatcher, error) {
	fd, err := unix.FanotifyInit(unix.FAN_CLASS_NOTIF|unix.FAN_CLOEXEC, unix.O_RDONLY|unix.O_LARGEFILE|unix.O_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("fanotify_init: %w", err)
	}
	if err := unix.FanotifyMark(fd, unix.FAN_MARK_ADD|unix.FAN_MARK_FILESYSTEM, unix.FAN_OPEN, unix.AT_FDCWD, path); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("fanotify_mark filesystem %q: %w", path, err)
	}
	return &OpenWatcher{fd: fd}, nil
}

// Close releases the descriptor; call it after Run returned.
func (w *OpenWatcher) Close() error {
	if w.fd < 0 {
		return nil
	}
	err := unix.Close(w.fd)
	w.fd = -1
	return err
}

// Run forwards opens on out until ctx is canceled, then closes out.
func (w *OpenWatcher) Run(ctx context.Context, out chan<- Open) error {
	defer close(out)
	return readUntilDone(ctx, w.fd, func(b []byte) error { return dispatchOpens(ctx, b, out) })
}

// dispatchOpens walks fanotify_event_metadata records. Each carries an fd
// the kernel opened on the file; its path comes from /proc/self/fd, and
// the fd is closed whatever happens.
func dispatchOpens(ctx context.Context, b []byte, out chan<- Open) error {
	const metaLen = 24 // event_len, vers, reserved, metadata_len, mask, fd, pid
	for len(b) >= metaLen {
		eventLen := binary.NativeEndian.Uint32(b[0:4])
		if eventLen < metaLen || int(eventLen) > len(b) {
			return fmt.Errorf("fanotify: bad event length %d", eventLen)
		}
		if vers := b[4]; vers != unix.FANOTIFY_METADATA_VERSION {
			return fmt.Errorf("fanotify: metadata version %d", vers)
		}
		fd := int32(binary.NativeEndian.Uint32(b[16:20]))
		pid := int32(binary.NativeEndian.Uint32(b[20:24]))
		b = b[eventLen:]
		if fd < 0 {
			continue // FAN_NOFD: a queue overflow, nothing to resolve
		}
		path, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(int(fd)))
		unix.Close(int(fd))
		if err != nil {
			continue
		}
		select {
		case out <- Open{PID: pid, Path: path}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
