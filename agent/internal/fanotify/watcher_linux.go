//go:build linux

package fanotify

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// Event masks; values mirror the FAN_* constants in fanotify(7).
const (
	MaskCreate    = unix.FAN_CREATE
	MaskMovedTo   = unix.FAN_MOVED_TO
	MaskDelete    = unix.FAN_DELETE
	MaskMovedFrom = unix.FAN_MOVED_FROM
	MaskOnDir     = unix.FAN_ONDIR
)

// Event is a single fanotify directory-change event.
type Event struct {
	// Mask is the raw FAN_* mask reported by the kernel.
	Mask uint64

	// PID is the process that triggered the event, as reported by
	// the kernel. May be 0 if the kernel cannot attribute it.
	PID int32

	// Name is the entry within the marked directory that changed.
	Name string
}

// IsCreate reports whether the event includes any creation flag.
func (e Event) IsCreate() bool {
	return e.Mask&(MaskCreate|MaskMovedTo) != 0
}

// IsRemove reports whether the event includes any removal flag.
func (e Event) IsRemove() bool {
	return e.Mask&(MaskDelete|MaskMovedFrom) != 0
}

// Watcher wraps a fanotify file descriptor configured for FID/DIR_FID
// /NAME reporting on a single inode mark.
type Watcher struct {
	fd int
}

// New initializes a fanotify watcher in FAN_REPORT_DFID_NAME mode.
// Requires kernel >= 5.17 and CAP_SYS_ADMIN.
func New() (*Watcher, error) {
	fd, err := unix.FanotifyInit(
		unix.FAN_CLASS_NOTIF|unix.FAN_REPORT_DFID_NAME|unix.FAN_CLOEXEC,
		unix.O_RDONLY|unix.O_LARGEFILE,
	)
	if err != nil {
		return nil, fmt.Errorf("fanotify_init: %w", err)
	}
	return &Watcher{fd: fd}, nil
}

// MarkInode adds an inode-scoped mark on path with the given mask.
// Events fire for entries created, moved, or deleted under path.
func (w *Watcher) MarkInode(path string, mask uint64) error {
	if err := unix.FanotifyMark(
		w.fd,
		unix.FAN_MARK_ADD|unix.FAN_MARK_ONLYDIR,
		mask,
		unix.AT_FDCWD,
		path,
	); err != nil {
		return fmt.Errorf("fanotify_mark %q: %w", path, err)
	}
	return nil
}

// Close releases the fanotify file descriptor. A concurrent Run call
// returns once the kernel surfaces the close as EBADF.
func (w *Watcher) Close() error {
	if w.fd < 0 {
		return nil
	}
	err := unix.Close(w.fd)
	w.fd = -1
	return err
}

// Run reads events from the kernel and forwards them on out until ctx
// is canceled or the underlying fd is closed. Closes out on return.
func (w *Watcher) Run(ctx context.Context, out chan<- Event) error {
	defer close(out)

	// When ctx is canceled, close the fd to unblock the read syscall.
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-ctx.Done():
			_ = w.Close()
		case <-doneCh:
		}
	}()

	buf := make([]byte, 8192)
	for {
		n, err := unix.Read(w.fd, buf)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EBADF) {
				return nil
			}
			return fmt.Errorf("read fanotify: %w", err)
		}
		if n == 0 {
			return nil
		}
		if err := dispatch(ctx, buf[:n], out); err != nil {
			return err
		}
	}
}

// fanotify metadata + info-record layout constants.
const (
	metaSize          = 24
	infoHeaderSize    = 4
	fanFSIDSize       = 8
	fileHandleHdrSize = 8 // handle_bytes(u32) + handle_type(s32)
	infoTypeDFIDName  = 2 // FAN_EVENT_INFO_TYPE_DFID_NAME
)

func dispatch(ctx context.Context, buf []byte, out chan<- Event) error {
	for len(buf) > 0 {
		ev, consumed, err := parseOne(buf)
		if err != nil {
			return err
		}
		if consumed == 0 {
			// Defensive: parseOne always returns non-zero on success.
			return fmt.Errorf("zero-length event consumed")
		}
		if ev != nil {
			select {
			case out <- *ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		buf = buf[consumed:]
	}
	return nil
}

// parseOne extracts one event from the head of buf. Returns (nil, n)
// for events we don't decode (e.g. lacking a DFID_NAME info record).
func parseOne(buf []byte) (*Event, int, error) {
	if len(buf) < metaSize {
		return nil, 0, fmt.Errorf("event truncated: %d < %d", len(buf), metaSize)
	}
	eventLen := binary.LittleEndian.Uint32(buf[0:4])
	if int(eventLen) > len(buf) || eventLen < metaSize {
		return nil, 0, fmt.Errorf("invalid event_len %d (buf=%d)", eventLen, len(buf))
	}
	mask := binary.LittleEndian.Uint64(buf[8:16])
	pid := int32(binary.LittleEndian.Uint32(buf[20:24]))

	ev := &Event{Mask: mask, PID: pid}
	name := ""

	cursor := metaSize
	for cursor < int(eventLen) {
		if cursor+infoHeaderSize > int(eventLen) {
			return nil, 0, fmt.Errorf("info header truncated")
		}
		infoType := buf[cursor]
		infoLen := binary.LittleEndian.Uint16(buf[cursor+2 : cursor+4])
		if int(infoLen) < infoHeaderSize || cursor+int(infoLen) > int(eventLen) {
			return nil, 0, fmt.Errorf("invalid info_len %d", infoLen)
		}

		if infoType == infoTypeDFIDName {
			payload := buf[cursor+infoHeaderSize : cursor+int(infoLen)]
			n, err := extractName(payload)
			if err != nil {
				return nil, 0, err
			}
			name = n
		}

		cursor += int(infoLen)
	}

	if name == "" {
		// Event without a usable DFID_NAME record; skip silently.
		return nil, int(eventLen), nil
	}
	ev.Name = name
	return ev, int(eventLen), nil
}

// extractName parses an fsid + struct file_handle + null-terminated
// name payload (the body of an info_type=DFID_NAME record) and
// returns the entry name.
func extractName(payload []byte) (string, error) {
	if len(payload) < fanFSIDSize+fileHandleHdrSize {
		return "", fmt.Errorf("dfid_name payload too short (%d)", len(payload))
	}
	// Skip fsid (8 bytes).
	off := fanFSIDSize
	handleBytes := binary.LittleEndian.Uint32(payload[off : off+4])
	off += fileHandleHdrSize
	off += int(handleBytes)
	if off > len(payload) {
		return "", fmt.Errorf("file_handle overruns payload")
	}
	nameBytes := payload[off:]
	if i := bytes.IndexByte(nameBytes, 0); i >= 0 {
		nameBytes = nameBytes[:i]
	}
	return string(nameBytes), nil
}
