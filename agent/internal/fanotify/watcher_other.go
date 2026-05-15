//go:build !linux

package fanotify

import (
	"context"
	"errors"
)

// ErrUnsupported is returned by all calls on non-Linux platforms.
var ErrUnsupported = errors.New("fanotify is only available on Linux")

const (
	MaskCreate    = 0
	MaskMovedTo   = 0
	MaskDelete    = 0
	MaskMovedFrom = 0
	MaskOnDir     = 0
)

// Event is the cross-platform shape of a fanotify event.
type Event struct {
	Mask uint64
	PID  int32
	Name string
}

// IsCreate reports whether the event represents a creation.
func (e Event) IsCreate() bool { return false }

// IsRemove reports whether the event represents a removal.
func (e Event) IsRemove() bool { return false }

// Watcher is a placeholder so the package compiles on non-Linux.
type Watcher struct{}

// New always returns ErrUnsupported on non-Linux.
func New() (*Watcher, error) { return nil, ErrUnsupported }

// MarkInode always returns ErrUnsupported on non-Linux.
func (w *Watcher) MarkInode(path string, mask uint64) error { return ErrUnsupported }

// Close is a no-op on non-Linux.
func (w *Watcher) Close() error { return nil }

// Run always returns ErrUnsupported on non-Linux.
func (w *Watcher) Run(ctx context.Context, out chan<- Event) error {
	close(out)
	return ErrUnsupported
}
