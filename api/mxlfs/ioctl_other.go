//go:build !linux

package mxlfs

import (
	"errors"
	"os"
)

// SetNode is unsupported off Linux.
func SetNode(*os.File, int32) error { return errors.ErrUnsupported }

// NodePages is unsupported off Linux.
func NodePages(*os.File) (Pages, error) { return Pages{}, errors.ErrUnsupported }

// MoveTo is unsupported off Linux.
func MoveTo(_ *os.File, node int32, flags uint32) (Move, error) {
	return Move{Node: node, Flags: flags}, errors.ErrUnsupported
}

// Hold is unsupported off Linux.
func Hold(*os.File, bool) error { return errors.ErrUnsupported }

// MountNodes is unsupported off Linux.
func MountNodes(*os.File) ([]NodeStat, error) { return nil, errors.ErrUnsupported }

// ReadStats is unsupported off Linux.
func ReadStats(*os.File) (Stats, error) { return Stats{}, errors.ErrUnsupported }
