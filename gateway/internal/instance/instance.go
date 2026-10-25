// Package instance owns the long-lived libmxl handles the gateway
// uses for the lifetime of the process: an mxl.Instance bound to the
// local MXL domain, and a fabrics.Instance built on top of it.
package instance

import (
	"fmt"
	"sync"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/mxlfs"
)

// Handles bundles the gateway-scoped MXL handles. The zero value is
// not usable; obtain one via Open. Methods are safe for concurrent
// use after construction.
type Handles struct {
	mu      sync.RWMutex
	domain  string
	onMxlfs bool
	mxlInst *mxl.Instance
	fabInst *fabrics.Instance
	closed  bool
}

// Seams onto package mxlfs for tests.
var (
	isMxlfs = mxlfs.IsMxlfs
	setLazy = mxlfs.SetLazy
)

// Open opens the MXL domain at domainPath and the corresponding
// fabrics instance. Both stay open until Close. With withMxlfs and the
// domain on an mxlfs, the process allocates lazily from then on; see
// package mxlfs. Without it the domain is never looked at as one.
func Open(domainPath string, withMxlfs bool) (*Handles, error) {
	var onMxlfs bool
	if withMxlfs {
		var err error
		if onMxlfs, err = isMxlfs(domainPath); err != nil {
			return nil, err
		}
	}
	if onMxlfs {
		if err := setLazy(domainPath, true); err != nil {
			return nil, err
		}
	}
	inst, err := mxl.NewInstance(domainPath, "")
	if err != nil {
		return nil, fmt.Errorf("mxl.NewInstance %q: %w", domainPath, err)
	}
	fab, err := fabrics.NewInstance(inst)
	if err != nil {
		_ = inst.Close()
		return nil, fmt.Errorf("fabrics.NewInstance: %w", err)
	}
	return &Handles{
		domain:  domainPath,
		onMxlfs: onMxlfs,
		mxlInst: inst,
		fabInst: fab,
	}, nil
}

// DomainPath returns the path the underlying mxl.Instance was opened
// against.
func (h *Handles) DomainPath() string { return h.domain }

// OnMxlfs reports whether the domain is on an mxlfs, where flows this
// process creates or opens through libmxl hold no ring until
// mxlfs.AllocateFlow allocates it.
func (h *Handles) OnMxlfs() bool { return h.onMxlfs }

// MXL returns the underlying mxl.Instance. Returns nil after Close.
func (h *Handles) MXL() *mxl.Instance {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil
	}
	return h.mxlInst
}

// Fabrics returns the underlying fabrics.Instance. Returns nil after
// Close.
func (h *Handles) Fabrics() *fabrics.Instance {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil
	}
	return h.fabInst
}

// Close releases both handles. Safe to call multiple times.
func (h *Handles) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	var first error
	if h.fabInst != nil {
		if err := h.fabInst.Close(); err != nil {
			first = err
		}
		h.fabInst = nil
	}
	if h.mxlInst != nil {
		if err := h.mxlInst.Close(); err != nil && first == nil {
			first = err
		}
		h.mxlInst = nil
	}
	return first
}
