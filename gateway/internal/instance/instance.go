// Package instance owns the long-lived libmxl handles the gateway
// uses for the lifetime of the process: an mxl.Instance bound to the
// local MXL domain, and a fabrics.Instance built on top of it.
package instance

import (
	"fmt"
	"sync"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// Handles bundles the gateway-scoped MXL handles. The zero value is
// not usable; obtain one via Open. Methods are safe for concurrent
// use after construction.
type Handles struct {
	mu      sync.RWMutex
	domain  string
	mxlInst *mxl.Instance
	fabInst *fabrics.Instance
	closed  bool
}

// Open opens the MXL domain at domainPath and the corresponding
// fabrics instance. Both stay open until Close.
func Open(domainPath string) (*Handles, error) {
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
		mxlInst: inst,
		fabInst: fab,
	}, nil
}

// DomainPath returns the path the underlying mxl.Instance was opened
// against.
func (h *Handles) DomainPath() string { return h.domain }

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
