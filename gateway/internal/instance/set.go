package instance

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Set holds one Handles per MXL domain the gateway mirrors in. The
// primary domain is opened at start; every other one is opened on
// first use and stays open until Close, because libmxl reads a
// domain's options at instance open and a mirror in that domain may be
// set up at any time.
type Set struct {
	// Primary is the domain --domain-path names, which a mirror with no
	// spec.domain uses.
	Primary *Handles

	// Directory resolves an MxlDomain name to its directory relative
	// to the runtime root, the primary domain's parent.
	Directory func(ctx context.Context, domain string) (string, error)

	// open is the seam onto Open; nil takes Open.
	open func(path string) (*Handles, error)

	mu       sync.Mutex
	byDir    map[string]*Handles
	watchers []func(*Handles)
	closed   bool
}

// For returns the Handles of the named MxlDomain, opening them when
// this is the first mirror in that domain.
func (s *Set) For(ctx context.Context, domain string) (*Handles, error) {
	if domain == "" {
		return s.Primary, nil
	}
	dir, err := s.Directory(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("resolve MxlDomain %s: %w", domain, err)
	}
	// Only a domain at domains/<id> is named on a mirror: the primary
	// domain is spelled empty, and one in any other directory is not
	// mounted here. Opening the primary under its name as well would let
	// a teardown judge the flow by an MxlFlow that does not exist and
	// remove a local producer's flow.
	if !filepath.IsLocal(dir) || filepath.Clean(dir) != dir || filepath.Dir(dir) != mxlv1alpha1.DomainsDir {
		return nil, fmt.Errorf("MxlDomain %s is in %q, not below %s/, and is not mirrored by name", domain, dir, mxlv1alpha1.DomainsDir)
	}
	primary := s.Primary.DomainPath()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("instance set closed")
	}
	if h, ok := s.byDir[dir]; ok {
		return h, nil
	}
	open := s.open
	if open == nil {
		open = Open
	}
	h, err := open(filepath.Join(filepath.Dir(primary), dir))
	if err != nil {
		return nil, err
	}
	if s.byDir == nil {
		s.byDir = make(map[string]*Handles)
	}
	s.byDir[dir] = h
	for _, w := range s.watchers {
		w(h)
	}
	return h, nil
}

// Watch calls fn with every Handles in the set, now and as each later
// domain is opened. fn runs under the set's lock and must not call
// back into it.
func (s *Set) Watch(fn func(*Handles)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.Primary)
	for _, h := range s.byDir {
		fn(h)
	}
	s.watchers = append(s.watchers, fn)
}

// Close releases every domain's handles, the primary's included.
func (s *Set) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	first := s.Primary.Close()
	for _, h := range s.byDir {
		if err := h.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
