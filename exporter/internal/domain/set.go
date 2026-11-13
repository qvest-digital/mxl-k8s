package domain

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Set is every MXL domain on the node: the primary one, and each
// directory below the runtime root's domains/, which is where an
// MxlDomain without a directory of its own is materialised.
//
// Reporting the primary domain alone left a flow in any other domain
// with nothing but mxl_flow_location_info on a node that held it, which
// is what a flow the control plane places on a node and the node does
// not have looks like.
type Set struct {
	primary  *Domain
	root     string
	lifetime time.Duration

	// open is the seam onto Open; nil takes Open.
	open func(path string, lifetime time.Duration) (*Domain, error)

	mu     sync.Mutex
	others map[string]*Domain // by path
	// failed are the paths that would not open, so each is reported once
	// rather than on every scan until it opens or goes.
	failed map[string]bool
}

// OpenSet opens the primary domain at primaryPath. The others are found
// below domains/ next to it on each Scan.
func OpenSet(primaryPath string, lifetime time.Duration) (*Set, error) {
	primary, err := Open(primaryPath, lifetime)
	if err != nil {
		return nil, err
	}
	return &Set{
		primary:  primary,
		root:     filepath.Join(filepath.Dir(filepath.Clean(primaryPath)), mxlv1alpha1.DomainsDir),
		lifetime: lifetime,
		others:   map[string]*Domain{},
		failed:   map[string]bool{},
	}, nil
}

// Scan scans the primary domain, opens every domain that appeared below
// domains/, closes every one that left, and scans the rest. Only
// domains/ itself is listed to find them: a path walk into a missing
// flow name on an mxlfs waits for it and asks for it to be mirrored. A
// domain that fails to open or scan is retried on the next Scan. A scan
// error is returned with the others' on every Scan, an open error on the
// first that fails to open that path.
func (s *Set) Scan() error {
	errs := []error{s.primary.Scan()}

	found := map[string]bool{}
	dirents, err := os.ReadDir(s.root)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		errs = append(errs, fmt.Errorf("read %q: %w", s.root, err))
	}
	for _, de := range dirents {
		if de.IsDir() {
			found[filepath.Join(s.root, de.Name())] = true
		}
	}
	open := s.open
	if open == nil {
		open = Open
	}

	s.mu.Lock()
	var gone []*Domain
	for path, d := range s.others {
		switch {
		case err != nil:
			// A listing that failed says nothing about which domains
			// left, so the ones already open are scanned as before.
			found[path] = true
		case !found[path]:
			gone = append(gone, d)
			delete(s.others, path)
		}
	}
	for path := range s.failed {
		if !found[path] {
			delete(s.failed, path)
		}
	}
	var scan []*Domain
	for path := range found {
		d, ok := s.others[path]
		if !ok {
			var openErr error
			if d, openErr = open(path, s.lifetime); openErr != nil {
				if !s.failed[path] {
					errs = append(errs, openErr)
				}
				s.failed[path] = true
				continue
			}
			delete(s.failed, path)
			s.others[path] = d
		}
		scan = append(scan, d)
	}
	s.mu.Unlock()

	for _, d := range gone {
		_ = d.Close()
	}
	for _, d := range scan {
		errs = append(errs, d.Scan())
	}
	return errors.Join(errs...)
}

// Domains returns the primary domain first, then the others by path.
func (s *Set) Domains() []*Domain {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Domain, 0, 1+len(s.others))
	for _, d := range s.others {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return append([]*Domain{s.primary}, out...)
}

// Close closes every domain in the set.
func (s *Set) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	errs := []error{s.primary.Close()}
	for path, d := range s.others {
		errs = append(errs, d.Close())
		delete(s.others, path)
	}
	return errors.Join(errs...)
}
