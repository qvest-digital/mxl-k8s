package instance

import (
	"context"
	"errors"
	"testing"
)

func newTestSet(dirs map[string]string) (*Set, *[]string) {
	var opened []string
	s := &Set{
		Primary: &Handles{domain: "/run/mxl/domain"},
		Directory: func(_ context.Context, name string) (string, error) {
			d, ok := dirs[name]
			if !ok {
				return "", errors.New("not found")
			}
			return d, nil
		},
		open: func(path string) (*Handles, error) {
			opened = append(opened, path)
			return &Handles{domain: path}, nil
		},
	}
	return s, &opened
}

// A mirror in the primary domain names it either not at all or by the
// MxlDomain whose directory --domain-path is; both must reach the one
// instance the gateway opened at start, or the same flow would be
// opened through two instances.
func TestSetPrimaryByEmptyOrName(t *testing.T) {
	s, opened := newTestSet(map[string]string{"default": "domain"})
	for _, name := range []string{"", "default"} {
		h, err := s.For(context.Background(), name)
		if err != nil || h != s.Primary {
			t.Fatalf("For(%q) = %v, %v; want the primary", name, h, err)
		}
	}
	if len(*opened) != 0 {
		t.Fatalf("opened %v for the primary domain", *opened)
	}
}

// Another domain opens its own instance under the runtime root, once,
// however many mirrors use it.
func TestSetOpensOtherDomainOnce(t *testing.T) {
	s, opened := newTestSet(map[string]string{"studio": "domains/studio-a"})
	a, err := s.For(context.Background(), "studio")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := s.For(context.Background(), "studio")
	if a != b || a == s.Primary {
		t.Fatalf("got %p and %p, primary %p", a, b, s.Primary)
	}
	if len(*opened) != 1 || (*opened)[0] != "/run/mxl/domains/studio-a" {
		t.Fatalf("opened %v, want [/run/mxl/domains/studio-a]", *opened)
	}
}

// A directory outside the runtime root would open an instance there, and one
// in the root other than the primary or below domains/ is not mounted in the
// gateway.
func TestSetRefusesDirectoryOutsideRoot(t *testing.T) {
	for _, dir := range []string{"", ".", "..", "../x", "a/../b", "a//b", "/etc", "studio-b", "other/x", "domains/x/y"} {
		s, opened := newTestSet(map[string]string{"x": dir})
		if _, err := s.For(context.Background(), "x"); err == nil {
			t.Errorf("directory %q accepted", dir)
		}
		if len(*opened) != 0 {
			t.Errorf("directory %q opened %v", dir, *opened)
		}
	}
}

func TestSetUnknownDomainFails(t *testing.T) {
	s, _ := newTestSet(nil)
	if _, err := s.For(context.Background(), "missing"); err == nil {
		t.Fatal("unknown MxlDomain resolved")
	}
}

// Watch sees the domains already open and every one opened later, so
// a per-domain sweeper starts for each exactly once.
func TestSetWatchSeesEveryDomainOnce(t *testing.T) {
	s, _ := newTestSet(map[string]string{"a": "domains/a", "b": "domains/b"})
	if _, err := s.For(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	s.Watch(func(h *Handles) { seen[h.DomainPath()]++ })
	_, _ = s.For(context.Background(), "b")
	_, _ = s.For(context.Background(), "b")
	want := map[string]int{"/run/mxl/domain": 1, "/run/mxl/domains/a": 1, "/run/mxl/domains/b": 1}
	if len(seen) != len(want) {
		t.Fatalf("seen %v, want %v", seen, want)
	}
	for k, v := range want {
		if seen[k] != v {
			t.Fatalf("seen %v, want %v", seen, want)
		}
	}
}

func TestSetClosedRefusesNewDomains(t *testing.T) {
	s, _ := newTestSet(map[string]string{"a": "domains/a"})
	_ = s.Close()
	if _, err := s.For(context.Background(), "a"); err == nil {
		t.Fatal("opened a domain after Close")
	}
}
