// Package placeholder keeps, on an mxlfs domain, a flow directory for
// every MxlFlow that has no copy on this node, so every pod on the node
// sees the whole domain.
//
// A placeholder is an ordinary libmxl flow the gateway creates and holds
// open as a writer. The gateway allocates lazily on mxlfs, so its ring
// takes no memory until a mirror fills it. Holding it open keeps libmxl's
// garbage collection away, and a marker file under MarkerDir tells the
// agent and the exporter that the directory is a placeholder rather than
// a copy.
//
// A placeholder stops being one in one of two ways. A mirror fills it, and
// the target reconciler calls Filled. Or a process on this node opens it
// as a writer, a producer whose flow happened to be absent here: the
// local writer wins, and the gateway gives the flow up to it.
package placeholder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/qvest-digital/go-mxl/mxl"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/flowlocks"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

// MarkerDir is the directory in a domain that holds the placeholder
// markers; see mxlv1alpha1.PlaceholderMarkerDir.
const MarkerDir = mxlv1alpha1.PlaceholderMarkerDir

// MarkerPath is the marker of flow id in domain.
func MarkerPath(domain, id string) string { return mxlv1alpha1.PlaceholderMarker(domain, id) }

// IsPlaceholder reports whether flow id in domain is a placeholder.
func IsPlaceholder(domain, id string) bool { return mxlv1alpha1.IsPlaceholder(domain, id) }

// Handles is what the manager needs of an open domain.
type Handles interface {
	DomainPath() string
	OnMxlfs() bool
	MXL() *mxl.Instance
}

// Manager reconciles MxlFlows into placeholders.
type Manager struct {
	Client   ctrl.Client
	NodeName string
	// Domain opens the domain an MxlFlow's spec.domain names.
	Domain func(ctx context.Context, name string) (Handles, error)
	// Period is how often held placeholders are checked for a local
	// writer.
	Period time.Duration
	// LocalWriter is called before a placeholder is given up to a local
	// writer, so whatever fills the flow stops first.
	LocalWriter func(domain, id string)

	mu   sync.Mutex
	held map[string]*entry // by MxlFlow name
}

type entry struct {
	domain string // directory
	id     string
	writer *mxl.Writer
	filled bool
}

func (m *Manager) dataPath(e *entry) string {
	return filepath.Join(e.domain, e.id+".mxl-flow", "data")
}

// Reconcile creates or drops the placeholder of one MxlFlow.
func (m *Manager) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	l := log.FromContext(ctx).WithValues("flow", req.Name)
	var flow mxlv1alpha1.MxlFlow
	if err := m.Client.Get(ctx, req.NamespacedName, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			m.drop(l, req.Name)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if !flow.DeletionTimestamp.IsZero() {
		m.drop(l, req.Name)
		return reconcile.Result{}, nil
	}

	m.mu.Lock()
	_, have := m.held[req.Name]
	m.mu.Unlock()
	if have || m.hasLocation(&flow) || len(flow.Spec.Definition.Raw) == 0 {
		return reconcile.Result{}, nil
	}

	h, err := m.Domain(ctx, flow.Spec.Domain)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !h.OnMxlfs() {
		return reconcile.Result{}, nil
	}
	domain := h.DomainPath()
	e := &entry{domain: domain, id: flow.Spec.ID}

	// A flow some process here already writes is not absent.
	if n, err := flowlocks.ForeignWriters(m.dataPath(e)); err != nil || n > 0 {
		return reconcile.Result{}, err
	}
	if err := os.MkdirAll(filepath.Join(domain, MarkerDir), 0o755); err != nil {
		return reconcile.Result{}, err
	}
	// The marker goes first, so the agent never sees the directory
	// without it.
	if err := os.WriteFile(MarkerPath(domain, e.id), nil, 0o644); err != nil {
		return reconcile.Result{}, err
	}
	inst := h.MXL()
	if inst == nil {
		_ = os.Remove(MarkerPath(domain, e.id))
		return reconcile.Result{}, errors.New("mxl instance closed")
	}
	w, created, err := inst.NewWriter(string(flow.Spec.Definition.Raw))
	if err != nil {
		_ = os.Remove(MarkerPath(domain, e.id))
		return reconcile.Result{}, fmt.Errorf("create placeholder: %w", err)
	}
	e.writer = w

	m.mu.Lock()
	if m.held == nil {
		m.held = make(map[string]*entry)
	}
	m.held[req.Name] = e
	m.mu.Unlock()
	l.Info("holding placeholder", "domain", domain, "created", created)
	return reconcile.Result{}, nil
}

// hasLocation reports whether the flow has a copy on this node already.
func (m *Manager) hasLocation(flow *mxlv1alpha1.MxlFlow) bool {
	for _, loc := range flow.Status.Locations {
		if loc.NodeName == m.NodeName && loc.Phase != mxlv1alpha1.MxlFlowLocationStale {
			return true
		}
	}
	return false
}

// drop releases a placeholder. libmxl removes the flow when this was its
// last writer.
func (m *Manager) drop(l logr.Logger, name string) {
	m.mu.Lock()
	e := m.held[name]
	delete(m.held, name)
	m.mu.Unlock()
	if e == nil {
		return
	}
	_ = e.writer.Close()
	_ = os.Remove(MarkerPath(e.domain, e.id))
	l.Info("dropped placeholder", "domain", e.domain)
}

// Filled marks the placeholder of flow id in domain as a mirror's copy:
// the marker goes, so the agent publishes the directory as a location.
func (m *Manager) Filled(domain, id string) {
	m.mu.Lock()
	for _, e := range m.held {
		if e.domain == domain && e.id == id {
			e.filled = true
		}
	}
	m.mu.Unlock()
	_ = os.Remove(MarkerPath(domain, id))
}

// CheckLocalWriters gives up every held placeholder some other process
// on this node has opened as a writer: the local writer wins.
func (m *Manager) CheckLocalWriters(l logr.Logger) {
	m.mu.Lock()
	var taken []string
	for name, e := range m.held {
		if n, err := flowlocks.ForeignWriters(m.dataPath(e)); err == nil && n > 0 {
			taken = append(taken, name)
		}
	}
	m.mu.Unlock()
	for _, name := range taken {
		m.mu.Lock()
		e := m.held[name]
		delete(m.held, name)
		m.mu.Unlock()
		if e == nil {
			continue
		}
		if m.LocalWriter != nil {
			m.LocalWriter(e.domain, e.id)
		}
		// Detach, not Close: the flow is the local writer's now.
		_ = e.writer.Detach()
		_ = os.Remove(MarkerPath(e.domain, e.id))
		l.Info("local writer took over placeholder", "flow", name, "domain", e.domain)
	}
}

// Start runs the local-writer check every Period until ctx ends, then
// releases every placeholder.
func (m *Manager) Start(ctx context.Context) error {
	l := log.FromContext(ctx).WithName("placeholder")
	period := m.Period
	if period <= 0 {
		period = time.Second
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			names := make([]string, 0, len(m.held))
			for name := range m.held {
				names = append(names, name)
			}
			m.mu.Unlock()
			for _, name := range names {
				m.drop(l, name)
			}
			return nil
		case <-t.C:
			m.CheckLocalWriters(l)
		}
	}
}

// Held reports the MxlFlow names this manager holds placeholders for.
func (m *Manager) Held() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.held))
	for name := range m.held {
		names = append(names, name)
	}
	return names
}

// FromSet adapts an instance.Set to the Domain func.
func FromSet(s *instance.Set) func(ctx context.Context, name string) (Handles, error) {
	return func(ctx context.Context, name string) (Handles, error) {
		h, err := s.For(ctx, name)
		if err != nil {
			return nil, err
		}
		return h, nil
	}
}
