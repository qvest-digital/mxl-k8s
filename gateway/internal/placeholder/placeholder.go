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
//
// libmxl opens a flow that exists instead of creating it, whatever
// geometry it was asked for, so a placeholder of a geometry the flow no
// longer has would be taken over as it is, by a mirror or by a producer.
// A placeholder is therefore held only while the flow has an Origin, the
// producer whose geometry the MxlFlow describes, and one whose geometry
// no longer matches the MxlFlow is replaced.
package placeholder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/qvest-digital/go-mxl/mxl"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
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
	// writer and against their MxlFlow.
	Period time.Duration
	// LocalWriter is called before a placeholder is given up to a local
	// writer, so whatever fills the flow stops first.
	LocalWriter func(domain, id string)

	rmu  sync.Mutex // one Reconcile at a time: the controller's and Start's
	mu   sync.Mutex
	held map[string]*entry // by MxlFlow name
}

type entry struct {
	domain string // directory
	id     string
	writer flowWriter
	filled bool
}

// flowWriter is the part of an mxl.Writer a placeholder uses; tests
// replace it.
type flowWriter interface {
	Close() error
	Detach() error
}

func (m *Manager) dataPath(e *entry) string {
	return filepath.Join(e.domain, e.id+".mxl-flow", "data")
}

// geometryKeys are the definition keys a flow's ring is laid out from.
var geometryKeys = []string{"format", "media_type", "grain_rate", "sample_rate", "frame_width", "frame_height",
	"interlace_mode", "components", "channel_count", "bit_depth"}

// SameGeometry reports whether two flow definitions describe rings of
// one layout. A definition that is no JSON object matches none.
func SameGeometry(a, b []byte) bool {
	var x, y map[string]any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	for _, k := range geometryKeys {
		if !reflect.DeepEqual(x[k], y[k]) {
			return false
		}
	}
	return true
}

// onDiskDefinition is the definition the flow directory dir was built
// from.
func onDiskDefinition(dir string) ([]byte, error) {
	return os.ReadFile(filepath.Join(dir, "flow_def.json"))
}

// Reconcile creates, replaces or drops the placeholder of one MxlFlow.
func (m *Manager) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	m.rmu.Lock()
	defer m.rmu.Unlock()
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
	e := m.held[req.Name]
	m.mu.Unlock()
	if e != nil {
		// A placeholder a mirror fills is that mirror's copy until it goes.
		if e.filled {
			return reconcile.Result{}, nil
		}
		def, err := onDiskDefinition(filepath.Dir(m.dataPath(e)))
		if err == nil && hasOrigin(&flow) && SameGeometry(def, flow.Spec.Definition.Raw) {
			return reconcile.Result{}, nil
		}
		// One a producer here opened is the producer's: CheckLocalWriters
		// gives it up.
		if n, err := foreignWriters(m.dataPath(e)); err != nil || n > 0 {
			return reconcile.Result{}, err
		}
		l.Info("retiring placeholder", "origin", hasOrigin(&flow), "domain", e.domain)
		m.drop(l, req.Name)
	}
	if m.hasLocation(&flow) || len(flow.Spec.Definition.Raw) == 0 || !hasOrigin(&flow) {
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
	e = &entry{domain: domain, id: flow.Spec.ID}

	// A flow some process here already writes is not absent. Whether it
	// exists at all is read from the directory: a path walk into a
	// missing flow on mxlfs waits for it and posts it as wanted, under
	// the gateway's pid, which the agent would mirror.
	exists, err := present(domain, e.id)
	if err != nil {
		return reconcile.Result{}, err
	}
	if exists {
		if n, err := foreignWriters(m.dataPath(e)); err != nil || n > 0 {
			return reconcile.Result{}, err
		}
		// libmxl would open a directory of another geometry as it is. One
		// nothing holds is a leftover and goes; one a mirror here still
		// writes is tried again once it is released.
		dir := filepath.Dir(m.dataPath(e))
		if def, err := onDiskDefinition(dir); err != nil || !SameGeometry(def, flow.Spec.Definition.Raw) {
			if n, err := flowlocks.Writers(m.dataPath(e)); err != nil || n > 0 {
				return reconcile.Result{RequeueAfter: m.period()}, err
			}
			if err := os.RemoveAll(dir); err != nil {
				return reconcile.Result{}, err
			}
			l.Info("removed a leftover copy of another geometry", "domain", domain)
		}
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

// ClearMarkers removes every placeholder marker in domain. A gateway
// that exits without dropping its placeholders leaves their markers, and
// the agent and the exporter would take each such directory for a
// placeholder for good: a copy or a local writer's flow under that id
// would never become a location. Call it when the gateway holds no
// placeholder in domain, as on opening the domain.
func ClearMarkers(domain string) error {
	dir := filepath.Join(domain, MarkerDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// foreignWriters is flowlocks.ForeignWriters; tests replace it.
var foreignWriters = flowlocks.ForeignWriters

// present reports whether domain holds a directory for flow id, without
// a lookup of the flow's own name.
func present(domain, id string) (bool, error) {
	entries, err := os.ReadDir(domain)
	if err != nil {
		return false, err
	}
	for _, d := range entries {
		if d.Name() == id+".mxl-flow" {
			return true, nil
		}
	}
	return false, nil
}

// hasOrigin reports whether a producer publishes the flow, which is what
// makes the MxlFlow's definition the geometry it is written in.
func hasOrigin(flow *mxlv1alpha1.MxlFlow) bool {
	for _, loc := range flow.Status.Locations {
		if loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			return true
		}
	}
	return false
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

// leaveAll gives every placeholder up on shutdown and leaves its
// directory and its marker in place, as a crash does. A reader on the
// node keeps the flow it opened, a consumer opening the flow before the
// next gateway is up still finds a placeholder and asks for a mirror,
// and the next gateway holds the same directory again. A gateway that
// does not come back leaves the markers to ClearMarkers.
//
// Deleting the directories instead is no use to a process that is
// exiting, and it is slow: libmxl, releasing the last writer of a
// discrete flow it just deleted, looks the flow's directory up again,
// and on an mxlfs mounted with wait= that walk through the missing name
// waits the whole wait out. Serially, a dozen placeholders outlasted
// the pod's termination grace period.
func (m *Manager) leaveAll(l logr.Logger) {
	m.mu.Lock()
	held := m.held
	m.held = nil
	m.mu.Unlock()
	for name, e := range held {
		// A writer whose data file could not be guarded when it was
		// created cannot be kept from deleting the flow; it is
		// released all the same.
		if err := e.writer.Detach(); errors.Is(err, mxl.ErrNoFlowGuard) {
			_ = e.writer.Close()
		}
		l.Info("left placeholder", "flow", name, "domain", e.domain)
	}
}

// Retire releases the placeholder of flow id in domain, filled or not,
// for a mirror that has to replace a directory of another geometry.
func (m *Manager) Retire(domain, id string) {
	m.mu.Lock()
	var name string
	for n, e := range m.held {
		if e.domain == domain && strings.EqualFold(e.id, id) {
			name = n
		}
	}
	m.mu.Unlock()
	if name != "" {
		m.drop(log.Log.WithName("placeholder"), name)
	}
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

// Unfilled turns a placeholder whose mirror is gone back into one: the
// marker returns, so the agent stops publishing the directory as a
// location. Its ring keeps what the mirror wrote, which nothing reads
// until the next mirror fills it. Without this the gateway's writer,
// still holding the placeholder, made the directory look like a local
// producer's and the node claimed Origin.
func (m *Manager) Unfilled(domain, id string) {
	m.mu.Lock()
	var held bool
	for _, e := range m.held {
		if e.domain == domain && strings.EqualFold(e.id, id) && e.filled {
			e.filled, held = false, true
		}
	}
	m.mu.Unlock()
	if held {
		_ = os.WriteFile(MarkerPath(domain, id), nil, 0o644)
	}
}

// CheckLocalWriters gives up every held placeholder some other process
// on this node has opened as a writer: the local writer wins.
func (m *Manager) CheckLocalWriters(l logr.Logger) {
	m.mu.Lock()
	var taken []string
	for name, e := range m.held {
		if n, err := foreignWriters(m.dataPath(e)); err == nil && n > 0 {
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

func (m *Manager) period() time.Duration {
	if m.Period <= 0 {
		return time.Second
	}
	return m.Period
}

// Start runs the local-writer check every Period until ctx ends, then
// gives every placeholder up. Each pass also reconciles every held
// placeholder again: one a mirror released is retired then if its flow
// lost its Origin or changed geometry while the mirror held it.
func (m *Manager) Start(ctx context.Context) error {
	l := log.FromContext(ctx).WithName("placeholder")
	t := time.NewTicker(m.period())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.leaveAll(l)
			return nil
		case <-t.C:
			m.CheckLocalWriters(l)
			for _, name := range m.Held() {
				if _, err := m.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
					l.Error(err, "reconcile held placeholder", "flow", name)
				}
			}
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
