// Package watcher keeps an in-memory view of MXL CRDs for NMOS serving.
package watcher

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// EventKind describes the type of CRD change observed by Watcher.
type EventKind string

const (
	EventCreate EventKind = "create"
	EventUpdate EventKind = "update"
	EventDelete EventKind = "delete"
)

// ResourceKind describes the watched CRD family that changed.
type ResourceKind string

const (
	ResourceFlow   ResourceKind = "MxlFlow"
	ResourceDomain ResourceKind = "MxlDomain"
)

// Event reports a watched CRD change. Future reconciliation code can
// subscribe to these events without coupling to Kubernetes watch types.
type Event struct {
	Kind     EventKind
	Resource ResourceKind
	ID       string
}

// Option customizes a Watcher.
type Option func(*Watcher)

// WithEventBuffer sets the size of the event channel. Zero keeps the
// default buffer.
func WithEventBuffer(size int) Option {
	return func(w *Watcher) {
		if size > 0 {
			w.events = make(chan Event, size)
		}
	}
}

// Watcher maintains a thread-safe cache of MxlDomain and MxlFlow CRDs.
type Watcher struct {
	client    client.WithWatch
	events    chan Event
	started   chan struct{}
	startOnce sync.Once

	mu             sync.RWMutex
	flowsByID      map[string]*mxlv1alpha1.MxlFlow
	domainsByID    map[string]*mxlv1alpha1.MxlDomain
	domainIDByNode map[string]string
	flowIDByDomain map[string]map[string]struct{}
	domainIDByFlow map[string]string
}

// New returns a watcher using the supplied Kubernetes client.
func New(c client.WithWatch, opts ...Option) *Watcher {
	w := &Watcher{
		client:         c,
		events:         make(chan Event, 32),
		started:        make(chan struct{}),
		flowsByID:      map[string]*mxlv1alpha1.MxlFlow{},
		domainsByID:    map[string]*mxlv1alpha1.MxlDomain{},
		domainIDByNode: map[string]string{},
		flowIDByDomain: map[string]map[string]struct{}{},
		domainIDByFlow: map[string]string{},
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Events returns the stream of CRD change events observed by Run.
func (w *Watcher) Events() <-chan Event {
	return w.events
}

// Started is closed after Run has opened its watches and completed the
// initial list. It is intended for tests and startup coordination.
func (w *Watcher) Started() <-chan struct{} {
	return w.started
}

// Sync lists MxlDomain and MxlFlow resources and replaces the cache.
func (w *Watcher) Sync(ctx context.Context) error {
	var domains mxlv1alpha1.MxlDomainList
	if err := w.client.List(ctx, &domains); err != nil {
		return fmt.Errorf("list MxlDomains: %w", err)
	}
	var flows mxlv1alpha1.MxlFlowList
	if err := w.client.List(ctx, &flows); err != nil {
		return fmt.Errorf("list MxlFlows: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.flowsByID = map[string]*mxlv1alpha1.MxlFlow{}
	w.domainsByID = map[string]*mxlv1alpha1.MxlDomain{}
	w.domainIDByNode = map[string]string{}
	w.flowIDByDomain = map[string]map[string]struct{}{}
	w.domainIDByFlow = map[string]string{}
	for i := range domains.Items {
		d := domains.Items[i].DeepCopy()
		w.domainsByID[d.Name] = d
		w.domainIDByNode[d.Spec.NodeName] = d.Name
	}
	for i := range flows.Items {
		w.indexFlowLocked(flows.Items[i].DeepCopy())
	}
	return nil
}

// Run watches MxlDomain and MxlFlow changes until ctx is canceled.
func (w *Watcher) Run(ctx context.Context) error {
	flowWatch, err := w.client.Watch(ctx, &mxlv1alpha1.MxlFlowList{})
	if err != nil {
		return fmt.Errorf("watch MxlFlows: %w", err)
	}
	defer flowWatch.Stop()
	domainWatch, err := w.client.Watch(ctx, &mxlv1alpha1.MxlDomainList{})
	if err != nil {
		return fmt.Errorf("watch MxlDomains: %w", err)
	}
	defer domainWatch.Stop()

	if err := w.Sync(ctx); err != nil {
		return err
	}
	w.startOnce.Do(func() { close(w.started) })

	for {
		select {
		case <-ctx.Done():
			return nil
		case e, ok := <-flowWatch.ResultChan():
			if !ok {
				return fmt.Errorf("MxlFlow watch closed")
			}
			if err := w.handleFlowEvent(e); err != nil {
				return err
			}
		case e, ok := <-domainWatch.ResultChan():
			if !ok {
				return fmt.Errorf("MxlDomain watch closed")
			}
			if err := w.handleDomainEvent(e); err != nil {
				return err
			}
		}
	}
}

// GetFlows returns all cached flows.
func (w *Watcher) GetFlows() []mxlv1alpha1.MxlFlow {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]mxlv1alpha1.MxlFlow, 0, len(w.flowsByID))
	for _, f := range w.flowsByID {
		out = append(out, *f.DeepCopy())
	}
	return out
}

// GetDomainFlows returns the cached flows whose Origin location belongs to domainID.
func (w *Watcher) GetDomainFlows(domainID string) []mxlv1alpha1.MxlFlow {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ids := w.flowIDByDomain[domainID]
	out := make([]mxlv1alpha1.MxlFlow, 0, len(ids))
	for id := range ids {
		if f := w.flowsByID[id]; f != nil {
			out = append(out, *f.DeepCopy())
		}
	}
	return out
}

// GetFlow returns one flow by ID.
func (w *Watcher) GetFlow(id string) (mxlv1alpha1.MxlFlow, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	f, ok := w.flowsByID[id]
	if !ok {
		return mxlv1alpha1.MxlFlow{}, false
	}
	return *f.DeepCopy(), true
}

// GetDomain returns one domain by ID.
func (w *Watcher) GetDomain(id string) (mxlv1alpha1.MxlDomain, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	d, ok := w.domainsByID[id]
	if !ok {
		return mxlv1alpha1.MxlDomain{}, false
	}
	return *d.DeepCopy(), true
}

func (w *Watcher) handleFlowEvent(e watch.Event) error {
	flow, ok := e.Object.(*mxlv1alpha1.MxlFlow)
	if !ok {
		return fmt.Errorf("watch MxlFlow event contained %T", e.Object)
	}
	switch e.Type {
	case watch.Added:
		w.applyFlow(flow, EventCreate)
	case watch.Modified:
		w.applyFlow(flow, EventUpdate)
	case watch.Deleted:
		w.deleteFlow(flow.Name)
		w.emit(Event{Kind: EventDelete, Resource: ResourceFlow, ID: flow.Name})
	case watch.Bookmark:
		return nil
	case watch.Error:
		return fmt.Errorf("MxlFlow watch error")
	}
	return nil
}

func (w *Watcher) handleDomainEvent(e watch.Event) error {
	domain, ok := e.Object.(*mxlv1alpha1.MxlDomain)
	if !ok {
		return fmt.Errorf("watch MxlDomain event contained %T", e.Object)
	}
	switch e.Type {
	case watch.Added:
		w.applyDomain(domain, EventCreate)
	case watch.Modified:
		w.applyDomain(domain, EventUpdate)
	case watch.Deleted:
		w.deleteDomain(domain.Name)
		w.emit(Event{Kind: EventDelete, Resource: ResourceDomain, ID: domain.Name})
	case watch.Bookmark:
		return nil
	case watch.Error:
		return fmt.Errorf("MxlDomain watch error")
	}
	return nil
}

func (w *Watcher) applyFlow(flow *mxlv1alpha1.MxlFlow, kind EventKind) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.indexFlowLocked(flow.DeepCopy())
	w.emit(Event{Kind: kind, Resource: ResourceFlow, ID: flow.Name})
}

func (w *Watcher) applyDomain(domain *mxlv1alpha1.MxlDomain, kind EventKind) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.domainsByID[domain.Name] = domain.DeepCopy()
	w.reindexLocked()
	w.emit(Event{Kind: kind, Resource: ResourceDomain, ID: domain.Name})
}

func (w *Watcher) deleteFlow(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.removeFlowIndexLocked(id)
	delete(w.flowsByID, id)
}

func (w *Watcher) deleteDomain(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.domainsByID, id)
	w.reindexLocked()
}

func (w *Watcher) indexFlowLocked(flow *mxlv1alpha1.MxlFlow) {
	id := flow.Name
	if id == "" {
		id = flow.Spec.ID
	}
	w.removeFlowIndexLocked(id)
	w.flowsByID[id] = flow
	domainID := w.domainIDForFlowLocked(flow)
	if domainID == "" {
		return
	}
	if w.flowIDByDomain[domainID] == nil {
		w.flowIDByDomain[domainID] = map[string]struct{}{}
	}
	w.flowIDByDomain[domainID][id] = struct{}{}
	w.domainIDByFlow[id] = domainID
}

func (w *Watcher) removeFlowIndexLocked(id string) {
	if domainID := w.domainIDByFlow[id]; domainID != "" {
		delete(w.flowIDByDomain[domainID], id)
		if len(w.flowIDByDomain[domainID]) == 0 {
			delete(w.flowIDByDomain, domainID)
		}
		delete(w.domainIDByFlow, id)
	}
}

func (w *Watcher) reindexLocked() {
	w.domainIDByNode = map[string]string{}
	for id, d := range w.domainsByID {
		w.domainIDByNode[d.Spec.NodeName] = id
	}
	w.flowIDByDomain = map[string]map[string]struct{}{}
	w.domainIDByFlow = map[string]string{}
	for _, flow := range w.flowsByID {
		w.indexFlowDomainLocked(flow)
	}
}

func (w *Watcher) indexFlowDomainLocked(flow *mxlv1alpha1.MxlFlow) {
	id := flow.Name
	if id == "" {
		id = flow.Spec.ID
	}
	domainID := w.domainIDForFlowLocked(flow)
	if domainID == "" {
		return
	}
	if w.flowIDByDomain[domainID] == nil {
		w.flowIDByDomain[domainID] = map[string]struct{}{}
	}
	w.flowIDByDomain[domainID][id] = struct{}{}
	w.domainIDByFlow[id] = domainID
}

func (w *Watcher) domainIDForFlowLocked(flow *mxlv1alpha1.MxlFlow) string {
	for _, loc := range flow.Status.Locations {
		if loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			return w.domainIDByNode[loc.NodeName]
		}
	}
	return ""
}

func (w *Watcher) emit(e Event) {
	select {
	case w.events <- e:
	default:
	}
}
