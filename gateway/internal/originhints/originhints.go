// Package originhints publishes, on the MxlFlow of every flow whose
// origin is this node, the batch size hints libmxl reports for it.
//
// libmxl takes a flow's hints from the options of the writer that
// creates it, and neither the flow definition nor libmxl-fabrics carries
// them to another node. A mirror's target creates its copy with them,
// see placeholder.Hints, so they are read where the producer's flow is.
package originhints

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/placeholder"
)

// Publisher reconciles MxlFlows whose origin is this node.
type Publisher struct {
	Client   client.Client
	NodeName string
	// Domain opens the domain an MxlFlow's spec.domain names.
	Domain func(ctx context.Context, name string) (placeholder.Handles, error)

	// config reads a flow's configuration; nil reads it through a reader.
	config func(inst *mxl.Instance, id string) (mxl.FlowConfig, error)

	mu sync.Mutex
	// read is, per MxlFlow, the appearedAt of the origin copy whose hints
	// were last published. A rescan refreshes the MxlFlow without a new
	// copy, which has the hints it had.
	read map[string]time.Time
}

// SetupWithManager registers the publisher for MxlFlows.
func (p *Publisher) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("originhints").
		For(&mxlv1alpha1.MxlFlow{}).
		Complete(p)
}

// Reconcile publishes the hints of one flow when its origin is here.
func (p *Publisher) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var flow mxlv1alpha1.MxlFlow
	if err := p.Client.Get(ctx, req.NamespacedName, &flow); err != nil {
		p.forget(req.Name)
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	appeared, here := p.originHere(&flow)
	if !here || !flow.DeletionTimestamp.IsZero() {
		p.forget(req.Name)
		return reconcile.Result{}, nil
	}
	p.mu.Lock()
	last, seen := p.read[req.Name]
	p.mu.Unlock()
	if seen && last.Equal(appeared) && placeholder.HintsOf(&flow) != (placeholder.Hints{}) {
		return reconcile.Result{}, nil
	}

	h, err := p.Domain(ctx, flow.Spec.Domain)
	if err != nil {
		return reconcile.Result{}, err
	}
	// A path walk into a missing flow on mxlfs waits for it and posts it
	// as wanted; a producer that has just exited leaves none.
	if ok, err := placeholder.Present(h.DomainPath(), flow.Spec.ID); err != nil || !ok {
		return reconcile.Result{}, err
	}
	inst := h.MXL()
	if inst == nil {
		return reconcile.Result{}, fmt.Errorf("mxl instance closed")
	}
	read := p.config
	if read == nil {
		read = readConfig
	}
	cfg, err := read(inst, flow.Spec.ID)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("read flow config: %w", err)
	}
	hints := placeholder.Hints{Sync: cfg.Common.MaxSyncBatchSizeHint, Commit: cfg.Common.MaxCommitBatchSizeHint}
	if hints != placeholder.HintsOf(&flow) {
		patch := fmt.Appendf(nil, `{"status":{"maxSyncBatchSizeHint":%d,"maxCommitBatchSizeHint":%d}}`, hints.Sync, hints.Commit)
		if err := p.Client.Status().Patch(ctx, &flow, client.RawPatch(types.MergePatchType, patch)); err != nil {
			return reconcile.Result{}, fmt.Errorf("publish hints: %w", err)
		}
	}
	p.mu.Lock()
	if p.read == nil {
		p.read = make(map[string]time.Time)
	}
	p.read[req.Name] = appeared
	p.mu.Unlock()
	return reconcile.Result{}, nil
}

// originHere reports whether this node holds the flow's Origin, and when
// that copy appeared.
func (p *Publisher) originHere(flow *mxlv1alpha1.MxlFlow) (time.Time, bool) {
	for _, loc := range flow.Status.Locations {
		if loc.NodeName == p.NodeName && loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			if loc.AppearedAt == nil {
				return time.Time{}, true
			}
			return loc.AppearedAt.Time, true
		}
	}
	return time.Time{}, false
}

func (p *Publisher) forget(name string) {
	p.mu.Lock()
	delete(p.read, name)
	p.mu.Unlock()
}

func readConfig(inst *mxl.Instance, id string) (mxl.FlowConfig, error) {
	r, err := inst.NewReader(id)
	if err != nil {
		return mxl.FlowConfig{}, err
	}
	defer func() { _ = r.Close() }()
	return r.Config()
}
