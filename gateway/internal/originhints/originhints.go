// Package originhints publishes, on the MxlFlow of every flow whose
// origin is this node, the batch size hints libmxl reports for it and
// its payload rate.
//
// libmxl takes a flow's hints from the options of the writer that
// creates it, and neither the flow definition nor libmxl-fabrics carries
// them to another node. A mirror's target creates its copy with them,
// see placeholder.Hints, so they are read where the producer's flow is.
//
// The payload rate is what mirror placement charges a mirror of the
// flow. It is read off libmxl's view of the flow rather than computed
// from the flow definition, so no format is known here: libmxl sizes
// each grain from the definition when the flow is created.
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
	"sigs.k8s.io/controller-runtime/pkg/log"
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

	// config reads a flow's configuration and payload rate; nil reads
	// them through a reader.
	config func(inst *mxl.Instance, id string) (facts, error)

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
	f, err := read(inst, flow.Spec.ID)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("read flow config: %w", err)
	}
	cfg := f.cfg
	hints := placeholder.Hints{Sync: cfg.Common.MaxSyncBatchSizeHint, Commit: cfg.Common.MaxCommitBatchSizeHint}
	if hints != placeholder.HintsOf(&flow) || (f.bits > 0 && f.bits != flow.Status.BitsPerSecond) {
		patch := fmt.Appendf(nil, `{"status":{"maxSyncBatchSizeHint":%d,"maxCommitBatchSizeHint":%d`, hints.Sync, hints.Commit)
		if f.bits > 0 {
			patch = fmt.Appendf(patch, `,"bitsPerSecond":%d`, f.bits)
		}
		patch = append(patch, "}}"...)
		if err := p.Client.Status().Patch(ctx, &flow, client.RawPatch(types.MergePatchType, patch)); err != nil {
			return reconcile.Result{}, fmt.Errorf("publish hints: %w", err)
		}
	}
	if f.rateErr != nil {
		// Read again on the next change to the flow rather than marking
		// this copy done: the hints are in, its rate is not.
		log.FromContext(ctx).V(1).Info("flow payload rate not readable yet", "flow", req.Name, "error", f.rateErr.Error())
		return reconcile.Result{RequeueAfter: rateRetry}, nil
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

// rateRetry is how soon a flow whose rate could not be read is read
// again when nothing else changes it.
const rateRetry = 5 * time.Second

// facts is what one read of a flow yields.
type facts struct {
	cfg mxl.FlowConfig
	// bits is the payload rate in bits per second, zero when the format
	// has none or it could not be read.
	bits int64
	// rateErr is why a rate the format has could not be read.
	rateErr error
}

func readConfig(inst *mxl.Instance, id string) (facts, error) {
	r, err := inst.NewReader(id)
	if err != nil {
		return facts{}, err
	}
	defer func() { _ = r.Close() }()
	cfg, err := r.Config()
	if err != nil {
		return facts{}, err
	}
	bits, rateErr := payloadRate(r, cfg)
	return facts{cfg: cfg, bits: bits, rateErr: rateErr}, nil
}

// payloadRate is the flow's payload in bits per second. A discrete
// flow carries one grain of the size libmxl gave every grain at
// creation per grain interval; a continuous flow carries one sample per
// channel per sample, of the width libmxl's channel buffers hold. An
// event flow carries no fixed rate.
func payloadRate(r *mxl.Reader, cfg mxl.FlowConfig) (int64, error) {
	rate := cfg.Common.GrainRate
	if rate.Num <= 0 || rate.Den <= 0 {
		return 0, nil
	}
	rt, err := r.Runtime()
	if err != nil {
		return 0, err
	}
	var bytesPerUnit uint64
	switch {
	case cfg.Common.Format == mxl.FormatAudio:
		if cfg.Continuous.BufferLength == 0 {
			return 0, nil
		}
		v, err := r.GetSamplesNonBlocking(rt.HeadIndex, 1)
		if err != nil {
			return 0, fmt.Errorf("read sample layout: %w", err)
		}
		// A channel buffer's stride is its sample width times its length.
		bytesPerUnit = v.Stride / uint64(cfg.Continuous.BufferLength) * v.ChannelCount
	case cfg.Common.Format.IsDiscrete():
		g, err := r.GetGrainSliceNonBlocking(rt.HeadIndex, 0)
		if err != nil {
			return 0, fmt.Errorf("read grain size: %w", err)
		}
		bytesPerUnit = uint64(g.GrainSize)
	default:
		return 0, nil
	}
	return int64(bytesPerUnit) * 8 * rate.Num / rate.Den, nil
}
