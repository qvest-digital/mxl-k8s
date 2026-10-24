package merge

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Domains is what the reconciler needs of the gateway's open domains.
type Domains interface {
	DomainPath(ctx context.Context, name string) (dir string, onMxlfs bool, err error)
}

// Reconciler runs an engine for every MxlFlowRedundancySet whose output
// is wanted on this node.
//
// An engine starts once every member has a directory here, a copy or the
// gateway's placeholder: a view slot only ever selects a member that
// delivered the grain, so a placeholder's unallocated ring is never
// mapped through the output, and a mirror filling it later is seen
// through the same files.
type Reconciler struct {
	Client   client.Client
	NodeName string
	Domains  Domains

	mu      sync.Mutex
	engines map[string]*running // by set name
}

type running struct {
	engine *Engine
	gen    int64
	domain string
}

// Reconcile starts, keeps or stops the engine of one set.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("set", req.Name)
	var set mxlv1alpha1.MxlFlowRedundancySet
	if err := r.Client.Get(ctx, types.NamespacedName{Name: req.Name}, &set); err != nil {
		if apierrors.IsNotFound(err) {
			r.stop(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !set.DeletionTimestamp.IsZero() || !wantedHere(&set, r.NodeName) {
		r.stop(req.Name)
		return ctrl.Result{}, nil
	}
	r.mu.Lock()
	cur := r.engines[req.Name]
	r.mu.Unlock()
	if cur != nil && cur.gen == set.Generation {
		return ctrl.Result{}, nil
	}
	r.stop(req.Name) // a changed spec builds a new output

	dir, onMxlfs, err := r.Domains.DomainPath(ctx, set.Spec.Output.Domain)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !onMxlfs {
		l.Info("output domain is not on mxlfs; a redundancy set needs its views")
		return ctrl.Result{}, nil
	}
	spec := Spec{DomainDir: dir, OutputID: set.Spec.Output.FlowID, Skew: set.Spec.SkewBudget.Duration,
		Ordered: set.Spec.Preference != mxlv1alpha1.RedundancyFirstArrival}
	if spec.Skew <= 0 {
		spec.Skew = 40 * time.Millisecond
	}
	for _, m := range set.Spec.Members {
		if m.Domain != set.Spec.Output.Domain {
			l.Info("member is in another domain than the output; views cannot cross domains", "member", m.FlowID)
			return ctrl.Result{}, nil
		}
	}
	for _, m := range set.Spec.Members {
		if _, err := os.Stat(filepath.Join(flowDir(dir, m.FlowID), "data")); err != nil {
			// The placeholder or the mirror is still on its way.
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		spec.Members = append(spec.Members, m.FlowID)
	}
	// An output left behind by an engine that did not stop cleanly is no
	// one's: its head is frozen. Readers holding it reopen on the inode
	// change of the rebuilt one.
	_ = os.RemoveAll(flowDir(dir, spec.OutputID))
	e, err := Start(context.Background(), spec, l)
	if err != nil {
		l.Error(err, "start merge")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	r.mu.Lock()
	if r.engines == nil {
		r.engines = map[string]*running{}
	}
	r.engines[req.Name] = &running{engine: e, gen: set.Generation, domain: dir}
	r.mu.Unlock()
	l.Info("serving redundancy set", "output", spec.OutputID, "members", spec.Members)
	return ctrl.Result{}, nil
}

func wantedHere(set *mxlv1alpha1.MxlFlowRedundancySet, node string) bool {
	for _, n := range set.Status.Nodes {
		if n == node {
			return true
		}
	}
	return false
}

func (r *Reconciler) stop(name string) {
	r.mu.Lock()
	cur := r.engines[name]
	delete(r.engines, name)
	r.mu.Unlock()
	if cur != nil {
		cur.engine.Close()
	}
}

// SamplesCommitted forwards an audio arrival to every engine merging that
// flow in that domain.
func (r *Reconciler) SamplesCommitted(domain, id string, start, end uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cur := range r.engines {
		if cur.domain != domain {
			continue
		}
		if m := cur.engine.MemberIndex(id); m >= 0 {
			cur.engine.Committed(m, start, end)
		}
	}
}

// Start implements manager.Runnable: it stops every engine on shutdown.
func (r *Reconciler) Start(ctx context.Context) error {
	<-ctx.Done()
	r.mu.Lock()
	names := make([]string, 0, len(r.engines))
	for n := range r.engines {
		names = append(names, n)
	}
	r.mu.Unlock()
	for _, n := range names {
		r.stop(n)
	}
	return nil
}

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.Add(r); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).Named("redundancyset").
		For(&mxlv1alpha1.MxlFlowRedundancySet{}).Complete(r)
}
