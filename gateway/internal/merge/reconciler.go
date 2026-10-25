package merge

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/flowlocks"
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
	uid    types.UID
	gen    int64
	domain string
}

// serves reports whether cur still serves set as it stands: the same
// object at the same generation, over the member flows it mapped.
func (cur *running) serves(set *mxlv1alpha1.MxlFlowRedundancySet) bool {
	return cur != nil && cur.uid == set.UID && cur.gen == set.Generation && !cur.engine.Stale()
}

// recheck is how often a served set is looked at again for a member
// replaced underneath its engine, which raises no event on the set.
const recheck = 5 * time.Second

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
	if !set.DeletionTimestamp.IsZero() || !wantedHere(&set, r.NodeName) || !valid(&set) {
		r.stop(req.Name)
		return ctrl.Result{}, nil
	}
	r.mu.Lock()
	cur := r.engines[req.Name]
	r.mu.Unlock()
	if cur.serves(&set) {
		return ctrl.Result{RequeueAfter: recheck}, nil
	}
	r.stop(req.Name) // a changed spec or member builds a new output

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
	// change of the rebuilt one. A directory some writer holds, a
	// producer's or a mirror's, is a live flow of that id and stays.
	out := flowDir(dir, spec.OutputID)
	// This set's engine is stopped by now, so every flock is another
	// writer's, the gateway's own mirrors and engines included.
	if n, err := flowlocks.Writers(filepath.Join(out, "data")); err != nil || n > 0 {
		l.Info("output id is a flow some writer holds; not replacing it", "output", spec.OutputID, "writers", n, "error", err)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	_ = os.RemoveAll(out)
	e, err := Start(context.Background(), spec, l)
	if err != nil {
		l.Error(err, "start merge")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	r.mu.Lock()
	if r.engines == nil {
		r.engines = map[string]*running{}
	}
	r.engines[req.Name] = &running{engine: e, uid: set.UID, gen: set.Generation, domain: dir}
	r.mu.Unlock()
	l.Info("serving redundancy set", "output", spec.OutputID, "members", spec.Members)
	return ctrl.Result{RequeueAfter: recheck}, nil
}

// valid reports whether the operator found the set's members mergeable
// at its current generation. A set it has not checked yet, or checked
// before a spec change, is not served: its output may name a member.
func valid(set *mxlv1alpha1.MxlFlowRedundancySet) bool {
	c := meta.FindStatusCondition(set.Status.Conditions, mxlv1alpha1.ConditionTypeValid)
	return c != nil && c.Status == metav1.ConditionTrue && c.ObservedGeneration == set.Generation
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
