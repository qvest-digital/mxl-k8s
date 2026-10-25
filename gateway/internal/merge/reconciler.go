package merge

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/flowlocks"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/placeholder"
)

// Domains is what the reconciler needs of the gateway's open domains.
type Domains interface {
	DomainPath(ctx context.Context, name string) (dir string, onMxlfs bool, err error)
}

// Reconciler runs an engine for every MxlFlowRedundancySet whose output
// is wanted on this node.
//
// An engine starts over the members that have a directory here, a copy
// or the gateway's placeholder, and agree with the output's geometry: a
// view slot only ever selects a member that delivered the grain, so a
// placeholder's unallocated ring is never mapped through the output, and
// a mirror filling it later is seen through the same files. A member
// that goes away for good, its producer gone and its placeholder or copy
// with it, leaves the engine serving from the others; one that comes back
// in a new directory is put in place of its old ring.
type Reconciler struct {
	Client   client.Client
	NodeName string
	Domains  Domains

	mu      sync.Mutex
	engines map[string]*running // by set name
}

type running struct {
	engine  *Engine
	uid     types.UID
	gen     int64
	domain  string
	def     []byte   // the flow definition the output was built from
	seen    []uint64 // per spec member, its data file's inode at build; 0 for none
	started []int    // the spec members the engine was started over
	failed  bool     // a Replace failed, which may leave the engine half replaced
	// retrying holds the spec members replaced underneath the engine
	// whose new ring could not be opened yet; serves keeps the engine
	// for them until the next recheck tries again.
	retrying map[int]bool
}

// serves reports whether cur still serves set as it stands: the same
// object at the same generation, with no member directory appeared or
// replaced since the engine was built, or since it took the replacement
// in place, and no Replace failed. A member whose new ring is still to be
// opened keeps the engine as well. One removed is no reason to
// rebuild: the engine keeps its ring mapped, which stops advancing, and
// the other members serve. Once every member it was started over is
// gone, a member it left out, of another geometry, is the only one still
// delivering, and the output is built again over it.
func (cur *running) serves(set *mxlv1alpha1.MxlFlowRedundancySet) bool {
	if cur == nil || cur.failed || cur.uid != set.UID || cur.gen != set.Generation {
		return false
	}
	startedLeft, leftOut := false, false
	for i, ino := range inodes(cur.domain, set.Spec.Members) {
		switch {
		case ino == 0:
		case ino != cur.seen[i] && !cur.retrying[i]:
			return false
		case slices.Contains(cur.started, i):
			startedLeft = true
		default:
			leftOut = true
		}
	}
	return startedLeft || !leftOut
}

// inodes returns, per member, the inode of its data file in domain, or 0
// for a member with no directory there. Which flows exist is read from
// the directory: on mxlfs a path walk into a missing flow waits for it
// and posts it as wanted.
func inodes(domain string, members []mxlv1alpha1.RedundancyFlowRef) []uint64 {
	names := map[string]bool{}
	entries, _ := os.ReadDir(domain)
	for _, d := range entries {
		names[d.Name()] = true
	}
	out := make([]uint64, len(members))
	for i, m := range members {
		var st unix.Stat_t
		if names[m.FlowID+".mxl-flow"] && unix.Stat(filepath.Join(flowDir(domain, m.FlowID), "data"), &st) == nil {
			out[i] = st.Ino
		}
	}
	return out
}

// replace is (*Engine).Replace; tests replace it.
var replace = (*Engine).Replace

// replaceRestarted puts each member the engine was started over whose
// directory was replaced since, by a producer restarted in the output's
// geometry, into the engine in place of its old ring. A restarted
// producer recreates its flow directory; building the output again for
// it would remove the output under every reader. A member the engine
// cannot take that way, in another geometry or on an mxlfs without
// MXLFS_IOC_VIEW_REPLACE_SOURCE, is left to serves, which rebuilds. One
// whose directory could not be read, gone again or not yet whole, is
// tried again at the next recheck, and is in retrying until then.
func (cur *running) replaceRestarted(l logr.Logger, set *mxlv1alpha1.MxlFlowRedundancySet) {
	if cur == nil || cur.uid != set.UID || cur.gen != set.Generation {
		return
	}
	cur.retrying = map[int]bool{}
	for i, ino := range inodes(cur.domain, set.Spec.Members) {
		m := slices.Index(cur.started, i)
		if ino == 0 || ino == cur.seen[i] || m < 0 {
			continue
		}
		id := set.Spec.Members[i].FlowID
		def, err := os.ReadFile(filepath.Join(flowDir(cur.domain, id), "flow_def.json"))
		if err != nil {
			l.Info("cannot read a restarted member's definition; trying again", "member", id, "error", err.Error())
			cur.retrying[i] = true
			continue
		}
		if !placeholder.SameGeometry(cur.def, def) {
			continue
		}
		got, err := replace(cur.engine, m)
		switch {
		case errors.Is(err, ErrNotOpened):
			l.Info("cannot open a restarted member's ring; trying again", "member", id, "error", err.Error())
			cur.retrying[i] = true
		case err != nil:
			l.Info("cannot replace a restarted member in place; building the output again", "member", id,
				"error", err.Error())
			cur.failed = true
			return
		default:
			cur.seen[i] = got
		}
	}
}

// recheck is how often a served set is looked at again for a member
// that appeared or was replaced underneath its engine, which raises no
// event on the set.
const recheck = 5 * time.Second

// start is Start; tests replace it.
var start = Start

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
	cur.replaceRestarted(l, &set)
	if cur.serves(&set) {
		return ctrl.Result{RequeueAfter: recheck}, nil
	}
	var prev []byte
	if cur != nil {
		prev = cur.def
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
	// A member with no directory is on its way, or gone with its
	// producer; the engine is built again when it appears.
	seen := inodes(dir, set.Spec.Members)
	defs := make([][]byte, len(set.Spec.Members))
	for i, m := range set.Spec.Members {
		if seen[i] != 0 {
			if d, err := os.ReadFile(filepath.Join(flowDir(dir, m.FlowID), "flow_def.json")); err == nil {
				defs[i] = d
			}
		}
	}
	def := outputGeometry(prev, defs)
	var started []int
	for i, m := range set.Spec.Members {
		switch {
		case seen[i] == 0:
		case defs[i] == nil:
			l.Info("leaving out a member with no readable flow definition", "member", m.FlowID)
		case placeholder.SameGeometry(def, defs[i]):
			spec.Members = append(spec.Members, m.FlowID)
			started = append(started, i)
		default:
			l.Info("leaving out a member of another geometry than the output", "member", m.FlowID)
		}
	}
	if len(spec.Members) == 0 {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
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
	e, err := start(context.Background(), spec, l)
	if err != nil {
		l.Error(err, "start merge")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	r.mu.Lock()
	if r.engines == nil {
		r.engines = map[string]*running{}
	}
	r.engines[req.Name] = &running{engine: e, uid: set.UID, gen: set.Generation, domain: dir, def: def, seen: seen,
		started: started}
	r.mu.Unlock()
	l.Info("serving redundancy set", "output", spec.OutputID, "members", spec.Members)
	return ctrl.Result{RequeueAfter: recheck}, nil
}

// outputGeometry is the definition whose geometry the output is built
// in: the one it was built in before while a member still has it, so a
// member restarted in another geometry is left out rather than the
// others; else the first member's present, in order of preference.
func outputGeometry(prev []byte, defs [][]byte) []byte {
	var first []byte
	for _, d := range defs {
		if d == nil {
			continue
		}
		if prev != nil && placeholder.SameGeometry(prev, d) {
			return prev
		}
		if first == nil {
			first = d
		}
	}
	return first
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
