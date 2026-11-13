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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

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
// or the gateway's placeholder, and agree with the output's geometry and
// ring layout: a view slot only ever selects a member that delivered the
// grain, so a placeholder's unallocated ring is never mapped through the
// output, and a mirror filling it later is seen through the same files.
// A member whose ring cannot be opened is left out as well. A member
// that goes away for good, its producer gone and its placeholder or copy
// with it, leaves the engine serving from the others; one that comes back
// in a new directory is put in place of its old ring.
type Reconciler struct {
	Client   client.Client
	NodeName string
	Domains  Domains

	mu      sync.Mutex
	engines map[string]*running // by set name
	// stalls brings a set back when its engine finds a member stalled or
	// moving again, which raises no event on the set.
	stalls chan event.GenericEvent
	pruned sync.Once // logs a CRD without status.stalledMembers
}

type running struct {
	engine  *Engine
	uid     types.UID
	gen     int64
	domain  string
	def     []byte   // the flow definition the output was built from
	layout  string   // the ring layout it was built in
	seen    []uint64 // per spec member, its data file's inode at build; 0 for none
	started []int    // the spec members the engine was started over
	failed  bool     // a Replace failed, which may leave the engine half replaced
	// retrying holds the spec members replaced underneath the engine
	// whose new ring could not be opened yet; serves keeps the engine
	// for them until the next reconcile tries again. failing holds when
	// each was first tried in vain, in a row.
	retrying map[int]bool
	failing  map[int]time.Time
}

// serves reports whether cur still serves set as it stands: the same
// object at the same generation, with no member directory appeared or
// replaced since the engine was built, or since it took the replacement
// in place, and no Replace failed. A member whose new ring is still to be
// opened keeps the engine as well. One removed is no reason to
// rebuild: the engine keeps its ring mapped, which stops advancing, and
// the other members serve. Once every member it was started over is
// gone, a member it left out, of another geometry or ring layout, is
// the only one still delivering, and the output is built again over it.
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
// tried again at the next reconcile, and is in retrying until then, for
// at most retryFor: the engine is kept for it meanwhile, so it must not
// hold off a rebuild the other members need for long.
func (cur *running) replaceRestarted(l logr.Logger, set *mxlv1alpha1.MxlFlowRedundancySet) {
	if cur == nil || cur.uid != set.UID || cur.gen != set.Generation {
		return
	}
	if cur.failing == nil {
		cur.failing = map[int]time.Time{}
	}
	cur.retrying = map[int]bool{}
	retry := func(i int, msg, id string, err error) bool {
		if _, ok := cur.failing[i]; !ok {
			cur.failing[i] = clock()
		}
		if clock().Sub(cur.failing[i]) >= retryFor {
			l.Info(msg+"; building the output again", "member", id, "error", err.Error())
			cur.failed = true
			return false
		}
		l.Info(msg+"; trying again", "member", id, "error", err.Error())
		cur.retrying[i] = true
		return true
	}
	for i, ino := range inodes(cur.domain, set.Spec.Members) {
		m := slices.Index(cur.started, i)
		if ino == 0 || ino == cur.seen[i] || m < 0 {
			delete(cur.failing, i)
			continue
		}
		id := set.Spec.Members[i].FlowID
		def, err := os.ReadFile(filepath.Join(flowDir(cur.domain, id), "flow_def.json"))
		if err != nil {
			if !retry(i, "cannot read a restarted member's definition", id, err) {
				return
			}
			continue
		}
		if !placeholder.SameGeometry(cur.def, def) {
			continue
		}
		got, err := replace(cur.engine, m)
		switch {
		case errors.Is(err, ErrNotOpened):
			if !retry(i, "cannot open a restarted member's ring", id, err) {
				return
			}
		case err != nil:
			l.Info("cannot replace a restarted member in place; building the output again", "member", id,
				"error", err.Error())
			cur.failed = true
			return
		default:
			cur.seen[i] = got
			delete(cur.failing, i)
		}
	}
}

// retryFor is how long a restarted member whose ring could not be opened
// keeps the engine before the output is built again: three rechecks. It
// is a time, not a count of reconciles, which come at every event on the
// set and every stalledRecheck while it reports a stall.
const retryFor = 3 * recheck

// clock is time.Now; tests replace it.
var clock = time.Now

// recheck is how often a served set is looked at again for a member
// that appeared or was replaced underneath its engine, which raises no
// event on the set.
const recheck = 5 * time.Second

// stalledRecheck replaces recheck while this node reports a member
// stalled. A producer restarted out of a stall raises no event on the
// set, nor does a recovery whose report the queue had no room for; at
// recheck either would reach the set only after the five seconds plus
// RecoverAfter, past the ten MembersReady is held to. A reconcile of a
// set whose report is unchanged reads the cache and writes nothing.
const stalledRecheck = time.Second

// start is Start; tests replace it.
var start = Start

// Reconcile starts, keeps or stops the engine of one set, and reports
// the members its engine finds stalled.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	l := log.FromContext(ctx).WithValues("set", req.Name)
	var set mxlv1alpha1.MxlFlowRedundancySet
	if err := r.Client.Get(ctx, types.NamespacedName{Name: req.Name}, &set); err != nil {
		if apierrors.IsNotFound(err) {
			r.stop(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	// A failed report is tried again at the next recheck; returned, it
	// would turn the recheck into a backoff that holds off this set's
	// replacements of restarted members as well. A path that stops the
	// engine asks for no recheck of its own, and its report is the one
	// removing this node's entry, so it gets one.
	defer func() {
		named, rerr := r.reportStalled(ctx, &set)
		switch {
		case rerr != nil:
			l.Error(rerr, "report stalled members")
			if err == nil && res.IsZero() {
				res = ctrl.Result{RequeueAfter: recheck}
			}
		case named && res.RequeueAfter == recheck:
			res.RequeueAfter = stalledRecheck
		}
	}()
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
	var prevLayout string
	// The members the new engine takes as stalled: what this node's
	// engine reported, by its own judgement where it has one, which the
	// cache may not have caught up with yet.
	carried := r.reported(&set)
	if cur != nil {
		prev, prevLayout = cur.def, cur.layout
		carried = reportOf(cur, carried)
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
		Ordered: set.Spec.Preference != mxlv1alpha1.RedundancyFirstArrival, Stalls: r.stallsOf(set.Name),
		Stalled: carried}
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
	// The flow definition does not carry a ring's depth, which comes from
	// the domain's history duration: members of one geometry differ in it
	// once that duration changed between their starts. A ring the engine
	// cannot open, or cannot align with the others, fails its start and
	// with it the output; it is left out instead, as a member of another
	// geometry is.
	var agree []int
	layouts := make([]string, len(set.Spec.Members))
	for i, m := range set.Spec.Members {
		switch {
		case seen[i] == 0:
		case defs[i] == nil:
			l.Info("leaving out a member with no readable flow definition", "member", m.FlowID)
		case !placeholder.SameGeometry(def, defs[i]):
			l.Info("leaving out a member of another geometry than the output", "member", m.FlowID)
		default:
			var err error
			if layouts[i], err = ringLayout(dir, m.FlowID); err != nil {
				l.Info("leaving out a member whose ring cannot be opened", "member", m.FlowID, "error", err.Error())
				continue
			}
			agree = append(agree, i)
		}
	}
	layout := outputLayout(prevLayout, agree, layouts)
	var started []int
	for _, i := range agree {
		if layouts[i] != layout {
			l.Info("leaving out a member of another ring layout than the output", "member", set.Spec.Members[i].FlowID,
				"layout", layouts[i], "output", layout)
			continue
		}
		spec.Members = append(spec.Members, set.Spec.Members[i].FlowID)
		started = append(started, i)
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
	r.engines[req.Name] = &running{engine: e, uid: set.UID, gen: set.Generation, domain: dir, def: def,
		layout: layout, seen: seen, started: started}
	r.mu.Unlock()
	l.Info("serving redundancy set", "output", spec.OutputID, "members", spec.Members, "replaceable", e.replaceable)
	return ctrl.Result{RequeueAfter: recheck}, nil
}

// reportStalled applies this node's entry in the set's
// status.stalledMembers when it differs from the engine's stalled
// members, and removes it when none is, or no engine serves the set
// here. A member the engine has not decided on yet, one not moved since
// the engine started, keeps what the entry said: an engine rebuilt
// under a stalled member must not report it moving, nor one built under
// a moving member report it stalled.
// It returns whether the entry names a member once it is applied.
func (r *Reconciler) reportStalled(ctx context.Context, set *mxlv1alpha1.MxlFlowRedundancySet) (bool, error) {
	r.mu.Lock()
	cur := r.engines[set.Name]
	r.mu.Unlock()
	reported := r.reported(set)
	var stalled []string
	if cur != nil {
		stalled = reportOf(cur, reported)
	}
	if slices.Equal(stalled, slices.Sorted(slices.Values(reported))) {
		return len(stalled) > 0, nil
	}
	log.FromContext(ctx).Info("reporting stalled members", "set", set.Name, "members", stalled)
	patch := mxlv1alpha1.StalledMembersApply(set.Name, r.NodeName, stalled)
	if err := r.Client.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(mxlv1alpha1.StalledMembersFieldOwner(r.NodeName)), client.ForceOwnership); err != nil {
		return false, err
	}
	if got, _, _ := unstructured.NestedSlice(patch.Object, "status", "stalledMembers"); len(stalled) > 0 && len(got) == 0 {
		r.pruned.Do(func() {
			log.FromContext(ctx).Info("the API server dropped status.stalledMembers; its MxlFlowRedundancySet CRD " +
				"predates the field, so no stalled member is reported")
		})
	}
	return len(stalled) > 0, nil
}

// reportOf returns, sorted, the members cur's engine finds stalled, and
// those of reported it has not decided on yet.
func reportOf(cur *running, reported []string) []string {
	stalled, undecided := cur.engine.Stalled()
	for _, id := range reported {
		if slices.Contains(undecided, id) {
			stalled = append(stalled, id)
		}
	}
	slices.Sort(stalled)
	return stalled
}

// reported returns the members this node's entry in the set's
// status.stalledMembers names.
func (r *Reconciler) reported(set *mxlv1alpha1.MxlFlowRedundancySet) []string {
	for _, s := range set.Status.StalledMembers {
		if s.Node == r.NodeName {
			return s.FlowIDs
		}
	}
	return nil
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

// outputLayout is the ring layout the output is built in, of the members
// cands in order of preference: the one it was built in before while one
// of them still has it, as outputGeometry keeps the geometry; else the
// first's.
func outputLayout(prev string, cands []int, layouts []string) string {
	for _, i := range cands {
		if layouts[i] == prev {
			return prev
		}
	}
	if len(cands) == 0 {
		return ""
	}
	return layouts[cands[0]]
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

// stallsOf returns what set's engine calls on a stall: it brings the set
// back to be reported. An event the queue has no room for is not waited
// for; the set's recheck reports it.
func (r *Reconciler) stallsOf(set string) func() {
	return func() {
		select {
		case r.stalls <- event.GenericEvent{Object: &mxlv1alpha1.MxlFlowRedundancySet{
			ObjectMeta: metav1.ObjectMeta{Name: set}}}:
		default:
		}
	}
}

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.Add(r); err != nil {
		return err
	}
	r.stalls = make(chan event.GenericEvent, 16)
	return ctrl.NewControllerManagedBy(mgr).Named("redundancyset").
		For(&mxlv1alpha1.MxlFlowRedundancySet{}).
		WatchesRawSource(source.Channel(r.stalls, &handler.EnqueueRequestForObject{})).
		Complete(r)
}
