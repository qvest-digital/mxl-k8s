package merge

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// These cover what the reconciler decides before an engine starts; the
// engine itself needs a real mxlfs and is covered in engine_test.go.

type fakeDomains struct {
	dir     string
	onMxlfs bool
	asked   []string
}

func (d *fakeDomains) DomainPath(_ context.Context, name string) (string, bool, error) {
	d.asked = append(d.asked, name)
	return d.dir, d.onMxlfs, nil
}

func reconcilerFor(t *testing.T, set *mxlv1alpha1.MxlFlowRedundancySet, d *fakeDomains) *Reconciler {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(set).Build()
	return &Reconciler{Client: c, NodeName: "reader", Domains: d}
}

func wantedSet(nodes ...string) *mxlv1alpha1.MxlFlowRedundancySet {
	return &mxlv1alpha1.MxlFlowRedundancySet{
		ObjectMeta: metav1.ObjectMeta{Name: "programme", Generation: 1},
		Spec: mxlv1alpha1.MxlFlowRedundancySetSpec{
			Members: []mxlv1alpha1.RedundancyFlowRef{{FlowID: memberA}, {FlowID: memberB}},
			Output:  mxlv1alpha1.RedundancyFlowRef{FlowID: output},
		},
		Status: mxlv1alpha1.MxlFlowRedundancySetStatus{Nodes: nodes, Conditions: []metav1.Condition{{
			Type: mxlv1alpha1.ConditionTypeValid, Status: metav1.ConditionTrue, ObservedGeneration: 1}}},
	}
}

func reconcileSet(t *testing.T, r *Reconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "programme"}})
	require.NoError(t, err)
	return res
}

// A node merges only the outputs read on it: every other node would
// otherwise hold an output nobody reads, and the member mirrors only
// reach the nodes in status.nodes.
func TestReconcilerIgnoresASetNotWantedHere(t *testing.T) {
	d := &fakeDomains{dir: t.TempDir(), onMxlfs: true}
	r := reconcilerFor(t, wantedSet("elsewhere"), d)

	require.Zero(t, reconcileSet(t, r))
	require.Empty(t, d.asked, "a set not wanted here opens no domain")
	require.Empty(t, r.engines)
}

// Views exist only on mxlfs; on tmpfs there is nothing to merge with,
// and retrying would not change that.
func TestReconcilerLeavesAnOutputOffMxlfsAlone(t *testing.T) {
	d := &fakeDomains{dir: t.TempDir(), onMxlfs: false}
	r := reconcilerFor(t, wantedSet("reader"), d)

	require.Zero(t, reconcileSet(t, r))
	require.Empty(t, r.engines)
}

// fakeStart replaces the engine for the reconciler's decisions: it
// records the members each engine is started over.
func fakeStart(t *testing.T) *[][]string {
	t.Helper()
	var started [][]string
	orig := start
	start = func(_ context.Context, spec Spec, _ logr.Logger) (*Engine, error) {
		started = append(started, spec.Members)
		return &Engine{outDir: flowDir(spec.DomainDir, spec.OutputID)}, nil
	}
	t.Cleanup(func() { start = orig })
	return &started
}

// memberDir creates flow id's directory in domain, as a copy, a
// placeholder or a producer leaves it, in the geometry of def. It
// replaces one there, with files of other inodes, as libmxl does.
func memberDir(t *testing.T, domain, id, def string) {
	t.Helper()
	tmp, err := os.MkdirTemp(domain, ".tmp-")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "flow_def.json"), []byte(def), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "data"), nil, 0o644))
	require.NoError(t, os.RemoveAll(flowDir(domain, id)))
	require.NoError(t, os.Rename(tmp, flowDir(domain, id)))
}

// Before any member has a directory here there is nothing to serve
// from: the member mirrors start when the output is first wanted.
func TestReconcilerWaitsForAMemberDirectory(t *testing.T) {
	started := fakeStart(t)
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: t.TempDir(), onMxlfs: true})

	res := reconcileSet(t, r)
	require.Equal(t, 2*time.Second, res.RequeueAfter)
	require.Empty(t, *started)
	require.Empty(t, r.engines)
}

// A member whose producer is gone has no directory here, and may never
// have one again. The output must not wait for it: it is served from
// the members there are, and built again once the missing one appears.
func TestReconcilerServesFromTheMembersPresent(t *testing.T) {
	started := fakeStart(t)
	dir := t.TempDir()
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})

	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Equal(t, [][]string{{memberB}}, *started)

	reconcileSet(t, r)
	require.Len(t, *started, 1, "the engine was rebuilt with nothing changed")

	memberDir(t, dir, memberA, videoDef(memberA))
	reconcileSet(t, r)
	require.Equal(t, [][]string{{memberB}, {memberA, memberB}}, *started)
}

// A member that goes away for good takes its directory with it: its
// mirror is collected, its placeholder retired. The engine keeps the
// member's ring mapped, where it stops advancing, and the others serve.
// Rebuilding instead would remove the output under every reader.
func TestReconcilerKeepsServingWhenAMemberGoesAway(t *testing.T) {
	started := fakeStart(t)
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	require.Len(t, *started, 1)

	require.NoError(t, os.RemoveAll(flowDir(dir, memberB)))
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Len(t, *started, 1, "a member gone away rebuilt the output")
	require.Contains(t, r.engines, "programme")
}

// A member restarted in another geometry replaces its directory. The
// output keeps its geometry while another member has it, and leaves the
// restarted member out rather than the others; it takes the new one
// once no member is left in the old.
func TestReconcilerRebuildsWithTheMembersOfTheOutputsGeometry(t *testing.T) {
	started := fakeStart(t)
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)

	small := func(id string) string {
		return strings.Replace(videoDef(id), `"frame_width":320`, `"frame_width":160`, 1)
	}
	memberDir(t, dir, memberA, small(memberA))
	reconcileSet(t, r)
	reconcileSet(t, r)
	require.Equal(t, [][]string{{memberA, memberB}, {memberB}}, *started, "the member in another geometry is left out, once")

	memberDir(t, dir, memberB, small(memberB))
	reconcileSet(t, r)
	require.Equal(t, []string{memberA, memberB}, (*started)[2], "both members in the new geometry")
}

// Once the members an engine serves from are all gone, a member it left
// out for another geometry is the only one still delivering. Keeping the
// engine would hold the output frozen on the gone members' rings; it is
// built again over the one left, in that one's geometry.
func TestReconcilerRebuildsOverALeftOutMemberOnceTheServedOnesAreGone(t *testing.T) {
	started := fakeStart(t)
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)

	small := strings.Replace(videoDef(memberA), `"frame_width":320`, `"frame_width":160`, 1)
	memberDir(t, dir, memberA, small)
	reconcileSet(t, r)
	require.Equal(t, [][]string{{memberA, memberB}, {memberB}}, *started)

	require.NoError(t, os.RemoveAll(flowDir(dir, memberB)))
	reconcileSet(t, r)
	require.Equal(t, [][]string{{memberA, memberB}, {memberB}, {memberA}}, *started,
		"the output stayed on a gone member while a left-out one delivers")
	require.Equal(t, small, string(r.engines["programme"].def))
}

// A view selects among pages of one superblock.
func TestReconcilerRefusesAMemberInAnotherDomain(t *testing.T) {
	set := wantedSet("reader")
	set.Spec.Members[1].Domain = "studio"
	d := &fakeDomains{dir: t.TempDir(), onMxlfs: true}
	r := reconcilerFor(t, set, d)

	require.Zero(t, reconcileSet(t, r))
	require.Empty(t, r.engines)
}

// A producer restarted in another geometry, or a mirror target rebuilt
// for one, replaces a member's flow directory underneath a running
// engine, which then merges the old copy's frozen ring and never
// publishes. A member that was absent appears. A set deleted and created
// again under the same name starts at the same generation. Each time the
// engine must be built again; a member removed is no reason to.
func TestRunningEngineServesOnlyTheFlowsAndSetItWasBuiltFor(t *testing.T) {
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	set := wantedSet("n1")
	set.UID, set.Generation = "first", 1
	// The engine keeps each member file open, so its inode is not reused.
	f, err := os.Open(filepath.Join(flowDir(dir, memberA), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	cur := &running{engine: &Engine{}, uid: set.UID, gen: 1, domain: dir, seen: inodes(dir, set.Spec.Members),
		started: []int{0}}
	require.True(t, cur.serves(set))

	again := set.DeepCopy()
	again.UID = "second"
	require.False(t, cur.serves(again), "a set created again under the same name kept the old engine")

	memberDir(t, dir, memberB, videoDef(memberB))
	require.False(t, cur.serves(set), "a member that appeared is not served")
	cur.seen, cur.started = inodes(dir, set.Spec.Members), []int{0, 1}
	require.True(t, cur.serves(set))

	require.NoError(t, os.RemoveAll(flowDir(dir, memberA)))
	require.True(t, cur.serves(set), "a removed member rebuilt the output")

	memberDir(t, dir, memberA, videoDef(memberA))
	require.False(t, cur.serves(set), "a member replaced underneath the engine kept it")
}

// The operator alone reads the members' definitions and the output's
// MxlFlow. A set it has not found valid, or found valid before the spec
// changed, may name a live flow as its output, which building the
// output would remove.
func TestReconcilerServesOnlyASetValidAtItsGeneration(t *testing.T) {
	for name, conds := range map[string][]metav1.Condition{
		"unchecked": nil,
		"invalid":   {{Type: mxlv1alpha1.ConditionTypeValid, Status: metav1.ConditionFalse, ObservedGeneration: 2}},
		"valid for the previous generation": {{Type: mxlv1alpha1.ConditionTypeValid, Status: metav1.ConditionTrue,
			ObservedGeneration: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			set := wantedSet("reader")
			set.Generation = 2
			set.Status.Conditions = conds
			d := &fakeDomains{dir: t.TempDir(), onMxlfs: true}
			r := reconcilerFor(t, set, d)

			require.Zero(t, reconcileSet(t, r))
			require.Empty(t, d.asked, "a set not valid at its generation opens no domain")
			require.Empty(t, r.engines)
		})
	}
}

// A directory under the output's id that a writer holds is a live flow,
// a producer's or a mirror's: it is not a leftover output to replace.
func TestReconcilerLeavesAnOutputIDAWriterHolds(t *testing.T) {
	dir := t.TempDir()
	for _, id := range []string{memberA, memberB, output} {
		memberDir(t, dir, id, videoDef(id))
	}
	f, err := os.Open(filepath.Join(flowDir(dir, output), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	require.NoError(t, unix.Flock(int(f.Fd()), unix.LOCK_SH), "libmxl's writer lock")
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})

	res := reconcileSet(t, r)
	require.Equal(t, 10*time.Second, res.RequeueAfter)
	require.FileExists(t, filepath.Join(flowDir(dir, output), "data"), "the writer's flow was removed")
	require.Empty(t, r.engines)
}

// fakeReplace replaces Engine.Replace: it records the engine members it
// was asked to replace and answers with err, or with the inode now in
// the member's directory. then, if set, runs before it answers.
func fakeReplace(t *testing.T, dir string, err error, then func()) *[]int {
	t.Helper()
	var asked []int
	orig := replace
	replace = func(e *Engine, m int) (uint64, error) {
		asked = append(asked, m)
		if then != nil {
			then()
		}
		if err != nil {
			return 0, err
		}
		id := e.spec.Members[m]
		return inodes(dir, []mxlv1alpha1.RedundancyFlowRef{{FlowID: id}})[0], nil
	}
	t.Cleanup(func() { replace = orig })
	return &asked
}

// startWithSpec is fakeStart whose engines keep their spec, which
// fakeReplace reads.
func startWithSpec(t *testing.T) *[][]string {
	t.Helper()
	var started [][]string
	orig := start
	start = func(_ context.Context, spec Spec, _ logr.Logger) (*Engine, error) {
		started = append(started, spec.Members)
		return &Engine{spec: spec, outDir: flowDir(spec.DomainDir, spec.OutputID)}, nil
	}
	t.Cleanup(func() { start = orig })
	return &started
}

// A producer that restarts recreates its flow directory. Building the
// output again for it removes the output under every reader, which then
// reopens and loses grains the other member covered all along; the
// engine takes the new ring in place of the old instead.
func TestReconcilerReplacesARestartedMemberInPlace(t *testing.T) {
	started := startWithSpec(t)
	dir := t.TempDir()
	asked := fakeReplace(t, dir, nil, nil)
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	engine := r.engines["programme"].engine
	// The engine keeps each member file open, so its inode is not reused.
	f, err := os.Open(filepath.Join(flowDir(dir, memberB), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	require.NoError(t, os.RemoveAll(flowDir(dir, memberB)))
	reconcileSet(t, r)
	memberDir(t, dir, memberB, videoDef(memberB))
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Equal(t, []int{1}, *asked)
	require.Len(t, *started, 1, "a restarted member rebuilt the output")
	require.Same(t, engine, r.engines["programme"].engine)

	reconcileSet(t, r)
	require.Equal(t, []int{1}, *asked, "a member replaced once was replaced again with nothing changed")
}

// Without MXLFS_IOC_VIEW_REPLACE_SOURCE, or on any failed replacement,
// which may leave the views half replaced, the output is built again as
// before, even when the member is gone again by then. A member restarted
// in another geometry is never put in place: the output's geometry is
// decided on a build.
func TestReconcilerRebuildsForARestartedMemberItCannotReplace(t *testing.T) {
	for name, tc := range map[string]struct {
		def   func(string) string
		err   error
		gone  bool
		asked []int
	}{
		"no ioctl":                {videoDef, ErrNoReplace, false, []int{0}},
		"failed, then gone again": {videoDef, unix.EINVAL, true, []int{0}},
		"another geometry":        {func(id string) string { return strings.Replace(videoDef(id), "320", "160", 1) }, nil, false, nil},
	} {
		t.Run(name, func(t *testing.T) {
			started := startWithSpec(t)
			dir := t.TempDir()
			var then func()
			if tc.gone {
				then = func() { require.NoError(t, os.RemoveAll(flowDir(dir, memberA))) }
			}
			asked := fakeReplace(t, dir, tc.err, then)
			memberDir(t, dir, memberA, videoDef(memberA))
			memberDir(t, dir, memberB, videoDef(memberB))
			r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
			reconcileSet(t, r)
			f, err := os.Open(filepath.Join(flowDir(dir, memberA), "data"))
			require.NoError(t, err)
			t.Cleanup(func() { f.Close() })

			memberDir(t, dir, memberA, tc.def(memberA))
			reconcileSet(t, r)
			require.Equal(t, tc.asked, *asked)
			require.Len(t, *started, 2, "the output was not built again")
		})
	}
}
