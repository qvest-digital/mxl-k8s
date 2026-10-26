package merge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(set).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowRedundancySet{}).Build()
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
// placeholder or a producer leaves it, in the geometry of def and with
// the ring of a domain of mxlfsDomain's history duration. It replaces
// one there, with files of other inodes, as libmxl does.
func memberDir(t *testing.T, domain, id, def string) {
	t.Helper()
	memberDirWith(t, domain, id, def, ringData(t, def, 400000000))
}

// memberDirWith is memberDir with data as the ring's data file.
func memberDirWith(t *testing.T, domain, id, def string, data []byte) {
	t.Helper()
	tmp, err := os.MkdirTemp(domain, ".tmp-")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "flow_def.json"), []byte(def), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "data"), data, 0o644))
	require.NoError(t, os.RemoveAll(flowDir(domain, id)))
	require.NoError(t, os.Rename(tmp, flowDir(domain, id)))
}

// ringData returns the data file libmxl writes for def in a domain of
// history nanoseconds: the ring's depth comes from the domain, not from
// the flow's definition.
func ringData(t *testing.T, def string, history int) []byte {
	t.Helper()
	domain := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(domain, "options.json"),
		[]byte(fmt.Sprintf(`{"urn:x-mxl:option:history_duration/v1.0": %d}`, history)), 0o644))
	inst, err := mxl.NewInstance(domain, "")
	require.NoError(t, err)
	defer inst.Close()
	w, _, err := inst.NewWriter(def)
	require.NoError(t, err)
	defer w.Close()
	var flow struct{ ID string }
	require.NoError(t, json.Unmarshal([]byte(def), &flow))
	data, err := os.ReadFile(filepath.Join(flowDir(domain, flow.ID), "data"))
	require.NoError(t, err)
	return data
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

// Whether the node's mxlfs can put a restarted member's ring in place,
// or the output is built again instead, is probed once per engine. The
// log is the only place it shows before a member restarts.
func TestReconcilerLogsWhetherTheEngineCanReplaceMembers(t *testing.T) {
	for _, replaceable := range []bool{true, false} {
		t.Run(fmt.Sprint(replaceable), func(t *testing.T) {
			orig := start
			start = func(_ context.Context, spec Spec, _ logr.Logger) (*Engine, error) {
				return &Engine{outDir: flowDir(spec.DomainDir, spec.OutputID), replaceable: replaceable}, nil
			}
			t.Cleanup(func() { start = orig })
			dir := t.TempDir()
			memberDir(t, dir, memberA, videoDef(memberA))
			r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
			var serving []map[string]any
			l := funcr.NewJSON(func(obj string) {
				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(obj), &entry))
				if entry["msg"] == "serving redundancy set" {
					serving = append(serving, entry)
				}
			}, funcr.Options{})

			_, err := r.Reconcile(log.IntoContext(context.Background(), l),
				ctrl.Request{NamespacedName: types.NamespacedName{Name: "programme"}})
			require.NoError(t, err)
			require.Len(t, serving, 1)
			require.Equal(t, replaceable, serving[0]["replaceable"])
		})
	}
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
		"another ring depth":      {videoDef, fmt.Errorf("%w: 21 grains, the output 22", ErrOtherLayout), false, []int{0}},
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

// A restarted member's directory can be read before it is whole, or be
// gone again by the time the engine opens it. Nothing has been touched
// then: the engine keeps serving and the member is tried again at the
// next recheck, rather than the output being built again under its
// readers.
func TestReconcilerTriesAgainForARingItCouldNotOpen(t *testing.T) {
	started := startWithSpec(t)
	dir := t.TempDir()
	fail := true
	var asked []int
	orig := replace
	replace = func(e *Engine, m int) (uint64, error) {
		asked = append(asked, m)
		if fail {
			return 0, fmt.Errorf("%w: %w", ErrNotOpened, unix.ENOENT)
		}
		return inodes(dir, []mxlv1alpha1.RedundancyFlowRef{{FlowID: e.spec.Members[m]}})[0], nil
	}
	t.Cleanup(func() { replace = orig })
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	f, err := os.Open(filepath.Join(flowDir(dir, memberA), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	memberDir(t, dir, memberA, videoDef(memberA))
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Len(t, *started, 1, "a ring not yet opened rebuilt the output")

	fail = false
	reconcileSet(t, r)
	require.Equal(t, []int{0, 0}, asked)
	require.Len(t, *started, 1)
	reconcileSet(t, r)
	require.Equal(t, []int{0, 0}, asked, "a member replaced once was tried again")
}

// The flow definition does not carry a ring's depth: a member restarted
// after the domain's history duration changed between the output's build
// and the restart is of the output's geometry by its definition and still
// cannot take its old place. Only a build can take it, and with
// the other member gone for good the output must be built over it rather
// than stay frozen on the gone member's ring.
func TestReconcilerRebuildsOverAMemberRestartedWithAnotherRingDepth(t *testing.T) {
	started := startWithSpec(t)
	dir := t.TempDir()
	asked := fakeReplace(t, dir, fmt.Errorf("%w: 21 grains, the output 22", ErrOtherLayout), nil)
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	f, err := os.Open(filepath.Join(flowDir(dir, memberA), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	require.NoError(t, os.RemoveAll(flowDir(dir, memberB)))
	memberDir(t, dir, memberA, videoDef(memberA))
	reconcileSet(t, r)
	require.Equal(t, []int{0}, *asked)
	require.Equal(t, [][]string{{memberA, memberB}, {memberA}}, *started)
}

// A ring that cannot be opened keeps the engine while it is tried again,
// but not for long: meanwhile the restarted member counts as one still
// served, and the output would stay frozen on the others' rings when
// they are gone too.
func TestReconcilerStopsTryingARingItCannotOpen(t *testing.T) {
	started := startWithSpec(t)
	dir := t.TempDir()
	asked := fakeReplace(t, dir, fmt.Errorf("%w: %w", ErrNotOpened, unix.ENOENT), nil)
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	f, err := os.Open(filepath.Join(flowDir(dir, memberA), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	require.NoError(t, os.RemoveAll(flowDir(dir, memberB)))
	memberDir(t, dir, memberA, videoDef(memberA))
	for range maxRetries {
		require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	}
	require.Len(t, *started, 1, "the engine was not kept while the ring was tried again")
	reconcileSet(t, r)
	require.Len(t, *asked, maxRetries+1)
	require.Equal(t, [][]string{{memberA, memberB}, {memberA}}, *started,
		"the output stayed on a gone member's ring while the other could not be opened")
}

// alignedStart is startWithSpec that opens the members' rings and
// refuses them as Start does when they cannot be opened or merged slot
// for slot.
func alignedStart(t *testing.T) *[][]string {
	t.Helper()
	var started [][]string
	orig := start
	start = func(_ context.Context, spec Spec, _ logr.Logger) (*Engine, error) {
		e := &Engine{spec: spec, outDir: flowDir(spec.DomainDir, spec.OutputID)}
		defer e.release()
		for _, id := range spec.Members {
			h, f, err := mapHeader(filepath.Join(flowDir(spec.DomainDir, id), "data"), false)
			if err != nil {
				return nil, err
			}
			e.members, e.files = append(e.members, h), append(e.files, f)
		}
		if err := e.checkAligned(); err != nil {
			return nil, err
		}
		started = append(started, spec.Members)
		return &Engine{spec: spec, outDir: e.outDir}, nil
	}
	t.Cleanup(func() { start = orig })
	return &started
}

// A member restarted after the domain's history duration changed has a
// ring of another depth, which its definition does not show, and cannot
// take its old place. The output is built again, and the engine cannot
// start over rings of two depths: it is built over the member still in
// the output's, and over the restarted one once that one is gone, rather
// than over none.
func TestReconcilerLeavesOutAMemberOfAnotherRingDepth(t *testing.T) {
	started := alignedStart(t)
	dir := t.TempDir()
	asked := fakeReplace(t, dir, fmt.Errorf("%w: 10 grains, the output 20", ErrOtherLayout), nil)
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	// The engine keeps each member file open, so its inode is not reused.
	f, err := os.Open(filepath.Join(flowDir(dir, memberA), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	memberDirWith(t, dir, memberA, videoDef(memberA), ringData(t, videoDef(memberA), 200000000))
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Equal(t, []int{0}, *asked)
	require.Equal(t, [][]string{{memberA, memberB}, {memberB}}, *started,
		"the output was not built over the member still in its ring depth")
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Len(t, *started, 2, "the output was built again with nothing changed")

	require.NoError(t, os.RemoveAll(flowDir(dir, memberB)))
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Equal(t, [][]string{{memberA, memberB}, {memberB}, {memberA}}, *started,
		"the output stayed on a gone member while one of another ring depth delivers")
}

// A member whose ring cannot be opened, a data file not yet whole, would
// fail the engine's start and with it the output. It is left out, and
// taken once a whole ring is in its directory.
func TestReconcilerLeavesOutAMemberWhoseRingCannotBeOpened(t *testing.T) {
	started := alignedStart(t)
	dir := t.TempDir()
	memberDirWith(t, dir, memberA, videoDef(memberA), nil)
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})

	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Equal(t, [][]string{{memberB}}, *started, "the output was not built over the member that can be opened")
	reconcileSet(t, r)
	require.Len(t, *started, 1, "the output was built again with nothing changed")

	memberDir(t, dir, memberA, videoDef(memberA))
	reconcileSet(t, r)
	require.Equal(t, [][]string{{memberB}, {memberA, memberB}}, *started)
}

// stalledStart replaces the engine with one over the members present
// that started at started, all heads at zero, and has looked at them
// now.
func stalledStart(t *testing.T, started time.Time) **Engine {
	t.Helper()
	var e *Engine
	orig := start
	start = func(_ context.Context, spec Spec, _ logr.Logger) (*Engine, error) {
		e = &Engine{spec: spec, outDir: flowDir(spec.DomainDir, spec.OutputID),
			progress: newProgress(make([]uint64, len(spec.Members)), started, StallAfter)}
		e.progress.observe(make([]uint64, len(spec.Members)), time.Now())
		return e, nil
	}
	t.Cleanup(func() { start = orig })
	return &e
}

func stalledOf(t *testing.T, r *Reconciler) []mxlv1alpha1.NodeStalledMembers {
	t.Helper()
	var got mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, r.Client.Get(context.Background(), types.NamespacedName{Name: "programme"}, &got))
	return got.Status.StalledMembers
}

// countPatches counts the status writes r makes, which fail with err
// when it is set.
func countPatches(r *Reconciler, err error) *int {
	writes := 0
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch,
			opts ...client.SubResourcePatchOption) error {
			writes++
			if err != nil {
				return err
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
	return &writes
}

// A producer that stops committing while its pod runs on, or a mirror
// that stops delivering, leaves the output served from the other
// members, with every object of the member reading as before. Only the
// engine sees the member's head stand still; the set has to carry it
// for the operator to report, and lose it once the head moves again.
func TestReconcilerReportsTheMembersItsEngineFindsStalled(t *testing.T) {
	engine := stalledStart(t, time.Now().Add(-StartGrace))
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})

	reconcileSet(t, r)
	require.Equal(t, []mxlv1alpha1.NodeStalledMembers{{Node: "reader", FlowIDs: []string{memberA, memberB}}},
		stalledOf(t, r), "members that never moved were not reported after the start grace")

	(*engine).progress.observe([]uint64{1, 0}, time.Now())
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter, "a served set is not looked at again in time")
	require.Equal(t, []mxlv1alpha1.NodeStalledMembers{{Node: "reader", FlowIDs: []string{memberB}}}, stalledOf(t, r))

	(*engine).progress.observe([]uint64{1, 1}, time.Now())
	reconcileSet(t, r)
	require.Empty(t, stalledOf(t, r))
}

// Every pass of every gateway reconciles every set; one that wrote its
// status each time would load the API server with nothing to say.
func TestReconcilerWritesStalledMembersOnlyWhenTheyChange(t *testing.T) {
	for name, tc := range map[string]struct {
		started time.Time
		heads   []uint64
		writes  int
	}{
		"moving":  {time.Now(), []uint64{1}, 0},
		"stalled": {time.Now().Add(-StartGrace), []uint64{0}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			engine := stalledStart(t, tc.started)
			dir := t.TempDir()
			memberDir(t, dir, memberA, videoDef(memberA))
			r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
			writes := countPatches(r, nil)

			for range 3 {
				reconcileSet(t, r)
				(*engine).progress.observe(tc.heads, time.Now())
			}
			require.Equal(t, tc.writes, *writes)
		})
	}
}

// An engine is built again when a member appears or is replaced, and
// at every gateway restart, over members whose heads it has not seen
// move yet; a placeholder waits seconds for its mirror. Until it has
// seen a member move or StartGrace pass, what the node reported stands:
// clearing a stalled member then reported it ready, and reporting a
// placeholder stalled flapped MembersReady at every set start.
func TestReconcilerKeepsItsReportWhileANewEngineHasNotSeenAMemberMove(t *testing.T) {
	engine := stalledStart(t, time.Now())
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	// What the node's previous engine applied.
	require.NoError(t, r.Client.Status().Patch(context.Background(),
		mxlv1alpha1.StalledMembersApply("programme", "reader", []string{memberA}), client.Apply,
		client.FieldOwner(mxlv1alpha1.StalledMembersFieldOwner("reader")), client.ForceOwnership))
	writes := countPatches(r, nil)

	reconcileSet(t, r)
	require.Zero(t, *writes)
	require.Equal(t, []mxlv1alpha1.NodeStalledMembers{{Node: "reader", FlowIDs: []string{memberA}}}, stalledOf(t, r))

	(*engine).progress.observe([]uint64{1, 0}, time.Now())
	reconcileSet(t, r)
	require.Empty(t, stalledOf(t, r), "a member seen moving stayed reported")
}

// The report is the set's status, the engine serves its readers: a
// failed write must neither turn the set's recheck into a backoff, which
// holds off replacing a restarted member, nor fail the reconcile.
func TestReconcilerServesOnWhenItsReportFails(t *testing.T) {
	stalledStart(t, time.Now().Add(-StartGrace))
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	writes := countPatches(r, errors.New("apiserver unavailable"))

	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter)
	require.Equal(t, 1, *writes)
	require.Contains(t, r.engines, "programme")
	reconcileSet(t, r)
	require.Equal(t, 2, *writes, "a failed report was not tried again at the recheck")
}

// A node that stops serving the set removes its entry, and asks for no
// recheck of its own: a failed removal would leave the member reported
// stalled with nothing to try it again.
func TestReconcilerTriesAFailedRemovalAgain(t *testing.T) {
	stalledStart(t, time.Now().Add(-StartGrace))
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	require.Len(t, stalledOf(t, r), 1)

	var set mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, r.Client.Get(context.Background(), types.NamespacedName{Name: "programme"}, &set))
	set.Status.Nodes = []string{"elsewhere"}
	require.NoError(t, r.Client.Status().Update(context.Background(), &set))
	ok := r.Client
	writes := countPatches(r, errors.New("apiserver unavailable"))
	require.Equal(t, recheck, reconcileSet(t, r).RequeueAfter, "a failed removal was not tried again")
	require.Equal(t, 1, *writes)
	require.Empty(t, r.engines)

	r.Client = ok
	require.Zero(t, reconcileSet(t, r), "a set not served here was looked at again with nothing to remove")
	require.Empty(t, stalledOf(t, r))
}

// An API server with a CRD from before status.stalledMembers accepts the
// apply and drops the field: nothing would show why no member is ever
// reported.
func TestReconcilerSaysOnceWhenTheAPIServerDropsItsReport(t *testing.T) {
	engine := stalledStart(t, time.Now().Add(-StartGrace))
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	memberDir(t, dir, memberB, videoDef(memberB))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, _ string, obj client.Object, _ client.Patch,
			_ ...client.SubResourcePatchOption) error {
			unstructured.RemoveNestedField(obj.(*unstructured.Unstructured).Object, "status", "stalledMembers")
			return nil
		},
	})
	var said int
	l := funcr.New(func(_, args string) {
		if strings.Contains(args, "predates the field") {
			said++
		}
	}, funcr.Options{})
	ctx := log.IntoContext(context.Background(), l)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "programme"}}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	(*engine).progress.observe([]uint64{1, 0}, time.Now())
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, said)
}

// A node whose engine stopped, because no reader wants the output there
// any more, must take its entry back: nothing else would on a node the
// output may be wanted on again.
func TestReconcilerTakesItsStalledMembersBackWhenItStopsServing(t *testing.T) {
	stalledStart(t, time.Now().Add(-StartGrace))
	dir := t.TempDir()
	memberDir(t, dir, memberA, videoDef(memberA))
	r := reconcilerFor(t, wantedSet("reader"), &fakeDomains{dir: dir, onMxlfs: true})
	reconcileSet(t, r)
	require.Len(t, stalledOf(t, r), 1)

	var set mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, r.Client.Get(context.Background(), types.NamespacedName{Name: "programme"}, &set))
	set.Status.Nodes = []string{"elsewhere"}
	require.NoError(t, r.Client.Status().Update(context.Background(), &set))
	reconcileSet(t, r)
	require.Empty(t, r.engines)
	require.Empty(t, stalledOf(t, r))
}

// The engine finds a member stalled in its own loop, between the set's
// rechecks: the set has to come back at once to be reported within the
// seconds a stall is reported in. The engine's loop must never wait on
// the queue for it.
func TestAStallBringsItsSetBack(t *testing.T) {
	r := &Reconciler{stalls: make(chan event.GenericEvent, 1)}
	stalls := r.stallsOf("programme")

	stalls()
	stalls()
	require.Equal(t, "programme", (<-r.stalls).Object.GetName())
	require.Empty(t, r.stalls, "a full queue was waited on")
}
