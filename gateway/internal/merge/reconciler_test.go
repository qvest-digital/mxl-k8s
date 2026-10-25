package merge

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

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

// The member mirrors start when the output is first wanted, so the
// members' directories appear after the set does: wait for them.
func TestReconcilerWaitsForTheMembersDirectories(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(flowDir(dir, memberA)), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flowDir(dir, memberA), "data"), nil, 0o644))
	d := &fakeDomains{dir: dir, onMxlfs: true}
	r := reconcilerFor(t, wantedSet("reader"), d)

	res := reconcileSet(t, r)
	require.Equal(t, 2*time.Second, res.RequeueAfter, "member B has no directory yet")
	require.Empty(t, r.engines)
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
// publishes. A set deleted and created again under the same name starts
// at the same generation. Either way the engine must be built again.
func TestRunningEngineServesOnlyTheFlowsAndSetItWasBuiltFor(t *testing.T) {
	dir := t.TempDir()
	member := func(id string) string {
		p := filepath.Join(dir, id+".mxl-flow")
		require.NoError(t, os.MkdirAll(p, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(p, "data"), nil, 0o644))
		return filepath.Join(p, "data")
	}
	// The engine keeps each member file open, so its inode is not reused.
	inode := func(path string) uint64 {
		f, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })
		fi, err := f.Stat()
		require.NoError(t, err)
		return fi.Sys().(*syscall.Stat_t).Ino
	}
	a, b := member("a"), member("b")
	set := wantedSet("n1")
	set.UID, set.Generation = "first", 1
	cur := &running{engine: &Engine{data: []string{a, b}, inodes: []uint64{inode(a), inode(b)}}, uid: set.UID, gen: 1}
	require.True(t, cur.serves(set))

	again := set.DeepCopy()
	again.UID = "second"
	require.False(t, cur.serves(again), "a set created again under the same name kept the old engine")

	require.NoError(t, os.RemoveAll(filepath.Dir(b)))
	member("b")
	require.False(t, cur.serves(set), "a member replaced underneath the engine kept it")

	require.NoError(t, os.RemoveAll(filepath.Dir(a)))
	require.True(t, cur.engine.Stale(), "a removed member")
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
		require.NoError(t, os.MkdirAll(flowDir(dir, id), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(flowDir(dir, id), "data"), nil, 0o644))
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
