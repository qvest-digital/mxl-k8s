package merge

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		Status: mxlv1alpha1.MxlFlowRedundancySetStatus{Nodes: nodes},
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
