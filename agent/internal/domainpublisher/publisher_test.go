package domainpublisher

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/agent/internal/domainfs"
)

// goleak checks every test in this package starts and ends with the
// same set of goroutines; RunSyncLoop must honour ctx cancellation.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const (
	idA = "1ac254d9-a5eb-475f-a2b6-3d02a5cfbc82"
	idB = "3310f209-9351-47c0-b9a2-14c59b6a4c23"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	return s
}

func domain(name, id, dir string) *mxlv1alpha1.MxlDomain {
	return &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       mxlv1alpha1.MxlDomainSpec{ID: id, Directory: dir},
	}
}

func node(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func setup(t *testing.T, objs ...client.Object) (client.Client, *Publisher, string) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlDomain{}).
		WithObjects(objs...).
		Build()
	root := t.TempDir()
	p := NewFromDomainPath(c, "n1", filepath.Join(root, "domain"),
		func(string) (int64, int64, error) { return 1024, 512, nil },
		func() bool { return true })
	return c, p, root
}

func get(t *testing.T, c client.Client, name string) mxlv1alpha1.MxlDomain {
	t.Helper()
	var d mxlv1alpha1.MxlDomain
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: name}, &d))
	return d
}

// The domain the agent tracks is written with its identity and
// reported mirrored; a second domain is written with its own identity
// but reported as not mirrored, because nothing tracks its flows.
func TestSyncMaterialisesEverySelectedDomain(t *testing.T) {
	c, p, root := setup(t, node("n1", nil),
		domain("studio", idA, "domain"), domain("scratch", idB, "scratch"))

	require.NoError(t, p.Sync(context.Background()))

	for _, tc := range []struct {
		name, id, dir string
		mirrored      bool
	}{{"studio", idA, "domain", true}, {"scratch", idB, "scratch", false}} {
		def, err := domainfs.ReadDefinition(filepath.Join(root, tc.dir, "domain_def.json"))
		require.NoError(t, err)
		assert.Equal(t, tc.id, def.ID)

		got := get(t, c, tc.name)
		e := got.Status.Node("n1")
		require.NotNil(t, e, tc.name)
		assert.True(t, e.Ready, tc.name)
		assert.Equal(t, tc.mirrored, e.Mirrored, tc.name)
		assert.Equal(t, tc.mirrored, e.FanotifyReady, tc.name)
		assert.EqualValues(t, 1024, e.CapacityBytes)
		assert.NotNil(t, e.LastSeen)
	}
}

// Every node writes its own entry of one list; another node's entry is
// left as it was.
func TestSyncKeepsOtherNodesEntries(t *testing.T) {
	d := domain("studio", idA, "domain")
	d.Status.Nodes = []mxlv1alpha1.MxlDomainNodeStatus{{NodeName: "n2", Ready: true, Mirrored: true}}
	c, p, _ := setup(t, node("n1", nil), d)

	require.NoError(t, p.Sync(context.Background()))
	require.NoError(t, p.Sync(context.Background()))

	got := get(t, c, "studio").Status
	require.Len(t, got.Nodes, 2)
	assert.NotNil(t, got.Node("n2"))
	assert.NotNil(t, got.Node("n1"))
}

// A node outside the selector writes nothing and withdraws an entry it
// wrote while it was still selected.
func TestSyncHonoursTheNodeSelector(t *testing.T) {
	d := domain("studio", idA, "studio")
	d.Spec.NodeSelector = map[string]string{"mxl": "yes"}
	d.Status.Nodes = []mxlv1alpha1.MxlDomainNodeStatus{{NodeName: "n1", Ready: true}}
	c, p, root := setup(t, node("n1", map[string]string{"mxl": "no"}), d)

	require.NoError(t, p.Sync(context.Background()))

	assert.NoDirExists(t, filepath.Join(root, "studio"))
	got := get(t, c, "studio")
	assert.Nil(t, got.Status.Node("n1"))
}

// Two domains naming one directory would each overwrite the other's
// identity; neither is written and both say why.
func TestSyncRefusesTwoDomainsInOneDirectory(t *testing.T) {
	c, p, root := setup(t, node("n1", nil),
		domain("a", idA, "shared"), domain("b", idB, "shared"))

	require.NoError(t, p.Sync(context.Background()))

	assert.NoFileExists(t, filepath.Join(root, "shared", "domain_def.json"))
	for _, n := range []string{"a", "b"} {
		got := get(t, c, n)
		e := got.Status.Node("n1")
		require.NotNil(t, e)
		assert.False(t, e.Ready)
		assert.Contains(t, e.Message, "claimed by a, b")
	}
}

// An object in the per-node shape has no id; the agent leaves it to the
// operator rather than writing a domain_def.json with an empty id.
func TestSyncSkipsDomainsWithoutAnID(t *testing.T) {
	c, p, root := setup(t, node("n1", nil), domain("n1", "", "domain"))

	require.NoError(t, p.Sync(context.Background()))

	assert.NoFileExists(t, filepath.Join(root, "domain", "domain_def.json"))
	assert.Empty(t, get(t, c, "n1").Status.Nodes)
}

func TestRunSyncLoopStopsOnCancel(t *testing.T) {
	_, p, _ := setup(t, node("n1", nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.RunSyncLoop(ctx, time.Hour); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunSyncLoop did not return after cancel")
	}
}
