package redundancy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const out = "33333333-3333-4333-8333-333333333333"

func newSets(t *testing.T, node string) (*Sets, *mxlv1alpha1.MxlFlowRedundancySet) {
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	set := &mxlv1alpha1.MxlFlowRedundancySet{
		ObjectMeta: metav1.ObjectMeta{Name: "programme"},
		Spec: mxlv1alpha1.MxlFlowRedundancySetSpec{
			Members: []mxlv1alpha1.RedundancyFlowRef{{FlowID: "11111111-1111-4111-8111-111111111111"}, {FlowID: "22222222-2222-4222-8222-222222222222"}},
			Output:  mxlv1alpha1.RedundancyFlowRef{FlowID: out},
		},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(set).WithObjects(set).Build()
	return &Sets{Client: c, NodeName: node}, set
}

func TestWantRecordsTheNodeOnce(t *testing.T) {
	s, set := newSets(t, "node-a")
	ctx := context.Background()

	for range 2 {
		ok, err := s.Want(ctx, "", out)
		require.NoError(t, err)
		require.True(t, ok)
	}

	var got mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, s.Client.Get(ctx, types.NamespacedName{Name: set.Name}, &got))
	require.Equal(t, []string{"node-a"}, got.Status.Nodes)
}

func TestWantIgnoresFlowsThatAreNoOutput(t *testing.T) {
	s, _ := newSets(t, "node-a")

	ok, err := s.Want(context.Background(), "", "11111111-1111-4111-8111-111111111111")
	require.NoError(t, err)
	require.False(t, ok, "a member is not an output")

	ok, err = s.Want(context.Background(), "studio", out)
	require.NoError(t, err)
	require.False(t, ok, "the same id in another domain is another flow")
}

func TestIsOutput(t *testing.T) {
	s, _ := newSets(t, "node-a")
	require.True(t, s.IsOutput(context.Background(), "", out))
	require.False(t, s.IsOutput(context.Background(), "", "11111111-1111-4111-8111-111111111111"))
}

// A node stays in status.nodes while a reader there maps the output, and
// for the grace after the last one stops, so a reader that restarts
// keeps its output. Only then does it leave, so the gateway stops
// merging and the member mirrors onto the node go.
func TestReleaseLeavesOnlyAfterTheGraceWithNoReader(t *testing.T) {
	s, set := newSets(t, "node-a")
	ctx := context.Background()
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }
	_, err := s.Want(ctx, "", out)
	require.NoError(t, err)

	reading := true
	asked := 0
	inUse := func() (map[mxlv1alpha1.FlowRef]bool, error) {
		asked++
		return map[mxlv1alpha1.FlowRef]bool{{ID: out}: reading}, nil
	}
	nodes := func() []string {
		var got mxlv1alpha1.MxlFlowRedundancySet
		require.NoError(t, s.Client.Get(ctx, types.NamespacedName{Name: set.Name}, &got))
		return got.Status.Nodes
	}

	now = now.Add(5 * time.Minute)
	require.NoError(t, s.Release(ctx, inUse, time.Minute))
	require.Equal(t, []string{"node-a"}, nodes(), "a reader maps the output")

	reading = false
	now = now.Add(59 * time.Second)
	require.NoError(t, s.Release(ctx, inUse, time.Minute))
	require.Equal(t, []string{"node-a"}, nodes(), "inside the grace a restarting reader keeps it")

	now = now.Add(time.Second)
	require.NoError(t, s.Release(ctx, inUse, time.Minute))
	require.Empty(t, nodes(), "no reader for the grace")

	n := asked
	require.NoError(t, s.Release(ctx, inUse, time.Minute))
	require.Equal(t, n, asked, "no /proc scan while no set lists this node")
}

// The output of another domain is another flow; a reader of it keeps no
// set alive. An agent that restarts starts every grace afresh.
func TestReleaseCountsOnlyTheOutputItself(t *testing.T) {
	s, set := newSets(t, "node-a")
	ctx := context.Background()
	set.Status.Nodes = []string{"node-b", "node-a"}
	require.NoError(t, s.Client.Status().Update(ctx, set))
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }
	inUse := func() (map[mxlv1alpha1.FlowRef]bool, error) {
		return map[mxlv1alpha1.FlowRef]bool{{Domain: "studio", ID: out}: true}, nil
	}

	require.NoError(t, s.Release(ctx, inUse, time.Minute))
	now = now.Add(time.Minute)
	require.NoError(t, s.Release(ctx, inUse, time.Minute))

	var got mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, s.Client.Get(ctx, types.NamespacedName{Name: set.Name}, &got))
	require.Equal(t, []string{"node-b"}, got.Status.Nodes, "only this node leaves")
}

// A reader that waits for the output after Release found it idle, but
// before this node leaves, finds the node still listed and adds nothing.
// Leaving then would take the output away under the reader, so leave
// checks the last use again.
func TestReleaseStaysForAReaderThatArrivesWhileLeaving(t *testing.T) {
	s, set := newSets(t, "node-a")
	ctx := context.Background()
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }
	_, err := s.Want(ctx, "", out)
	require.NoError(t, err)
	now = now.Add(5 * time.Minute)

	base := s.Client
	s.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			// The reader's Want records its use as leave reads the set.
			s.used(set.Name)
			return c.Get(ctx, key, obj, opts...)
		},
	})
	none := func() (map[mxlv1alpha1.FlowRef]bool, error) { return nil, nil }
	require.NoError(t, s.Release(ctx, none, time.Minute))

	var got mxlv1alpha1.MxlFlowRedundancySet
	require.NoError(t, base.Get(ctx, types.NamespacedName{Name: set.Name}, &got))
	require.Equal(t, []string{"node-a"}, got.Status.Nodes)
}
