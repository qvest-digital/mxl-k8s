package intent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// globFlow is the i-th flow a process walking the whole domain opens.
func globFlow(i int) mxlv1alpha1.FlowRef {
	return mxlv1alpha1.FlowRef{ID: fmt.Sprintf("11111111-2222-3333-4444-%012d", i)}
}

func limitedDispatcher(t *testing.T, limits Limits, objs ...client.Object) (*Dispatcher, client.Client, *time.Time) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(objs...).
		Build()
	now := time.Unix(1_800_000_000, 0)
	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
		Limits:     limits,
		nowFn:      func() time.Time { return now },
	}
	return d, c, &now
}

func consumer(name, uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID(uid)}}
}

func intentMirrors(t *testing.T, c client.Client) int {
	t.Helper()
	var l mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(context.Background(), &l,
		client.MatchingLabels{mxlv1alpha1.LabelCreatedByIntent: "n-target"}))
	return len(l.Items)
}

func limitEvents(t *testing.T, c client.Client) []corev1.Event {
	t.Helper()
	var l corev1.EventList
	require.NoError(t, c.List(context.Background(), &l))
	var out []corev1.Event
	for _, e := range l.Items {
		if e.Reason == ReasonMirrorLimitReached {
			out = append(out, e)
		}
	}
	return out
}

// A glob over the domain opens every flow at once from one pod. Each
// open used to become a mirror onto the node; past the burst they are
// refused, the pod is told why in one event, and the flows it reaches
// again after the window are mirrored then.
func TestEnsureMirror_AGlobIsHeldToTheBurst(t *testing.T) {
	d, c, now := limitedDispatcher(t, Limits{Burst: 8, BurstWindow: 10 * time.Second})
	pod := consumer("globber", "uid-glob")

	// One creation a second, so the k-th oldest creation leaves the window
	// k seconds after the first.
	start := *now
	var refused int
	for i := range 26 {
		if _, err := d.ensureMirror(context.Background(), globFlow(i), "n-src", pod); err != nil {
			assert.Contains(t, err.Error(), "in the last 10s")
			var le *LimitError
			require.ErrorAs(t, err, &le, "a refusal by a limit says when it could pass")
			// Told all at once to ask when the oldest creation leaves,
			// every refused flow would ask together each window, and all
			// but one be refused again, each at a request's full price.
			k := i - 8
			want := start.Add(time.Duration(k%8)*time.Second + time.Duration(k/8+1)*10*time.Second)
			assert.Equal(t, want.Sub(*now), le.RetryAfter(),
				"the %d-th refused flow asks when the place it is given frees", k)
			refused++
		}
		if i < 7 {
			*now = now.Add(time.Second)
		}
	}
	assert.Equal(t, 8, intentMirrors(t, c), "a glob of 26 flows created more than the burst")
	assert.Equal(t, 18, refused)

	// A flow refused again, asking too early, keeps its place.
	_, err := d.ensureMirror(context.Background(), globFlow(9), "n-src", pod)
	var le *LimitError
	require.ErrorAs(t, err, &le)
	assert.Equal(t, start.Add(11*time.Second).Sub(*now), le.RetryAfter())

	ev := limitEvents(t, c)
	require.Len(t, ev, 1, "one event per pod per window, not one per refused flow")
	assert.Equal(t, "Pod", ev[0].InvolvedObject.Kind)
	assert.Equal(t, "globber", ev[0].InvolvedObject.Name)
	assert.Equal(t, types.UID("uid-glob"), ev[0].InvolvedObject.UID)
	assert.Equal(t, corev1.EventTypeWarning, ev[0].Type)
	assert.Contains(t, ev[0].Message, "ENOENT")

	*now = start.Add(10 * time.Second)
	_, err = d.ensureMirror(context.Background(), globFlow(8), "n-src", pod)
	require.NoError(t, err, "a flow opened again after the window is mirrored")

	// The place the flow that passed held goes to the one behind it.
	_, err = d.ensureMirror(context.Background(), globFlow(9), "n-src", pod)
	require.ErrorAs(t, err, &le)
	assert.Equal(t, time.Second, le.RetryAfter())
}

// Requests arrive concurrently, one goroutine per open. Counting and
// creating has to be one step, or all of them pass the count first. A
// slow create holds every request that counted inside that window, so a
// count taken outside the step lets all twenty through.
func TestEnsureMirror_ConcurrentRequestsShareTheLimit(t *testing.T) {
	d, c, _ := limitedDispatcher(t, Limits{PerPod: 4})
	d.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*mxlv1alpha1.MxlFlowMirror); ok {
				time.Sleep(20 * time.Millisecond)
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	pod := consumer("globber", "uid-glob")

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = d.ensureMirror(context.Background(), globFlow(i), "n-src", pod)
		}()
	}
	wg.Wait()
	assert.Equal(t, 4, intentMirrors(t, c))
}

// The per-pod limit counts the live mirrors created for that pod,
// across windows; another pod has its own.
func TestEnsureMirror_PerPodLimitCountsLiveMirrors(t *testing.T) {
	d, c, now := limitedDispatcher(t, Limits{PerPod: 3, BurstWindow: 10 * time.Second})
	a, b := consumer("a", "uid-a"), consumer("b", "uid-b")

	for i := range 3 {
		_, err := d.ensureMirror(context.Background(), globFlow(i), "n-src", a)
		require.NoError(t, err)
		*now = now.Add(time.Hour)
	}
	_, err := d.ensureMirror(context.Background(), globFlow(3), "n-src", a)
	require.ErrorContains(t, err, "already has 3 mirrors created on demand")
	var le *LimitError
	require.ErrorAs(t, err, &le)
	assert.Zero(t, le.RetryAfter(), "a pod at its count of live mirrors stays there while it lives, "+
		"whatever the burst window; asking again would only poll the API")

	_, err = d.ensureMirror(context.Background(), globFlow(4), "n-src", b)
	require.NoError(t, err, "another pod's mirrors count against its own limit")

	// One of a's mirrors going frees its place.
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: MirrorName(globFlow(0).ID, "n-target")}, &m))
	require.NoError(t, c.Delete(context.Background(), &m))
	_, err = d.ensureMirror(context.Background(), globFlow(3), "n-src", a)
	require.NoError(t, err)
}

// The node limit counts every pod's mirrors onto the node.
func TestEnsureMirror_PerNodeLimitCountsEveryPod(t *testing.T) {
	d, _, _ := limitedDispatcher(t, Limits{PerNode: 2, BurstWindow: 10 * time.Second})

	_, err := d.ensureMirror(context.Background(), globFlow(0), "n-src", consumer("a", "uid-a"))
	require.NoError(t, err)
	_, err = d.ensureMirror(context.Background(), globFlow(1), "n-src", consumer("b", "uid-b"))
	require.NoError(t, err)
	_, err = d.ensureMirror(context.Background(), globFlow(2), "n-src", consumer("c", "uid-c"))
	require.ErrorContains(t, err, "node n-target already has 2 mirrors created on demand")
	var le *LimitError
	require.ErrorAs(t, err, &le)
	assert.Zero(t, le.RetryAfter(), "a node at its count stays there while its pods live, whatever the burst window")
}

// Only mirrors this agent created on demand count. A receiver's mirror
// onto the node, another node's on-demand mirror and a mirror being
// deleted are not, and a request an existing mirror already serves
// creates nothing and is never refused.
func TestEnsureMirror_LimitsCountOnlyLiveOnDemandMirrorsOntoThisNode(t *testing.T) {
	declared := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: MirrorName(globFlow(10).ID, "n-target"),
			Labels: map[string]string{mxlv1alpha1.LabelCreatedByReceiver: "rcv-1"}},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: globFlow(10).ID, SourceNode: "n-src", TargetNode: "n-target"},
	}
	elsewhere := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: MirrorName(globFlow(11).ID, "n-other"),
			Labels: map[string]string{mxlv1alpha1.LabelCreatedByIntent: "n-other",
				mxlv1alpha1.LabelRequestorPodUID: "uid-a"}},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: globFlow(11).ID, SourceNode: "n-src", TargetNode: "n-other"},
	}
	going := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: MirrorName(globFlow(12).ID, "n-target"),
			Labels: map[string]string{mxlv1alpha1.LabelCreatedByIntent: "n-target",
				mxlv1alpha1.LabelRequestorPodUID: "uid-a"},
			Finalizers:        []string{"test/keep"},
			DeletionTimestamp: &metav1.Time{Time: time.Now()}},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: globFlow(12).ID, SourceNode: "n-src", TargetNode: "n-target"},
	}
	d, _, _ := limitedDispatcher(t, Limits{PerPod: 1, PerNode: 1, Burst: 1, BurstWindow: time.Minute},
		declared, elsewhere, going)
	pod := consumer("a", "uid-a")

	_, err := d.ensureMirror(context.Background(), globFlow(0), "n-src", pod)
	require.NoError(t, err, "a declared, a foreign and a terminating mirror were counted")

	got, err := d.ensureMirror(context.Background(), globFlow(10), "n-src", pod)
	require.NoError(t, err, "a request the receiver's mirror serves was refused")
	assert.Equal(t, declared.Name, got.Name)
	got, err = d.ensureMirror(context.Background(), globFlow(0), "n-src", pod)
	require.NoError(t, err, "a request its own existing mirror serves was refused")
	assert.Equal(t, MirrorName(globFlow(0).ID, "n-target"), got.Name)
}

// Without limits the dispatcher creates as before.
func TestEnsureMirror_ZeroLimitsCreateWithoutBound(t *testing.T) {
	d, c, _ := limitedDispatcher(t, Limits{})
	pod := consumer("globber", "uid-glob")
	for i := range 30 {
		_, err := d.ensureMirror(context.Background(), globFlow(i), "n-src", pod)
		require.NoError(t, err)
	}
	assert.Equal(t, 30, intentMirrors(t, c))
	assert.Empty(t, limitEvents(t, c))
}

// The open trigger finds a burst refusal by errors.As on what Materialize
// returns; a wrap that drops the error chain would disable every retry.
func TestMaterialize_ReturnsTheLimitErrorItWasRefusedWith(t *testing.T) {
	var objs []client.Object
	for i := range 2 {
		objs = append(objs, &mxlv1alpha1.MxlFlow{
			ObjectMeta: metav1.ObjectMeta{Name: globFlow(i).ID},
			Spec:       mxlv1alpha1.MxlFlowSpec{ID: globFlow(i).ID},
			Status: mxlv1alpha1.MxlFlowStatus{Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-src", Phase: mxlv1alpha1.MxlFlowLocationOrigin}}},
		})
	}
	d, _, _ := limitedDispatcher(t, Limits{Burst: 1, BurstWindow: time.Minute}, objs...)
	d.Resolver = &waitingReader{pod: consumer("reader", "uid-r")}
	d.FlowChecker = func(string) bool { return false }
	d.MaterializeTimeout = 20 * time.Millisecond
	path := func(i int) string { return "/run/mxl/domain/" + globFlow(i).ID + ".mxl-flow/data" }

	require.Error(t, d.Materialize(context.Background(), 42, path(0)), "no gateway fills the first mirror")
	err := d.Materialize(context.Background(), 42, path(1))
	var le *LimitError
	require.ErrorAs(t, err, &le, "Materialize lost the refusal's type: %v", err)
	assert.Positive(t, le.RetryAfter())
}

// The agent's pod lookup names a pid's pod UID without the API, which is
// what lets a full burst refuse before the lookup.
var _ uidResolver = (*podlookup.Resolver)(nil)

// uidReader is the agent's pod lookup: the UID from /proc, the pod from a
// List of the node's pods, which it counts.
type uidReader struct {
	pod   *corev1.Pod
	lists atomic.Int64
}

func (r *uidReader) UIDForPID(int32) (string, error) { return string(r.pod.UID), nil }

func (r *uidReader) PodForPID(context.Context, int32) (*corev1.Pod, error) {
	r.lists.Add(1)
	return r.pod, nil
}

// A flow the burst refused asks again until it passes, and the pod List
// and the provider's lookups ahead of the limit made every refusal cost
// what a creation does. A pod whose window is full is refused before
// them; what the full path answers ahead of the limit it still answers.
func TestMaterialize_AFullBurstRefusesBeforeThePodLookup(t *testing.T) {
	var objs []client.Object
	for i := range 4 {
		objs = append(objs, &mxlv1alpha1.MxlFlow{
			ObjectMeta: metav1.ObjectMeta{Name: globFlow(i).ID},
			Spec:       mxlv1alpha1.MxlFlowSpec{ID: globFlow(i).ID},
			Status: mxlv1alpha1.MxlFlowStatus{Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-src", Phase: mxlv1alpha1.MxlFlowLocationOrigin}}},
		})
	}
	objs = append(objs, &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: MirrorName(globFlow(2).ID, "n-target"),
			Labels: map[string]string{mxlv1alpha1.LabelCreatedByReceiver: "rcv-1"}},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{FlowID: globFlow(2).ID, SourceNode: "n-src", TargetNode: "n-target"},
	})
	d, c, _ := limitedDispatcher(t, Limits{Burst: 1, BurstWindow: time.Minute}, objs...)
	var capsGets atomic.Int64
	d.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
			if _, ok := o.(*mxlv1alpha1.MxlNodeCapabilities); ok {
				capsGets.Add(1)
			}
			return cl.Get(ctx, k, o, opts...)
		},
	})
	d.Provider = ""
	reader := &uidReader{pod: consumer("reader", "uid-r")}
	d.Resolver = reader
	local := globFlow(3).ID
	d.FlowChecker = func(id string) bool { return id == local }
	d.MaterializeTimeout = 20 * time.Millisecond
	path := func(id string) string { return "/run/mxl/domain/" + id + ".mxl-flow/data" }

	require.Error(t, d.Materialize(context.Background(), 42, path(globFlow(0).ID)), "no gateway fills the first mirror")
	lists, caps := reader.lists.Load(), capsGets.Load()

	err := d.Materialize(context.Background(), 42, path(globFlow(1).ID))
	var le *LimitError
	require.ErrorAs(t, err, &le)
	assert.Positive(t, le.RetryAfter())
	assert.Equal(t, lists, reader.lists.Load(), "a full window still looked the pod up")
	assert.Equal(t, caps, capsGets.Load(), "a full window still looked the provider up")
	require.Len(t, limitEvents(t, c), 1, "a refusal ahead of the pod lookup reached the pod as no event")

	err = d.Materialize(context.Background(), 42, path(globFlow(2).ID))
	require.Error(t, err, "no gateway makes the receiver's mirror Ready here")
	assert.NotErrorAs(t, err, &le, "a request the receiver's mirror serves was refused")
	require.NoError(t, d.Materialize(context.Background(), 42, path(local)), "a flow already here was refused")
}
