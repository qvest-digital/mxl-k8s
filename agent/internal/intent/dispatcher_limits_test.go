package intent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

	var refused int
	for i := range 26 {
		if _, err := d.ensureMirror(context.Background(), globFlow(i), "n-src", pod); err != nil {
			assert.Contains(t, err.Error(), "in the last 10s")
			refused++
		}
	}
	assert.Equal(t, 8, intentMirrors(t, c), "a glob of 26 flows created more than the burst")
	assert.Equal(t, 18, refused)

	ev := limitEvents(t, c)
	require.Len(t, ev, 1, "one event per pod per window, not one per refused flow")
	assert.Equal(t, "Pod", ev[0].InvolvedObject.Kind)
	assert.Equal(t, "globber", ev[0].InvolvedObject.Name)
	assert.Equal(t, types.UID("uid-glob"), ev[0].InvolvedObject.UID)
	assert.Equal(t, corev1.EventTypeWarning, ev[0].Type)
	assert.Contains(t, ev[0].Message, "ENOENT")

	*now = now.Add(10 * time.Second)
	_, err := d.ensureMirror(context.Background(), globFlow(8), "n-src", pod)
	require.NoError(t, err, "a flow opened again after the window is mirrored")
}

// Requests arrive concurrently, one goroutine per open. Counting and
// creating has to be one step, or all of them pass the count first.
func TestEnsureMirror_ConcurrentRequestsShareTheLimit(t *testing.T) {
	d, c, _ := limitedDispatcher(t, Limits{PerPod: 4})
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
	d, c, now := limitedDispatcher(t, Limits{PerPod: 3})
	a, b := consumer("a", "uid-a"), consumer("b", "uid-b")

	for i := range 3 {
		_, err := d.ensureMirror(context.Background(), globFlow(i), "n-src", a)
		require.NoError(t, err)
		*now = now.Add(time.Hour)
	}
	_, err := d.ensureMirror(context.Background(), globFlow(3), "n-src", a)
	require.ErrorContains(t, err, "already has 3 mirrors created on demand")

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
	d, _, _ := limitedDispatcher(t, Limits{PerNode: 2})

	_, err := d.ensureMirror(context.Background(), globFlow(0), "n-src", consumer("a", "uid-a"))
	require.NoError(t, err)
	_, err = d.ensureMirror(context.Background(), globFlow(1), "n-src", consumer("b", "uid-b"))
	require.NoError(t, err)
	_, err = d.ensureMirror(context.Background(), globFlow(2), "n-src", consumer("c", "uid-c"))
	require.ErrorContains(t, err, "node n-target already has 2 mirrors created on demand")
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
