package podlookup

import (
	"context"
	"fmt"
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
)

// parsePodUID is what stands between a UDS request and the correct
// Pod. Wrong format detection here means SO_PEERCRED-based access
// control routes the wrong consumer's mirrors. Each cgroup format the
// kubelet might emit must round-trip to a canonical hyphenated UID.

func TestParsePodUID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{
			name: "systemd format with underscores",
			in:   "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod11111111_2222_3333_4444_555555555555.slice/cri-containerd-abcd.scope",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "cgroupfs format with hyphens",
			in:   "0::/kubepods/burstable/pod11111111-2222-3333-4444-555555555555/abcd",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "cgroupfs at the end of the line",
			in:   "0::/kubepods/burstable/pod11111111-2222-3333-4444-555555555555",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "multi-line cgroup, first match wins",
			in:   "10:devices:/\n0::/kubepods.slice/kubepods-pod11111111_2222_3333_4444_555555555555.slice/x.scope\n",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "no kubepods entry",
			in:   "0::/user.slice/user-1000.slice/session-1.scope",
			want: "",
			ok:   false,
		},
		{
			name: "uid shape wrong (too short)",
			in:   "0::/kubepods/burstable/pod11111111-2222-3333-4444-55555555",
			want: "",
			ok:   false,
		},
		{
			name: "empty input",
			in:   "",
			want: "",
			ok:   false,
		},
		{
			name: "raw UUID-looking string outside pod-prefix is rejected",
			in:   "0::/something/11111111-2222-3333-4444-555555555555/x",
			want: "",
			ok:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parsePodUID(tc.in)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The client is uncached and every placeholder open resolves a pid, so
// a list of every pod in the cluster per open would load the API server
// with the cluster's size; only this node's pods can own a local pid.
func TestPodForUIDListsOnlyThisNodesPods(t *testing.T) {
	pod := func(name, uid, node string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid)},
			Spec: corev1.PodSpec{NodeName: node}}
	}
	var selectors []string
	c := fake.NewClientBuilder().
		WithObjects(pod("here", "11111111-2222-3333-4444-555555555555", "n1"),
			pod("there", "66666666-2222-3333-4444-555555555555", "n2")).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			lo := (&client.ListOptions{}).ApplyOptions(opts)
			sel := ""
			if lo.FieldSelector != nil {
				sel = lo.FieldSelector.String()
			}
			selectors = append(selectors, sel)
			return c.List(ctx, l, opts...)
		}}).Build()
	r := &Resolver{Client: c, NodeName: "n1"}

	p, err := r.PodForUID(context.Background(), "11111111-2222-3333-4444-555555555555")
	require.NoError(t, err)
	assert.Equal(t, "here", p.Name)
	assert.Equal(t, []string{"spec.nodeName=n1"}, selectors)
}

func nodePod(name, uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid)},
		Spec: corev1.PodSpec{NodeName: "n1"}}
}

// countingClient counts the pod Lists made on c.
func countingClient(lists *int, objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			*lists++
			return c.List(ctx, l, opts...)
		}}).Build()
}

func shortenListEvery(t *testing.T, d time.Duration) {
	t.Helper()
	old := listEvery
	listEvery = d
	t.Cleanup(func() { listEvery = old })
}

const (
	uidA = "11111111-2222-3333-4444-555555555555"
	uidB = "22222222-2222-3333-4444-555555555555"
	uidC = "33333333-2222-3333-4444-555555555555"
)

// Every placeholder open and every wanted flow resolves its pid, so a
// List of the node's pods per lookup cost the API one List per want.
func TestLookupsOfKnownPodsShareOneList(t *testing.T) {
	var lists int
	r := &Resolver{Client: countingClient(&lists, nodePod("a", uidA), nodePod("b", uidB)), NodeName: "n1"}

	for i := range 100 {
		uid := uidA
		if i%2 == 1 {
			uid = uidB
		}
		p, err := r.PodForUID(context.Background(), uid)
		require.NoError(t, err)
		assert.Equal(t, types.UID(uid), p.UID)
	}
	assert.Equal(t, 1, lists)
}

// A pod started after the last List is not in it; its lookup lists again
// rather than report it missing, or its reader never gets a mirror.
func TestAnUnknownUIDListsAgain(t *testing.T) {
	shortenListEvery(t, 10*time.Millisecond)
	var lists int
	c := countingClient(&lists, nodePod("a", uidA))
	r := &Resolver{Client: c, NodeName: "n1"}

	_, err := r.PodForUID(context.Background(), uidA)
	require.NoError(t, err)
	require.NoError(t, c.Create(context.Background(), nodePod("c", uidC)))

	p, err := r.PodForUID(context.Background(), uidC)
	require.NoError(t, err)
	assert.Equal(t, "c", p.Name)
	assert.Equal(t, 2, lists)
}

// A pid whose pod never shows up misses every time; those misses list
// again, but never sooner than listEvery after the last List started.
// Checked on when each List started, not on how many fit a wall-clock
// span, which a loaded runner shifts.
func TestMissesListAtMostOncePerInterval(t *testing.T) {
	shortenListEvery(t, 50*time.Millisecond)
	var starts []time.Time
	c := fake.NewClientBuilder().
		WithObjects(nodePod("a", uidA)).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			starts = append(starts, time.Now())
			return c.List(ctx, l, opts...)
		}}).Build()
	r := &Resolver{Client: c, NodeName: "n1"}

	for deadline := time.Now().Add(10 * time.Second); len(starts) < 4 && time.Now().Before(deadline); {
		_, err := r.PodForUID(context.Background(), uidC)
		require.EqualError(t, err, "no pod with uid "+uidC+" on node n1")
	}
	require.Len(t, starts, 4, "misses list again once the interval has passed")
	for i := 1; i < len(starts); i++ {
		assert.GreaterOrEqual(t, starts[i].Sub(starts[i-1]), listEvery, "List %d followed the last within the interval", i)
	}
}

// The wait for the next List ends with the request.
func TestAMissWaitsNoLongerThanItsContext(t *testing.T) {
	shortenListEvery(t, time.Hour)
	var lists int
	r := &Resolver{Client: countingClient(&lists, nodePod("a", uidA)), NodeName: "n1"}
	_, err := r.PodForUID(context.Background(), uidA)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = r.PodForUID(ctx, uidC)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, lists)
}

// blockingClient lists only once release is closed or its context ends,
// and reports each List it is in.
func blockingClient(listing chan<- struct{}, release <-chan struct{}) client.WithWatch {
	return fake.NewClientBuilder().
		WithObjects(nodePod("a", uidA)).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			listing <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			return c.List(ctx, l, opts...)
		}}).Build()
}

// A lookup that misses while another's List hangs waits for it no longer
// than its own request: the wanted path runs a bounded number of
// handlers, and lookups held past their requests behind one stuck List
// would hold every one of them.
func TestALookupWaitingOnAHeldListEndsWithItsRequest(t *testing.T) {
	listing, release := make(chan struct{}, 2), make(chan struct{})
	defer close(release)
	r := &Resolver{Client: blockingClient(listing, release), NodeName: "n1"}

	first := make(chan error, 1)
	go func() { _, err := r.PodForUID(context.Background(), uidA); first <- err }()
	<-listing

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.PodForUID(ctx, uidB); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("the lookup waited for the held List past its own request")
	}
	release <- struct{}{}
	require.NoError(t, <-first)
}

// A List that hangs ends at listTimeout even for a request with no
// deadline, so the lookups waiting for it are not held without end.
func TestAHungListEndsAtTheListTimeout(t *testing.T) {
	old := listTimeout
	listTimeout = 50 * time.Millisecond
	t.Cleanup(func() { listTimeout = old })
	listing, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	r := &Resolver{Client: blockingClient(listing, release), NodeName: "n1"}

	done := make(chan error, 1)
	go func() { _, err := r.PodForUID(context.Background(), uidA); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "list pods: ")
	case <-time.After(5 * time.Second):
		t.Fatal("a List with no deadline hung the lookup")
	}
}

// switchClient lists the node's pods, or, while down is set, hangs
// until the List's context ends. It counts the Lists.
func switchClient(down *atomic.Bool, lists *atomic.Int32, objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			lists.Add(1)
			if down.Load() {
				<-ctx.Done()
				return ctx.Err()
			}
			return c.List(ctx, l, opts...)
		}}).Build()
}

// While the API server does not answer, a List that failed answers the
// lookups queued behind it for listEvery: each running its own List of
// listTimeout would hold the eighth of eight bounded wanted handlers
// eight times as long. Once the API answers again, a lookup past
// listEvery lists and resolves, and that List clears the failure: the
// next miss is answered by a List of its own, not by the old error.
func TestAFailedListAnswersTheLookupsBehindIt(t *testing.T) {
	oldTimeout := listTimeout
	listTimeout = 50 * time.Millisecond
	t.Cleanup(func() { listTimeout = oldTimeout })
	shortenListEvery(t, 300*time.Millisecond)
	var down atomic.Bool
	var lists atomic.Int32
	down.Store(true)
	r := &Resolver{Client: switchClient(&down, &lists, nodePod("a", uidA)), NodeName: "n1"}

	const waiters = 8
	errs := make(chan error, waiters)
	start := time.Now()
	for i := range waiters {
		go func() {
			_, err := r.PodForUID(context.Background(), fmt.Sprintf("%08d-2222-3333-4444-555555555555", i))
			errs <- err
		}()
	}
	for range waiters {
		require.ErrorIs(t, <-errs, context.DeadlineExceeded)
	}
	assert.Equal(t, int32(1), lists.Load(), "lookups behind a failed List listed again")
	assert.Less(t, time.Since(start), 8*listTimeout, "the lookups behind it waited for Lists of their own")

	down.Store(false)
	time.Sleep(listEvery)
	p, err := r.PodForUID(context.Background(), uidA)
	require.NoError(t, err, "a lookup past listEvery with the API back")
	assert.Equal(t, "a", p.Name)
	assert.Equal(t, int32(2), lists.Load())

	_, err = r.PodForUID(context.Background(), uidC)
	require.EqualError(t, err, "no pod with uid "+uidC+" on node n1", "the old failure answered after a List succeeded")
	assert.Equal(t, int32(3), lists.Load())
}

// A List that ended because the lookup's own request ended says nothing
// about the API: the dispatcher bounds each request at its materialize
// timeout, and a request that ran out while its List was slow must not
// fail every other lookup for listEvery on an API that answers.
func TestAListEndedByItsOwnRequestIsNoFailureForOthers(t *testing.T) {
	shortenListEvery(t, time.Hour)
	var down atomic.Bool
	var lists atomic.Int32
	down.Store(true)
	r := &Resolver{Client: switchClient(&down, &lists, nodePod("a", uidA)), NodeName: "n1"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := r.PodForUID(ctx, uidA)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	down.Store(false)
	p, err := r.PodForUID(context.Background(), uidA)
	require.NoError(t, err)
	assert.Equal(t, "a", p.Name)
	assert.Equal(t, int32(2), lists.Load())
}
