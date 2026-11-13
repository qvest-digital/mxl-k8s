package mxlfswatch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
	"github.com/qvest-digital/mxl-k8s/agent/internal/intent"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

type limited struct{}

func (limited) Error() string             { return "pod had 8 mirrors created on demand in the last 10s" }
func (limited) RetryAfter() time.Duration { return 10 * time.Millisecond }

// scripted answers each request with the next error of its script, nil
// once the script is spent.
type scripted struct {
	mu     sync.Mutex
	errs   []error
	calls  int
	before func() // runs ahead of every request after the first
}

func (s *scripted) Materialize(context.Context, int32, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls > 1 && s.before != nil {
		s.before()
	}
	if len(s.errs) == 0 {
		return nil
	}
	err := s.errs[0]
	s.errs = s.errs[1:]
	return err
}

func (s *scripted) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// openRing has pid open the placeholder's ring and waits for every
// request that open started.
func openRing(t *testing.T, tr *Trigger, d string, pid int32) {
	t.Helper()
	require.True(t, tr.Handle(context.Background(),
		fanotify.Open{PID: pid, Path: filepath.Join(d, id+".mxl-flow", "grains", "data.0")}))
	done := make(chan struct{})
	go func() { tr.requests.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the request did not end")
	}
}

func retryTrigger(t *testing.T, d string, m Materializer, start func(int32) (string, bool)) *Trigger {
	t.Helper()
	saved := minRetry
	minRetry = time.Millisecond
	t.Cleanup(func() { minRetry = saved })
	return &Trigger{Domain: d, Dispatcher: m, Window: time.Minute, Log: logr.Discard(), ProcessStart: start}
}

func alive(int32) (string, bool) { return "4242", true }

// The open of a placeholder has succeeded before the agent hears of it,
// so a refusal by a limit reaches no reader: the process maps a ring
// nothing fills and never opens it again. The request is made again
// while that process runs, until it passes.
func TestARefusedPlaceholderRequestIsMadeAgainWhileTheProcessRuns(t *testing.T) {
	d := domainWithPlaceholder(t)
	m := &scripted{errs: []error{limited{}, limited{}}}
	openRing(t, retryTrigger(t, d, m, alive), d, 77)
	require.Equal(t, 3, m.count(), "a refused request was not made again until it passed")
}

// A process that is gone, or whose pid now names another process, no
// longer reads the ring.
func TestARefusedRequestEndsWithItsProcess(t *testing.T) {
	for name, start := range map[string]func(int32) (string, bool){
		"exited": func(int32) (string, bool) { return "", false },
		"replaced": func() func(int32) (string, bool) {
			n := 0
			return func(int32) (string, bool) { n++; return string(rune('0' + n)), true }
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			d := domainWithPlaceholder(t)
			m := &scripted{errs: []error{limited{}, limited{}}}
			openRing(t, retryTrigger(t, d, m, start), d, 77)
			require.Equal(t, 1, m.count())
		})
	}
}

// A flow that is no placeholder here any more, filled by a mirror another
// request brought, needs nothing.
func TestARefusedRequestEndsOnceThePlaceholderIsFilled(t *testing.T) {
	d := domainWithPlaceholder(t)
	m := &scripted{errs: []error{limited{}, limited{}, limited{}}}
	tr := retryTrigger(t, d, m, alive)
	m.before = func() { _ = os.Remove(mxlv1alpha1.PlaceholderMarker(d, id)) }
	openRing(t, tr, d, 77)
	require.Equal(t, 2, m.count(), "requests went on for a flow no longer a placeholder")
}

type capped struct{}

func (capped) Error() string             { return "pod already has 64 mirrors created on demand, its limit" }
func (capped) RetryAfter() time.Duration { return 0 }

// A refusal waiting cannot lift, by a count of live mirrors or by
// anything but a limit, is not made again: each request costs API calls
// on the agent's one client, and a pod at its count stays there.
func TestARefusalWaitingCannotLiftIsFinal(t *testing.T) {
	for name, err := range map[string]error{
		"count of live mirrors": capped{},
		"no limit":              errors.New("MxlFlow not yet known cluster-wide"),
	} {
		t.Run(name, func(t *testing.T) {
			d := domainWithPlaceholder(t)
			m := &scripted{errs: []error{err}}
			openRing(t, retryTrigger(t, d, m, alive), d, 77)
			require.Equal(t, 1, m.count())
		})
	}
}

// A process opening the flow again after the dedup window, while its
// first request still waits to ask again, starts no second request.
func TestAReopenWhileARequestWaitsStartsNoOther(t *testing.T) {
	d := domainWithPlaceholder(t)
	release := make(chan struct{})
	m := &scripted{errs: []error{limited{}}}
	tr := retryTrigger(t, d, m, alive)
	tr.Window = time.Nanosecond
	m.before = func() { <-release }
	open := fanotify.Open{PID: 77, Path: filepath.Join(d, id+".mxl-flow", "grains", "data.0")}
	require.True(t, tr.Handle(context.Background(), open))
	require.Eventually(t, func() bool { return m.count() >= 1 }, 5*time.Second, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	require.False(t, tr.Handle(context.Background(), open), "a second request for the same process and flow")
	close(release)
	tr.requests.Wait()
	require.Equal(t, 2, m.count(), "one refused request and its one retry")
}

// The start time tells a process from a later one under the same pid,
// so a child started after this test process has read a later one.
func TestProcessStartReadsTheKernelsStartTime(t *testing.T) {
	mine, ok := processStart(int32(os.Getpid()))
	require.True(t, ok)
	again, _ := processStart(int32(os.Getpid()))
	require.Equal(t, mine, again)

	time.Sleep(50 * time.Millisecond) // past one clock tick
	child := exec.Command("sleep", "5")
	require.NoError(t, child.Start())
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	theirs, ok := processStart(int32(child.Process.Pid))
	require.True(t, ok)
	a, err := strconv.ParseUint(mine, 10, 64)
	require.NoError(t, err)
	b, err := strconv.ParseUint(theirs, 10, 64)
	require.NoError(t, err)
	require.Greater(t, b, a, "a later process read no later start time")

	_, ok = processStart(1 << 30)
	require.False(t, ok)
}

// Once a request has ended, the same process opening the flow again after
// the dedup window starts a fresh one: the entry that blocks a second
// concurrent request is released.
func TestAReopenAfterARequestEndedStartsANewOne(t *testing.T) {
	d := domainWithPlaceholder(t)
	m := &scripted{}
	tr := retryTrigger(t, d, m, alive)
	tr.Window = time.Nanosecond
	open := fanotify.Open{PID: 77, Path: filepath.Join(d, id+".mxl-flow", "grains", "data.0")}
	require.True(t, tr.Handle(context.Background(), open))
	tr.requests.Wait()
	time.Sleep(time.Millisecond)
	require.True(t, tr.Handle(context.Background(), open), "a finished request still blocked the next one")
	tr.requests.Wait()
	require.Equal(t, 2, m.count())
}

// podsByPID is the agent's pod lookup: the UID from /proc, the pod from
// a List of the node's pods, which it counts as the API call it is.
type podsByPID struct {
	pods  map[int32]*corev1.Pod
	calls func()
}

func (p podsByPID) UIDForPID(pid int32) (string, error) { return string(p.pods[pid].UID), nil }

func (p podsByPID) PodForPID(_ context.Context, pid int32) (*corev1.Pod, error) {
	p.calls()
	return p.pods[pid], nil
}

// A process opening many placeholders at once is held to its pod's
// burst, and each refused request asks again while it runs. Each request
// costs API calls on the agent's one client, which renewing its leases
// shares. Had every refused request been told to ask when the oldest
// creation leaves the window, all of them would ask together each window,
// and all but the places that freed be refused again, each at a full
// request's price.
func TestRefusedRequestsAskAgainAsPlacesFree(t *testing.T) {
	const n, burst = 50, 8
	const window = 200 * time.Millisecond
	saved := minRetry
	minRetry = 20 * time.Millisecond
	t.Cleanup(func() { minRetry = saved })

	d := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(d, mxlv1alpha1.PlaceholderMarkerDir), 0o755))
	var objs []client.Object
	ids := make([]string, n+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("11111111-2222-3333-4444-%012d", i)
		require.NoError(t, os.MkdirAll(filepath.Join(d, ids[i]+".mxl-flow", "grains"), 0o755))
		require.NoError(t, os.WriteFile(mxlv1alpha1.PlaceholderMarker(d, ids[i]), nil, 0o644))
		objs = append(objs, &mxlv1alpha1.MxlFlow{
			ObjectMeta: metav1.ObjectMeta{Name: ids[i]},
			Spec:       mxlv1alpha1.MxlFlowSpec{ID: ids[i]},
			Status: mxlv1alpha1.MxlFlowStatus{Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-src", Phase: mxlv1alpha1.MxlFlowLocationOrigin}}},
		})
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(mxlv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	var mu sync.Mutex
	var calls []time.Time
	call := func() {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, time.Now())
	}
	counted := interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
			call()
			return cl.Get(ctx, k, o, opts...)
		},
		List: func(ctx context.Context, cl client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			call()
			return cl.List(ctx, l, opts...)
		},
		Create: func(ctx context.Context, cl client.WithWatch, o client.Object, opts ...client.CreateOption) error {
			call()
			return cl.Create(ctx, o, opts...)
		},
	})
	pod := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID("uid-" + name)}}
	}
	disp := &intent.Dispatcher{
		Client:     counted,
		Resolver:   podsByPID{pods: map[int32]*corev1.Pod{77: pod("globber"), 78: pod("other")}, calls: call},
		DomainPath: d,
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
		Limits:     intent.Limits{PerPod: 64, Burst: burst, BurstWindow: window},
		// No gateway fills a mirror here; an admitted request ends at once.
		MaterializeTimeout: time.Millisecond,
	}
	tr := &Trigger{Domain: d, Dispatcher: disp, Window: time.Minute, Log: logr.Discard(), ProcessStart: alive}
	open := func(pid int32, id string) {
		tr.Handle(context.Background(),
			fanotify.Open{PID: pid, Path: filepath.Join(d, id+".mxl-flow", "grains", "data.0")})
	}
	wait := func() {
		done := make(chan struct{})
		go func() { tr.requests.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("the requests did not end")
		}
	}

	// What one admitted request costs, from another pod.
	open(78, ids[n])
	wait()
	mu.Lock()
	admitted := len(calls)
	calls = nil
	mu.Unlock()

	start := time.Now()
	for _, id := range ids[:n] {
		open(77, id)
	}
	wait()

	var l mxlv1alpha1.MxlFlowMirrorList
	require.NoError(t, c.List(context.Background(), &l, client.MatchingLabels{mxlv1alpha1.LabelRequestorPodUID: "uid-globber"}))
	require.Len(t, l.Items, n, "every refused request passed in the end")

	var perWindow []int
	for _, at := range calls {
		w := int(at.Sub(start) / window)
		for len(perWindow) <= w {
			perWindow = append(perWindow, 0)
		}
		perWindow[w]++
	}
	t.Logf("%d API calls for %d flows, per window %v; an admitted request %d", len(calls), n, perWindow, admitted)
	// The first window carries the open of every flow. In each one after,
	// a place that frees lets one request in, which may have asked once
	// before it was free; a refusal costs less than an admission.
	for w := 1; w < len(perWindow); w++ {
		require.LessOrEqual(t, perWindow[w], 2*burst*admitted,
			"window %d: %d API calls for %d places", w, perWindow[w], burst)
	}
}
