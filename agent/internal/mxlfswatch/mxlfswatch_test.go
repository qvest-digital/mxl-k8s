package mxlfswatch

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
	"github.com/qvest-digital/mxl-k8s/api/mxlfs/uapitest"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const id = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"

type recorder struct {
	mu    sync.Mutex
	calls []string
	done  chan struct{}
}

func (r *recorder) Materialize(_ context.Context, pid int32, path string) error {
	r.mu.Lock()
	r.calls = append(r.calls, path)
	r.mu.Unlock()
	r.done <- struct{}{}
	return nil
}

func domainWithPlaceholder(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(d, id+".mxl-flow", "grains"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(d, mxlv1alpha1.PlaceholderMarkerDir), 0o755))
	require.NoError(t, os.WriteFile(mxlv1alpha1.PlaceholderMarker(d, id), nil, 0o644))
	return d
}

func newTrigger(r *recorder, ours bool, domain string) *Trigger {
	return &Trigger{
		Domain:     domain,
		Dispatcher: r,
		Ours:       func(context.Context, int32) bool { return ours },
		Window:     time.Minute,
		Log:        logr.Discard(),
	}
}

func TestOpeningAPlaceholderRingRequestsOneMirror(t *testing.T) {
	d := domainWithPlaceholder(t)
	r := &recorder{done: make(chan struct{}, 8)}
	tr := newTrigger(r, false, d)

	// libmxl maps every grain of the flow when it opens it.
	for i := 0; i < 5; i++ {
		tr.Handle(context.Background(), fanotify.Open{PID: 77, Path: filepath.Join(d, id+".mxl-flow", "grains", "data."+string(rune('0'+i)))})
	}
	<-r.done
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, []string{filepath.Join(d, id+".mxl-flow", "data")}, r.calls)
}

// The path of an open is the opener's. A pod that mounts the domain
// through the CSI volume at /mxl opens /mxl/<id>.mxl-flow/..., which is
// no path in the agent's mount namespace; the flow is the placeholder
// of that name in the domain the opens are watched on.
func TestAnOpenThroughAnotherMountPathRequestsTheMirror(t *testing.T) {
	d := domainWithPlaceholder(t)
	r := &recorder{done: make(chan struct{}, 8)}
	tr := newTrigger(r, false, d)

	require.True(t, tr.Handle(context.Background(),
		fanotify.Open{PID: 77, Path: "/mxl/" + id + ".mxl-flow/grains/data.0"}))
	<-r.done
	require.Equal(t, []string{filepath.Join(d, id+".mxl-flow", "data")}, r.calls)
}

func TestOpensThatAreNotPlaceholderRingsRequestNothing(t *testing.T) {
	d := domainWithPlaceholder(t)
	r := &recorder{done: make(chan struct{}, 8)}
	tr := newTrigger(r, false, d)

	notRing := filepath.Join(d, id+".mxl-flow", "flow_def.json")
	require.False(t, tr.Handle(context.Background(), fanotify.Open{PID: 77, Path: notRing}), "flow_def.json is read by scans")

	require.NoError(t, os.Remove(mxlv1alpha1.PlaceholderMarker(d, id)))
	copyRing := filepath.Join(d, id+".mxl-flow", "grains", "data.0")
	require.False(t, tr.Handle(context.Background(), fanotify.Open{PID: 77, Path: copyRing}), "a copy is not a placeholder")
}

func TestOwnProcessesRequestNothing(t *testing.T) {
	d := domainWithPlaceholder(t)
	r := &recorder{done: make(chan struct{}, 8)}
	tr := newTrigger(r, true, d)

	ring := filepath.Join(d, id+".mxl-flow", "channels")
	require.False(t, tr.Handle(context.Background(), fanotify.Open{PID: 77, Path: ring}))
}

func TestMountLocal(t *testing.T) {
	m := Mount{Root: "/", MountPoint: "/run/mxl"}
	p, ok := m.Local("/domain/" + id + ".mxl-flow")
	require.True(t, ok)
	require.Equal(t, "/run/mxl/domain/"+id+".mxl-flow", p)

	m = Mount{Root: "/domain", MountPoint: "/run/mxl/domain"}
	p, ok = m.Local("/domain/" + id + ".mxl-flow")
	require.True(t, ok)
	require.Equal(t, "/run/mxl/domain/"+id+".mxl-flow", p)
	_, ok = m.Local("/other/" + id + ".mxl-flow")
	require.False(t, ok)
}

// The request layout must match struct mxlfs_wanted_req, or the kernel
// copies a path into the wrong place.
func TestWantedRequestLayout(t *testing.T) {
	h, err := uapitest.Parse()
	require.NoError(t, err)
	require.NoError(t, h.CheckStruct("mxlfs_wanted_req", reflect.TypeFor[wantedReq]()))
	require.Equal(t, h.Ioctls["MXLFS_IOC_WANTED_NEXT"], iocWantedNext)
	require.Equal(t, h.Consts["MXLFS_MAGIC"], int64(Magic))
}

// The gateway and the exporter walk into flows too; neither reads one, and
// a mirror started for them would copy a flow onto a node nobody reads it
// on. On EFA that mirrored every redundancy set member onto the other
// member's node.
func TestWantedByOurOwnProcessStartsNoMirror(t *testing.T) {
	r := &recorder{done: make(chan struct{}, 1)}
	wanted(context.Background(), "/d/"+id+".mxl-flow", 7, r, nil,
		func(context.Context, int32) bool { return true }, logr.Discard())
	require.Empty(t, r.calls)
}

func TestWantedByAReaderStartsAMirror(t *testing.T) {
	r := &recorder{done: make(chan struct{}, 1)}
	wanted(context.Background(), "/d/"+id+".mxl-flow", 7, r, nil,
		func(context.Context, int32) bool { return false }, logr.Discard())
	require.Equal(t, []string{"/d/" + id + ".mxl-flow/data"}, r.calls)
}

func TestWantedOutputIsClaimedNotMirrored(t *testing.T) {
	r := &recorder{done: make(chan struct{}, 1)}
	wanted(context.Background(), "/d/"+id+".mxl-flow", 7, r,
		func(context.Context, string) bool { return true }, nil, logr.Discard())
	require.Empty(t, r.calls)
}

// A module that predates MXLFS_IOC_WANTED_NEXT answers it with ENOTTY,
// as any directory that is no mxlfs does. Opening the queue asks once, so
// the domain fails to start rather than run without its wanted flows.
func TestOpeningTheWantedQueueFailsWithoutTheIoctl(t *testing.T) {
	f, err := os.Open(t.TempDir())
	require.NoError(t, err)
	defer f.Close()
	_, err = newWanted(Mount{}, f, wantedNext(f))
	require.ErrorIs(t, err, unix.ENOTTY)

	_, err = OpenWanted(t.TempDir())
	require.ErrorContains(t, err, "no mxlfs mount holds")
}

// fakeQueue hands out queued paths, then times out as an empty queue
// does. Like mxlfs, it fails a request with flags set, and it counts the
// requests that carry anything an earlier answer wrote.
type fakeQueue struct {
	mu     sync.Mutex
	queued []string
	stale  int
}

func (q *fakeQueue) next(req *wantedReq) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if req.Flags != 0 {
		return unix.EINVAL
	}
	if req.ID != 0 || req.PID != 0 || req.Path != [len(req.Path)]byte{} {
		q.stale++
	}
	if len(q.queued) == 0 {
		time.Sleep(time.Millisecond)
		return unix.ETIMEDOUT
	}
	copy(req.Path[:], q.queued[0])
	req.PID = 7
	q.queued = q.queued[1:]
	return nil
}

// The probe takes the entry it finds; the kernel reports each flow once,
// so Run has to handle it or the walk waits for a mirror nobody starts.
func TestWantedRunHandlesTheEntryTheProbeTook(t *testing.T) {
	q := &fakeQueue{queued: []string{"/domain/" + id + ".mxl-flow"}}
	w, err := newWanted(Mount{Root: "/", MountPoint: "/run/mxl"}, nil, q.next)
	require.NoError(t, err)
	require.NotNil(t, w.first)

	r := &recorder{done: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx, r, nil, nil, logr.Discard()) }()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("no mirror for the probed flow")
	}
	require.Equal(t, []string{"/run/mxl/domain/" + id + ".mxl-flow/data"}, r.calls)
	q.mu.Lock()
	defer q.mu.Unlock()
	require.Zero(t, q.stale, "a request reused an answer")
}

// Every request Run sends after the probe is a fresh one with flags 0:
// mxlfs fails any flag with EINVAL, which would end Run.
func TestWantedRunSendsFreshRequests(t *testing.T) {
	q := &fakeQueue{}
	w, err := newWanted(Mount{Root: "/", MountPoint: "/run/mxl"}, nil, q.next)
	require.NoError(t, err)
	require.Nil(t, w.first)

	r := &recorder{done: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx, r, nil, nil, logr.Discard()) }()
	for _, flow := range []string{id, "6fbec3b1-1b0f-417d-9059-8b94a47197ed"} {
		q.mu.Lock()
		q.queued = append(q.queued, "/domain/"+flow+".mxl-flow")
		q.mu.Unlock()
		select {
		case <-r.done:
		case err := <-errc:
			t.Fatalf("Run ended: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatalf("no mirror for %s", flow)
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	require.Zero(t, q.stale, "a request reused an answer")
}

// blocking holds every Materialize until release is closed.
type blocking struct {
	started chan struct{}
	release chan struct{}
}

func (b *blocking) Materialize(context.Context, int32, string) error {
	b.started <- struct{}{}
	<-b.release
	return nil
}

// A tracker's stop releases the domain's leases once runMxlfs has ended;
// a mirror request still running then would act on a domain no longer
// tracked.
func requireRunWaitsForRequests(t *testing.T, run func(ctx context.Context, d Materializer) error) {
	t.Helper()
	b := &blocking{started: make(chan struct{}, 1), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = run(ctx, b); close(done) }()
	<-b.started
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned while a mirror request was running")
	case <-time.After(100 * time.Millisecond):
	}
	close(b.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return once the request ended")
	}
}

func TestWantedRunWaitsForTheRequestsItStarted(t *testing.T) {
	requireRunWaitsForRequests(t, func(ctx context.Context, d Materializer) error {
		q := &fakeQueue{queued: []string{"/domain/" + id + ".mxl-flow"}}
		w := &Wanted{mount: Mount{Root: "/", MountPoint: "/run/mxl"}, next: q.next}
		return w.Run(ctx, d, nil, nil, logr.Discard())
	})
}

func TestOpenWatchRunWaitsForTheRequestsItStarted(t *testing.T) {
	d := domainWithPlaceholder(t)
	requireRunWaitsForRequests(t, func(ctx context.Context, m Materializer) error {
		o := &OpenWatch{
			t: &Trigger{Domain: d, Dispatcher: m, Log: logr.Discard()},
			run: func(ctx context.Context, out chan<- fanotify.Open) error {
				defer close(out)
				out <- fanotify.Open{PID: 77, Path: filepath.Join(d, id+".mxl-flow", "channels")}
				<-ctx.Done()
				return ctx.Err()
			},
			close: func() error { return nil },
		}
		return o.Run(ctx)
	})
}

type locations struct{ appeared, vanished []string }

func (l *locations) PublishAppeared(_ context.Context, dir string) error {
	l.appeared = append(l.appeared, dir)
	return nil
}

func (l *locations) PublishVanished(_ context.Context, dir string) error {
	l.vanished = append(l.vanished, dir)
	return nil
}

// A marker that comes back means the mirror filling the placeholder was
// deleted: the gateway still holds the directory open, so publishing it
// would make the node look like the flow's producer.
func TestMarkerChanged(t *testing.T) {
	l := &locations{}
	require.NoError(t, MarkerChanged(context.Background(), l, id, false))
	require.NoError(t, MarkerChanged(context.Background(), l, id, true))
	require.Equal(t, []string{id + ".mxl-flow"}, l.appeared, "a marker that goes publishes the location")
	require.Equal(t, []string{id + ".mxl-flow"}, l.vanished, "a marker that returns withdraws it")
}
