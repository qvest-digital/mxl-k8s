package mxlfswatch

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
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
	require.Equal(t, uintptr(4096), unsafe.Sizeof(wantedReq{}))
	// _IOWR('x', 5, 4096 bytes): read|write, size, type, number.
	require.Equal(t, uintptr(3<<30|4096<<16|0x78<<8|5), iocWantedNext)
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
