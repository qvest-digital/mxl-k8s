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

func newTrigger(r *recorder, ours bool) *Trigger {
	return &Trigger{
		Dispatcher: r,
		Ours:       func(context.Context, int32) bool { return ours },
		Window:     time.Minute,
		Log:        logr.Discard(),
	}
}

func TestOpeningAPlaceholderRingRequestsOneMirror(t *testing.T) {
	d := domainWithPlaceholder(t)
	r := &recorder{done: make(chan struct{}, 8)}
	tr := newTrigger(r, false)

	// libmxl maps every grain of the flow when it opens it.
	for i := 0; i < 5; i++ {
		tr.Handle(context.Background(), fanotify.Open{PID: 77, Path: filepath.Join(d, id+".mxl-flow", "grains", "data."+string(rune('0'+i)))})
	}
	<-r.done
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, []string{filepath.Join(d, id+".mxl-flow", "data")}, r.calls)
}

func TestOpensThatAreNotPlaceholderRingsRequestNothing(t *testing.T) {
	d := domainWithPlaceholder(t)
	r := &recorder{done: make(chan struct{}, 8)}
	tr := newTrigger(r, false)

	notRing := filepath.Join(d, id+".mxl-flow", "flow_def.json")
	require.False(t, tr.Handle(context.Background(), fanotify.Open{PID: 77, Path: notRing}), "flow_def.json is read by scans")

	require.NoError(t, os.Remove(mxlv1alpha1.PlaceholderMarker(d, id)))
	copyRing := filepath.Join(d, id+".mxl-flow", "grains", "data.0")
	require.False(t, tr.Handle(context.Background(), fanotify.Open{PID: 77, Path: copyRing}), "a copy is not a placeholder")
}

func TestOwnProcessesRequestNothing(t *testing.T) {
	d := domainWithPlaceholder(t)
	r := &recorder{done: make(chan struct{}, 8)}
	tr := newTrigger(r, true)

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
