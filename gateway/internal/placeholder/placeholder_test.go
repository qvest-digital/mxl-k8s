package placeholder

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/flowlocks"
)

const flowID = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"

const flowDef = `{"description":"placeholder test","id":"` + flowID + `",
"tags":{"urn:x-nmos:tag:grouphint/v1.0":["placeholder-test:Video"]},"format":"urn:x-nmos:format:video",
"label":"placeholder test","parents":[],"media_type":"video/v210","grain_rate":{"numerator":50,"denominator":1},
"frame_width":320,"frame_height":180,"interlace_mode":"progressive","colorspace":"BT709",
"components":[{"name":"Y","width":320,"height":180,"bit_depth":10},
{"name":"Cb","width":160,"height":180,"bit_depth":10},{"name":"Cr","width":160,"height":180,"bit_depth":10}]}`

// handles is a real libmxl domain that claims to be on mxlfs: the
// manager's logic does not depend on the filesystem, only on the claim.
type handles struct {
	dir  string
	inst *mxl.Instance
}

func (h *handles) DomainPath() string { return h.dir }
func (h *handles) OnMxlfs() bool      { return true }
func (h *handles) MXL() *mxl.Instance { return h.inst }

func newDomain(t *testing.T) *handles {
	t.Helper()
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skip("/dev/shm not present")
	}
	dir, err := os.MkdirTemp("/dev/shm", "placeholder-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	require.NoError(t, os.WriteFile(filepath.Join(dir, "options.json"),
		[]byte(`{"urn:x-mxl:option:history_duration/v1.0": 200000000}`), 0o644))
	inst, err := mxl.NewInstance(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	return &handles{dir: dir, inst: inst}
}

func newManager(t *testing.T, h *handles, objs ...*mxlv1alpha1.MxlFlow) *Manager {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	b := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&mxlv1alpha1.MxlFlow{})
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return &Manager{
		Client:   b.Build(),
		NodeName: "node-a",
		Domain:   func(context.Context, string) (Handles, error) { return h, nil },
	}
}

func flow(locations ...mxlv1alpha1.MxlFlowLocation) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec: mxlv1alpha1.MxlFlowSpec{
			ID:         flowID,
			Definition: runtime.RawExtension{Raw: []byte(flowDef)},
		},
		Status: mxlv1alpha1.MxlFlowStatus{Locations: locations},
	}
}

// origin is the producer the MxlFlow's definition describes. A flow
// without one gets no placeholder.
var origin = mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationOrigin}

func reconcileFlow(t *testing.T, m *Manager) {
	t.Helper()
	_, err := m.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: flowID}})
	require.NoError(t, err)
}

func flowDir(h *handles) string { return filepath.Join(h.dir, flowID+".mxl-flow") }

func TestAbsentFlowGetsAPlaceholder(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationOrigin}))

	reconcileFlow(t, m)

	require.Equal(t, []string{flowID}, m.Held())
	require.DirExists(t, flowDir(h))
	require.True(t, IsPlaceholder(h.dir, flowID), "no marker for the placeholder")
}

func TestFlowWithALocalCopyGetsNone(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(mxlv1alpha1.MxlFlowLocation{NodeName: "node-a", Phase: mxlv1alpha1.MxlFlowLocationReady}))

	reconcileFlow(t, m)

	require.Empty(t, m.Held())
	require.NoDirExists(t, flowDir(h))
}

func TestDeletedFlowDropsItsPlaceholder(t *testing.T) {
	h := newDomain(t)
	f := flow(origin)
	m := newManager(t, h, f)
	reconcileFlow(t, m)
	require.DirExists(t, flowDir(h))

	require.NoError(t, m.Client.Delete(context.Background(), f))
	reconcileFlow(t, m)

	require.Empty(t, m.Held())
	require.False(t, IsPlaceholder(h.dir, flowID))
	require.NoDirExists(t, flowDir(h), "libmxl keeps a flow whose last writer was the placeholder")
}

func TestFilledPlaceholderLosesItsMarker(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)

	m.Filled(h.dir, flowID)

	require.False(t, IsPlaceholder(h.dir, flowID))
	require.Equal(t, []string{flowID}, m.Held())
}

// lockShared holds a shared flock on path from another process, as a
// producer's libmxl writer does on a flow's data file.
func lockShared(t *testing.T, path string) {
	t.Helper()
	cmd := exec.Command("flock", "--shared", path, "sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Skipf("flock(1) not available: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	time.Sleep(200 * time.Millisecond)
}

func TestLocalWriterWins(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	var stopped []string
	m.LocalWriter = func(domain, id string) { stopped = append(stopped, id) }
	reconcileFlow(t, m)

	lockShared(t, filepath.Join(flowDir(h), "data"))
	m.CheckLocalWriters(logr.Discard())

	require.Equal(t, []string{flowID}, stopped, "whatever filled the flow was not stopped first")
	require.Empty(t, m.Held())
	require.False(t, IsPlaceholder(h.dir, flowID))
	require.DirExists(t, flowDir(h), "giving the flow up to the local writer removed it")
}

func TestFlowAProcessHereWritesGetsNoPlaceholder(t *testing.T) {
	h := newDomain(t)
	require.NoError(t, os.MkdirAll(flowDir(h), 0o755))
	data := filepath.Join(flowDir(h), "data")
	require.NoError(t, os.WriteFile(data, nil, 0o644))
	lockShared(t, data)
	m := newManager(t, h, flow(origin))

	reconcileFlow(t, m)

	require.Empty(t, m.Held())
	require.False(t, IsPlaceholder(h.dir, flowID))
}

// On mxlfs a path walk into a flow that does not exist waits for it and
// posts it as wanted under the walker's pid; the agent then mirrors the
// flow here for the gateway, which reads nothing. Deciding that a flow
// is absent must not look the flow up.
func TestAbsentFlowIsNotLookedUp(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationOrigin}))
	saved := foreignWriters
	t.Cleanup(func() { foreignWriters = saved })
	foreignWriters = func(path string) (int, error) {
		if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
			t.Errorf("looked up %s, which does not exist", path)
		}
		return saved(path)
	}

	reconcileFlow(t, m)

	require.Equal(t, []string{flowID}, m.Held())
}

// A mirror filling a placeholder makes it a copy; the mirror going must
// make it a placeholder again. The gateway's writer still holds it, and
// without the marker the agent reads that writer as a local producer
// and the node claims Origin of a flow nothing here produces.
func TestUnfilledPlaceholderGetsItsMarkerBack(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationOrigin}))
	reconcileFlow(t, m)
	m.Filled(h.dir, flowID)
	require.False(t, IsPlaceholder(h.dir, flowID))

	m.Unfilled(h.dir, flowID)

	require.True(t, IsPlaceholder(h.dir, flowID))
	require.Equal(t, []string{flowID}, m.Held())
}

// A flow a local writer took over is not a placeholder at all; a
// mirror going later must not mark it as one.
func TestUnfilledLeavesATakenOverFlowAlone(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h)
	m.Unfilled(h.dir, flowID)
	require.False(t, IsPlaceholder(h.dir, flowID))
}

// larger is flowDef at twice the size: another ring layout.
var larger = strings.NewReplacer("320", "640", "180", "360", "160", "320").Replace(flowDef)

func onDiskWidth(t *testing.T, h *handles) any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(flowDir(h), "flow_def.json"))
	require.NoError(t, err)
	var def map[string]any
	require.NoError(t, json.Unmarshal(raw, &def))
	return def["frame_width"]
}

func setDefinition(t *testing.T, m *Manager, def string) {
	t.Helper()
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, m.Client.Get(context.Background(), types.NamespacedName{Name: flowID}, &f))
	f.Spec.Definition = runtime.RawExtension{Raw: []byte(def)}
	require.NoError(t, m.Client.Update(context.Background(), &f))
}

func setLocations(t *testing.T, m *Manager, locs ...mxlv1alpha1.MxlFlowLocation) {
	t.Helper()
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, m.Client.Get(context.Background(), types.NamespacedName{Name: flowID}, &f))
	f.Status.Locations = locs
	require.NoError(t, m.Client.Status().Update(context.Background(), &f))
}

// A flow nothing produces has no geometry anyone vouches for: the next
// producer may bring another one, and libmxl would hand it a
// placeholder of the old one as its own.
func TestFlowWithoutAnOriginGetsNoPlaceholder(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationStale}))

	reconcileFlow(t, m)

	require.Empty(t, m.Held())
	require.NoDirExists(t, flowDir(h))
}

func TestPlaceholderGoesWithItsOrigin(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)
	require.DirExists(t, flowDir(h))

	setLocations(t, m, mxlv1alpha1.MxlFlowLocation{NodeName: "node-b", Phase: mxlv1alpha1.MxlFlowLocationStale})
	reconcileFlow(t, m)

	require.Empty(t, m.Held())
	require.False(t, IsPlaceholder(h.dir, flowID))
	require.NoDirExists(t, flowDir(h), "a producer starting here would open the old placeholder as its flow")
}

func TestPlaceholderOfAnotherGeometryIsReplaced(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)
	require.EqualValues(t, 320, onDiskWidth(t, h))

	setDefinition(t, m, larger)
	reconcileFlow(t, m)

	require.Equal(t, []string{flowID}, m.Held())
	require.True(t, IsPlaceholder(h.dir, flowID))
	require.EqualValues(t, 640, onDiskWidth(t, h), "the placeholder kept the geometry the flow no longer has")
}

// A producer that opened the placeholder writes into it: only giving it
// up to the producer is left, which CheckLocalWriters does.
func TestPlaceholderAProducerOpenedIsNotReplaced(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)
	lockShared(t, filepath.Join(flowDir(h), "data"))

	setDefinition(t, m, larger)
	reconcileFlow(t, m)

	require.EqualValues(t, 320, onDiskWidth(t, h))
	require.Equal(t, []string{flowID}, m.Held())
}

// A mirror's copy is not replaced under it. Once the mirror goes, the
// periodic pass retires it.
func TestFilledPlaceholderIsReplacedOnceReleased(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	m.Period = 10 * time.Millisecond
	reconcileFlow(t, m)
	m.Filled(h.dir, flowID)

	setDefinition(t, m, larger)
	reconcileFlow(t, m)
	require.EqualValues(t, 320, onDiskWidth(t, h), "a filled placeholder was replaced under its mirror")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = m.Start(ctx) }()
	defer func() { cancel(); <-done }()
	m.Unfilled(h.dir, flowID)

	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(filepath.Join(flowDir(h), "flow_def.json"))
		return err == nil && strings.Contains(string(raw), `"frame_width":640`)
	}, 5*time.Second, 10*time.Millisecond, "the released placeholder kept its old geometry")
	require.True(t, IsPlaceholder(h.dir, flowID))
}

// A directory nothing holds, left by a copy of the old geometry, is not
// opened as the placeholder: libmxl would keep its layout.
func TestLeftoverOfAnotherGeometryIsReplaced(t *testing.T) {
	h := newDomain(t)
	require.NoError(t, os.MkdirAll(flowDir(h), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flowDir(h), "flow_def.json"), []byte(flowDef), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(flowDir(h), "data"), nil, 0o644))
	m := newManager(t, h, flow(origin))
	setDefinition(t, m, larger)

	reconcileFlow(t, m)

	require.Equal(t, []string{flowID}, m.Held())
	require.EqualValues(t, 640, onDiskWidth(t, h))
}

// A mirror replacing a directory of another geometry releases the
// placeholder held there, filled or not.
func TestRetireReleasesThePlaceholder(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)
	m.Filled(h.dir, flowID)

	m.Retire(h.dir, strings.ToUpper(flowID))

	require.Empty(t, m.Held())
	require.NoDirExists(t, flowDir(h))
}

// leaveBehind leaves in h what a gateway that shuts down leaves of its
// placeholder: the directory and its marker, with no writer.
func leaveBehind(t *testing.T, h *handles) {
	t.Helper()
	old := newManager(t, h, flow(origin))
	reconcileFlow(t, old)
	shutDown(t, old)
	require.True(t, IsPlaceholder(h.dir, flowID))
}

func writeMarker(t *testing.T, h *handles, id string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(h.dir, MarkerDir), 0o755))
	require.NoError(t, os.WriteFile(MarkerPath(h.dir, id), nil, 0o644))
}

// Between a new gateway opening the domain and its first reconcile of
// the flow, the directory the previous gateway left keeps its marker.
// Without it the agent reads the directory as a local copy and mirrors
// nothing into it, and a consumer opening the flow maps a ring nothing
// fills.
func TestOpeningADomainKeepsTheMarkersLeftBehind(t *testing.T) {
	h := newDomain(t)
	leaveBehind(t, h)
	m := newManager(t, h, flow(origin))

	m.DomainOpened(h.dir)
	require.True(t, IsPlaceholder(h.dir, flowID), "the marker went before the placeholder was held again")

	require.True(t, m.sweep(context.Background(), logr.Discard()))
	require.Equal(t, []string{flowID}, m.Held())
	require.True(t, IsPlaceholder(h.dir, flowID), "the first pass cleared the marker of a placeholder it holds")
}

// A marker the first pass does not hold again goes: left in place it
// would keep a copy or a local writer's flow under that id from ever
// becoming a location.
func TestFirstPassClearsTheMarkersNothingHolds(t *testing.T) {
	h := newDomain(t)
	leaveBehind(t, h)
	const gone = "0f5d3a52-58a3-4f0e-9a4b-2c1f0d6e7b81"
	writeMarker(t, h, gone)
	// The flow has a copy here now, so it gets no placeholder.
	m := newManager(t, h, flow(origin, mxlv1alpha1.MxlFlowLocation{NodeName: "node-a", Phase: mxlv1alpha1.MxlFlowLocationReady}))
	m.DomainOpened(h.dir)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = m.Start(ctx) }()
	defer func() { cancel(); <-done }()

	require.Eventually(t, func() bool {
		return !IsPlaceholder(h.dir, flowID) && !IsPlaceholder(h.dir, gone)
	}, 5*time.Second, 10*time.Millisecond, "a marker nothing holds outlived the first pass")
	require.Empty(t, m.Held())
}

// A domain first opened after the first pass holds no placeholder of
// this gateway yet, so every marker in it is left over.
func TestADomainOpenedAfterTheFirstPassIsClearedAtOnce(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h)
	require.True(t, m.sweep(context.Background(), logr.Discard()))
	writeMarker(t, h, flowID)

	m.DomainOpened(h.dir)

	require.False(t, IsPlaceholder(h.dir, flowID))
}

func TestSameGeometry(t *testing.T) {
	relabelled := strings.Replace(flowDef, `"label":"placeholder test"`, `"label":"renamed"`, 1)
	require.True(t, SameGeometry([]byte(flowDef), []byte(relabelled)), "a label is no part of the ring")
	require.False(t, SameGeometry([]byte(flowDef), []byte(larger)))
	require.False(t, SameGeometry([]byte(flowDef), []byte(strings.Replace(flowDef, `"numerator":50`, `"numerator":25`, 1))))
	require.False(t, SameGeometry([]byte(flowDef), []byte("not json")))
}

// flowNamed is flow under another id: its own MxlFlow and directory.
func flowNamed(id string, locations ...mxlv1alpha1.MxlFlowLocation) *mxlv1alpha1.MxlFlow {
	f := flow(locations...)
	f.Name = id
	f.Spec.ID = id
	f.Spec.Definition = runtime.RawExtension{Raw: []byte(strings.ReplaceAll(flowDef, flowID, id))}
	return f
}

// shutDown runs m until its context ends, as the manager does on
// SIGTERM, and reports how long it took to return.
func shutDown(t *testing.T, m *Manager) time.Duration {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = m.Start(ctx) }()
	start := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("Start did not return after its context ended")
	}
	return time.Since(start)
}

func inode(t *testing.T, path string) uint64 {
	t.Helper()
	var st syscall.Stat_t
	require.NoError(t, syscall.Stat(path, &st))
	return st.Ino
}

// A reader on the node holds the placeholder's flow open. A restarting
// gateway that deleted it took the flow from under the reader, and the
// next gateway's placeholder was another flow the reader had to find
// again.
func TestShutdownLeavesPlaceholdersToTheNextGateway(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)
	data := filepath.Join(flowDir(h), "data")
	before := inode(t, data)

	shutDown(t, m)

	require.Empty(t, m.Held())
	require.DirExists(t, flowDir(h), "the shutdown deleted the placeholder")

	next := newManager(t, h, flow(origin))
	reconcileFlow(t, next)
	require.Equal(t, []string{flowID}, next.Held())
	require.True(t, IsPlaceholder(h.dir, flowID))
	require.Equal(t, before, inode(t, data), "the next gateway holds another flow than the one left")
}

// Until the next gateway is up, the agent decides from the directory
// alone. One with a flow definition and no marker it takes for a local
// copy: a consumer opening it would get no mirror and read a ring
// nothing fills. With the marker the open asks for a mirror, as it does
// on a placeholder that is held.
func TestPlaceholderLeftOnShutdownKeepsItsMarker(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)

	shutDown(t, m)

	require.FileExists(t, filepath.Join(flowDir(h), "flow_def.json"))
	require.True(t, IsPlaceholder(h.dir, flowID), "the shutdown left the directory looking like a local copy")
}

type fakeWriter struct {
	detach           error
	closed, detached bool
}

func (w *fakeWriter) Close() error  { w.closed = true; return nil }
func (w *fakeWriter) Detach() error { w.detached = true; return w.detach }

// A writer whose data file could not be guarded refuses Detach and
// would stay attached through the shutdown: the flow would look written
// to the next gateway and to libmxl's garbage collection until the
// process is gone.
func TestShutdownReleasesAWriterThatCannotDetach(t *testing.T) {
	m := newManager(t, nil)
	unguarded := &fakeWriter{detach: mxl.ErrNoFlowGuard}
	guarded := &fakeWriter{}
	m.held = map[string]*entry{
		"unguarded": {domain: t.TempDir(), id: flowID, writer: unguarded},
		"guarded":   {domain: t.TempDir(), id: flowID, writer: guarded},
	}

	shutDown(t, m)

	require.True(t, unguarded.closed, "a writer that cannot detach was not released")
	require.True(t, guarded.detached)
	require.False(t, guarded.closed, "a guarded writer was offered for deletion")
}

// What a shutdown leaves is a flow without a writer, which libmxl's
// garbage collection reclaims should no gateway hold it again.
func TestPlaceholderLeftOnShutdownIsCollectable(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, flow(origin))
	reconcileFlow(t, m)
	data := filepath.Join(flowDir(h), "data")

	shutDown(t, m)

	require.DirExists(t, flowDir(h), "the shutdown deleted the placeholder")
	n, err := flowlocks.Writers(data)
	require.NoError(t, err)
	require.Zero(t, n, "the shutdown left a writer attached to the placeholder")
	require.NoError(t, h.inst.GarbageCollect())
	require.NoDirExists(t, flowDir(h), "garbage collection kept the flow the shutdown left")
}

// mxlfsWaits reports whether the mxlfs at dir is mounted with a wait=
// for missing flows.
func mxlfsWaits(t *testing.T, dir string) bool {
	t.Helper()
	raw, err := os.ReadFile("/proc/self/mounts")
	require.NoError(t, err)
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) >= 4 && f[1] == filepath.Clean(dir) && f[2] == "mxlfs" {
			for _, o := range strings.Split(f[3], ",") {
				if strings.HasPrefix(o, "wait=") && o != "wait=0" {
					return true
				}
			}
		}
	}
	return false
}

// TestShutdownIsQuickOnMxlfs needs a writable mxlfs mounted with wait=
// at MXLFS_TEST_DIR; it is skipped otherwise. Deleting a discrete
// flow's placeholder there waited the mount's wait out in libmxl, one
// placeholder after the other, so a gateway holding a dozen outlasted
// its termination grace period and was killed.
func TestShutdownIsQuickOnMxlfs(t *testing.T) {
	dir := os.Getenv("MXLFS_TEST_DIR")
	if dir == "" {
		t.Skip("MXLFS_TEST_DIR not set")
	}
	if !mxlfsWaits(t, dir) {
		t.Skipf("%s is no mxlfs mounted with wait=", dir)
	}
	domain, err := os.MkdirTemp(dir, "placeholder-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(domain) })
	require.NoError(t, os.WriteFile(filepath.Join(domain, "options.json"),
		[]byte(`{"urn:x-mxl:option:history_duration/v1.0": 200000000}`), 0o644))
	inst, err := mxl.NewInstance(domain, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	h := &handles{dir: domain, inst: inst}

	ids := []string{flowID,
		"5fbec3b1-1b0f-417d-9059-8b94a47197e1",
		"5fbec3b1-1b0f-417d-9059-8b94a47197e2",
		"5fbec3b1-1b0f-417d-9059-8b94a47197e3"}
	var flows []*mxlv1alpha1.MxlFlow
	for _, id := range ids {
		flows = append(flows, flowNamed(id, origin))
	}
	m := newManager(t, h, flows...)
	for _, id := range ids {
		_, err := m.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: id}})
		require.NoError(t, err)
	}
	require.Len(t, m.Held(), len(ids))

	took := shutDown(t, m)

	require.Less(t, took, 2*time.Second, "giving %d placeholders up took %s", len(ids), took)
}
