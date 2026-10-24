package placeholder

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
	f := flow()
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
	m := newManager(t, h, flow())
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
	m := newManager(t, h, flow())
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
	m := newManager(t, h, flow())

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
