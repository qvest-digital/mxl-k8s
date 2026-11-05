package mirror

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/placeholder"
)

const sharedFlowAudioID = "8c3e0b5f-6a1d-4f2e-b7c9-1d4a2e6f8b03"

// producerHints are hints above libmxl's 10 ms default at 48 kHz, the
// case where a copy with the defaults has entries too small for the
// source's batches.
var producerHints = placeholder.Hints{Sync: 960, Commit: 480}

func hintsDomain(t *testing.T) (string, *mxl.Instance) {
	t.Helper()
	dir := t.TempDir()
	inst, err := mxl.NewInstance(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	return dir, inst
}

// leftover leaves def in dir created with options, as a placeholder or
// an earlier target of another gateway leaves it, and returns the
// writer when hold is set.
func leftover(t *testing.T, dir, def, options string, hold bool) *mxl.Writer {
	t.Helper()
	inst, err := mxl.NewInstance(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	w, created, err := inst.NewWriterOpts(def, options)
	require.NoError(t, err)
	require.True(t, created)
	if hold {
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	require.NoError(t, w.Detach())
	return nil
}

func syncCommit(w *mxl.Writer) placeholder.Hints {
	c := w.Config().Common
	return placeholder.Hints{Sync: c.MaxSyncBatchSizeHint, Commit: c.MaxCommitBatchSizeHint}
}

// libmxl keeps the hints of a flow's creator, and libmxl-fabrics sizes
// an audio target's bounce-buffer entries from them, so the target's
// copy has to be created with the origin's.
func TestOpenWriter_CreatesTheCopyWithTheOriginsHints(t *testing.T) {
	dir, inst := hintsDomain(t)
	retired := false
	w, err := openWriter(inst, dir, sharedFlowAudioDef, producerHints, func(string, string) { retired = true })
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	assert.Equal(t, producerHints, syncCommit(w))
	assert.False(t, retired, "a fresh copy was not created with the hints at once")
}

func TestOpenWriter_NoHintsKeepsLibmxlDefaults(t *testing.T) {
	dir, inst := hintsDomain(t)
	w, err := openWriter(inst, dir, sharedFlowAudioDef, placeholder.Hints{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	assert.Equal(t, placeholder.Hints{Sync: 480, Commit: 480}, syncCommit(w))
}

// An audio flow already there with the defaults would be opened as it
// is; nothing holds it, so it is created anew, after a placeholder held
// there is retired.
func TestOpenWriter_RecreatesAnAudioFlowWithOtherHints(t *testing.T) {
	dir, inst := hintsDomain(t)
	leftover(t, dir, sharedFlowAudioDef, "", false)

	var retired []string
	w, err := openWriter(inst, dir, sharedFlowAudioDef, producerHints, func(domain, id string) {
		retired = append(retired, domain+"|"+id)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	assert.Equal(t, producerHints, syncCommit(w))
	assert.Equal(t, []string{dir + "|" + sharedFlowAudioID}, retired)
}

func TestOpenWriter_LeavesAFlowAnotherWriterHolds(t *testing.T) {
	dir, inst := hintsDomain(t)
	leftover(t, dir, sharedFlowAudioDef, "", true)

	_, err := openWriter(inst, dir, sharedFlowAudioDef, producerHints, nil)
	require.ErrorIs(t, err, errOtherHints)
	assert.DirExists(t, filepath.Join(dir, sharedFlowAudioID+".mxl-flow"))
}

// A grain flow's hint sizes nothing on the fabric side, so a copy with
// other hints is opened as it is.
func TestOpenWriter_OpensAVideoFlowWithOtherHintsAsItIs(t *testing.T) {
	dir, inst := hintsDomain(t)
	leftover(t, dir, sharedFlowVideoDef, `{"maxSyncBatchSizeHint":540,"maxCommitBatchSizeHint":540}`, false)

	w, err := openWriter(inst, dir, sharedFlowVideoDef, placeholder.Hints{Sync: 1080, Commit: 1080}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	assert.Equal(t, placeholder.Hints{Sync: 540, Commit: 540}, syncCommit(w))
}

// A live audio target whose origin came back with other hints has
// entries sized for the old ones; it is closed and opened again, on
// any domain. One whose MxlFlow publishes no hints stays.
func TestReconcile_ReplacesALiveAudioTargetWithOtherHints(t *testing.T) {
	live := mxl.FlowConfig{Common: mxl.CommonFlowConfig{Format: mxl.FormatAudio, MaxSyncBatchSizeHint: 480, MaxCommitBatchSizeHint: 480}}
	for _, tc := range []struct {
		name      string
		published mxlv1alpha1.MxlFlowStatus
		replaced  bool
	}{
		{"same hints", mxlv1alpha1.MxlFlowStatus{MaxSyncBatchSizeHint: 480, MaxCommitBatchSizeHint: 480}, false},
		{"no hints published", mxlv1alpha1.MxlFlowStatus{}, false},
		{"other hints", mxlv1alpha1.MxlFlowStatus{MaxSyncBatchSizeHint: 960, MaxCommitBatchSizeHint: 480}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newSourceTestScheme(t)
			key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
			fresh := metav1.NewTime(time.Now())
			mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", sharedFlowAudioID,
				mxlv1alpha1.MxlFlowMirrorStatus{Phase: mxlv1alpha1.MxlFlowMirrorReady, TargetInfo: "info-1", LastGrainAt: &fresh})
			mirror.Spec.Provider = mxlv1alpha1.ProviderTCP
			flow := &mxlv1alpha1.MxlFlow{
				ObjectMeta: metav1.ObjectMeta{Name: sharedFlowAudioID},
				Spec:       mxlv1alpha1.MxlFlowSpec{ID: sharedFlowAudioID, Definition: runtime.RawExtension{Raw: []byte(sharedFlowAudioDef)}},
				Status:     tc.published,
			}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
				WithObjects(mirror, flow).Build()
			opened := 0
			r := &TargetReconciler{
				Client: c, Scheme: scheme, NodeName: "node-a", DegradedAfter: time.Hour,
				targets: map[types.NamespacedName]*targetEntry{
					key: {def: sharedFlowAudioDef, cfg: live, provider: fabrics.ProviderTCP},
				},
				attempts: attemptTable[targetOpenInputs]{},
				openTargetFn: func(types.NamespacedName, string, fabrics.Provider) (*targetEntry, error) {
					opened++
					return nil, errOtherHints
				},
			}

			_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
			require.NoError(t, err)

			if tc.replaced {
				assert.NotContains(t, r.targets, key, "the target sized for the old hints is still live")
				assert.Equal(t, 1, opened)
			} else {
				assert.Contains(t, r.targets, key)
				assert.Zero(t, opened)
			}
		})
	}
}
