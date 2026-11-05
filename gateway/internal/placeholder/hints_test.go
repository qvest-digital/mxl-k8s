package placeholder

import (
	"context"
	"testing"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const audioID = "8c3e0b5f-6a1d-4f2e-b7c9-1d4a2e6f8b03"

const audioDef = `{"description":"placeholder hints test","format":"urn:x-nmos:format:audio",
"label":"placeholder hints test","id":"` + audioID + `",
"tags":{"urn:x-nmos:tag:grouphint/v1.0":["placeholder hints test:Audio"]},
"media_type":"audio/float32","sample_rate":{"numerator":48000},"channel_count":2,"bit_depth":32,"parents":[]}`

func audioFlow(sync, commit int64) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: audioID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: audioID, Definition: runtime.RawExtension{Raw: []byte(audioDef)}},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations:            []mxlv1alpha1.MxlFlowLocation{origin},
			MaxSyncBatchSizeHint: sync, MaxCommitBatchSizeHint: commit,
		},
	}
}

func reconcileAudio(t *testing.T, m *Manager) {
	t.Helper()
	_, err := m.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: audioID}})
	require.NoError(t, err)
}

func setHints(t *testing.T, m *Manager, sync, commit int64) {
	t.Helper()
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, m.Client.Get(context.Background(), types.NamespacedName{Name: audioID}, &f))
	f.Status.MaxSyncBatchSizeHint, f.Status.MaxCommitBatchSizeHint = sync, commit
	require.NoError(t, m.Client.Status().Update(context.Background(), &f))
}

// onDiskHints reads the hints the flow in h was created with.
func onDiskHints(t *testing.T, h *handles) Hints {
	t.Helper()
	r, err := h.inst.NewReader(audioID)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	cfg, err := r.Config()
	require.NoError(t, err)
	return Hints{Sync: cfg.Common.MaxSyncBatchSizeHint, Commit: cfg.Common.MaxCommitBatchSizeHint}
}

// A mirror filling the placeholder opens it as it is, so the placeholder
// creates the flow with the origin's hints.
func TestPlaceholderTakesTheOriginsHints(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, audioFlow(960, 480))

	reconcileAudio(t, m)

	require.Equal(t, []string{audioID}, m.Held())
	assert.Equal(t, Hints{Sync: 960, Commit: 480}, onDiskHints(t, h))
}

func TestPlaceholderOfOtherHintsIsReplaced(t *testing.T) {
	h := newDomain(t)
	m := newManager(t, h, audioFlow(0, 0))
	reconcileAudio(t, m)
	require.Equal(t, Hints{Sync: 480, Commit: 480}, onDiskHints(t, h), "no published hints leave libmxl's defaults")

	setHints(t, m, 960, 480)
	reconcileAudio(t, m)

	require.Equal(t, []string{audioID}, m.Held())
	assert.Equal(t, Hints{Sync: 960, Commit: 480}, onDiskHints(t, h), "the placeholder kept hints the origin no longer has")
}

// A leftover nothing holds is opened with the hints it was created
// with; one of other hints is created anew.
func TestLeftoverOfOtherHintsIsReplaced(t *testing.T) {
	h := newDomain(t)
	other, err := mxl.NewInstance(h.dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })
	w, _, err := other.NewWriter(audioDef)
	require.NoError(t, err)
	require.NoError(t, w.Detach())
	m := newManager(t, h, audioFlow(960, 480))

	reconcileAudio(t, m)

	require.Equal(t, []string{audioID}, m.Held())
	assert.Equal(t, Hints{Sync: 960, Commit: 480}, onDiskHints(t, h))
}

func TestHints(t *testing.T) {
	assert.Empty(t, Hints{}.Options())
	assert.JSONEq(t, `{"maxSyncBatchSizeHint":960,"maxCommitBatchSizeHint":480}`, Hints{Sync: 960, Commit: 480}.Options())
	assert.Equal(t, Hints{}, HintsOf(audioFlow(960, 0)), "one hint alone is not published")

	audio := mxl.FlowConfig{Common: mxl.CommonFlowConfig{Format: mxl.FormatAudio, MaxSyncBatchSizeHint: 480, MaxCommitBatchSizeHint: 480}}
	assert.True(t, Hints{Sync: 960, Commit: 480}.Differ(audio))
	assert.False(t, Hints{Sync: 480, Commit: 480}.Differ(audio))
	assert.False(t, Hints{}.Differ(audio), "unpublished hints differ from nothing")
	video := audio
	video.Common.Format = mxl.FormatVideo
	assert.False(t, Hints{Sync: 960, Commit: 480}.Differ(video), "a grain flow's hints size nothing on the fabric side")
}
