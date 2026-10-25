package mirror

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const geometryFlowID = "5fbec3b1-1b0f-417d-9059-8b94a47197ed"

func videoDefAt(width string) string {
	return `{"id":"` + geometryFlowID + `","format":"urn:x-nmos:format:video","media_type":"video/v210",` +
		`"grain_rate":{"numerator":50,"denominator":1},"frame_width":` + width + `,"frame_height":180}`
}

// copyAt lays down a flow directory built from def, as a placeholder or
// an earlier target leaves it.
func copyAt(t *testing.T, def string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), geometryFlowID+".mxl-flow")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flow_def.json"), []byte(def), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data"), nil, 0o644))
	return dir
}

// libmxl opens a flow that exists as it is, whatever it was asked to
// create, so a target must not open a directory of another layout.
func TestClearOtherGeometry(t *testing.T) {
	t.Run("same geometry is opened as it is", func(t *testing.T) {
		dir := copyAt(t, videoDefAt("320"))
		retired := false
		require.NoError(t, clearOtherGeometry(dir, strings.Replace(videoDefAt("320"), `"id"`, `"label":"x","id"`, 1),
			func(string, string) { retired = true }))
		assert.DirExists(t, dir)
		assert.False(t, retired)
	})
	t.Run("absent is nothing to clear", func(t *testing.T) {
		require.NoError(t, clearOtherGeometry(filepath.Join(t.TempDir(), geometryFlowID+".mxl-flow"), videoDefAt("320"), nil))
	})
	t.Run("another geometry nothing holds is removed", func(t *testing.T) {
		dir := copyAt(t, videoDefAt("320"))
		var retired []string
		require.NoError(t, clearOtherGeometry(dir, videoDefAt("640"), func(domain, id string) {
			retired = append(retired, domain+"|"+id)
		}))
		assert.NoDirExists(t, dir)
		assert.Equal(t, []string{filepath.Dir(dir) + "|" + geometryFlowID}, retired,
			"a placeholder held there was not released first")
	})
	t.Run("another geometry a writer holds is left until released", func(t *testing.T) {
		dir := copyAt(t, videoDefAt("320"))
		cmd := exec.Command("flock", "--shared", filepath.Join(dir, "data"), "sleep", "60")
		if err := cmd.Start(); err != nil {
			t.Skipf("flock(1) not available: %v", err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		time.Sleep(200 * time.Millisecond)

		require.ErrorIs(t, clearOtherGeometry(dir, videoDefAt("640"), nil), errOtherGeometry)
		assert.DirExists(t, dir)
	})
}

// A live target registered a ring of the geometry its flow had. When
// the producer comes back in another one, the target is closed and
// opened again from the new definition; one whose geometry still
// matches stays as it is.
func TestReconcile_ReplacesALiveTargetOfAnotherGeometry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		live     string
		replaced bool
	}{
		{"same geometry", videoDefAt("640"), false},
		{"another geometry", videoDefAt("320"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newSourceTestScheme(t)
			key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
			fresh := metav1.NewTime(time.Now())
			mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", geometryFlowID,
				mxlv1alpha1.MxlFlowMirrorStatus{Phase: mxlv1alpha1.MxlFlowMirrorReady, TargetInfo: "info-1", LastGrainAt: &fresh})
			mirror.Spec.Provider = mxlv1alpha1.ProviderTCP
			flow := &mxlv1alpha1.MxlFlow{
				ObjectMeta: metav1.ObjectMeta{Name: geometryFlowID},
				Spec:       mxlv1alpha1.MxlFlowSpec{ID: geometryFlowID, Definition: runtime.RawExtension{Raw: []byte(videoDefAt("640"))}},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
				WithObjects(mirror, flow).Build()
			var opened []string
			r := &TargetReconciler{
				Client: c, Scheme: scheme, NodeName: "node-a", DegradedAfter: time.Hour,
				targets:  map[types.NamespacedName]*targetEntry{key: {def: tc.live}},
				attempts: attemptTable[targetOpenInputs]{},
				Retire:   func(string, string) {},
				openTargetFn: func(_ types.NamespacedName, def string, _ fabrics.Provider) (*targetEntry, error) {
					opened = append(opened, def)
					return nil, errOtherGeometry
				},
			}

			_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
			require.NoError(t, err)

			if tc.replaced {
				assert.NotContains(t, r.targets, key, "the target of the old geometry is still live")
				assert.Equal(t, []string{videoDefAt("640")}, opened, "the target was not opened from the new definition")
			} else {
				assert.Contains(t, r.targets, key)
				assert.Empty(t, opened)
			}
		})
	}
}
