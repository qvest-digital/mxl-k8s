package mxlfsmount

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fallback struct {
	dir string
	err error
}

// harness is a Mounter whose mount syscall and statfs are recorded and
// answered from mxlfs.
func harness(mxlfs map[string]bool, mountErr error) (*Mounter, *[]string, *[]fallback) {
	var mounted []string
	var fell []fallback
	m := &Mounter{
		IsMxlfs: func(dir string) bool { return mxlfs[dir] },
		Mount: func(dir string) error {
			mounted = append(mounted, dir)
			return mountErr
		},
		Fallback: func(dir string, err error) { fell = append(fell, fallback{dir, err}) },
		Log:      logr.Discard(),
	}
	return m, &mounted, &fell
}

func TestEnsureLeavesAnMxlfsAlone(t *testing.T) {
	dir := t.TempDir()
	m, mounted, fell := harness(map[string]bool{dir: true}, nil)

	require.NoError(t, m.Ensure(dir))
	assert.Empty(t, *mounted)
	assert.Empty(t, *fell)
	assert.True(t, m.Decided(dir))
}

func TestEnsureMountsAPlainDirectoryOnce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "domains", "id")
	m, mounted, fell := harness(nil, nil)

	require.NoError(t, m.Ensure(dir))
	require.NoError(t, m.Ensure(dir+"/"))
	assert.Equal(t, []string{dir}, *mounted, "created, then mounted once")
	assert.Empty(t, *fell)
	assert.DirExists(t, dir)
	assert.True(t, m.Decided(dir))
}

// A node without the module keeps working on a plain directory, and the
// reason reaches the Node rather than only the agent's log.
func TestEnsureFallsBackWithoutTheModule(t *testing.T) {
	dir := t.TempDir()
	m, mounted, fell := harness(nil, unix.ENODEV)

	require.NoError(t, m.Ensure(dir))
	require.NoError(t, m.Ensure(dir))
	assert.Len(t, *mounted, 1, "a failed mount is not retried")
	require.Len(t, *fell, 1)
	assert.Equal(t, dir, (*fell)[0].dir)
	assert.ErrorIs(t, (*fell)[0].err, unix.ENODEV)
	assert.True(t, m.Decided(dir), "a plain directory is a decision too")
}

func TestEnsureDoesNotHideFlows(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "5fbec3b1-1b0f-417d-9059-8b94a47197ed.mxl-flow"), 0o755))
	m, mounted, fell := harness(nil, nil)

	require.NoError(t, m.Ensure(dir))
	assert.Empty(t, *mounted)
	require.Len(t, *fell, 1)
	assert.Contains(t, (*fell)[0].err.Error(), "holds 1 flows")
}

func TestDecidedIsFalseBeforeEnsure(t *testing.T) {
	m, _, _ := harness(nil, nil)
	assert.False(t, m.Decided(t.TempDir()))
}

func TestReadyFileRoundTrip(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, ClearReady(root), "nothing to clear")
	require.NoError(t, MarkReady(root))
	assert.FileExists(t, filepath.Join(root, ReadyFile))
	require.NoError(t, ClearReady(root))
	assert.NoFileExists(t, filepath.Join(root, ReadyFile))
}

func TestNodeEventWarnsOnTheNode(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	c := fake.NewClientBuilder().WithScheme(s).Build()

	NodeEvent(c, "n1", logr.Discard())("/run/mxl/domain", errors.New("no such device"))

	var list corev1.EventList
	require.NoError(t, c.List(context.Background(), &list, client.InNamespace("default")))
	require.Len(t, list.Items, 1)
	ev := list.Items[0]
	assert.Equal(t, corev1.EventTypeWarning, ev.Type)
	assert.Equal(t, ReasonUnavailable, ev.Reason)
	assert.Equal(t, "Node", ev.InvolvedObject.Kind)
	assert.Equal(t, "n1", ev.InvolvedObject.Name)
	assert.Contains(t, ev.Message, "/run/mxl/domain stays a plain directory: no such device")
}
