package mxlfsmount

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	m, mounted, fell, _ := harnessOpts(mxlfs, mountErr)
	return m, mounted, fell
}

// harnessOpts is harness, and the options of the last mount.
func harnessOpts(mxlfs map[string]bool, mountErr error) (*Mounter, *[]string, *[]fallback, *string) {
	var mounted []string
	var fell []fallback
	var options string
	m := &Mounter{
		IsMxlfs: func(dir string) bool { return mxlfs[dir] },
		Mount: func(dir, opts string) error {
			mounted = append(mounted, dir)
			options = opts
			return mountErr
		},
		Fallback: func(dir string, err error) { fell = append(fell, fallback{dir, err}) },
		Log:      logr.Discard(),
	}
	return m, &mounted, &fell, &options
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

// huge= reaches the mount only when it is set: a module without the
// option refuses a mount that names it.
func TestEnsureMountsWithHugeOnlyWhenSet(t *testing.T) {
	m, _, _, options := harnessOpts(nil, nil)
	require.NoError(t, m.Ensure(t.TempDir()))
	assert.Equal(t, "mode=0777,wait=5000", *options)

	m, _, _, options = harnessOpts(nil, nil)
	m.Huge = "always"
	require.NoError(t, m.Ensure(t.TempDir()))
	assert.Equal(t, "mode=0777,wait=5000,huge=always", *options)
}

// A module that does not know huge= refuses a mount that names it: the
// domain is mounted without it, still an mxlfs, and the Node says why
// under its own reason rather than as a plain directory.
func TestEnsureMountsWithoutHugeWhenTheModuleRefusesIt(t *testing.T) {
	dir := t.TempDir()
	var mounts []string
	var refused []fallback
	m, _, fell := harness(nil, nil)
	m.Huge = "always"
	m.Mount = func(dir, opts string) error {
		mounts = append(mounts, opts)
		if strings.Contains(opts, "huge=") {
			return unix.EINVAL
		}
		return nil
	}
	m.HugeRefused = func(dir string, err error) { refused = append(refused, fallback{dir, err}) }

	require.NoError(t, m.Ensure(dir))
	assert.Equal(t, []string{"mode=0777,wait=5000,huge=always", "mode=0777,wait=5000"}, mounts)
	assert.Empty(t, *fell, "the domain is an mxlfs")
	require.Len(t, refused, 1)
	assert.Equal(t, dir, refused[0].dir)
	assert.ErrorIs(t, refused[0].err, unix.EINVAL)
	assert.True(t, m.Decided(dir))
}

// Only a refused huge= is retried: a mount that fails without it too
// leaves the domain plain, and so does any other error.
func TestEnsureRetriesWithoutHugeOnlyForEINVAL(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		mounts int
	}{
		{"EINVAL again", unix.EINVAL, 2},
		{"no module", unix.ENODEV, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, mounted, fell := harness(nil, tc.err)
			m.Huge = "always"
			m.HugeRefused = func(string, error) { t.Error("HugeRefused told of a mount that failed") }

			require.NoError(t, m.Ensure(t.TempDir()))
			assert.Len(t, *mounted, tc.mounts)
			require.Len(t, *fell, 1)
			assert.ErrorIs(t, (*fell)[0].err, tc.err)
		})
	}
}

// Without huge= set, EINVAL is not a refused option and is not retried.
func TestEnsureDoesNotRetryWithoutHuge(t *testing.T) {
	m, mounted, fell := harness(nil, unix.EINVAL)

	require.NoError(t, m.Ensure(t.TempDir()))
	assert.Len(t, *mounted, 1)
	assert.Len(t, *fell, 1)
}

// numa_node= reaches the mount only when it is set, after huge=: a
// module without it refuses a mount that names it.
func TestEnsureMountsWithNumaNodeOnlyWhenSet(t *testing.T) {
	m, _, _, options := harnessOpts(nil, nil)
	m.NumaNode = "1"
	require.NoError(t, m.Ensure(t.TempDir()))
	assert.Equal(t, "mode=0777,wait=5000,numa_node=1", *options)

	m, _, _, options = harnessOpts(nil, nil)
	m.Huge = "always"
	m.NumaNode = "0"
	require.NoError(t, m.Ensure(t.TempDir()))
	assert.Equal(t, "mode=0777,wait=5000,huge=always,numa_node=0", *options)
}

// A module that has huge= but not numa_node=, or that finds no memory on
// the node, refuses the mount: the domain is mounted with huge= alone,
// and the Node says why under numa_node='s own reason.
func TestEnsureMountsWithoutNumaNodeWhenTheModuleRefusesIt(t *testing.T) {
	dir := t.TempDir()
	var mounts []string
	var refused []fallback
	m, _, fell := harness(nil, nil)
	m.Huge = "always"
	m.NumaNode = "1"
	m.Mount = func(dir, opts string) error {
		mounts = append(mounts, opts)
		if strings.Contains(opts, "numa_node=") {
			return unix.EINVAL
		}
		return nil
	}
	m.HugeRefused = func(string, error) { t.Error("HugeRefused told of a mount with huge=") }
	m.NumaNodeRefused = func(dir string, err error) { refused = append(refused, fallback{dir, err}) }

	require.NoError(t, m.Ensure(dir))
	assert.Equal(t, []string{
		"mode=0777,wait=5000,huge=always,numa_node=1",
		"mode=0777,wait=5000,huge=always",
	}, mounts)
	assert.Empty(t, *fell, "the domain is an mxlfs")
	require.Len(t, refused, 1)
	assert.Equal(t, dir, refused[0].dir)
	assert.ErrorIs(t, refused[0].err, unix.EINVAL)
}

// A module that predates both options refuses each in turn, the newest
// first, and the Node hears of both.
func TestEnsureMountsWithoutEitherOptionWhenTheModuleRefusesBoth(t *testing.T) {
	var mounts []string
	var told []string
	m, _, fell := harness(nil, nil)
	m.Huge = "always"
	m.NumaNode = "1"
	m.Mount = func(dir, opts string) error {
		mounts = append(mounts, opts)
		if strings.Contains(opts, "huge=") || strings.Contains(opts, "numa_node=") {
			return unix.EINVAL
		}
		return nil
	}
	m.HugeRefused = func(string, error) { told = append(told, "huge") }
	m.NumaNodeRefused = func(string, error) { told = append(told, "numa_node") }

	require.NoError(t, m.Ensure(t.TempDir()))
	assert.Equal(t, []string{
		"mode=0777,wait=5000,huge=always,numa_node=1",
		"mode=0777,wait=5000,huge=always",
		"mode=0777,wait=5000",
	}, mounts)
	assert.Empty(t, *fell)
	assert.Equal(t, []string{"numa_node", "huge"}, told)
}

// A mount that fails without numa_node= too leaves the domain plain, and
// no option is told refused.
func TestEnsureTellsNoRefusalWhenEveryMountFails(t *testing.T) {
	m, mounted, fell := harness(nil, unix.EINVAL)
	m.NumaNode = "1"
	m.NumaNodeRefused = func(string, error) { t.Error("NumaNodeRefused told of a mount that failed") }

	require.NoError(t, m.Ensure(t.TempDir()))
	assert.Len(t, *mounted, 2)
	require.Len(t, *fell, 1)
	assert.ErrorIs(t, (*fell)[0].err, unix.EINVAL)
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

func TestHugeRefusedEventHasItsOwnReason(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	c := fake.NewClientBuilder().WithScheme(s).Build()

	HugeRefusedEvent(c, "n1", logr.Discard())("/run/mxl/domain", unix.EINVAL)

	var list corev1.EventList
	require.NoError(t, c.List(context.Background(), &list, client.InNamespace("default")))
	require.Len(t, list.Items, 1)
	ev := list.Items[0]
	assert.Equal(t, corev1.EventTypeWarning, ev.Type)
	assert.Equal(t, ReasonHugeRefused, ev.Reason)
	assert.Equal(t, "n1", ev.InvolvedObject.Name)
	assert.Contains(t, ev.Message, "/run/mxl/domain is mounted without huge=: invalid argument")
}

func TestNumaNodeRefusedEventHasItsOwnReason(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	c := fake.NewClientBuilder().WithScheme(s).Build()

	NumaNodeRefusedEvent(c, "n1", logr.Discard())("/run/mxl/domain", unix.EINVAL)

	var list corev1.EventList
	require.NoError(t, c.List(context.Background(), &list, client.InNamespace("default")))
	require.Len(t, list.Items, 1)
	ev := list.Items[0]
	assert.Equal(t, corev1.EventTypeWarning, ev.Type)
	assert.Equal(t, ReasonNumaNodeRefused, ev.Reason)
	assert.Equal(t, "n1", ev.InvolvedObject.Name)
	assert.Contains(t, ev.Message, "/run/mxl/domain is mounted without numa_node=: invalid argument")
}

// rdmaClass builds a sysfs RDMA class in a temporary directory: each
// device with the numa_node its device reports, or none without one.
func rdmaClass(t *testing.T, devices map[string]string) string {
	t.Helper()
	class := t.TempDir()
	for name, node := range devices {
		dir := filepath.Join(class, name)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		if node == "" {
			continue
		}
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "device"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "device", "numa_node"), []byte(node+"\n"), 0o644))
	}
	return class
}

func TestRDMANumaNode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		devices map[string]string
		want    string
		wantErr string
	}{
		{"one device", map[string]string{"mlx5_0": "1"}, "1", ""},
		{"devices on one node", map[string]string{"mlx5_0": "0", "mlx5_1": "0"}, "0", ""},
		{"a device without a PCI device is passed over", map[string]string{"mlx5_0": "1", "rxe0": ""}, "1", ""},
		{"devices on two nodes", map[string]string{"mlx5_0": "0", "mlx5_1": "1"}, "", "nodes"},
		{"a device on no node", map[string]string{"mlx5_0": "-1"}, "", "reports NUMA node -1"},
		{"no device with a node", map[string]string{"rxe0": ""}, "", "no RDMA device"},
		{"no device", map[string]string{}, "", "no RDMA device"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RDMANumaNode(rdmaClass(t, tc.devices))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A node without RDMA has no class directory at all.
func TestRDMANumaNodeWithoutTheClass(t *testing.T) {
	_, err := RDMANumaNode(filepath.Join(t.TempDir(), "infiniband"))
	require.ErrorContains(t, err, "no RDMA device")
}

func TestNumaNode(t *testing.T) {
	class := rdmaClass(t, map[string]string{"mlx5_0": "1"})
	for _, tc := range []struct {
		value, want, wantErr string
	}{
		{"", "", ""},
		{"none", "", ""},
		{"0", "0", ""},
		{"01", "1", ""}, // the kernel would read 01 as octal
		{"auto", "1", ""},
		{"near", "", "NUMA node"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got, err := NumaNode(tc.value, class)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
