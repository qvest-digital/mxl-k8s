package csi

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type bind struct {
	source   string
	readOnly bool
}

type fakeMounter struct {
	mounts map[string]bind
}

func (f *fakeMounter) Bind(source, target string, readOnly bool) error {
	f.mounts[target] = bind{source, readOnly}
	return nil
}

func (f *fakeMounter) Unmount(target string) error {
	delete(f.mounts, target)
	return nil
}

func (f *fakeMounter) IsMountPoint(path string) (bool, error) {
	_, ok := f.mounts[path]
	return ok, nil
}

func newPlugin(t *testing.T) (*Plugin, *fakeMounter) {
	m := &fakeMounter{mounts: map[string]bind{}}
	return &Plugin{
		NodeName: "node-a",
		Mounter:  m,
		DomainDir: func(_ context.Context, domain string) (string, error) {
			switch domain {
			case "":
				return "/run/mxl/domain", nil
			case "studio":
				return "/run/mxl/domains/studio", nil
			}
			return "", errors.New("no such MxlDomain")
		},
	}, m
}

func publish(p *Plugin, target string, vc map[string]string, readonly bool) error {
	_, err := p.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId: "csi-1", TargetPath: target, VolumeContext: vc, Readonly: readonly,
	})
	return err
}

func TestPublishBindsTheDomainReadOnlyForReaders(t *testing.T) {
	p, m := newPlugin(t)
	target := filepath.Join(t.TempDir(), "mount")

	require.NoError(t, publish(p, target, map[string]string{
		AttrAccess: "read", attrPodName: "reader", attrPodNamespace: "prod", attrPodUID: "u1",
	}, false))

	require.Equal(t, bind{"/run/mxl/domain", true}, m.mounts[target])
	require.Equal(t, Pod{Namespace: "prod", Name: "reader", UID: "u1", ReadOnly: true}, p.Pods()[target])
}

func TestPublishBindsANamedDomainWritable(t *testing.T) {
	p, m := newPlugin(t)
	target := filepath.Join(t.TempDir(), "mount")

	require.NoError(t, publish(p, target, map[string]string{AttrDomain: "studio", AttrAccess: "write"}, false))

	require.Equal(t, bind{"/run/mxl/domains/studio", false}, m.mounts[target])
}

// A volume that names no access gets the least: a pod writes into a
// shared domain only when it says it does.
func TestPublishBindsReadOnlyWhenNoAccessIsNamed(t *testing.T) {
	p, m := newPlugin(t)
	target := filepath.Join(t.TempDir(), "mount")

	require.NoError(t, publish(p, target, map[string]string{AttrDomain: "studio"}, false))

	require.Equal(t, bind{"/run/mxl/domains/studio", true}, m.mounts[target])
}

func TestPublishIsIdempotent(t *testing.T) {
	p, m := newPlugin(t)
	target := filepath.Join(t.TempDir(), "mount")
	require.NoError(t, publish(p, target, nil, false))
	m.mounts[target] = bind{"kept", false}

	require.NoError(t, publish(p, target, nil, false))

	require.Equal(t, bind{"kept", false}, m.mounts[target], "a second publish mounted over the first")
}

func TestPublishRefusesBadInput(t *testing.T) {
	p, _ := newPlugin(t)
	target := filepath.Join(t.TempDir(), "mount")

	err := publish(p, target, map[string]string{AttrAccess: "exec"}, false)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	err = publish(p, target, map[string]string{AttrDomain: "nowhere"}, false)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestUnpublishUnmountsAndForgetsThePod(t *testing.T) {
	p, m := newPlugin(t)
	target := filepath.Join(t.TempDir(), "mount")
	require.NoError(t, publish(p, target, map[string]string{attrPodName: "reader"}, false))

	_, err := p.NodeUnpublishVolume(context.Background(), &csipb.NodeUnpublishVolumeRequest{VolumeId: "csi-1", TargetPath: target})
	require.NoError(t, err)
	require.Empty(t, m.mounts)
	require.NoDirExists(t, target)
	require.Empty(t, p.Pods())

	_, err = p.NodeUnpublishVolume(context.Background(), &csipb.NodeUnpublishVolumeRequest{VolumeId: "csi-1", TargetPath: target})
	require.NoError(t, err, "unpublishing twice must succeed")
}

func TestSysMounterIsMountPoint(t *testing.T) {
	dir := t.TempDir()
	mounted, err := SysMounter{}.IsMountPoint(dir)
	require.NoError(t, err)
	require.False(t, mounted)

	mounted, err = SysMounter{}.IsMountPoint("/proc")
	require.NoError(t, err)
	require.True(t, mounted)
}

// A pod scheduled before the agent mounted the domain must not get the
// directory beneath the mount: it would never see a flow. The kubelet
// retries the publish until the domain is ready.
func TestPublishWaitsForTheDomainMount(t *testing.T) {
	p, m := newPlugin(t)
	ready := false
	p.Ready = func(dir string) bool { return ready && dir == "/run/mxl/domain" }
	target := filepath.Join(t.TempDir(), "mount")

	err := publish(p, target, nil, false)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Empty(t, m.mounts)

	ready = true
	require.NoError(t, publish(p, target, nil, false))
	require.Equal(t, bind{"/run/mxl/domain", true}, m.mounts[target])
}
