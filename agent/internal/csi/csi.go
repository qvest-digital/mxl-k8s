// Package csi is a CSI node plugin that delivers an MXL domain to pods as
// an inline ephemeral volume.
//
// A volume is a bind mount of the node's domain directory, read-only for
// a pod that only reads: libmxl readers need no write access. The volume
// attributes name the domain (an MxlDomain name; empty for the primary
// domain) and the access. With podInfoOnMount the kubelet passes the pod
// a volume is for, which the plugin keeps per mount so what a pod opens
// in the domain can be traced back to it.
package csi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DriverName is the CSIDriver this plugin serves.
const DriverName = "mxl.qvest-digital.com"

// Volume attributes.
const (
	AttrDomain = "domain"
	AttrAccess = "access" // "read" or "write" (the default)
)

// Pod attributes the kubelet adds with podInfoOnMount.
const (
	attrPodName      = "csi.storage.k8s.io/pod.name"
	attrPodNamespace = "csi.storage.k8s.io/pod.namespace"
	attrPodUID       = "csi.storage.k8s.io/pod.uid"
)

// Pod is the pod a volume was published for.
type Pod struct {
	Namespace, Name, UID string
	Domain               string
	ReadOnly             bool
}

// Mounter is the mount syscalls the plugin needs; a seam for tests.
type Mounter interface {
	Bind(source, target string, readOnly bool) error
	Unmount(target string) error
	IsMountPoint(path string) (bool, error)
}

// Plugin implements the CSI Identity and Node services.
type Plugin struct {
	csipb.UnimplementedIdentityServer
	csipb.UnimplementedNodeServer

	NodeName string
	Version  string
	// DomainDir resolves a domain attribute to the directory to bind.
	DomainDir func(ctx context.Context, domain string) (string, error)
	Mounter   Mounter

	mu   sync.Mutex
	pods map[string]Pod // by target path
}

// Pods returns the pods volumes are currently published for, by target.
func (p *Plugin) Pods() map[string]Pod {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]Pod, len(p.pods))
	for k, v := range p.pods {
		out[k] = v
	}
	return out
}

func (p *Plugin) GetPluginInfo(context.Context, *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{Name: DriverName, VendorVersion: p.Version}, nil
}

func (p *Plugin) GetPluginCapabilities(context.Context, *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
	return &csipb.GetPluginCapabilitiesResponse{}, nil
}

func (p *Plugin) Probe(context.Context, *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	return &csipb.ProbeResponse{}, nil
}

func (p *Plugin) NodeGetCapabilities(context.Context, *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	return &csipb.NodeGetCapabilitiesResponse{}, nil
}

func (p *Plugin) NodeGetInfo(context.Context, *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	return &csipb.NodeGetInfoResponse{NodeId: p.NodeName}, nil
}

// NodePublishVolume bind-mounts the domain onto the pod's target path.
// Publishing a target that is already mounted succeeds, as CSI requires.
func (p *Plugin) NodePublishVolume(ctx context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	vc := req.GetVolumeContext()
	access := vc[AttrAccess]
	if access != "" && access != "read" && access != "write" {
		return nil, status.Errorf(codes.InvalidArgument, "access %q is neither read nor write", access)
	}
	readOnly := access == "read" || req.GetReadonly()
	source, err := p.DomainDir(ctx, vc[AttrDomain])
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "domain %q: %v", vc[AttrDomain], err)
	}
	if err := os.MkdirAll(target, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "create %s: %v", target, err)
	}
	mounted, err := p.Mounter.IsMountPoint(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect %s: %v", target, err)
	}
	if !mounted {
		if err := p.Mounter.Bind(source, target, readOnly); err != nil {
			return nil, status.Errorf(codes.Internal, "bind %s on %s: %v", source, target, err)
		}
	}
	p.mu.Lock()
	if p.pods == nil {
		p.pods = make(map[string]Pod)
	}
	p.pods[target] = Pod{
		Namespace: vc[attrPodNamespace], Name: vc[attrPodName], UID: vc[attrPodUID],
		Domain: vc[AttrDomain], ReadOnly: readOnly,
	}
	p.mu.Unlock()
	return &csipb.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the target. An unmounted or missing target
// succeeds, as CSI requires.
func (p *Plugin) NodeUnpublishVolume(ctx context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	mounted, err := p.Mounter.IsMountPoint(target)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "inspect %s: %v", target, err)
	}
	if mounted {
		if err := p.Mounter.Unmount(target); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount %s: %v", target, err)
		}
	}
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "remove %s: %v", target, err)
	}
	p.mu.Lock()
	delete(p.pods, target)
	p.mu.Unlock()
	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

// Serve runs the plugin on the unix socket endpoint until ctx ends.
func (p *Plugin) Serve(ctx context.Context, endpoint string) error {
	path := strings.TrimPrefix(endpoint, "unix://")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen %s: %w", path, err)
	}
	s := grpc.NewServer()
	csipb.RegisterIdentityServer(s, p)
	csipb.RegisterNodeServer(s, p)
	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()
	return s.Serve(l)
}

// SysMounter mounts with the mount syscalls.
type SysMounter struct{}

// Bind bind-mounts source on target, read-only if asked. A bind mount
// takes the read-only flag only on a remount.
func (SysMounter) Bind(source, target string, readOnly bool) error {
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return err
	}
	if readOnly {
		if err := unix.Mount("", target, "", unix.MS_REMOUNT|unix.MS_BIND|unix.MS_RDONLY, ""); err != nil {
			_ = unix.Unmount(target, unix.MNT_DETACH)
			return err
		}
	}
	return nil
}

// Unmount detaches target.
func (SysMounter) Unmount(target string) error {
	return unix.Unmount(target, unix.MNT_DETACH)
}

// IsMountPoint reports whether path is the root of a mount: its mount id
// differs from its parent's.
func (SysMounter) IsMountPoint(path string) (bool, error) {
	var st, parent unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &st); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return false, os.ErrNotExist
		}
		return false, err
	}
	if err := unix.Statx(unix.AT_FDCWD, path+"/..", 0, unix.STATX_MNT_ID, &parent); err != nil {
		return false, err
	}
	return st.Mnt_id != parent.Mnt_id, nil
}
