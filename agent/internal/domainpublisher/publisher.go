package domainpublisher

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// FilesystemStats reports the capacity of the domain mount. The agent
// passes the statfs implementation in so the publisher itself stays
// platform-independent.
type FilesystemStats func(path string) (capacityBytes, freeBytes int64, err error)

// Publisher creates and refreshes the MxlDomain CR for this agent.
type Publisher struct {
	Client        client.Client
	NodeName      string
	HostPath      string
	Stats         FilesystemStats
	FanotifyReady func() bool
}

// Name is the metadata.name used for this agent's MxlDomain. One
// MxlDomain per (node, domain path) — we use the node name for v0
// (assumes one domain per node).
func (p *Publisher) Name() string {
	return p.NodeName
}

// EnsureExists creates the MxlDomain if it isn't present yet. Status
// is left to be populated by Refresh.
func (p *Publisher) EnsureExists(ctx context.Context) error {
	l := log.FromContext(ctx)
	obj := &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: p.Name()},
		Spec: mxlv1alpha1.MxlDomainSpec{
			NodeName: p.NodeName,
			HostPath: p.HostPath,
		},
	}
	if err := p.Client.Create(ctx, obj); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create MxlDomain: %w", err)
		}
		l.V(1).Info("MxlDomain already exists", "name", p.Name())
		return nil
	}
	l.Info("created MxlDomain", "name", p.Name())
	return nil
}

// Refresh updates MxlDomain status from current statfs output and the
// supplied fanotify readiness signal.
func (p *Publisher) Refresh(ctx context.Context) error {
	var obj mxlv1alpha1.MxlDomain
	if err := p.Client.Get(ctx, types.NamespacedName{Name: p.Name()}, &obj); err != nil {
		return fmt.Errorf("get MxlDomain: %w", err)
	}

	capacity, free, err := p.Stats(p.HostPath)
	if err != nil {
		return fmt.Errorf("statfs %q: %w", p.HostPath, err)
	}

	now := metav1.Now()
	obj.Status.CapacityBytes = capacity
	obj.Status.FreeBytes = free
	obj.Status.FanotifyReady = p.FanotifyReady()
	obj.Status.LastSeen = &now

	if err := p.Client.Status().Update(ctx, &obj); err != nil {
		return fmt.Errorf("update MxlDomain status: %w", err)
	}
	return nil
}

// RunRefreshLoop calls Refresh on every tick until ctx is canceled.
func (p *Publisher) RunRefreshLoop(ctx context.Context, period time.Duration) {
	l := log.FromContext(ctx).WithName("domainpublisher")
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		if err := p.Refresh(ctx); err != nil {
			l.Error(err, "refresh failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
