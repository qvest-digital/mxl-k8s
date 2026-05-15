package capabilities

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Publisher creates and refreshes the MxlNodeCapabilities CR for this
// gateway. v0 advertises the providers the gateway was configured
// with; real per-provider probing comes with the first flow setup.
type Publisher struct {
	Client    client.Client
	NodeName  string
	Providers []fabrics.Provider
}

// Name is the metadata.name used for the gateway's
// MxlNodeCapabilities resource (one per node, keyed by node name).
func (p *Publisher) Name() string { return p.NodeName }

// EnsureExists creates the MxlNodeCapabilities resource if it isn't
// present. Status is left to be populated by Refresh.
func (p *Publisher) EnsureExists(ctx context.Context) error {
	l := log.FromContext(ctx)
	obj := &mxlv1alpha1.MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: p.Name()},
		Spec:       mxlv1alpha1.MxlNodeCapabilitiesSpec{NodeName: p.NodeName},
	}
	if err := p.Client.Create(ctx, obj); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create MxlNodeCapabilities: %w", err)
		}
		l.V(1).Info("MxlNodeCapabilities already exists", "name", p.Name())
		return nil
	}
	l.Info("created MxlNodeCapabilities", "name", p.Name())
	return nil
}

// Refresh updates MxlNodeCapabilities status with the configured
// provider list and lastSeen.
func (p *Publisher) Refresh(ctx context.Context) error {
	var obj mxlv1alpha1.MxlNodeCapabilities
	if err := p.Client.Get(ctx, types.NamespacedName{Name: p.Name()}, &obj); err != nil {
		return fmt.Errorf("get MxlNodeCapabilities: %w", err)
	}

	desired := make([]mxlv1alpha1.MxlFabricsProviderCapability, 0, len(p.Providers))
	for _, prov := range p.Providers {
		name := prov.String()
		if name == "" {
			continue
		}
		desired = append(desired, mxlv1alpha1.MxlFabricsProviderCapability{
			Name: mxlv1alpha1.MxlFabricsProvider(name),
		})
	}

	now := metav1.Now()
	obj.Status.Providers = desired
	obj.Status.LastSeen = &now

	if err := p.Client.Status().Update(ctx, &obj); err != nil {
		return fmt.Errorf("update MxlNodeCapabilities status: %w", err)
	}
	return nil
}

// RunRefreshLoop calls Refresh on every tick until ctx is canceled.
func (p *Publisher) RunRefreshLoop(ctx context.Context, period time.Duration) {
	l := log.FromContext(ctx).WithName("capabilities")
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
