// Package domainpublisher materialises every MxlDomain selected for
// this node and reports each one's state on the node back into the
// domain's status.
package domainpublisher

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/mxl-k8s/agent/internal/domainfs"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// FilesystemStats reports the capacity of a domain directory. The
// agent passes the statfs implementation in so the publisher itself
// stays platform-independent.
type FilesystemStats func(path string) (capacityBytes, freeBytes int64, err error)

// Publisher reconciles MxlDomains onto this node.
type Publisher struct {
	Client   client.Client
	NodeName string

	// Root is the runtime root domain directories are created in.
	Root string

	// MirroredDir is the directory, below Root, whose flows this
	// node's agent tracks and mxl-k8s mirrors. A domain in any other
	// directory is materialised with its identity but not mirrored.
	MirroredDir string

	Stats         FilesystemStats
	FanotifyReady func() bool

	// Apply writes a domain onto the host. Nil uses domainfs.Apply.
	Apply func(root string, spec *mxlv1alpha1.MxlDomainSpec) (domainfs.Result, error)
}

// NewFromDomainPath builds a publisher from the agent's --domain-path:
// its parent is the runtime root and its base the mirrored directory.
func NewFromDomainPath(c client.Client, node, domainPath string, stats FilesystemStats,
	fanotifyReady func() bool) *Publisher {
	clean := filepath.Clean(domainPath)
	return &Publisher{
		Client:        c,
		NodeName:      node,
		Root:          filepath.Dir(clean),
		MirroredDir:   filepath.Base(clean),
		Stats:         stats,
		FanotifyReady: fanotifyReady,
	}
}

// Sync reconciles every MxlDomain once.
func (p *Publisher) Sync(ctx context.Context) error {
	var node corev1.Node
	if err := p.Client.Get(ctx, types.NamespacedName{Name: p.NodeName}, &node); err != nil {
		return fmt.Errorf("get node %s: %w", p.NodeName, err)
	}
	var list mxlv1alpha1.MxlDomainList
	if err := p.Client.List(ctx, &list); err != nil {
		return fmt.Errorf("list MxlDomains: %w", err)
	}

	// Two domains in one directory would each claim the other's flows
	// and write their own id over the other's. Neither is written.
	claims := map[string][]string{}
	for i := range list.Items {
		d := &list.Items[i]
		if d.Spec.ID == "" || !d.Spec.Selects(node.Labels) {
			continue
		}
		claims[d.Spec.Directory] = append(claims[d.Spec.Directory], d.Name)
	}

	var errs []error
	for i := range list.Items {
		d := &list.Items[i]
		if d.Spec.ID == "" {
			// A leftover of the per-node shape; the operator removes it.
			continue
		}
		if !d.Spec.Selects(node.Labels) {
			if d.Status.Node(p.NodeName) != nil {
				errs = append(errs, p.dropEntry(ctx, d.Name))
			}
			continue
		}
		entry := p.materialise(ctx, d, claims[d.Spec.Directory])
		errs = append(errs, p.putEntry(ctx, d.Name, entry))
	}
	return errors.Join(errs...)
}

func (p *Publisher) materialise(ctx context.Context, d *mxlv1alpha1.MxlDomain,
	claimants []string) mxlv1alpha1.MxlDomainNodeStatus {

	now := metav1.Now()
	entry := mxlv1alpha1.MxlDomainNodeStatus{
		NodeName: p.NodeName,
		Mirrored: d.Spec.Directory == p.MirroredDir,
		LastSeen: &now,
	}
	if len(claimants) > 1 {
		sort.Strings(claimants)
		entry.Message = fmt.Sprintf("directory %q is claimed by %s; none is written",
			d.Spec.Directory, strings.Join(claimants, ", "))
		return entry
	}

	apply := p.Apply
	if apply == nil {
		apply = domainfs.Apply
	}
	res, err := apply(p.Root, &d.Spec)
	if err != nil {
		entry.Message = err.Error()
		return entry
	}
	if res.CreatedDir || res.WroteDefinition || res.WroteOptions {
		log.FromContext(ctx).Info("materialised MxlDomain", "domain", d.Name,
			"directory", d.Spec.Directory, "createdDir", res.CreatedDir,
			"wroteDefinition", res.WroteDefinition, "wroteOptions", res.WroteOptions)
	}
	entry.Ready = true
	if entry.Mirrored && p.FanotifyReady != nil {
		entry.FanotifyReady = p.FanotifyReady()
	}
	if p.Stats != nil {
		if c, f, err := p.Stats(filepath.Join(p.Root, d.Spec.Directory)); err == nil {
			entry.CapacityBytes, entry.FreeBytes = c, f
		}
	}
	return entry
}

// putEntry replaces this node's status entry. Every node writes its own
// entry of one list, so a conflict is another node's write landing
// first and is retried against the fresh object.
func (p *Publisher) putEntry(ctx context.Context, name string, entry mxlv1alpha1.MxlDomainNodeStatus) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var d mxlv1alpha1.MxlDomain
		if err := p.Client.Get(ctx, types.NamespacedName{Name: name}, &d); err != nil {
			return client.IgnoreNotFound(err)
		}
		if cur := d.Status.Node(p.NodeName); cur != nil {
			*cur = entry
		} else {
			d.Status.Nodes = append(d.Status.Nodes, entry)
		}
		return p.Client.Status().Update(ctx, &d)
	})
}

func (p *Publisher) dropEntry(ctx context.Context, name string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var d mxlv1alpha1.MxlDomain
		if err := p.Client.Get(ctx, types.NamespacedName{Name: name}, &d); err != nil {
			return client.IgnoreNotFound(err)
		}
		kept := d.Status.Nodes[:0]
		for _, n := range d.Status.Nodes {
			if n.NodeName != p.NodeName {
				kept = append(kept, n)
			}
		}
		d.Status.Nodes = kept
		return p.Client.Status().Update(ctx, &d)
	})
}

// RunSyncLoop calls Sync on every tick until ctx is canceled.
func (p *Publisher) RunSyncLoop(ctx context.Context, period time.Duration) {
	l := log.FromContext(ctx).WithName("domainpublisher")
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		if err := p.Sync(ctx); err != nil {
			l.Error(err, "domain sync failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
