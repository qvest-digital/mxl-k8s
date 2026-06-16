package flowpublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// FlowDirSuffix is the directory-name suffix the MXL SDK uses for
// per-flow directories under a domain.
const FlowDirSuffix = ".mxl-flow"

// FlowDefName is the manifest filename inside a flow directory.
const FlowDefName = "flow_def.json"

// LeaseRenewer is the slice of the originlease.Manager surface the
// Publisher needs. Splitting the concrete type out keeps tests free
// of a Lease-aware kube client and lets a future Manager change its
// internals without churning every Publisher test fixture.
type LeaseRenewer interface {
	Renew(ctx context.Context, flowID string) error
	Release(ctx context.Context, flowID string) error
}

// Publisher creates and updates MxlFlow resources based on flow
// directories the agent observes under the domain.
type Publisher struct {
	Client     client.Client
	DomainPath string
	NodeName   string

	// Lease publishes a coordination.k8s.io Lease per Origin flow so
	// the operator and dispatcher can skip Origin locations whose
	// owner has stopped renewing. Nil disables Lease publication --
	// existing tests that don't care about Leases continue to work.
	Lease LeaseRenewer
}

// FlowIDFromDirName extracts the flow UUID from a `<uuid>.mxl-flow`
// entry name. Returns ("", false) when name doesn't match the shape.
func FlowIDFromDirName(name string) (string, bool) {
	if !strings.HasSuffix(name, FlowDirSuffix) {
		return "", false
	}
	id := strings.TrimSuffix(name, FlowDirSuffix)
	if id == "" {
		return "", false
	}
	return id, true
}

// PublishAppeared loads flow_def.json from a freshly observed flow
// directory and writes an MxlFlow describing it. status.locations
// for this node is set to Origin when the agent saw the directory
// before any MxlFlowMirror claims this node as target, otherwise to
// Ready (the gateway target reconciler created the local copy as a
// mirror). Without this distinction every mirror target side would
// also publish Origin, and resolveSourceNode picking the first
// Origin would direct downstream lookups at a mirror instead of the
// real producer.
func (p *Publisher) PublishAppeared(ctx context.Context, dirName string) error {
	l := log.FromContext(ctx).WithName("flowpublisher")
	flowID, ok := FlowIDFromDirName(dirName)
	if !ok {
		l.V(1).Info("ignoring non-flow entry", "name", dirName)
		return nil
	}

	defPath := filepath.Join(p.DomainPath, dirName, FlowDefName)
	raw, err := os.ReadFile(defPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", defPath, err)
	}
	if !json.Valid(raw) {
		return fmt.Errorf("%s: not valid JSON", defPath)
	}

	now := metav1.Now()
	desired := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec: mxlv1alpha1.MxlFlowSpec{
			ID:         flowID,
			Definition: runtime.RawExtension{Raw: raw},
		},
	}

	if err := p.Client.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create MxlFlow: %w", err)
		}
		// Already there -- leave spec alone (writers may have richer
		// metadata than us), just refresh our location entry.
		l.V(1).Info("MxlFlow already exists", "flowID", flowID)
	} else {
		l.Info("created MxlFlow", "flowID", flowID)
	}

	phase := mxlv1alpha1.MxlFlowLocationOrigin
	isMirror, err := p.isMirrorTarget(ctx, flowID)
	if err != nil {
		l.Error(err, "list MxlFlowMirrors; defaulting to Origin", "flowID", flowID)
	} else if isMirror {
		phase = mxlv1alpha1.MxlFlowLocationReady
	}
	if err := p.upsertLocation(ctx, flowID, phase, &now); err != nil {
		return err
	}
	// Only the producer side renews a Lease; a mirror target's local
	// copy is not authoritative and must not claim Origin liveness.
	if phase == mxlv1alpha1.MxlFlowLocationOrigin && p.Lease != nil {
		if err := p.Lease.Renew(ctx, flowID); err != nil {
			l.Error(err, "renew origin lease", "flowID", flowID)
		}
	}
	return nil
}

// isMirrorTarget reports whether any MxlFlowMirror in the cluster
// names this node as the target for the given flow. Used to tell a
// producer-side flow directory (the agent should mark Origin) apart
// from a gateway-target-created mirror directory (Ready).
func (p *Publisher) isMirrorTarget(ctx context.Context, flowID string) (bool, error) {
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := p.Client.List(ctx, &mirrors); err != nil {
		return false, err
	}
	for i := range mirrors.Items {
		m := &mirrors.Items[i]
		if m.Spec.FlowID == flowID && m.Spec.TargetNode == p.NodeName {
			return true, nil
		}
	}
	return false, nil
}

// PublishVanished updates the MxlFlow status to mark this node's
// location as Stale. The MxlFlow itself is left in place -- other
// nodes may still hold a mirror.
func (p *Publisher) PublishVanished(ctx context.Context, dirName string) error {
	flowID, ok := FlowIDFromDirName(dirName)
	if !ok {
		return nil
	}
	if err := p.upsertLocation(ctx, flowID, mxlv1alpha1.MxlFlowLocationStale, nil); err != nil {
		return err
	}
	if p.Lease != nil {
		if err := p.Lease.Release(ctx, flowID); err != nil {
			log.FromContext(ctx).WithName("flowpublisher").
				Error(err, "release origin lease", "flowID", flowID)
		}
	}
	return nil
}

func (p *Publisher) upsertLocation(ctx context.Context, flowID string, phase mxlv1alpha1.MxlFlowLocationPhase, observed *metav1.Time) error {
	var obj mxlv1alpha1.MxlFlow
	if err := p.Client.Get(ctx, types.NamespacedName{Name: flowID}, &obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get MxlFlow %s: %w", flowID, err)
	}

	found := false
	for i := range obj.Status.Locations {
		if obj.Status.Locations[i].NodeName == p.NodeName {
			obj.Status.Locations[i].Phase = phase
			obj.Status.Locations[i].LastObserved = observed
			found = true
			break
		}
	}
	if !found {
		obj.Status.Locations = append(obj.Status.Locations, mxlv1alpha1.MxlFlowLocation{
			NodeName:     p.NodeName,
			Phase:        phase,
			LastObserved: observed,
		})
	}

	if err := p.Client.Status().Update(ctx, &obj); err != nil {
		return fmt.Errorf("update MxlFlow %s status: %w", flowID, err)
	}
	return nil
}

// InitialSync walks the domain directory at startup and calls
// PublishAppeared for each flow directory it finds, then demotes any
// MxlFlow.status.locations entry that claims this node but whose
// on-disk directory is gone. Without the demote step a node that
// restarts after a writer pod cleaned up its flow leaves a permanent
// Origin entry behind, and downstream resolveSourceNode picks it as
// the source for a flow that no longer lives here.
func (p *Publisher) InitialSync(ctx context.Context) error {
	onDisk, err := p.localFlowIDs()
	if err != nil {
		return err
	}
	for id := range onDisk {
		if err := p.PublishAppeared(ctx, id+FlowDirSuffix); err != nil {
			log.FromContext(ctx).Error(err, "initial sync entry failed", "flowID", id)
		}
	}
	return p.demoteVanishedLocalOrigins(ctx, onDisk)
}

// ReleaseAll deletes the Lease for every flow currently on disk.
// Called from the agent's shutdown path so the operator's freshness
// check sees a NotFound (and trips OriginFresh=False via the Lease
// delete watch) the moment the pod is gracefully evicted instead of
// after the 30s renewal window elapses. Errors per flow are logged
// and the loop continues so one stuck Release does not orphan the
// rest. A nil Lease (test wiring without a manager) is a no-op.
func (p *Publisher) ReleaseAll(ctx context.Context) error {
	if p.Lease == nil {
		return nil
	}
	ids, err := p.localFlowIDs()
	if err != nil {
		return fmt.Errorf("list local flow dirs: %w", err)
	}
	l := log.FromContext(ctx).WithName("flowpublisher.release")
	for id := range ids {
		if err := p.Lease.Release(ctx, id); err != nil {
			l.Error(err, "release lease on shutdown", "flowID", id)
		}
	}
	return nil
}

// localFlowIDs returns the set of flow IDs whose directories live
// under p.DomainPath right now.
func (p *Publisher) localFlowIDs() (map[string]struct{}, error) {
	entries, err := os.ReadDir(p.DomainPath)
	if err != nil {
		return nil, fmt.Errorf("read domain dir: %w", err)
	}
	out := map[string]struct{}{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id, ok := FlowIDFromDirName(e.Name())
		if !ok {
			continue
		}
		out[id] = struct{}{}
	}
	return out, nil
}

// demoteVanishedLocalOrigins flips any non-Stale location this node
// owns whose on-disk flow directory is gone. Shared between
// InitialSync and RunLocalRescan so a missed fanotify delete and a
// crash-recovery cold start both converge on the same fix.
func (p *Publisher) demoteVanishedLocalOrigins(ctx context.Context, onDisk map[string]struct{}) error {
	var flows mxlv1alpha1.MxlFlowList
	if err := p.Client.List(ctx, &flows); err != nil {
		return fmt.Errorf("list MxlFlows: %w", err)
	}
	for i := range flows.Items {
		flow := &flows.Items[i]
		if _, present := onDisk[flow.Spec.ID]; present {
			continue
		}
		for _, loc := range flow.Status.Locations {
			if loc.NodeName != p.NodeName {
				continue
			}
			if loc.Phase == mxlv1alpha1.MxlFlowLocationStale {
				continue
			}
			if err := p.upsertLocation(ctx, flow.Spec.ID, mxlv1alpha1.MxlFlowLocationStale, nil); err != nil {
				log.FromContext(ctx).Error(err, "demote vanished local origin failed", "flowID", flow.Spec.ID)
			}
			if p.Lease != nil {
				if err := p.Lease.Release(ctx, flow.Spec.ID); err != nil {
					log.FromContext(ctx).Error(err, "release vanished origin lease", "flowID", flow.Spec.ID)
				}
			}
			break
		}
	}
	return nil
}

// RunRenewLoop renews the Lease for every Origin flow currently on
// disk on a fixed interval. Stops when ctx is done. A zero or
// negative interval falls back to 10s -- a third of the agent's
// default Lease duration so two missed ticks still leave the Lease
// fresh.
func (p *Publisher) RunRenewLoop(ctx context.Context, interval time.Duration) {
	if p.Lease == nil {
		return
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	l := log.FromContext(ctx).WithName("flowpublisher.renew")
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ids, err := p.localFlowIDs()
			if err != nil {
				l.Error(err, "list local flow dirs")
				continue
			}
			for id := range ids {
				if err := p.Lease.Renew(ctx, id); err != nil {
					l.Error(err, "renew lease", "flowID", id)
				}
			}
		}
	}
}

// RunLocalRescan periodically re-runs the demote-stale-Origin pass
// from InitialSync so a missed fanotify delete event does not leave
// a permanent Origin location pointing at this node.
func (p *Publisher) RunLocalRescan(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	l := log.FromContext(ctx).WithName("flowpublisher.rescan")
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ids, err := p.localFlowIDs()
			if err != nil {
				l.Error(err, "list local flow dirs")
				continue
			}
			if err := p.demoteVanishedLocalOrigins(ctx, ids); err != nil {
				l.Error(err, "demote pass failed")
			}
		}
	}
}
