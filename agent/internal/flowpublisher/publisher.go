package flowpublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

// Publisher creates and updates MxlFlow resources based on flow
// directories the agent observes under the domain.
type Publisher struct {
	Client     client.Client
	DomainPath string
	NodeName   string
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
	return p.upsertLocation(ctx, flowID, phase, &now)
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
	return p.upsertLocation(ctx, flowID, mxlv1alpha1.MxlFlowLocationStale, nil)
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
	entries, err := os.ReadDir(p.DomainPath)
	if err != nil {
		return fmt.Errorf("read domain dir: %w", err)
	}
	onDisk := map[string]struct{}{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id, ok := FlowIDFromDirName(e.Name())
		if !ok {
			continue
		}
		onDisk[id] = struct{}{}
		if err := p.PublishAppeared(ctx, e.Name()); err != nil {
			log.FromContext(ctx).Error(err, "initial sync entry failed", "name", e.Name())
		}
	}

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
				log.FromContext(ctx).Error(err, "initial sync demote failed", "flowID", flow.Spec.ID)
			}
			break
		}
	}
	return nil
}
