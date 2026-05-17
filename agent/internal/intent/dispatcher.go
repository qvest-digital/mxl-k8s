// Package intent handles on-demand MxlFlowMirror materialization.
//
// A consumer pod that tries to open a flow_def.json for a flow not
// yet present on this node hits ENOENT. The libmxl-intent.so shim
// inside the pod intercepts that ENOENT and asks this dispatcher
// (via the agent's UDS) to materialize the flow. Materialize walks
// the same handshake the operator uses for declarative
// MxlReceivers — look up the source node, ensure the
// MxlFlowMirror, wait for the gateway to mark it Ready — and then
// returns success so the shim can retry the open.
package intent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	defaultMaterializeTimeout = 5 * time.Second
	defaultPollInterval       = 50 * time.Millisecond
)

// Dispatcher resolves a libmxl-intent.so request into an
// MxlFlowMirror reconciliation that completes (Ready) or fails.
type Dispatcher struct {
	Client     client.Client
	Resolver   *podlookup.Resolver
	DomainPath string
	NodeName   string

	// Provider is the libmxl-fabrics provider stamped onto mirrors
	// created on demand. Empty defaults to ProviderAuto.
	Provider mxlv1alpha1.MxlFabricsProvider

	// MaterializeTimeout caps the total wait per Materialize call;
	// zero means use the package default.
	MaterializeTimeout time.Duration

	// PollInterval governs how often the dispatcher rereads the
	// mirror status while waiting; zero means use the package
	// default.
	PollInterval time.Duration
}

// Materialize ensures that the flow referenced by path is, or will
// shortly be, available locally. Returns nil on success; on error
// the caller should propagate it back to the shim so the open()
// stays failed.
//
// pid is the host PID of the consumer process that triggered the
// request (typically obtained via SO_PEERCRED on the UDS).
func (d *Dispatcher) Materialize(ctx context.Context, pid int32, path string) error {
	flowID, ok := FlowIDFromPath(d.DomainPath, path)
	if !ok {
		return fmt.Errorf("%q is not a flow_def.json under %s", path, d.DomainPath)
	}

	if d.flowExistsLocally(flowID) {
		return nil
	}

	timeout := d.MaterializeTimeout
	if timeout <= 0 {
		timeout = defaultMaterializeTimeout
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pod, err := d.Resolver.PodForPID(wctx, pid)
	if err != nil {
		return fmt.Errorf("pod lookup: %w", err)
	}

	sourceNode, ok, err := d.resolveSourceNode(wctx, flowID)
	if err != nil {
		return fmt.Errorf("resolve source node: %w", err)
	}
	if !ok {
		return errors.New("MxlFlow not yet known cluster-wide")
	}
	if sourceNode == d.NodeName {
		// The flow's origin is this node; the producer should have
		// created the file already. Either we raced with the agent's
		// own MxlFlow publish or the producer crashed. Let the shim
		// retry; if the file is genuinely gone, ENOENT is the right
		// final answer.
		return nil
	}

	mirror, err := d.ensureMirror(wctx, flowID, sourceNode, pod)
	if err != nil {
		return fmt.Errorf("ensure mirror: %w", err)
	}

	return d.waitReady(wctx, mirror)
}

// FlowIDFromPath returns the flow id if path is
// <domain>/<uuid>.mxl-flow/flow_def.json. Exported so the UDS
// server can use the same parser for early-reject decisions.
func FlowIDFromPath(domain, path string) (string, bool) {
	domain = filepath.Clean(domain)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(domain, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 2 || parts[1] != "flow_def.json" {
		return "", false
	}
	const suffix = ".mxl-flow"
	if !strings.HasSuffix(parts[0], suffix) {
		return "", false
	}
	id := strings.TrimSuffix(parts[0], suffix)
	if id == "" {
		return "", false
	}
	return id, true
}

func (d *Dispatcher) flowExistsLocally(flowID string) bool {
	_, err := os.Stat(filepath.Join(d.DomainPath, flowID+".mxl-flow", "flow_def.json"))
	return err == nil
}

func (d *Dispatcher) resolveSourceNode(ctx context.Context, flowID string) (string, bool, error) {
	var flow mxlv1alpha1.MxlFlow
	if err := d.Client.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	for _, loc := range flow.Status.Locations {
		if loc.Phase == mxlv1alpha1.MxlFlowLocationOrigin {
			return loc.NodeName, true, nil
		}
	}
	return "", false, nil
}

func (d *Dispatcher) ensureMirror(ctx context.Context, flowID, sourceNode string, pod metav1.Object) (*mxlv1alpha1.MxlFlowMirror, error) {
	name := MirrorName(flowID, d.NodeName)

	var existing mxlv1alpha1.MxlFlowMirror
	err := d.Client.Get(ctx, types.NamespacedName{Namespace: pod.GetNamespace(), Name: name}, &existing)
	if err == nil {
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	provider := d.Provider
	if provider == "" {
		provider = mxlv1alpha1.ProviderAuto
	}

	desired := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.GetNamespace(),
			Name:      name,
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: sourceNode,
			TargetNode: d.NodeName,
			Provider:   provider,
			Requestor: &mxlv1alpha1.PodRef{
				Name:      pod.GetName(),
				Namespace: pod.GetNamespace(),
				UID:       string(pod.GetUID()),
			},
		},
	}
	if err := d.Client.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		if err := d.Client.Get(ctx, types.NamespacedName{Namespace: pod.GetNamespace(), Name: name}, &existing); err != nil {
			return nil, err
		}
		return &existing, nil
	}
	return desired, nil
}

func (d *Dispatcher) waitReady(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror) error {
	interval := d.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	key := types.NamespacedName{Namespace: mirror.Namespace, Name: mirror.Name}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		var current mxlv1alpha1.MxlFlowMirror
		if err := d.Client.Get(ctx, key, &current); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		} else if current.Status.Phase == mxlv1alpha1.MxlFlowMirrorReady &&
			current.Status.TargetInfo != "" {
			return nil
		} else if current.Status.Phase == mxlv1alpha1.MxlFlowMirrorFailed {
			return errors.New("mirror entered Failed phase")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// MirrorName mirrors the operator's helper (operator/internal/
// receiver.mirrorName). Keeping the algorithm identical here
// guarantees the agent's on-demand path and the operator's
// declarative path converge on the same MxlFlowMirror name for a
// given (flow, target node), so the gateway sees exactly one
// mirror per pair.
func MirrorName(flowID, targetNode string) string {
	joined := strings.ToLower(flowID + "--" + targetNode)
	var b strings.Builder
	b.Grow(len(joined))
	for _, c := range joined {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b.WriteRune(c)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}
