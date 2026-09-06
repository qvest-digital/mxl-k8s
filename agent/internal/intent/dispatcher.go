// Package intent handles on-demand MxlFlowMirror materialization.
//
// A consumer pod that probes a flow that has not yet materialised
// on this node hits ENOENT (libmxl calls access/stat/open against
// the <id>.mxl-flow directory and the files inside it). The
// libmxl-intent.so shim intercepts that ENOENT and asks this
// dispatcher (via the agent's UDS) to materialize the flow.
// Materialize walks the same handshake the operator uses for
// declarative MxlReceivers -- look up the source node, ensure the
// MxlFlowMirror, wait for the gateway to mark it Ready -- and
// returns success so the shim can retry the original call.
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
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	"github.com/qvest-digital/mxl-k8s/api/selection"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	defaultMaterializeTimeout = 5 * time.Second
	defaultPollInterval       = 50 * time.Millisecond
)

// FlowChecker reports whether the named flow's flow_def.json is
// present locally. The default implementation stats the filesystem
// under DomainPath; tests inject a closure that fakes the lookup.
type FlowChecker func(flowID string) bool

// LeaseChecker is the slice of the originlease.Manager surface the
// dispatcher needs to skip Origin locations whose Lease has expired.
// Kept as an interface so tests can drive resolveSourceNode without
// a coordination.k8s.io fake fixture. The deadline is unused here --
// a request-driven dispatcher schedules nothing -- and present so one
// interface serves the operator's controllers as well.
type LeaseChecker interface {
	IsFresh(ctx context.Context, flowID, nodeName string) (fresh bool, deadline time.Time, err error)
}

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

	// FlowChecker overrides the filesystem-based local-flow check.
	// Nil falls back to the default stat under DomainPath.
	FlowChecker FlowChecker

	// Lease, when set, gates resolveSourceNode's Origin picks on a
	// fresh Lease. Nil keeps the pre-Lease behaviour the existing
	// tests built around. The dispatcher only consults the checker;
	// the operator owns the OriginFresh condition writeback.
	Lease LeaseChecker

	// Origin records this node as a flow's origin when a local
	// producer attaches to a flow that already existed. Nil makes
	// NotifyProducerAttached a no-op, which is what a dispatcher
	// wired without a publisher wants.
	Origin OriginClaimer
}

// OriginClaimer is the slice of the flowpublisher.Publisher surface
// NotifyProducerAttached needs. Kept as an interface so the intent
// package does not depend on the publisher, matching how LeaseChecker
// keeps the Lease manager out.
type OriginClaimer interface {
	ClaimOrigin(ctx context.Context, flowID string) error
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

	l := log.FromContext(ctx).WithName("intent").WithValues(
		"flowID", flowID,
		"pod", pod.GetNamespace()+"/"+pod.GetName(),
		"pid", pid,
	)
	l.Info("intent request received")

	res, err := d.resolveSourceNode(wctx, flowID)
	if err != nil {
		return fmt.Errorf("resolve source node: %w", err)
	}
	if !res.Found {
		if res.AllStale {
			return errors.New("all Origin locations have an expired Lease")
		}
		return errors.New("MxlFlow not yet known cluster-wide")
	}
	sourceNode := res.Node
	if sourceNode == d.NodeName {
		// The flow's origin is this node; the producer should have
		// created the file already. Either we raced with the agent's
		// own MxlFlow publish or the producer crashed. Let the shim
		// retry; if the file is genuinely gone, ENOENT is the right
		// final answer.
		l.Info("intent request short-circuited: flow originates locally")
		return nil
	}

	mirror, err := d.ensureMirror(wctx, flowID, sourceNode, pod)
	if err != nil {
		return fmt.Errorf("ensure mirror: %w", err)
	}

	if err := d.waitReady(wctx, mirror); err != nil {
		return err
	}
	l.Info("intent request fulfilled", "sourceNode", sourceNode, "mirror", mirror.Name)
	return nil
}

// FlowIDFromPath returns the flow id if path is under
// <domain>/<uuid>.mxl-flow -- the directory itself or any entry
// inside it. libmxl probes the flow directory and the access
// file before flow_def.json, so the shim's intercept fires on
// whichever name hits ENOENT first; the dispatcher only needs
// the flow id and does not care which entry triggered the
// request. Exported so the UDS server can share the parser.
func FlowIDFromPath(domain, path string) (string, bool) {
	domain = filepath.Clean(domain)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(domain, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) == 0 {
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
	if d.FlowChecker != nil {
		return d.FlowChecker(flowID)
	}
	_, err := os.Stat(filepath.Join(d.DomainPath, flowID+".mxl-flow", "flow_def.json"))
	return err == nil
}

func (d *Dispatcher) resolveSourceNode(ctx context.Context, flowID string) (mxlv1alpha1.OriginResolution, error) {
	var flow mxlv1alpha1.MxlFlow
	if err := d.Client.Get(ctx, types.NamespacedName{Name: flowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			return mxlv1alpha1.OriginResolution{}, nil
		}
		return mxlv1alpha1.OriginResolution{}, err
	}
	return mxlv1alpha1.ResolveOrigin(&flow, d.leaseFreshness(ctx))
}

// leaseFreshness adapts the dispatcher's LeaseChecker to the callback
// ResolveOrigin takes. A nil checker yields a nil callback, which
// keeps a dispatcher wired without one resolving the same origin the
// pre-Lease code did.
func (d *Dispatcher) leaseFreshness(ctx context.Context) mxlv1alpha1.LeaseFreshness {
	if d.Lease == nil {
		return nil
	}
	return func(flowID, nodeName string) (bool, time.Time, error) {
		return d.Lease.IsFresh(ctx, flowID, nodeName)
	}
}

// NotifyProducerAttached records that a local process opened a flow
// that already existed on this node for writing, which makes this
// node the flow's origin.
//
// Without it the node stays labelled Ready forever. A producer
// rescheduled onto a node that already mirrors the same flow finds
// the directory in place, so libmxl opens it instead of creating it
// and no rename reaches the agent's fanotify watch. Once the
// previous origin is pruned the flow has no Origin location
// anywhere, which resolveSourceNode cannot answer from -- so no
// mirror can be repointed and no new consumer can materialize it.
// Nothing in the cluster recovers from that on its own.
//
// The claim is safe precisely because the evidence is positive. A
// reader opens read-only, and the gateway that fills a mirror links
// libmxl directly rather than through the shim, so neither reaches
// this path. Inferring the same thing from the absence of a viable
// mirror does not work: every node holding a mirror stranded by a
// reclaimed source would look identical to a real producer and claim
// a false Origin.
func (d *Dispatcher) NotifyProducerAttached(ctx context.Context, pid int32, path string) error {
	flowID, ok := FlowIDFromPath(d.DomainPath, path)
	if !ok {
		return fmt.Errorf("%q is not under %s", path, d.DomainPath)
	}
	if d.Origin == nil {
		return nil
	}

	l := log.FromContext(ctx).WithName("intent").WithValues("flowID", flowID, "pid", pid)
	if pod, err := d.Resolver.PodForPID(ctx, pid); err == nil {
		l = l.WithValues("pod", pod.GetNamespace()+"/"+pod.GetName())
	}
	l.V(1).Info("producer attached to existing flow")

	return d.Origin.ClaimOrigin(ctx, flowID)
}

func (d *Dispatcher) ensureMirror(ctx context.Context, flowID, sourceNode string, pod metav1.Object) (*mxlv1alpha1.MxlFlowMirror, error) {
	name := MirrorName(flowID, d.NodeName)

	var existing mxlv1alpha1.MxlFlowMirror
	err := d.Client.Get(ctx, types.NamespacedName{Namespace: pod.GetNamespace(), Name: name}, &existing)
	if err == nil {
		// A mirror working through its finalizers occupies the only
		// name this consumer can use and can never reach Ready. Fail
		// so the shim retries once the name frees up.
		if !existing.DeletionTimestamp.IsZero() {
			return nil, fmt.Errorf("mirror %s/%s is terminating",
				existing.Namespace, existing.Name)
		}
		// A mirror with the same (flow, target node) name already
		// exists. The pre-existing object is functionally
		// sufficient for this consumer pod; reuse it as-is. The
		// labels and Requestor field stay untouched: in particular
		// when the receiver reconciler authored the mirror
		// (LabelCreatedByReceiver, no Requestor), stamping the
		// intent label here would split the GC contract -- both
		// reconcilers would then claim the same mirror, racing on
		// delete.
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	provider, err := d.resolveProvider(ctx, flowID, sourceNode)
	if err != nil {
		return nil, err
	}

	desired := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.GetNamespace(),
			Name:      name,
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByIntent: d.NodeName,
				mxlv1alpha1.LabelRequestorPodUID: string(pod.GetUID()),
			},
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

// resolveProvider decides which libmxl-fabrics provider an on-demand
// mirror between sourceNode and this node should carry. A non-empty,
// non-auto d.Provider is an explicit per-cluster override and is used
// verbatim; otherwise the choice is resolved from the two nodes'
// MxlNodeCapabilities. The result is always concrete -- a mirror is
// never created with provider auto, which libmxl-fabrics can no longer
// resolve on its own.
func (d *Dispatcher) resolveProvider(ctx context.Context, flowID, sourceNode string) (mxlv1alpha1.MxlFabricsProvider, error) {
	if d.Provider != "" && d.Provider != mxlv1alpha1.ProviderAuto {
		return d.Provider, nil
	}

	srcCaps, err := d.nodeCapabilities(ctx, sourceNode)
	if err != nil {
		return "", fmt.Errorf("source node capabilities: %w", err)
	}
	tgtCaps, err := d.nodeCapabilities(ctx, d.NodeName)
	if err != nil {
		return "", fmt.Errorf("target node capabilities: %w", err)
	}

	provider, rerr := selection.Resolve(srcCaps, tgtCaps)
	l := log.FromContext(ctx).WithName("intent").WithValues(
		"flowID", flowID,
		"sourceNode", sourceNode,
		"targetNode", d.NodeName,
		"provider", provider,
	)
	if rerr != nil {
		l.Info("resolved mirror provider with fallback", "reason", rerr.Error())
	} else {
		l.Info("resolved mirror provider")
	}
	return provider, nil
}

// nodeCapabilities reads the cluster-scoped MxlNodeCapabilities the
// gateway publishes for nodeName (named after the node). A missing
// resource yields an empty status so the resolver falls back rather
// than failing the materialization on a node whose gateway has not
// probed yet.
func (d *Dispatcher) nodeCapabilities(ctx context.Context, nodeName string) (mxlv1alpha1.MxlNodeCapabilitiesStatus, error) {
	var caps mxlv1alpha1.MxlNodeCapabilities
	if err := d.Client.Get(ctx, types.NamespacedName{Name: nodeName}, &caps); err != nil {
		if apierrors.IsNotFound(err) {
			return mxlv1alpha1.MxlNodeCapabilitiesStatus{}, nil
		}
		return mxlv1alpha1.MxlNodeCapabilitiesStatus{}, err
	}
	return caps.Status, nil
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

// MirrorName re-exports the shared api/v1alpha1 helper. The agent's
// on-demand path and the operator's declarative path must land on
// the same name for a given (flow, target node) so the gateway sees
// one mirror per pair; the shared definition makes that agreement
// structural rather than a matter of two copies staying in step.
func MirrorName(flowID, targetNode string) string {
	return mxlv1alpha1.MirrorName(flowID, targetNode)
}
