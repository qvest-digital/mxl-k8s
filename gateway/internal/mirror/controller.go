// Package mirror contains the gateway's MxlFlowMirror reconciler.
//
// Target-side: for mirrors with spec.targetNode equal to this
// gateway's node, the reconciler opens a libmxl FlowWriter on the
// flow, registers its memory regions with libmxl-fabrics, sets up a
// fabrics.Target, and writes the serialized TargetInfo back to
// status.targetInfo for the source-side gateway to consume.
package mirror

import (
	"context"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

// FinalizerName is the target-side mirror finalizer the reconciler
// adds to keep ownership of libmxl-fabrics handles across deletion.
const FinalizerName = "gateway.mxl.qvest-digital.com/target-side"

// Reconciler reconciles MxlFlowMirror resources from the target side.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NodeName is the Kubernetes node this gateway runs on. Mirrors
	// with spec.targetNode set to a different node are ignored.
	NodeName string

	// BindAddress is the libmxl-fabrics endpoint node passed to each
	// Target Setup. Empty means "bind all interfaces" per
	// libmxl-fabrics semantics.
	BindAddress string

	// Handles owns the long-lived mxl + fabrics instances. Caller
	// keeps it alive for the lifetime of the reconciler.
	Handles *instance.Handles

	mu      sync.Mutex
	targets map[types.NamespacedName]*targetEntry
}

// targetEntry holds the live libmxl handles for one target-side
// mirror; they're all closed together by closeEntry.
type targetEntry struct {
	writer  *mxl.Writer
	regions *fabrics.Regions
	target  *fabrics.Target
	info    *fabrics.TargetInfo
	infoStr string
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/finalizers,verbs=update
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch

// Reconcile drives one MxlFlowMirror through its target-side lifecycle.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("mxlflowmirror", req.NamespacedName)

	var mirror mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &mirror); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Other-node mirrors are not ours.
	if mirror.Spec.TargetNode != r.NodeName {
		return ctrl.Result{}, nil
	}

	// Deletion path: tear down libmxl handles, drop the finalizer.
	if !mirror.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&mirror, FinalizerName) {
			return ctrl.Result{}, nil
		}
		r.closeEntry(req.NamespacedName)
		controllerutil.RemoveFinalizer(&mirror, FinalizerName)
		if err := r.Update(ctx, &mirror); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
		l.Info("torn down target-side mirror")
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is in place before we own any handles.
	if !controllerutil.ContainsFinalizer(&mirror, FinalizerName) {
		controllerutil.AddFinalizer(&mirror, FinalizerName)
		if err := r.Update(ctx, &mirror); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Idempotent fast-path.
	if mirror.Status.Phase == mxlv1alpha1.MxlFlowMirrorReady && mirror.Status.TargetInfo != "" {
		return ctrl.Result{}, nil
	}

	// Resolve the flow definition.
	var flow mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: mirror.Spec.FlowID}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.markMaterializing(ctx, &mirror); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get MxlFlow %s: %w", mirror.Spec.FlowID, err)
	}
	if len(flow.Spec.Definition.Raw) == 0 {
		return ctrl.Result{}, fmt.Errorf("MxlFlow %s has empty spec.definition", mirror.Spec.FlowID)
	}

	provider := mapProvider(mirror.Spec.Provider)

	entry, err := r.openTarget(string(flow.Spec.Definition.Raw), provider)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("open target: %w", err)
	}

	r.mu.Lock()
	if existing := r.targets[req.NamespacedName]; existing != nil {
		// Concurrent reconcile produced a stray entry; close the new
		// one and reuse the existing.
		r.mu.Unlock()
		closeHandles(entry)
		return ctrl.Result{}, nil
	}
	r.targets[req.NamespacedName] = entry
	r.mu.Unlock()

	mirror.Status.TargetInfo = entry.infoStr
	mirror.Status.Phase = mxlv1alpha1.MxlFlowMirrorReady
	mirror.Status.ObservedGeneration = mirror.Generation
	if err := r.Status().Update(ctx, &mirror); err != nil {
		// Status update lost; close the entry so the next pass can
		// retry cleanly.
		r.closeEntry(req.NamespacedName)
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	l.Info("target ready",
		"flowID", mirror.Spec.FlowID,
		"sourceNode", mirror.Spec.SourceNode,
		"provider", provider.String())
	return ctrl.Result{}, nil
}

// openTarget walks the libmxl handshake: open FlowWriter, get
// regions, create + setup fabrics.Target, marshal TargetInfo.
func (r *Reconciler) openTarget(flowDef string, provider fabrics.Provider) (*targetEntry, error) {
	mxlInst := r.Handles.MXL()
	if mxlInst == nil {
		return nil, fmt.Errorf("mxl instance closed")
	}
	fabInst := r.Handles.Fabrics()
	if fabInst == nil {
		return nil, fmt.Errorf("fabrics instance closed")
	}

	writer, _, err := mxlInst.NewWriter(flowDef)
	if err != nil {
		return nil, fmt.Errorf("NewWriter: %w", err)
	}
	regions, err := fabrics.RegionsForFlowWriter(writer)
	if err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("RegionsForFlowWriter: %w", err)
	}
	target, err := fabInst.NewTarget()
	if err != nil {
		_ = regions.Close()
		_ = writer.Close()
		return nil, fmt.Errorf("NewTarget: %w", err)
	}
	info, err := target.Setup(fabrics.TargetConfig{
		Endpoint: fabrics.EndpointAddress{Node: r.BindAddress},
		Provider: provider,
		Regions:  regions,
	})
	if err != nil {
		_ = target.Close()
		_ = regions.Close()
		_ = writer.Close()
		return nil, fmt.Errorf("Target.Setup: %w", err)
	}
	s, err := info.MarshalString()
	if err != nil {
		_ = info.Close()
		_ = target.Close()
		_ = regions.Close()
		_ = writer.Close()
		return nil, fmt.Errorf("TargetInfo.MarshalString: %w", err)
	}
	return &targetEntry{
		writer:  writer,
		regions: regions,
		target:  target,
		info:    info,
		infoStr: s,
	}, nil
}

func (r *Reconciler) closeEntry(key types.NamespacedName) {
	r.mu.Lock()
	entry := r.targets[key]
	delete(r.targets, key)
	r.mu.Unlock()
	if entry == nil {
		return
	}
	closeHandles(entry)
}

func closeHandles(e *targetEntry) {
	if e.info != nil {
		_ = e.info.Close()
	}
	if e.target != nil {
		_ = e.target.Close()
	}
	if e.regions != nil {
		_ = e.regions.Close()
	}
	if e.writer != nil {
		_ = e.writer.Close()
	}
}

func (r *Reconciler) markMaterializing(ctx context.Context, mirror *mxlv1alpha1.MxlFlowMirror) error {
	if mirror.Status.Phase == mxlv1alpha1.MxlFlowMirrorMaterializing {
		return nil
	}
	mirror.Status.Phase = mxlv1alpha1.MxlFlowMirrorMaterializing
	return r.Status().Update(ctx, mirror)
}

// mapProvider translates the API enum into the fabrics package enum.
func mapProvider(p mxlv1alpha1.MxlFabricsProvider) fabrics.Provider {
	switch p {
	case mxlv1alpha1.ProviderTCP:
		return fabrics.ProviderTCP
	case mxlv1alpha1.ProviderVerbs:
		return fabrics.ProviderVerbs
	case mxlv1alpha1.ProviderEFA:
		return fabrics.ProviderEFA
	case mxlv1alpha1.ProviderSHM:
		return fabrics.ProviderSHM
	}
	return fabrics.ProviderAuto
}

// SetupWithManager wires the reconciler into the controller-runtime
// Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.targets == nil {
		r.targets = make(map[types.NamespacedName]*targetEntry)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mxlv1alpha1.MxlFlowMirror{}).
		Named("mxlflowmirror-target").
		Complete(r)
}
