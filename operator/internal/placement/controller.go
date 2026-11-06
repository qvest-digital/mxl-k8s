package placement

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// FieldOwner is the server-side-apply field manager the Placed
// condition and status.demandBitsPerSecond are written under. It is not
// the mirror controller's: an apply removes the fields its manager
// applied before and leaves out, so two controllers sharing a manager
// would delete each other's conditions.
const FieldOwner = "mxl-operator-placement"

// Defaults for the operator's placement flags.
const (
	DefaultMaxUtilizationPercent = 80
	DefaultOverheadPercent       = 5
	DefaultDemandWait            = 5 * time.Second
	// unplacedRetry is how soon a mirror that found no pair is looked
	// at again. Capability and flow changes wake it sooner; this covers
	// room freed by a mirror elsewhere going away.
	unplacedRetry = 30 * time.Second
	// overlayTTL bounds how long a placement this controller wrote is
	// trusted over a cache that has not shown it yet.
	overlayTTL = 30 * time.Second
)

// Reconciler places MxlFlowMirrors on fabric interface pairs.
//
// It runs with one worker. Every decision reads the load the others
// committed, and two placed at once would each see the interfaces the
// other is about to fill as free.
type Reconciler struct {
	client.Client

	Settings Settings
	// OverheadPercent is added to a flow's published rate for protocol
	// framing.
	OverheadPercent int32
	// ProvisionalDemand is charged for a flow that has not published a
	// rate within DemandWait of the mirror's creation.
	ProvisionalDemand int64
	DemandWait        time.Duration
	// RedundancySets enables anti-affinity between the member mirrors
	// of MxlFlowRedundancySets. Off, the kind need not be served.
	RedundancySets bool

	now func() time.Time

	gauges gauges

	mu sync.Mutex
	// written holds the placements this controller wrote, until the
	// cache shows them.
	written map[types.NamespacedName]overlay
}

type overlay struct {
	placed Placed
	at     time.Time
}

func (r *Reconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowmirrors/status,verbs=get;patch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlnodecapabilities,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflows,verbs=get;list;watch
// +kubebuilder:rbac:groups=mxl.qvest-digital.com,resources=mxlflowredundancysets,verbs=get;list;watch

// Reconcile places one mirror, or confirms the placement it has.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var m mxlv1alpha1.MxlFlowMirror
	if err := r.Get(ctx, req.NamespacedName, &m); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !m.DeletionTimestamp.IsZero() {
		r.forget(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	// Placement is per provider. A mirror still on auto is stamped with
	// a concrete one by whoever created it or by the mirror controller,
	// and that update brings it back here.
	if m.Spec.Provider == "" || m.Spec.Provider == mxlv1alpha1.ProviderAuto {
		return ctrl.Result{}, nil
	}

	nodes, err := r.nodes(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	ledger, err := r.ledger(ctx, nodes, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}
	src, tgt := nodes[m.Spec.SourceNode], nodes[m.Spec.TargetNode]
	valid := func(p *mxlv1alpha1.MxlFlowMirrorPlacement) bool {
		return p != nil && src.Has(m.Spec.Provider, p.SourceInterface) && tgt.Has(m.Spec.Provider, p.TargetInterface)
	}

	request, err := r.request(ctx, &m, nodes)
	if err != nil {
		return ctrl.Result{}, err
	}

	if valid(m.Spec.Placement) {
		return ctrl.Result{}, r.confirm(ctx, &m, ledger, request)
	}

	// A mirror from before placement existed, which its gateways have
	// already bound, keeps the interfaces they bound. Only one that never
	// carried a Placed condition is such a mirror: one that waited for its
	// demand, or found no pair, was bound by a gateway that gave up
	// waiting for placement and took the first interface, which is the
	// choice placement exists to replace.
	if m.Spec.Placement == nil && meta.FindStatusCondition(m.Status.Conditions, mxlv1alpha1.ConditionTypePlaced) == nil {
		bound := &mxlv1alpha1.MxlFlowMirrorPlacement{SourceInterface: m.Status.SourceInterface, TargetInterface: m.Status.TargetInterface}
		if valid(bound) {
			demand, _ := r.demand(ctx, &m)
			request.Demand = demand
			return ctrl.Result{}, r.commit(ctx, &m, *bound, request, outcomeFor(Check(ledger, r.Settings, request, *bound)), false)
		}
	}

	demand, known := r.demand(ctx, &m)
	if !known {
		if wait := m.CreationTimestamp.Add(r.DemandWait).Sub(r.clock()); wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, r.condition(ctx, &m, false, mxlv1alpha1.ReasonAwaitingDemand,
				"Flow has not published its payload rate yet", 0)
		}
	}
	request.Demand = demand

	d := Place(ledger, r.Settings, request)
	switch d.Outcome {
	case OutcomeNoInterface:
		return ctrl.Result{RequeueAfter: unplacedRetry}, r.condition(ctx, &m, false, mxlv1alpha1.ReasonNoInterface,
			fmt.Sprintf("No %s interface pair between %s and %s", m.Spec.Provider, m.Spec.SourceNode, m.Spec.TargetNode), 0)
	case OutcomeHeld:
		return ctrl.Result{RequeueAfter: unplacedRetry}, r.condition(ctx, &m, false, mxlv1alpha1.ReasonHeld,
			fmt.Sprintf("No interface pair has room for %d bit/s", request.Demand), 0)
	}
	// Only a mirror's first placement measures how long placement took
	// from its creation; one placed again after its interface went away
	// would measure the age of the mirror.
	first := !placedBefore(&m)
	return ctrl.Result{}, r.commit(ctx, &m, d.Placement, request, d.Outcome, first)
}

// placedBefore reports whether m ever carried a placement: its Placed
// condition read True at some point, which is the only state that
// condition leaves behind.
func placedBefore(m *mxlv1alpha1.MxlFlowMirror) bool {
	c := meta.FindStatusCondition(m.Status.Conditions, mxlv1alpha1.ConditionTypePlaced)
	return c != nil && c.Status == metav1.ConditionTrue
}

func outcomeFor(headroom int64) Outcome {
	if headroom < 0 {
		return OutcomeOvercommitted
	}
	return OutcomePlaced
}

// confirm keeps a placement where it is and refreshes the condition
// that describes it.
func (r *Reconciler) confirm(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, l *Ledger, req Request) error {
	// A rate published after the mirror was placed replaces the
	// provisional demand it was charged, so the ledger and the
	// condition describe what it carries. The placement stays.
	demand, known := r.demand(ctx, m)
	if !known && m.Status.DemandBitsPerSecond > 0 {
		demand = m.Status.DemandBitsPerSecond
	}
	req.Demand = demand
	outcome := outcomeFor(Check(l, r.Settings, req, *m.Spec.Placement))
	return r.condition(ctx, m, true, string(outcome), message(outcome, *m.Spec.Placement, demand), demand)
}

// commit writes a placement and its condition.
func (r *Reconciler) commit(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, p mxlv1alpha1.MxlFlowMirrorPlacement, req Request, outcome Outcome, observe bool) error {
	patch := client.MergeFromWithOptions(m.DeepCopy(), client.MergeFromWithOptimisticLock{})
	m.Spec.Placement = &p
	if err := r.Patch(ctx, m, patch); err != nil {
		return client.IgnoreNotFound(err)
	}
	r.remember(client.ObjectKeyFromObject(m), Placed{SourceNode: m.Spec.SourceNode, TargetNode: m.Spec.TargetNode,
		Provider: m.Spec.Provider, Placement: p, Demand: req.Demand})
	if observe {
		placementSeconds.Observe(r.clock().Sub(m.CreationTimestamp.Time).Seconds())
	}
	log.FromContext(ctx).Info("placed mirror",
		"flowID", m.Spec.FlowID, "sourceNode", m.Spec.SourceNode, "targetNode", m.Spec.TargetNode,
		"sourceInterface", p.SourceInterface, "targetInterface", p.TargetInterface,
		"demand", req.Demand, "outcome", outcome)
	return r.condition(ctx, m, true, string(outcome), message(outcome, p, req.Demand), req.Demand)
}

func message(o Outcome, p mxlv1alpha1.MxlFlowMirrorPlacement, demand int64) string {
	if o == OutcomeOvercommitted {
		return fmt.Sprintf("%s -> %s carries %d bit/s beyond the planned capacity of an interface or budget",
			p.SourceInterface, p.TargetInterface, demand)
	}
	return fmt.Sprintf("%s -> %s carries %d bit/s", p.SourceInterface, p.TargetInterface, demand)
}

// condition applies the Placed condition and the charged demand.
func (r *Reconciler) condition(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, ok bool, reason, msg string, demand int64) error {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	conds := append([]metav1.Condition(nil), m.Status.Conditions...)
	meta.SetStatusCondition(&conds, metav1.Condition{
		Type: mxlv1alpha1.ConditionTypePlaced, Status: status, Reason: reason, Message: msg,
		ObservedGeneration: m.Generation,
	})
	c := meta.FindStatusCondition(conds, mxlv1alpha1.ConditionTypePlaced)
	if prev := meta.FindStatusCondition(m.Status.Conditions, mxlv1alpha1.ConditionTypePlaced); prev != nil &&
		prev.Status == c.Status && prev.Reason == c.Reason && prev.Message == c.Message &&
		prev.ObservedGeneration == c.ObservedGeneration && m.Status.DemandBitsPerSecond == demand {
		return nil
	}
	st := map[string]any{"conditions": []any{map[string]any{
		"type":               c.Type,
		"status":             string(c.Status),
		"reason":             c.Reason,
		"message":            c.Message,
		"observedGeneration": c.ObservedGeneration,
		"lastTransitionTime": c.LastTransitionTime.UTC().Format(time.RFC3339),
	}}}
	if demand > 0 {
		st["demandBitsPerSecond"] = demand
	}
	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(mxlv1alpha1.GroupVersion.WithKind("MxlFlowMirror"))
	patch.SetNamespace(m.Namespace)
	patch.SetName(m.Name)
	if err := unstructured.SetNestedField(patch.Object, st, "status"); err != nil {
		return fmt.Errorf("build SSA payload: %w", err)
	}
	if err := r.Status().Patch(ctx, patch, client.Apply, client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// demand returns the rate the mirror charges and whether its flow has
// published one.
func (r *Reconciler) demand(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror) (int64, bool) {
	var f mxlv1alpha1.MxlFlow
	if err := r.Get(ctx, types.NamespacedName{Name: m.Ref().Name()}, &f); err != nil || f.Status.BitsPerSecond <= 0 {
		return r.ProvisionalDemand, false
	}
	return Demand(f.Status.BitsPerSecond, r.OverheadPercent, r.ProvisionalDemand), true
}

// request builds the planner's request for m, with the interfaces its
// redundancy siblings use to the same node as the ones to avoid.
func (r *Reconciler) request(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror, nodes map[string]Node) (Request, error) {
	req := Request{SourceNode: m.Spec.SourceNode, TargetNode: m.Spec.TargetNode, Provider: m.Spec.Provider}
	if !r.RedundancySets {
		return req, nil
	}
	siblings, err := r.siblings(ctx, m)
	if err != nil {
		return req, err
	}
	for _, s := range siblings {
		p := r.placementOf(&s)
		if p == nil {
			continue
		}
		req.AvoidTarget = append(req.AvoidTarget, nodes[m.Spec.TargetNode].port(s.Spec.Provider, p.TargetInterface))
		if s.Spec.SourceNode == m.Spec.SourceNode {
			req.AvoidSource = append(req.AvoidSource, nodes[m.Spec.SourceNode].port(s.Spec.Provider, p.SourceInterface))
		}
	}
	return req, nil
}

// siblings returns the mirrors of the other members of every
// redundancy set m's flow is a member of, to m's target node.
func (r *Reconciler) siblings(ctx context.Context, m *mxlv1alpha1.MxlFlowMirror) ([]mxlv1alpha1.MxlFlowMirror, error) {
	var sets mxlv1alpha1.MxlFlowRedundancySetList
	if err := r.List(ctx, &sets); err != nil {
		return nil, fmt.Errorf("list redundancy sets: %w", err)
	}
	others := map[mxlv1alpha1.FlowRef]struct{}{}
	self := m.Ref()
	for _, s := range sets.Items {
		members := make([]mxlv1alpha1.FlowRef, 0, len(s.Spec.Members))
		in := false
		for _, mem := range s.Spec.Members {
			ref := mxlv1alpha1.FlowRef{Domain: mem.Domain, ID: mem.FlowID}
			in = in || ref == self
			members = append(members, ref)
		}
		if !in {
			continue
		}
		for _, ref := range members {
			if ref != self {
				others[ref] = struct{}{}
			}
		}
	}
	if len(others) == 0 {
		return nil, nil
	}
	var out []mxlv1alpha1.MxlFlowMirror
	for ref := range others {
		var mirrors mxlv1alpha1.MxlFlowMirrorList
		if err := r.List(ctx, &mirrors, client.MatchingFields{flowIndex: ref.Name()}, client.UnsafeDisableDeepCopy); err != nil {
			return nil, fmt.Errorf("list mirrors of %s: %w", ref.Name(), err)
		}
		for _, o := range mirrors.Items {
			if o.Spec.TargetNode == m.Spec.TargetNode && o.DeletionTimestamp.IsZero() {
				out = append(out, o)
			}
		}
	}
	return out, nil
}

// nodes reads every node's MxlNodeCapabilities.
func (r *Reconciler) nodes(ctx context.Context) (map[string]Node, error) {
	var caps mxlv1alpha1.MxlNodeCapabilitiesList
	if err := r.List(ctx, &caps); err != nil {
		return nil, fmt.Errorf("list node capabilities: %w", err)
	}
	out := make(map[string]Node, len(caps.Items))
	for _, c := range caps.Items {
		name := c.Spec.NodeName
		if name == "" {
			name = c.Name
		}
		out[name] = Node{Name: name, Status: c.Status}
	}
	return out, nil
}

// ledger charges every placed mirror but self.
func (r *Reconciler) ledger(ctx context.Context, nodes map[string]Node, self types.NamespacedName) (*Ledger, error) {
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	// Every mirror is read and none is written, and each carries its
	// target's whole descriptor in status, so the cache's objects are read
	// in place rather than copied once per reconcile.
	if err := r.List(ctx, &mirrors, client.UnsafeDisableDeepCopy); err != nil {
		return nil, fmt.Errorf("list mirrors: %w", err)
	}
	list := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		list = append(list, n)
	}
	var placed []Placed
	seen := map[types.NamespacedName]struct{}{}
	for i := range mirrors.Items {
		o := &mirrors.Items[i]
		key := client.ObjectKeyFromObject(o)
		seen[key] = struct{}{}
		if key == self || !o.DeletionTimestamp.IsZero() {
			continue
		}
		if ov, ok := r.overlay(key, o); ok {
			placed = append(placed, ov)
			continue
		}
		if o.Spec.Placement == nil {
			continue
		}
		placed = append(placed, Placed{SourceNode: o.Spec.SourceNode, TargetNode: o.Spec.TargetNode,
			Provider: o.Spec.Provider, Placement: *o.Spec.Placement, Demand: o.Status.DemandBitsPerSecond})
	}
	r.prune(seen)
	l := NewLedger(list, placed)
	r.gauges.publish(l, nodes, r.Settings)
	return l, nil
}

// placementOf is a mirror's placement as this controller knows it,
// which may be ahead of the cache.
func (r *Reconciler) placementOf(m *mxlv1alpha1.MxlFlowMirror) *mxlv1alpha1.MxlFlowMirrorPlacement {
	if ov, ok := r.overlay(client.ObjectKeyFromObject(m), m); ok {
		return &ov.Placement
	}
	return m.Spec.Placement
}

// overlay returns the placement this controller wrote for key while the
// cached object does not carry it yet.
func (r *Reconciler) overlay(key types.NamespacedName, cached *mxlv1alpha1.MxlFlowMirror) (Placed, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ov, ok := r.written[key]
	if !ok {
		return Placed{}, false
	}
	if (cached.Spec.Placement != nil && *cached.Spec.Placement == ov.placed.Placement &&
		cached.Status.DemandBitsPerSecond == ov.placed.Demand) || r.clock().Sub(ov.at) > overlayTTL {
		delete(r.written, key)
		return Placed{}, false
	}
	return ov.placed, true
}

func (r *Reconciler) remember(key types.NamespacedName, p Placed) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.written == nil {
		r.written = map[types.NamespacedName]overlay{}
	}
	r.written[key] = overlay{placed: p, at: r.clock()}
}

func (r *Reconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.written, key)
}

func (r *Reconciler) prune(seen map[types.NamespacedName]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.written {
		if _, ok := seen[k]; !ok {
			delete(r.written, k)
		}
	}
}

// SetupWithManager registers the controller with one worker.
//
// A change to a node's capabilities can invalidate placements on it or
// give an unplaced mirror an interface; a flow publishing its rate
// releases mirrors waiting for it; a redundancy set changing its
// members changes whom a member mirror avoids.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	for name, extract := range map[string]func(*mxlv1alpha1.MxlFlowMirror) string{
		sourceNodeIndex: func(m *mxlv1alpha1.MxlFlowMirror) string { return m.Spec.SourceNode },
		targetNodeIndex: func(m *mxlv1alpha1.MxlFlowMirror) string { return m.Spec.TargetNode },
		flowIndex:       func(m *mxlv1alpha1.MxlFlowMirror) string { return m.Ref().Name() },
	} {
		if err := idx.IndexField(context.Background(), &mxlv1alpha1.MxlFlowMirror{}, name, func(o client.Object) []string {
			m, ok := o.(*mxlv1alpha1.MxlFlowMirror)
			if !ok || extract(m) == "" {
				return nil
			}
			return []string{extract(m)}
		}); err != nil {
			return fmt.Errorf("index MxlFlowMirror by %s: %w", name, err)
		}
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named("placement").
		For(&mxlv1alpha1.MxlFlowMirror{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Watches(&mxlv1alpha1.MxlNodeCapabilities{}, handler.EnqueueRequestsFromMapFunc(r.capsToMirrors),
			builder.WithPredicates(placementInputsChanged())).
		Watches(&mxlv1alpha1.MxlFlow{}, handler.EnqueueRequestsFromMapFunc(r.flowToMirrors),
			builder.WithPredicates(flowRateChanged()))
	if r.RedundancySets {
		b = b.Watches(&mxlv1alpha1.MxlFlowRedundancySet{}, handler.EnqueueRequestsFromMapFunc(r.setToMirrors),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}))
	}
	return b.Complete(r)
}

// Field indexes over MxlFlowMirror the watch handlers look mirrors up by.
const (
	sourceNodeIndex = "placement.spec.sourceNode"
	targetNodeIndex = "placement.spec.targetNode"
	flowIndex       = "placement.flow"
)

// placementInputsChanged passes a capabilities update only when what
// placement reads of it changed. The gateway rewrites lastSeen on every
// resync, which would otherwise reconcile every mirror of the node each
// time.
func placementInputsChanged() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			o, ok1 := e.ObjectOld.(*mxlv1alpha1.MxlNodeCapabilities)
			n, ok2 := e.ObjectNew.(*mxlv1alpha1.MxlNodeCapabilities)
			if !ok1 || !ok2 {
				return true
			}
			return !equality.Semantic.DeepEqual(o.Status.Providers, n.Status.Providers) ||
				!equality.Semantic.DeepEqual(o.Status.Budgets, n.Status.Budgets) ||
				o.Status.MaxUtilizationPercent != n.Status.MaxUtilizationPercent
		},
	}
}

// flowRateChanged passes a flow update only when its published rate
// changed. The agents rewrite every flow's locations on each rescan.
func flowRateChanged() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			o, ok1 := e.ObjectOld.(*mxlv1alpha1.MxlFlow)
			n, ok2 := e.ObjectNew.(*mxlv1alpha1.MxlFlow)
			if !ok1 || !ok2 {
				return true
			}
			return o.Status.BitsPerSecond != n.Status.BitsPerSecond
		},
	}
}

// matching enqueues the mirrors the index field holds value for that
// keep accepts.
func (r *Reconciler) matching(ctx context.Context, field, value string, keep func(*mxlv1alpha1.MxlFlowMirror) bool) []reconcile.Request {
	var mirrors mxlv1alpha1.MxlFlowMirrorList
	if err := r.List(ctx, &mirrors, client.MatchingFields{field: value}, client.UnsafeDisableDeepCopy); err != nil {
		log.FromContext(ctx).Error(err, "list MxlFlowMirrors for a watch event")
		return nil
	}
	var out []reconcile.Request
	for i := range mirrors.Items {
		if keep == nil || keep(&mirrors.Items[i]) {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&mirrors.Items[i])})
		}
	}
	return out
}

func (r *Reconciler) capsToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	c, ok := obj.(*mxlv1alpha1.MxlNodeCapabilities)
	if !ok {
		return nil
	}
	name := c.Spec.NodeName
	if name == "" {
		name = c.Name
	}
	return append(r.matching(ctx, sourceNodeIndex, name, nil), r.matching(ctx, targetNodeIndex, name, nil)...)
}

func (r *Reconciler) flowToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	f, ok := obj.(*mxlv1alpha1.MxlFlow)
	if !ok {
		return nil
	}
	return r.matching(ctx, flowIndex, f.Ref().Name(), func(m *mxlv1alpha1.MxlFlowMirror) bool {
		return (m.Spec.Placement == nil ||
			(f.Status.BitsPerSecond > 0 && m.Status.DemandBitsPerSecond != Demand(f.Status.BitsPerSecond, r.OverheadPercent, 0)))
	})
}

func (r *Reconciler) setToMirrors(ctx context.Context, obj client.Object) []reconcile.Request {
	s, ok := obj.(*mxlv1alpha1.MxlFlowRedundancySet)
	if !ok {
		return nil
	}
	var out []reconcile.Request
	for _, mem := range s.Spec.Members {
		out = append(out, r.matching(ctx, flowIndex, mxlv1alpha1.FlowRef{Domain: mem.Domain, ID: mem.FlowID}.Name(), nil)...)
	}
	return out
}
