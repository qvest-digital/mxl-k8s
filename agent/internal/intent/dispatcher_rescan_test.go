package intent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// intentMirror builds a mirror shaped the way Materialize leaves one:
// stamped with the authoring node and carrying a Requestor.
func intentMirror(target, source string) *mxlv1alpha1.MxlFlowMirror {
	return &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      MirrorName(flowID, target),
			Labels: map[string]string{
				mxlv1alpha1.LabelCreatedByIntent: target,
				mxlv1alpha1.LabelRequestorPodUID: "uid-1",
			},
		},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     flowID,
			SourceNode: source,
			TargetNode: target,
			Provider:   mxlv1alpha1.ProviderTCP,
			Requestor: &mxlv1alpha1.PodRef{
				Name: "consumer", Namespace: "ns", UID: "uid-1",
			},
		},
	}
}

// flowAt builds an MxlFlow whose only Origin location is origin.
func flowAt(origin string) *mxlv1alpha1.MxlFlow {
	return &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: origin, Phase: mxlv1alpha1.MxlFlowLocationOrigin},
			},
		},
	}
}

// The origin moving raises no event the intent path observes: the
// consumer already opened the flow, so no further ENOENT reaches the
// shim and Materialize never runs again. Without a rescan the mirror
// addresses the departed node for as long as it exists.
func TestReconcileMirrors_RepointsAfterOriginMove(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(intentMirror("n-target", "n-old"), flowAt("n-new")).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}

	require.NoError(t, d.ReconcileMirrors(context.Background()))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: MirrorName(flowID, "n-target")}, &live))
	assert.Equal(t, "n-new", live.Spec.SourceNode,
		"a mirror left pointing at the node the flow departed keeps the "+
			"target gateway waiting on a source that will never publish again")
}

// Deleting is never this pass's business: the target gateway's
// teardown closes the FlowWriter, and closing it removes the on-disk
// flow definition the consumer reads. A source==target address is not
// worth that risk, so the mirror is left for the intent GC.
func TestReconcileMirrors_LeavesMirrorWhenOriginLandsLocally(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(intentMirror("n-target", "n-old"), flowAt("n-target")).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}

	require.NoError(t, d.ReconcileMirrors(context.Background()))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: MirrorName(flowID, "n-target")}, &live))
	assert.Equal(t, "n-old", live.Spec.SourceNode,
		"source==target addresses no transfer, so the mirror is left alone "+
			"rather than torn down under a consumer still reading the flow")
}

// patchMirrorIfDrifted owns spec.sourceNode for receiver-authored
// mirrors. Two writers on the same field would fight.
func TestReconcileMirrors_SkipsReceiverAuthoredMirror(t *testing.T) {
	scheme := newScheme(t)
	m := intentMirror("n-target", "n-old")
	delete(m.Labels, mxlv1alpha1.LabelCreatedByIntent)
	m.Labels[mxlv1alpha1.LabelCreatedByReceiver] = "recv"

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(m, flowAt("n-new")).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}

	require.NoError(t, d.ReconcileMirrors(context.Background()))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: MirrorName(flowID, "n-target")}, &live))
	assert.Equal(t, "n-old", live.Spec.SourceNode,
		"the agent must not write spec.sourceNode on a mirror the receiver owns")
}

// A mirror authored by another node's agent is that agent's to
// reconcile; touching it here would mean two writers per mirror.
func TestReconcileMirrors_SkipsMirrorTargetingAnotherNode(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(intentMirror("n-other", "n-old"), flowAt("n-new")).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}

	require.NoError(t, d.ReconcileMirrors(context.Background()))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: MirrorName(flowID, "n-other")}, &live))
	assert.Equal(t, "n-old", live.Spec.SourceNode)
}

// With no Origin to move to, the mirror and the flow directory it
// materialized are all the consumer has. Deleting it would take the
// directory away and turn a Degraded stream into ENOENT.
func TestReconcileMirrors_KeepsMirrorWhenNoOriginResolvable(t *testing.T) {
	scheme := newScheme(t)
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n-target", Phase: mxlv1alpha1.MxlFlowLocationReady},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(intentMirror("n-target", "n-old"), flow).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}

	require.NoError(t, d.ReconcileMirrors(context.Background()))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: MirrorName(flowID, "n-target")}, &live))
	assert.Equal(t, "n-old", live.Spec.SourceNode)
}

// The receiver's ensureMirror adopts a pre-existing intent mirror by
// name -- mirrorNameForReceiver and MirrorName agree for a same-
// namespace receiver -- and stamps LabelCreatedByReceiver only on the
// Create path. An adopted mirror therefore keeps the intent label and
// is distinguishable only by the ownerReference it gained.
func TestReconcileMirrors_SkipsMirrorAdoptedByReceiver(t *testing.T) {
	scheme := newScheme(t)
	m := intentMirror("n-target", "n-old")
	m.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: mxlv1alpha1.GroupVersion.String(),
		Kind:       "MxlReceiver",
		Name:       "recv",
		UID:        "recv-uid",
	}}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).
		WithObjects(m, flowAt("n-new")).
		Build()

	d := &Dispatcher{
		Client:     c,
		DomainPath: "/run/mxl/domain",
		NodeName:   "n-target",
		Provider:   mxlv1alpha1.ProviderTCP,
	}

	require.NoError(t, d.ReconcileMirrors(context.Background()))

	var live mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: MirrorName(flowID, "n-target")}, &live))
	assert.Equal(t, "n-old", live.Spec.SourceNode,
		"an adopted mirror keeps the intent label, so the ownerReference is "+
			"the only thing keeping the agent off a field the receiver writes")
}

// RunMirrorRescan is the package's first long-running loop; it must
// release its goroutine when the context is cancelled.
func TestRunMirrorRescan_StopsOnContextCancel(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	d := &Dispatcher{Client: c, DomainPath: "/run/mxl/domain", NodeName: "n-target"}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.RunMirrorRescan(ctx, time.Hour)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunMirrorRescan did not return after context cancel")
	}
}
