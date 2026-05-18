package testutil

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Functional-option builders for the CRD types used by the operator
// tests. Defaults are deliberately benign so the failure mode of a
// missing field is "assert this CR has the value the test expected"
// rather than "API server rejected the create with a validation
// error" - which would hide the actual regression behind a CRD
// schema complaint.

// FlowID is a canonical fixture UUID used in tests that need a
// concrete flow but do not care about the value itself.
const FlowID = "11111111-2222-3333-4444-555555555555"

// FlowOption mutates a MxlFlow under construction.
type FlowOption func(*mxlv1alpha1.MxlFlow)

// NewFlow builds a cluster-scoped MxlFlow with the given options
// layered on top of sensible defaults.
func NewFlow(opts ...FlowOption) *mxlv1alpha1.MxlFlow {
	f := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: FlowID},
		Spec: mxlv1alpha1.MxlFlowSpec{
			ID:         FlowID,
			Definition: runtime.RawExtension{Raw: []byte(`{"id":"` + FlowID + `"}`)},
		},
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// WithFlowOrigin pins an Origin location on the flow's status.
func WithFlowOrigin(node string) FlowOption {
	return func(f *mxlv1alpha1.MxlFlow) {
		f.Status.Locations = append(f.Status.Locations, mxlv1alpha1.MxlFlowLocation{
			NodeName: node,
			Phase:    mxlv1alpha1.MxlFlowLocationOrigin,
		})
	}
}

// WithFlowID overrides the default UUID. Use when a test needs more
// than one flow at the same time.
func WithFlowID(id string) FlowOption {
	return func(f *mxlv1alpha1.MxlFlow) {
		f.SetName(id)
		f.Spec.ID = id
		f.Spec.Definition = runtime.RawExtension{Raw: []byte(`{"id":"` + id + `"}`)}
	}
}

// ReceiverOption mutates a MxlReceiver under construction.
type ReceiverOption func(*mxlv1alpha1.MxlReceiver)

// NewReceiver builds a namespaced MxlReceiver with a pod label
// selector matching `app=consumer`. Override via the options.
func NewReceiver(namespace, name string, opts ...ReceiverOption) *mxlv1alpha1.MxlReceiver {
	r := &mxlv1alpha1.MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: mxlv1alpha1.MxlReceiverSpec{
			FlowID:      FlowID,
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "consumer"}},
			Provider:    mxlv1alpha1.ProviderTCP,
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// WithReceiverFlowID overrides the receiver's flow target.
func WithReceiverFlowID(id string) ReceiverOption {
	return func(r *mxlv1alpha1.MxlReceiver) {
		r.Spec.FlowID = id
	}
}

// WithReceiverPodRef pins the receiver to a single pod.
func WithReceiverPodRef(podName string) ReceiverOption {
	return func(r *mxlv1alpha1.MxlReceiver) {
		r.Spec.PodSelector = nil
		r.Spec.PodRef = &mxlv1alpha1.PodRef{
			Namespace: r.Namespace,
			Name:      podName,
		}
	}
}

// WithReceiverSelector replaces the default selector.
func WithReceiverSelector(matchLabels map[string]string) ReceiverOption {
	return func(r *mxlv1alpha1.MxlReceiver) {
		r.Spec.PodRef = nil
		r.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: matchLabels}
	}
}

// PodOption mutates a Pod under construction.
type PodOption func(*corev1.Pod)

// NewPod builds a minimally-valid Pod pinned to the given node and
// labelled `app=consumer`. PodSpec.Containers carries one filler
// entry so the API server's required-fields admission accepts it.
func NewPod(namespace, name, node string, opts ...PodOption) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    map[string]string{"app": "consumer"},
		},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "c", Image: "pause:3.10"}},
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// WithPodLabels overrides the default label set entirely.
func WithPodLabels(labels map[string]string) PodOption {
	return func(p *corev1.Pod) {
		p.SetLabels(labels)
	}
}

// MirrorOption mutates a MxlFlowMirror under construction.
type MirrorOption func(*mxlv1alpha1.MxlFlowMirror)

// NewMirror builds a MxlFlowMirror with sensible defaults for unit
// tests; envtest tests should usually let the reconciler under test
// produce the mirror instead.
func NewMirror(namespace, name string, opts ...MirrorOption) *mxlv1alpha1.MxlFlowMirror {
	m := &mxlv1alpha1.MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: mxlv1alpha1.MxlFlowMirrorSpec{
			FlowID:     FlowID,
			SourceNode: "src",
			TargetNode: "dst",
			Provider:   mxlv1alpha1.ProviderTCP,
		},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// WithMirrorFlowID overrides the default flow.
func WithMirrorFlowID(id string) MirrorOption {
	return func(m *mxlv1alpha1.MxlFlowMirror) {
		m.Spec.FlowID = id
	}
}
