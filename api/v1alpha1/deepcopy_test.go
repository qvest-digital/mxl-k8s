package v1alpha1

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// These tests guard against accidental shallow copies in the
// controller-gen-emitted DeepCopyInto helpers. Any future hand-edit (or
// controller-gen change) that drops the per-element copy of a slice or
// map will let the original and the copy share backing storage; a
// reconciler that mutates the copy will then leak into the cached
// object. The tests provoke exactly that aliasing.

func TestDeepCopy_MxlFlow_LocationsAreNotAliased(t *testing.T) {
	now := metav1.NewTime(time.Now())
	orig := &MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-a"},
		Spec: MxlFlowSpec{
			ID:         "11111111-2222-3333-4444-555555555555",
			Definition: runtime.RawExtension{Raw: []byte(`{"id":"x"}`)},
		},
		Status: MxlFlowStatus{
			Locations: []MxlFlowLocation{
				{NodeName: "n1", Phase: MxlFlowLocationOrigin, LastObserved: &now},
				{NodeName: "n2", Phase: MxlFlowLocationReady},
			},
			Conditions: []metav1.Condition{
				{Type: "Available", Status: metav1.ConditionTrue, Reason: "Live"},
			},
		},
	}

	copy := orig.DeepCopy()
	require.NotNil(t, copy)
	require.NotSame(t, orig, copy)

	// Mutate the original through every reference path that DeepCopy
	// must have severed. Each mutation must NOT show up in the copy.
	orig.Status.Locations[0].NodeName = "mutated"
	orig.Status.Locations[1].Phase = MxlFlowLocationStale
	orig.Status.Conditions[0].Reason = "Broken"
	orig.Spec.Definition.Raw[0] = 'Z'

	assert.Equal(t, "n1", copy.Status.Locations[0].NodeName)
	assert.Equal(t, MxlFlowLocationReady, copy.Status.Locations[1].Phase)
	assert.Equal(t, "Live", copy.Status.Conditions[0].Reason)
	assert.Equal(t, byte('{'), copy.Spec.Definition.Raw[0])
}

func TestDeepCopy_MxlFlowMirror_StatusFieldsAreNotAliased(t *testing.T) {
	orig := &MxlFlowMirror{
		ObjectMeta: metav1.ObjectMeta{Name: "m"},
		Spec: MxlFlowMirrorSpec{
			FlowID:     "11111111-2222-3333-4444-555555555555",
			SourceNode: "n1",
			TargetNode: "n2",
			Provider:   ProviderTCP,
			Requestor:  &PodRef{Name: "pod", Namespace: "ns"},
		},
		Status: MxlFlowMirrorStatus{
			Phase:      MxlFlowMirrorReady,
			TargetInfo: "info-a",
			Conditions: []metav1.Condition{
				{Type: "Materialized", Status: metav1.ConditionTrue},
			},
		},
	}

	copy := orig.DeepCopy()
	require.NotNil(t, copy)

	orig.Spec.Requestor.Name = "mutated"
	orig.Status.Conditions[0].Type = "BrokenType"
	orig.Status.Phase = MxlFlowMirrorFailed

	assert.Equal(t, "pod", copy.Spec.Requestor.Name)
	assert.Equal(t, "Materialized", copy.Status.Conditions[0].Type)
	assert.Equal(t, MxlFlowMirrorReady, copy.Status.Phase)
}

func TestDeepCopy_MxlNodeCapabilities_ProvidersAreNotAliased(t *testing.T) {
	orig := &MxlNodeCapabilities{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Spec:       MxlNodeCapabilitiesSpec{NodeName: "node-a"},
		Status: MxlNodeCapabilitiesStatus{
			Providers: []MxlFabricsProviderCapability{
				{Name: ProviderTCP, Version: "1.2", DeviceCount: 1},
				{Name: ProviderVerbs, Version: "3.4", DeviceCount: 2},
			},
		},
	}

	copy := orig.DeepCopy()
	require.NotNil(t, copy)

	orig.Status.Providers[0].DeviceCount = 99
	orig.Status.Providers = append(orig.Status.Providers,
		MxlFabricsProviderCapability{Name: ProviderEFA})

	assert.Equal(t, int32(1), copy.Status.Providers[0].DeviceCount)
	assert.Lenf(t, copy.Status.Providers, 2,
		"appending to the original must not extend the copy")
}

func TestDeepCopy_MxlReceiver_PodSelectorAndRefAreNotAliased(t *testing.T) {
	orig := &MxlReceiver{
		ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "ns"},
		Spec: MxlReceiverSpec{
			FlowID:      "11111111-2222-3333-4444-555555555555",
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
			Provider:    ProviderAuto,
		},
		Status: MxlReceiverStatus{
			Phase:       MxlReceiverBound,
			BoundMirror: &MirrorRef{Name: "m1", Namespace: "ns"},
		},
	}

	copy := orig.DeepCopy()
	require.NotNil(t, copy)

	orig.Spec.PodSelector.MatchLabels["app"] = "mutated"
	orig.Status.BoundMirror.Name = "mutated"

	assert.Equal(t, "x", copy.Spec.PodSelector.MatchLabels["app"])
	assert.Equal(t, "m1", copy.Status.BoundMirror.Name)
}

func TestDeepCopy_MxlDomain_StatusIsNotAliased(t *testing.T) {
	now := metav1.NewTime(time.Now())
	orig := &MxlDomain{
		Spec: MxlDomainSpec{NodeName: "n1", HostPath: "/run/mxl/domain"},
		Status: MxlDomainStatus{
			CapacityBytes: 1 << 30,
			FreeBytes:     1 << 20,
			FanotifyReady: true,
			LastSeen:      &now,
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
		},
	}

	copy := orig.DeepCopy()
	require.NotNil(t, copy)

	orig.Status.LastSeen.Time = time.Time{}
	orig.Status.Conditions[0].Status = metav1.ConditionFalse

	assert.NotEqual(t, time.Time{}, copy.Status.LastSeen.Time)
	assert.Equal(t, metav1.ConditionTrue, copy.Status.Conditions[0].Status)
}

func TestDeepCopy_Lists_ItemsAreNotAliased(t *testing.T) {
	// One assertion per List type. controller-gen has historically
	// emitted shallow Items copies when manually altered; if a list's
	// Items slice ever shares backing storage with the source, mutating
	// the source list leaks into the cached copy held by client-go.
	cases := []struct {
		name string
		mut  func() (orig runtime.Object, copy runtime.Object)
	}{
		{"MxlFlowList", func() (runtime.Object, runtime.Object) {
			o := &MxlFlowList{Items: []MxlFlow{{ObjectMeta: metav1.ObjectMeta{Name: "a"}}}}
			c := o.DeepCopy()
			o.Items[0].ObjectMeta.Name = "mutated"
			return o, c
		}},
		{"MxlReceiverList", func() (runtime.Object, runtime.Object) {
			o := &MxlReceiverList{Items: []MxlReceiver{{ObjectMeta: metav1.ObjectMeta{Name: "a"}}}}
			c := o.DeepCopy()
			o.Items[0].ObjectMeta.Name = "mutated"
			return o, c
		}},
		{"MxlFlowMirrorList", func() (runtime.Object, runtime.Object) {
			o := &MxlFlowMirrorList{Items: []MxlFlowMirror{{ObjectMeta: metav1.ObjectMeta{Name: "a"}}}}
			c := o.DeepCopy()
			o.Items[0].ObjectMeta.Name = "mutated"
			return o, c
		}},
		{"MxlDomainList", func() (runtime.Object, runtime.Object) {
			o := &MxlDomainList{Items: []MxlDomain{{ObjectMeta: metav1.ObjectMeta{Name: "a"}}}}
			c := o.DeepCopy()
			o.Items[0].ObjectMeta.Name = "mutated"
			return o, c
		}},
		{"MxlNodeCapabilitiesList", func() (runtime.Object, runtime.Object) {
			o := &MxlNodeCapabilitiesList{Items: []MxlNodeCapabilities{{ObjectMeta: metav1.ObjectMeta{Name: "a"}}}}
			c := o.DeepCopy()
			o.Items[0].ObjectMeta.Name = "mutated"
			return o, c
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c := tc.mut()
			// Pull the copy's first item name through reflection-free
			// type assertions for each concrete type.
			switch v := c.(type) {
			case *MxlFlowList:
				assert.Equal(t, "a", v.Items[0].ObjectMeta.Name)
			case *MxlReceiverList:
				assert.Equal(t, "a", v.Items[0].ObjectMeta.Name)
			case *MxlFlowMirrorList:
				assert.Equal(t, "a", v.Items[0].ObjectMeta.Name)
			case *MxlDomainList:
				assert.Equal(t, "a", v.Items[0].ObjectMeta.Name)
			case *MxlNodeCapabilitiesList:
				assert.Equal(t, "a", v.Items[0].ObjectMeta.Name)
			default:
				t.Fatalf("unexpected list type %T", v)
			}
		})
	}
}

func TestDeepCopy_RoundTrip_Equals(t *testing.T) {
	// For each top-level kind: building an object, deep-copying it,
	// and comparing the two with go-cmp must produce no diff. This
	// catches any field that DeepCopyInto silently leaves unset (a
	// regression class controller-gen has produced in the past when a
	// new field is added to a type but the generator is not re-run).
	cases := []runtime.Object{
		&MxlFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "f"},
			Spec: MxlFlowSpec{
				ID:         "11111111-2222-3333-4444-555555555555",
				Definition: runtime.RawExtension{Raw: []byte(`{"a":1}`)},
			},
			Status: MxlFlowStatus{
				Locations: []MxlFlowLocation{{NodeName: "n", Phase: MxlFlowLocationOrigin}},
			},
		},
		&MxlReceiver{
			ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"},
			Spec: MxlReceiverSpec{
				FlowID:   "11111111-2222-3333-4444-555555555555",
				PodRef:   &PodRef{Name: "p", Namespace: "ns", UID: "u"},
				Provider: ProviderTCP,
			},
			Status: MxlReceiverStatus{
				Phase:       MxlReceiverBound,
				BoundMirror: &MirrorRef{Name: "m", Namespace: "ns"},
			},
		},
		&MxlFlowMirror{
			Spec: MxlFlowMirrorSpec{
				FlowID:     "11111111-2222-3333-4444-555555555555",
				SourceNode: "a",
				TargetNode: "b",
				Provider:   ProviderVerbs,
			},
			Status: MxlFlowMirrorStatus{Phase: MxlFlowMirrorReady, TargetInfo: "info"},
		},
		&MxlDomain{
			Spec:   MxlDomainSpec{NodeName: "n", HostPath: "/run/mxl/domain"},
			Status: MxlDomainStatus{CapacityBytes: 100, FreeBytes: 50, FanotifyReady: true},
		},
		&MxlNodeCapabilities{
			Spec: MxlNodeCapabilitiesSpec{NodeName: "n"},
			Status: MxlNodeCapabilitiesStatus{
				Providers: []MxlFabricsProviderCapability{{Name: ProviderTCP, DeviceCount: 1}},
			},
		},
	}
	for _, in := range cases {
		t.Run(typeName(in), func(t *testing.T) {
			out := in.DeepCopyObject()
			if diff := cmp.Diff(in, out); diff != "" {
				t.Fatalf("DeepCopyObject diverged from source (-want +got):\n%s", diff)
			}
		})
	}
}

func typeName(o runtime.Object) string {
	return o.GetObjectKind().GroupVersionKind().Kind + "_" + objAddrName(o)
}

func objAddrName(o runtime.Object) string {
	type named interface {
		GetName() string
	}
	if n, ok := o.(metav1.Object); ok {
		return n.GetName()
	}
	if n, ok := o.(named); ok {
		return n.GetName()
	}
	return "unnamed"
}
