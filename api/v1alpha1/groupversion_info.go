package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the group/version pair for the v1alpha1 API.
var GroupVersion = schema.GroupVersion{Group: "mxl.qvest-digital.com", Version: "v1alpha1"}

// SchemeBuilder registers the v1alpha1 types with a runtime.Scheme.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme adds the v1alpha1 types to the given Scheme.
var AddToScheme = SchemeBuilder.AddToScheme

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&MxlDomain{}, &MxlDomainList{},
		&MxlFlow{}, &MxlFlowList{},
		&MxlFlowMirror{}, &MxlFlowMirrorList{},
		&MxlReceiver{}, &MxlReceiverList{},
		&MxlNodeCapabilities{}, &MxlNodeCapabilitiesList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
