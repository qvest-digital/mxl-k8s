package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MxlFlowMirrorTargetSpec carries the descriptor of one mirror's
// target endpoint.
type MxlFlowMirrorTargetSpec struct {
	// TargetInfo is the libmxl-fabrics target descriptor produced by
	// mxlFabricsTargetSetup and serialized via
	// mxlFabricsTargetInfoToString. The initiator-side gateway parses
	// it with mxlFabricsTargetInfoFromString. It names the target's
	// registered memory regions and their remote keys.
	// +kubebuilder:validation:MinLength=1
	TargetInfo string `json:"targetInfo"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=mxlfmt
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlFlowMirrorTarget holds the target descriptor of the MxlFlowMirror
// of the same name and namespace, which owns it.
//
// The descriptor lists every memory region the target registered, so
// for a video flow it runs to hundreds of kilobytes, and it changes
// only when the target's fabric side is opened or rebuilt. Kept on the
// mirror, it would be stored again with every revision of the mirror's
// status. The target gateway writes this object first and then
// status.targetInfoHash on the mirror; the source gateway dials the
// descriptor only once its hash matches the mirror's.
type MxlFlowMirrorTarget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec MxlFlowMirrorTargetSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// MxlFlowMirrorTargetList is a list of MxlFlowMirrorTarget resources.
type MxlFlowMirrorTargetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MxlFlowMirrorTarget `json:"items"`
}

// TargetInfoHash is the value status.targetInfoHash carries for a
// descriptor: the lowercase hex SHA-256 of its serialized form.
func TargetInfoHash(targetInfo string) string {
	sum := sha256.Sum256([]byte(targetInfo))
	return hex.EncodeToString(sum[:])
}
