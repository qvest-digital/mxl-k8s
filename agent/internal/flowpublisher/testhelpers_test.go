package flowpublisher

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ObjectMeta is a tiny convenience for the cluster-scoped MxlFlow
// fixtures used in tests.
func ObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name}
}
