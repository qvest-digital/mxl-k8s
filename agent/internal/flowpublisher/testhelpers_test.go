package flowpublisher

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ObjectMeta is a tiny convenience for the cluster-scoped MxlFlow
// fixtures used in tests.
func ObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name}
}

// writerAttached stands in for the flock probe in tests whose subject
// is what the publisher does with the answer rather than how it gets
// it. Every one of them fabricates a flow directory without the data
// file libmxl locks, so the real probe would correctly report no
// writer and every fixture would demote itself. The probe's own
// behaviour is covered against a real filesystem in
// writergate_test.go and in agent/internal/flowlock.
func writerAttached(string) (bool, error) { return true, nil }

// writerDetached is its opposite, for the tests that are about the
// publisher noticing a flow nothing writes to any more.
func writerDetached(string) (bool, error) { return false, nil }

// metaNS is ObjectMeta for the namespaced fixtures.
func metaNS(namespace, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace, Name: name}
}
