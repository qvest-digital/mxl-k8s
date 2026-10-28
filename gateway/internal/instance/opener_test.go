package instance

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func domainOn(name, directory, id string, nodes ...mxlv1alpha1.MxlDomainNodeStatus) *mxlv1alpha1.MxlDomain {
	return &mxlv1alpha1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       mxlv1alpha1.MxlDomainSpec{ID: id, Directory: directory},
		Status:     mxlv1alpha1.MxlDomainStatus{Nodes: nodes},
	}
}

// newOpener builds an Opener over a Set whose Directory reads the same
// fake client, so the domain is resolved the way the gateway resolves it.
func newOpener(t *testing.T, objs ...*mxlv1alpha1.MxlDomain) (*Opener, *[]string) {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	b := fake.NewClientBuilder().WithScheme(s)
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	c := b.Build()
	set, opened := newTestSet(nil)
	set.Directory = func(ctx context.Context, name string) (string, error) {
		var d mxlv1alpha1.MxlDomain
		if err := c.Get(ctx, types.NamespacedName{Name: name}, &d); err != nil {
			return "", err
		}
		return d.Spec.Path(), nil
	}
	return &Opener{Client: c, NodeName: "node-a", Set: set}, opened
}

func reconcileDomain(t *testing.T, o *Opener, name string) {
	t.Helper()
	if _, err := o.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
		t.Fatalf("Reconcile(%s): %v", name, err)
	}
}

// A domain no mirror and no placeholder on this node uses is opened all
// the same, and what watches the set -- the domain sweep -- sees it. A
// node that only produces into a domain otherwise never sweeps it, and a
// producer killed there leaves its flow behind for good.
func TestOpenerOpensReadyDomainNothingUses(t *testing.T) {
	o, opened := newOpener(t, domainOn("studio", "", "5d0c4a9e-0000-4000-8000-000000000001",
		mxlv1alpha1.MxlDomainNodeStatus{NodeName: "node-a", Ready: true}))
	var swept []string
	o.Set.Watch(func(h *Handles) { swept = append(swept, h.DomainPath()) })

	reconcileDomain(t, o, "studio")

	want := "/run/mxl/domains/5d0c4a9e-0000-4000-8000-000000000001"
	if len(*opened) != 1 || (*opened)[0] != want {
		t.Fatalf("opened %v, want [%s]", *opened, want)
	}
	if len(swept) != 2 || swept[0] != "/run/mxl/domain" || swept[1] != want {
		t.Fatalf("watchers saw %v, want the primary then %s", swept, want)
	}

	reconcileDomain(t, o, "studio")
	if len(*opened) != 1 {
		t.Fatalf("a second pass opened again: %v", *opened)
	}
}

// Opening a domain the agent has not materialised here would fail, and
// one materialised only on other nodes is not mounted here at all.
func TestOpenerLeavesDomainNotReadyHere(t *testing.T) {
	for name, nodes := range map[string][]mxlv1alpha1.MxlDomainNodeStatus{
		"absent":    nil,
		"not-ready": {{NodeName: "node-a", Ready: false}},
		"elsewhere": {{NodeName: "node-b", Ready: true}},
	} {
		o, opened := newOpener(t, domainOn(name, "", "5d0c4a9e-0000-4000-8000-000000000002", nodes...))
		reconcileDomain(t, o, name)
		if len(*opened) != 0 {
			t.Errorf("%s: opened %v", name, *opened)
		}
	}
}

// The primary domain is open from the start and is spelled empty on a
// mirror; opening it again under its name is what Set.For refuses, so
// the opener passes over it without an error to retry.
func TestOpenerPassesOverPrimaryAndMissingDomains(t *testing.T) {
	o, opened := newOpener(t, domainOn("default", "domain", "5d0c4a9e-0000-4000-8000-000000000003",
		mxlv1alpha1.MxlDomainNodeStatus{NodeName: "node-a", Ready: true}))
	reconcileDomain(t, o, "default")
	reconcileDomain(t, o, "deleted")
	if len(*opened) != 0 {
		t.Fatalf("opened %v", *opened)
	}
}
