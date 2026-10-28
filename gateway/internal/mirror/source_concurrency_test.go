package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// After a target gateway restart every mirror into that node has its
// TargetInfo rotated at once, and a re-add can take seconds. With one
// worker a busy mirror waited behind the slow re-adds queued ahead of
// it, so the source controller runs several.
func TestSourceController_RunsMoreThanOneWorker(t *testing.T) {
	require.Greater(t, sourceControllerOptions().MaxConcurrentReconciles, 1,
		"one worker holds every mirror on the node behind the slowest reconcile")
}

// Several workers only help if a reconcile that is stuck in libmxl does
// not hold the others up inside Reconcile itself. A mirror whose open
// blocks must leave a mirror of another flow free to be added.
func TestSourceReconcile_SlowOpenDoesNotHoldAnotherMirror(t *testing.T) {
	f := newFanoutFixture(t,
		fanoutMirror("slow", "flow-slow", "node-b", "info-slow", mxlv1alpha1.ProviderTCP),
		fanoutMirror("fast", "flow-fast", "node-b", "info-fast", mxlv1alpha1.ProviderTCP),
	)
	release := make(chan struct{})
	entered := make(chan struct{})
	f.opener.openFn = func(flowID string, _ fabrics.Provider) (*sharedSource, error) {
		if flowID == "flow-slow" {
			close(entered)
			<-release
		}
		return runningSource(), nil
	}

	slowDone := make(chan error, 1)
	go func() {
		_, err := f.r.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "slow"}})
		slowDone <- err
	}()
	<-entered

	fastDone := make(chan error, 1)
	go func() {
		_, err := f.r.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "fast"}})
		fastDone <- err
	}()
	select {
	case err := <-fastDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("a mirror's reconcile waited on another mirror's open")
	}
	require.Equal(t, []string{"info-fast"}, f.opener.addedTargets())

	close(release)
	require.NoError(t, <-slowDone)
}
