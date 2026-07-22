package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func TestReconcile_FlowOriginRotation_DetectedAfterStaleOpen(t *testing.T) {
	// A FlowReader can legally open while the MxlFlow carries no live
	// Origin location for this node: PublishVanished leaves the entry
	// Stale with LastObserved nil, and a dropped PublishAppeared (the
	// dispatcher fires once per fanotify event and only logs errors)
	// never repairs it. The on-disk flow directory exists either way.
	//
	// lastObservedOriginAt is stored only at open, so an open in that
	// window records a nil baseline -- and originRotated treats a nil
	// baseline as "never a rotation". A later writer restart then goes
	// undetected for the entry's whole lifetime: the reader tails the
	// dead ring until the mirror itself is deleted and recreated.
	scheme := newSourceTestScheme(t)
	flowID := "flow-1"
	mirror := mirrorWithFinalizer("m1", "ns1", "node-a", flowID, "info-1")
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{{
				NodeName: "node-a",
				Phase:    mxlv1alpha1.MxlFlowLocationStale,
			}},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, flow).
		Build()

	opener := &fakeOpener{
		openFn: func(string, string, fabrics.Provider) (*sourceEntry, error) {
			return &sourceEntry{infoStr: "info-1"}, nil
		},
	}
	r := &SourceReconciler{
		Client:        c,
		Scheme:        scheme,
		NodeName:      "node-a",
		opener:        opener,
		FlushInterval: time.Hour,
		sources:       map[types.NamespacedName]*sourceEntry{},
		attempts:      map[types.NamespacedName]uint32{},
	}

	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	req := reconcile.Request{NamespacedName: key}

	// Reader opens while the node's location is Stale/nil: the entry
	// records no origin observation.
	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int32(1), opener.calls.Load())
	t.Cleanup(func() { r.closeEntry(key) })

	// Writer restarts: the agent publishes Origin with a fresh
	// LastObserved that postdates the reader.
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: flowID}, &f))
	now := metav1.Now()
	f.Status.Locations[0].Phase = mxlv1alpha1.MxlFlowLocationOrigin
	f.Status.Locations[0].LastObserved = &now
	require.NoError(t, c.Status().Update(context.Background(), &f))

	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(2), opener.calls.Load(),
		"an Origin observation newer than the running reader must reopen "+
			"it even when the reader was opened during a Stale/nil window: "+
			"a nil baseline that never re-arms leaves the reader on a dead "+
			"ring until the mirror is deleted")
}
