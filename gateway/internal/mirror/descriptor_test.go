package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/qvest-digital/go-mxl/fabrics"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

func mirrorTarget(key types.NamespacedName, info string) *mxlv1alpha1.MxlFlowMirrorTarget {
	return &mxlv1alpha1.MxlFlowMirrorTarget{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name},
		Spec:       mxlv1alpha1.MxlFlowMirrorTargetSpec{TargetInfo: info},
	}
}

// The descriptor is written into its own object before its hash goes
// on the mirror, so a source that sees the hash finds the object it
// names. It stays in status.targetInfo for sources that predate the
// object.
func TestTarget_PublishesTheDescriptorBeforeItsHash(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{})
	mirror.UID = "mirror-uid"
	var order []string
	c := fake.NewClientBuilder().WithScheme(newSourceTestScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).WithObjects(mirror).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				order = append(order, obj.GetObjectKind().GroupVersionKind().Kind)
				return cl.Patch(ctx, obj, patch, opts...)
			},
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				order = append(order, "status")
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		}).Build()
	r := &TargetReconciler{Client: c, NodeName: "node-a", attempts: attemptTable[targetOpenInputs]{}}

	require.NoError(t, r.applyTargetInfo(context.Background(), mirror, "info-1", 0, ""))
	assert.Equal(t, []string{"MxlFlowMirrorTarget", "status"}, order)

	var obj mxlv1alpha1.MxlFlowMirrorTarget
	require.NoError(t, c.Get(context.Background(), key, &obj))
	assert.Equal(t, "info-1", obj.Spec.TargetInfo)
	require.Len(t, obj.OwnerReferences, 1, "the mirror owns its descriptor, which goes with it")
	assert.Equal(t, types.UID("mirror-uid"), obj.OwnerReferences[0].UID)
	st := getMirrorStatus(t, c, key)
	assert.Equal(t, mxlv1alpha1.TargetInfoHash("info-1"), st.TargetInfoHash)
	assert.Equal(t, "info-1", st.TargetInfo, "sources that predate the object still read the status")
}

// A cluster whose CRDs predate MxlFlowMirrorTarget does not serve it.
// The mirror then carries the descriptor in status.targetInfo alone,
// with no hash a source would wait on forever.
func TestTarget_WithoutTheKindPublishesNoHash(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithTargetFinalizer(key.Name, key.Namespace, "node-a", "flow-1", mxlv1alpha1.MxlFlowMirrorStatus{})
	c := fake.NewClientBuilder().WithScheme(newSourceTestScheme(t)).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}).WithObjects(mirror).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: mxlv1alpha1.GroupVersion.Group, Kind: "MxlFlowMirrorTarget"}}
			},
		}).Build()
	r := &TargetReconciler{Client: c, NodeName: "node-a", attempts: attemptTable[targetOpenInputs]{}}

	require.NoError(t, r.applyTargetInfo(context.Background(), mirror, "info-1", 0, ""))
	st := getMirrorStatus(t, c, key)
	assert.Empty(t, st.TargetInfoHash)
	assert.Equal(t, "info-1", st.TargetInfo)
}

func TestTargetDescriptor(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	resolve := func(t *testing.T, status mxlv1alpha1.MxlFlowMirrorStatus, objs ...client.Object) (string, bool) {
		t.Helper()
		c := fake.NewClientBuilder().WithScheme(newSourceTestScheme(t)).WithObjects(objs...).Build()
		r := &SourceReconciler{Client: c}
		m := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "")
		m.Status = status
		info, ok, err := r.targetDescriptor(context.Background(), m)
		require.NoError(t, err)
		return info, ok
	}
	h := mxlv1alpha1.TargetInfoHash

	info, ok := resolve(t, mxlv1alpha1.MxlFlowMirrorStatus{TargetInfoHash: h("new")}, mirrorTarget(key, "new"))
	assert.True(t, ok)
	assert.Equal(t, "new", info, "the object the hash names is the descriptor")

	_, ok = resolve(t, mxlv1alpha1.MxlFlowMirrorStatus{TargetInfoHash: h("new")}, mirrorTarget(key, "old"))
	assert.False(t, ok, "an object the cache has not caught up on is not dialed")

	_, ok = resolve(t, mxlv1alpha1.MxlFlowMirrorStatus{TargetInfoHash: h("new")})
	assert.False(t, ok, "nor is a missing one")

	info, ok = resolve(t, mxlv1alpha1.MxlFlowMirrorStatus{TargetInfoHash: h("new"), TargetInfo: "new"}, mirrorTarget(key, "old"))
	assert.True(t, ok)
	assert.Equal(t, "new", info, "the legacy field serves while it matches the hash")

	_, ok = resolve(t, mxlv1alpha1.MxlFlowMirrorStatus{TargetInfoHash: h("new"), TargetInfo: "old"})
	assert.False(t, ok, "a legacy field that does not match the hash is stale")

	info, ok = resolve(t, mxlv1alpha1.MxlFlowMirrorStatus{TargetInfo: "legacy"})
	assert.True(t, ok)
	assert.Equal(t, "legacy", info, "a target that writes no hash is read from the status")

	_, ok = resolve(t, mxlv1alpha1.MxlFlowMirrorStatus{})
	assert.False(t, ok)
}

// Once status.targetInfo is no longer written, the mirror carries only
// the hash, and the source dials the descriptor from the object.
func TestReconcile_SourceDialsTheDescriptorObject(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	mirror := mirrorWithFinalizer(key.Name, key.Namespace, "node-a", "flow-1", "")
	mirror.Status.TargetInfoHash = mxlv1alpha1.TargetInfoHash("info-obj")
	scheme := newSourceTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlowMirror{}, &mxlv1alpha1.MxlFlow{}).
		WithObjects(mirror, mirrorTarget(key, "info-obj"), &mxlv1alpha1.MxlFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "flow-1"},
			Spec:       mxlv1alpha1.MxlFlowSpec{ID: "flow-1"},
		}).Build()
	opener := &fakeOpener{openFn: func(string, fabrics.Provider) (*sharedSource, error) { return &sharedSource{}, nil }}
	r := &SourceReconciler{Client: c, Scheme: scheme, NodeName: "node-a", opener: opener,
		FlushInterval: time.Hour, sources: map[types.NamespacedName]*sourceEntry{}, attempts: attemptTable[sourceAddInputs]{}}

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	t.Cleanup(func() { r.closeEntry(key) })
	assert.Equal(t, []string{"info-obj"}, opener.addedTargets())

	// A rotation writes a new descriptor and then its hash; the entry
	// added against the old one is replaced.
	ctx := context.Background()
	var obj mxlv1alpha1.MxlFlowMirrorTarget
	require.NoError(t, c.Get(ctx, key, &obj))
	obj.Spec.TargetInfo = "info-rotated"
	require.NoError(t, c.Update(ctx, &obj))
	var m mxlv1alpha1.MxlFlowMirror
	require.NoError(t, c.Get(ctx, key, &m))
	m.Status.TargetInfoHash = mxlv1alpha1.TargetInfoHash("info-rotated")
	require.NoError(t, c.Status().Update(ctx, &m))

	_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, []string{"info-obj", "info-rotated"}, opener.addedTargets())
}

// An entry is compared with the mirror by hash, so a rotation is seen
// whichever way either side published.
func TestServesDescriptorOf(t *testing.T) {
	e := &sourceEntry{infoStr: "a", infoHash: mxlv1alpha1.TargetInfoHash("a")}
	m := &mxlv1alpha1.MxlFlowMirror{}
	m.Status.TargetInfo = "a"
	assert.True(t, e.servesDescriptorOf(m))
	m.Status.TargetInfoHash = mxlv1alpha1.TargetInfoHash("a")
	assert.True(t, e.servesDescriptorOf(m))
	m.Status.TargetInfoHash = mxlv1alpha1.TargetInfoHash("b")
	assert.False(t, e.servesDescriptorOf(m), "a new hash is a rotation, whatever the legacy field says")
	m.Status = mxlv1alpha1.MxlFlowMirrorStatus{TargetInfo: "b"}
	assert.False(t, e.servesDescriptorOf(m))
}
