package flowpublisher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// seedFlowOnDisk writes a flow directory and returns the domain path.
func seedFlowOnDisk(t *testing.T) string {
	t.Helper()
	domain := t.TempDir()
	dir := filepath.Join(domain, validFlowID+".mxl-flow")
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, FlowDefName),
		[]byte(`{"id":"`+validFlowID+`"}`), 0o644))
	return domain
}

func TestRefreshLocalObservations_StampsLastObservedAndLeavesAppearedAt(t *testing.T) {
	// The whole point of splitting the two fields: LastObserved has to
	// move so it means "confirmed just now", and AppearedAt has to stay
	// put so the source gateway does not read the refresh as a producer
	// restart and reopen its reader once per rescan.
	scheme := newScheme(t)
	domain := seedFlowOnDisk(t)

	old := metav1.NewTime(metav1.Now().Add(-time.Hour))
	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{
					NodeName:     "n1",
					Phase:        mxlv1alpha1.MxlFlowLocationOrigin,
					LastObserved: &old,
					AppearedAt:   &old,
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(existing).Build()

	var before mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &before))
	appearedBefore := before.Status.Locations[0].AppearedAt
	observedBefore := before.Status.Locations[0].LastObserved

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.refreshLocalObservations(context.Background(),
		map[string]struct{}{validFlowID: {}}))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	loc := got.Status.Locations[0]

	require.NotNil(t, loc.LastObserved)
	assert.True(t, loc.LastObserved.After(observedBefore.Time),
		"LastObserved must advance, or it keeps advertising the moment the "+
			"copy was established rather than that it is still being confirmed")
	require.NotNil(t, loc.AppearedAt)
	assert.True(t, loc.AppearedAt.Equal(appearedBefore),
		"AppearedAt must not move: it is the source gateway's rotation "+
			"baseline, and restamping it turns every rescan into a reader reopen")
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationOrigin, loc.Phase,
		"the refresh must not touch phase; promoteStaleLocalOrigins owns that")
}

func TestRefreshLocalObservations_SkipsStaleAndForeignLocations(t *testing.T) {
	// A Stale entry means the copy is gone, so confirming it would be a
	// lie. Another node's entry belongs to that node's agent.
	scheme := newScheme(t)
	domain := seedFlowOnDisk(t)

	old := metav1.NewTime(metav1.Now().Add(-time.Hour))
	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{NodeName: "n1", Phase: mxlv1alpha1.MxlFlowLocationStale, LastObserved: nil},
				{NodeName: "n2", Phase: mxlv1alpha1.MxlFlowLocationOrigin, LastObserved: &old},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(existing).Build()

	// Read the seeded value back rather than comparing against old:
	// the fake client's codec truncates metav1.Time to second
	// precision on the round trip, so the stored value never equals
	// the full-precision one written here.
	var before mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &before))
	var n2Before *metav1.Time
	for _, loc := range before.Status.Locations {
		if loc.NodeName == "n2" {
			n2Before = loc.LastObserved
		}
	}
	require.NotNil(t, n2Before)

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.refreshLocalObservations(context.Background(),
		map[string]struct{}{validFlowID: {}}))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	for _, loc := range got.Status.Locations {
		switch loc.NodeName {
		case "n1":
			assert.Nil(t, loc.LastObserved, "a Stale location must not be confirmed")
		case "n2":
			assert.True(t, loc.LastObserved.Equal(n2Before), "another node's entry is not ours to stamp")
		}
	}
}

func TestPublishVanished_ClearsAppearedAtSoTheNextAppearanceRotates(t *testing.T) {
	// The gateway compares AppearedAt against the value it captured at
	// reader open. If a vanish left the old stamp behind, a producer
	// that restarts and republishes the same second would be
	// indistinguishable from one that never left.
	scheme := newScheme(t)
	domain := seedFlowOnDisk(t)

	old := metav1.NewTime(metav1.Now().Add(-time.Hour))
	existing := &mxlv1alpha1.MxlFlow{
		ObjectMeta: ObjectMeta(validFlowID),
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: validFlowID},
		Status: mxlv1alpha1.MxlFlowStatus{
			Locations: []mxlv1alpha1.MxlFlowLocation{
				{
					NodeName:     "n1",
					Phase:        mxlv1alpha1.MxlFlowLocationOrigin,
					LastObserved: &old,
					AppearedAt:   &old,
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(existing).Build()

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.PublishVanished(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	require.Len(t, got.Status.Locations, 1)
	assert.Equal(t, mxlv1alpha1.MxlFlowLocationStale, got.Status.Locations[0].Phase)
	assert.Nil(t, got.Status.Locations[0].AppearedAt,
		"a vanished copy carries no appearance, so the next one reads as a rotation")
}

func TestPublishAppeared_StampsBothTimestamps(t *testing.T) {
	scheme := newScheme(t)
	domain := seedFlowOnDisk(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).Build()

	p := &Publisher{Client: c, DomainPath: domain, NodeName: "n1"}
	require.NoError(t, p.PublishAppeared(context.Background(), validFlowID+".mxl-flow"))

	var got mxlv1alpha1.MxlFlow
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: validFlowID}, &got))
	require.Len(t, got.Status.Locations, 1)
	assert.NotNil(t, got.Status.Locations[0].LastObserved)
	assert.NotNil(t, got.Status.Locations[0].AppearedAt,
		"an appearance is what the rotation detector keys on; without it a "+
			"reader opened now can never detect a later rotation")
}
