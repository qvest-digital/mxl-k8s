package leasecheck

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testFlowID = "11111111-2222-3333-4444-555555555555"
	testNode   = "n1"
)

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	return s
}

func TestChecker_FreshLeaseReturnsTrue(t *testing.T) {
	now := metav1.NewMicroTime(time.Now())
	duration := int32(30)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: LeaseNamespace,
			Name:      LeaseName(testFlowID, testNode),
		},
		Spec: coordinationv1.LeaseSpec{
			LeaseDurationSeconds: &duration,
			RenewTime:            &now,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(lease).Build()
	chk := &Checker{Client: c}
	fresh, err := chk.IsFresh(context.Background(), testFlowID, testNode)
	require.NoError(t, err)
	assert.True(t, fresh)
}

func TestChecker_ExpiredLeaseReturnsFalse(t *testing.T) {
	old := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
	duration := int32(30)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: LeaseNamespace,
			Name:      LeaseName(testFlowID, testNode),
		},
		Spec: coordinationv1.LeaseSpec{
			LeaseDurationSeconds: &duration,
			RenewTime:            &old,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(lease).Build()
	chk := &Checker{Client: c}
	fresh, err := chk.IsFresh(context.Background(), testFlowID, testNode)
	require.NoError(t, err)
	assert.False(t, fresh,
		"a Lease whose RenewTime+duration sits in the past must be reported "+
			"stale; the receiver uses this to demote a partitioned Origin")
}

func TestChecker_MissingLeaseReturnsFalseWithoutError(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme(t)).Build()
	chk := &Checker{Client: c}
	fresh, err := chk.IsFresh(context.Background(), testFlowID, testNode)
	require.NoError(t, err,
		"a missing Lease is the normal startup state, not a controller "+
			"error; otherwise the reconcile loop would back off on every "+
			"unbacked Origin")
	assert.False(t, fresh)
}

func TestLeaseName_MatchesAgentSideConvention(t *testing.T) {
	// Drift between the operator-side and agent-side LeaseName would
	// turn every IsFresh call into a not-found and silently demote
	// every Origin in the cluster. Pin the format here so any future
	// renaming surfaces with a failing test on both sides.
	assert.Equal(t, "mxl-flow-"+testFlowID+"-"+testNode, LeaseName(testFlowID, testNode))
}
