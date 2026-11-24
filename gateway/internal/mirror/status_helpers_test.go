package mirror

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// A transition time says when the status last changed. Stamping now on
// every publish made it say when the gateway last wrote, which is
// every heartbeat, and gave nobody a way to tell how long a mirror has
// been degraded.
func TestCarryTransitionTime(t *testing.T) {
	since := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	live := &metav1.Condition{
		Type:               mxlv1alpha1.ConditionTypeTargetProgress,
		Status:             metav1.ConditionTrue,
		Reason:             mxlv1alpha1.ReasonHandshakeComplete,
		LastTransitionTime: since,
	}

	got := carryTransitionTime(live, metav1.Condition{
		Type:   mxlv1alpha1.ConditionTypeTargetProgress,
		Status: metav1.ConditionTrue,
		Reason: mxlv1alpha1.ReasonRecovered,
	})
	assert.True(t, got.LastTransitionTime.Equal(&since),
		"a new reason under the same status is not a transition")
	assert.Equal(t, mxlv1alpha1.ReasonRecovered, got.Reason,
		"the reason and message are the publish's own")

	got = carryTransitionTime(live, metav1.Condition{
		Type:   mxlv1alpha1.ConditionTypeTargetProgress,
		Status: metav1.ConditionFalse,
		Reason: mxlv1alpha1.ReasonNoGrains,
	})
	assert.WithinDuration(t, time.Now(), got.LastTransitionTime.Time, 2*time.Second,
		"a status change is a transition, stamped when it is published")

	got = carryTransitionTime(nil, metav1.Condition{
		Type:   mxlv1alpha1.ConditionTypeSourceProgress,
		Status: metav1.ConditionTrue,
	})
	assert.WithinDuration(t, time.Now(), got.LastTransitionTime.Time, 2*time.Second,
		"a first publish has nothing to carry")
	assert.Equal(t, mxlv1alpha1.ReasonHandshakeComplete, live.Reason, "the live condition is not modified")
}

// An apiserver that rejects writes, as one does once its etcd is out of
// space, got one retry per mirror per flusher tick from each gateway
// for as long as the outage lasted. The wait doubles from the flush
// interval to the status quantum, and one success clears it.
func TestPublishBackoff_GrowsToTheQuantumAndResetsOnSuccess(t *testing.T) {
	const interval = time.Second
	var b publishBackoff
	now := time.Now()
	require.True(t, b.due(now), "nothing has failed yet")

	var waits []time.Duration
	for range 8 {
		b.failed(now, interval)
		waits = append(waits, b.wait)
		assert.False(t, b.due(now), "a retry waits for its backoff")
		assert.True(t, b.due(now.Add(b.wait)))
	}
	want := make([]time.Duration, len(waits))
	for i := range want {
		want[i] = min(interval<<i, statusQuantum)
	}
	assert.Equal(t, want, waits, "the wait doubles and stops at the quantum")
	assert.Equal(t, statusQuantum, waits[len(waits)-1], "the cap is reached")

	b.succeeded()
	assert.True(t, b.due(now), "a success clears the backoff")
	b.failed(now, interval)
	assert.Equal(t, interval, b.wait, "a failure after a success starts over")
}

// The informer cache can trail the gateway's own last write. A flip back
// to the status the cache still shows is a transition all the same, and
// takes a new transition time rather than the one from before the flip
// it reverses.
func TestPublishedConditions_CarryFromTheLastWriteNotTheCache(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns1", Name: "m1"}
	old := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	cached := &metav1.Condition{Type: mxlv1alpha1.ConditionTypeTargetProgress,
		Status: metav1.ConditionTrue, LastTransitionTime: old}
	var p publishedConditions

	seeded := p.carry(key, cached, metav1.Condition{Type: cached.Type, Status: metav1.ConditionTrue})
	assert.True(t, seeded.LastTransitionTime.Equal(&old), "before any write, the cache seeds the carry")

	down := p.carry(key, cached, metav1.Condition{Type: cached.Type, Status: metav1.ConditionFalse})
	p.written(key, down)
	up := p.carry(key, cached, metav1.Condition{Type: cached.Type, Status: metav1.ConditionTrue})
	assert.WithinDuration(t, time.Now(), up.LastTransitionTime.Time, 2*time.Second,
		"False then True is a transition, whatever the cache still shows")

	p.forget(key)
	again := p.carry(key, cached, metav1.Condition{Type: cached.Type, Status: metav1.ConditionTrue})
	assert.True(t, again.LastTransitionTime.Equal(&old), "a forgotten mirror is seeded from the cache again")
}
