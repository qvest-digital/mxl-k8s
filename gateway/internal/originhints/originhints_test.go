package originhints

import (
	"context"
	"testing"
	"time"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/placeholder"
)

const flowID = "8c3e0b5f-6a1d-4f2e-b7c9-1d4a2e6f8b03"

const audioDef = `{"description":"origin hints test","format":"urn:x-nmos:format:audio",
"label":"origin hints test","id":"` + flowID + `",
"tags":{"urn:x-nmos:tag:grouphint/v1.0":["origin hints test:Audio"]},
"media_type":"audio/float32","sample_rate":{"numerator":48000},"channel_count":2,"bit_depth":32,"parents":[]}`

type handles struct {
	dir  string
	inst *mxl.Instance
}

func (h *handles) DomainPath() string { return h.dir }
func (h *handles) OnMxlfs() bool      { return false }
func (h *handles) MXL() *mxl.Instance { return h.inst }

// produce creates the flow in a fresh domain the way a producer does,
// with options.
func produce(t *testing.T, options string) *handles {
	t.Helper()
	dir := t.TempDir()
	inst, err := mxl.NewInstance(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inst.Close() })
	w, _, err := inst.NewWriterOpts(audioDef, options)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return &handles{dir: dir, inst: inst}
}

func at(sec int64) *metav1.Time { t := metav1.NewTime(time.Unix(sec, 0)); return &t }

func newPublisher(t *testing.T, h *handles, origin string, appeared *metav1.Time) (*Publisher, *int) {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(mxlv1alpha1.AddToScheme(s))
	flow := &mxlv1alpha1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: flowID},
		Spec:       mxlv1alpha1.MxlFlowSpec{ID: flowID, Definition: runtime.RawExtension{Raw: []byte(audioDef)}},
		Status: mxlv1alpha1.MxlFlowStatus{Locations: []mxlv1alpha1.MxlFlowLocation{
			{NodeName: origin, Phase: mxlv1alpha1.MxlFlowLocationOrigin, AppearedAt: appeared},
		}},
	}
	reads := 0
	p := &Publisher{
		Client:   fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&mxlv1alpha1.MxlFlow{}).WithObjects(flow).Build(),
		NodeName: "node-a",
		Domain:   func(context.Context, string) (placeholder.Handles, error) { return h, nil },
	}
	p.config = func(inst *mxl.Instance, id string) (mxl.FlowConfig, error) {
		reads++
		return readConfig(inst, id)
	}
	return p, &reads
}

func reconcileFlow(t *testing.T, p *Publisher) mxlv1alpha1.MxlFlowStatus {
	t.Helper()
	_, err := p.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: flowID}})
	require.NoError(t, err)
	var f mxlv1alpha1.MxlFlow
	require.NoError(t, p.Client.Get(context.Background(), types.NamespacedName{Name: flowID}, &f))
	return f.Status
}

// The hints exist only in the producer's flow, so the origin node's
// gateway reads them there and publishes them for the target's.
func TestPublishesTheHintsOfAFlowProducedHere(t *testing.T) {
	h := produce(t, `{"maxSyncBatchSizeHint":960,"maxCommitBatchSizeHint":480}`)
	p, _ := newPublisher(t, h, "node-a", at(1))

	st := reconcileFlow(t, p)

	assert.EqualValues(t, 960, st.MaxSyncBatchSizeHint)
	assert.EqualValues(t, 480, st.MaxCommitBatchSizeHint)
}

func TestPublishesLibmxlsDefaultsOfAFlowCreatedWithoutOptions(t *testing.T) {
	h := produce(t, "")
	p, _ := newPublisher(t, h, "node-a", at(1))

	st := reconcileFlow(t, p)

	assert.EqualValues(t, 480, st.MaxSyncBatchSizeHint)
	assert.EqualValues(t, 480, st.MaxCommitBatchSizeHint)
}

func TestLeavesAFlowProducedElsewhere(t *testing.T) {
	h := produce(t, `{"maxSyncBatchSizeHint":960,"maxCommitBatchSizeHint":480}`)
	p, reads := newPublisher(t, h, "node-b", at(1))

	st := reconcileFlow(t, p)

	assert.Zero(t, st.MaxSyncBatchSizeHint)
	assert.Zero(t, *reads)
}

// An agent rescan rewrites the MxlFlow without a new copy; the flow is
// read again only once the origin copy is a new one.
func TestReadsTheFlowOncePerOriginCopy(t *testing.T) {
	h := produce(t, `{"maxSyncBatchSizeHint":960,"maxCommitBatchSizeHint":480}`)
	p, reads := newPublisher(t, h, "node-a", at(1))

	reconcileFlow(t, p)
	reconcileFlow(t, p)
	require.Equal(t, 1, *reads)

	var f mxlv1alpha1.MxlFlow
	require.NoError(t, p.Client.Get(context.Background(), types.NamespacedName{Name: flowID}, &f))
	f.Status.Locations[0].AppearedAt = at(2)
	require.NoError(t, p.Client.Status().Update(context.Background(), &f))
	reconcileFlow(t, p)
	assert.Equal(t, 2, *reads)
}
