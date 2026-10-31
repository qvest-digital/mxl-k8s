package mirror

import (
	"testing"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
)

// An initiator's write leaves the source slot's header in the target's
// slot: index, flags and valid slices. OpenGrain clears the invalid flag
// and the commit used to mark the grain whole and valid, so a grain its
// producer committed invalid, or a slot a later lap of the source wrote
// into, reached every reader on the target as a valid grain with an
// earlier or a later picture in it. Such an arrival is committed
// invalid; it still completes the grain, so the next one commits past
// it.
func TestCommitArrivedGrain_InvalidArrivalIsCommittedInvalid(t *testing.T) {
	const idx = 1000
	for _, tc := range []struct {
		name    string
		def, id string
		lap     bool // the slot carries the header of idx one ring lap later
		flags   uint32
		reason  invalidReason
	}{
		{name: "video", def: sharedFlowVideoDef, id: "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d34"},
		{name: "video flagged invalid", def: sharedFlowVideoDef, id: "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d34",
			flags: mxl.GrainFlagInvalid, reason: invalidFromSource},
		{name: "video other index", def: sharedFlowVideoDef, id: "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d34",
			lap: true, reason: invalidIndexMismatch},
		{name: "data flagged invalid", def: sharedFlowDataDef, id: "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d35",
			flags: mxl.GrainFlagInvalid, reason: invalidFromSource},
		{name: "data other index", def: sharedFlowDataDef, id: "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d35",
			lap: true, reason: invalidIndexMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inst, err := mxl.NewInstance(t.TempDir(), "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = inst.Close() })
			w, _, err := inst.NewWriter(tc.def)
			require.NoError(t, err)
			t.Cleanup(func() { _ = w.Close() })
			r, err := inst.NewReader(tc.id)
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Close() })

			// A partial commit writes the header an arrival carries
			// without completing the grain, so the arrival still opens.
			slot := uint64(idx)
			if tc.lap {
				slot += uint64(w.Config().Discrete.GrainCount)
			}
			ga, err := w.OpenGrain(slot)
			require.NoError(t, err)
			require.NoError(t, ga.Commit(64, tc.flags))

			_, reason, err := commitArrivedGrain(w, idx)
			require.NoError(t, err)
			require.Equal(t, tc.reason, reason)

			g, err := r.GetGrainSliceNonBlocking(idx, 0)
			require.NoError(t, err)
			require.Equal(t, uint64(idx), g.Index)
			require.Equal(t, tc.reason != "", g.Invalid(), "what a reader on the target is served")

			_, _, err = commitArrivedGrain(w, idx+1)
			require.NoError(t, err, "the next arrival commits past it")
		})
	}
}
