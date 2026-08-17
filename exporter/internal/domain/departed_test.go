package domain

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// removeFlowDir takes the flow out of the domain the way libmxl's
// garbage collector does, by unlinking its directory. The writer stays
// open, so nothing about the flow header changes: only the directory
// entry the domain lists is gone.
func removeFlowDir(t *testing.T, d *Domain, id string) {
	t.Helper()
	require.NoError(t, os.RemoveAll(filepath.Join(d.Path(), id+flowDirSuffix)))
	require.NoError(t, d.Scan())
}

func TestObserveNeverReportsADepartedFlowActive(t *testing.T) {
	// mxl_flow_active and mxl_flow_present are read together, and their
	// sum is what tells a flow being written from a writer that stopped
	// from a flow that left the domain. That only separates the three if
	// a departed flow reports inactive, and its last write says the
	// opposite: the flow was written moments before it was removed, so
	// the activity window it is judged against has not expired.
	d := newObservedFlow(t)
	require.True(t, d.Observe()[0].Active)

	removeFlowDir(t, d, activeTestFlowID)

	obs := d.Observe()
	require.Len(t, obs, 1, "a departed flow keeps its series until its lifetime expires")
	require.False(t, obs[0].Present)
	require.False(t, obs[0].Active,
		"a flow that is no longer in the domain cannot be active, however "+
			"recent its last write; reporting it active would leave a gone "+
			"flow indistinguishable from a live one")
}

func TestScanForgetsADepartedFlowAfterItsLifetime(t *testing.T) {
	// The series a departed flow keeps are an after-image, so that a flow
	// ending between two scrapes leaves a record rather than a gap. An
	// after-image that never expires is not a record but a leak: every
	// flow the node ever carried stays in the metric set, and anything
	// reading it has to tell the domain as it is from the domain as it
	// was.
	d := newObservedFlowWithLifetime(t, 50*time.Millisecond)
	require.Len(t, d.Observe(), 1)

	removeFlowDir(t, d, activeTestFlowID)
	require.Len(t, d.Observe(), 1, "the after-image outlives the flow directory")

	require.Eventually(t, func() bool {
		if err := d.Scan(); err != nil {
			return false
		}
		return len(d.Observe()) == 0
	}, 5*time.Second, 10*time.Millisecond,
		"a flow gone for longer than the lifetime must leave the metric set")
}
