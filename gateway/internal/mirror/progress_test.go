package mirror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// callsOver reports how many of n consecutive ticks actually called
// MakeProgress, given a call that always returns err.
func callsOver(n int, err error) int {
	var p progressThrottle
	calls := 0
	for i := 0; i < n; i++ {
		p.runProgress(func() error {
			calls++
			return err
		}, nil)
	}
	return calls
}

func TestProgressThrottle_HealthyMirrorCallsOnEveryTick(t *testing.T) {
	// The throttle must be invisible while a mirror is delivering.
	assert.Equal(t, 1000, callsOver(1000, nil))
}

func TestProgressThrottle_IdleIsNotThrottled(t *testing.T) {
	// ErrNotReady is MakeProgress's ordinary "there is still progress
	// to be made" answer. Backing off on it would drain a mirror's
	// queued transfers at the backoff rate instead of the tick rate.
	assert.Equal(t, 1000, callsOver(1000, fabrics.ErrNotReady))
}

func TestProgressThrottle_TransientFailureBacksOff(t *testing.T) {
	// libmxl-fabrics throws "No more targets available" as an
	// interrupted exception on every call while the initiator holds no
	// target, and logs it at error level each time. On a 2ms tick that
	// was ~500 lines a second per mirror for as long as a peer gateway
	// took to come back.
	calls := callsOver(1000, mxl.ErrInterrupted)

	assert.Less(t, calls, 30,
		"a persistent transient failure must not be retried on every tick")
	// Doubling to the cap: 1,2,4,...,64 skipped ticks, then one call
	// per 65 ticks. Over 1000 ticks that is a couple of dozen calls.
	assert.Greater(t, calls, 5,
		"the backoff must stay bounded so recovery is noticed promptly")
}

func TestProgressThrottle_RecoveryRestoresTheTick(t *testing.T) {
	// The window closes when the reconciler rebuilds the initiator, and
	// the loop has to return to full rate immediately - a mirror still
	// pacing its progress calls after recovery would lag its producer.
	var p progressThrottle
	for i := 0; i < 200; i++ {
		p.runProgress(func() error { return mxl.ErrInterrupted }, nil)
	}
	require.Positive(t, p.skip, "expected to be mid-backoff")

	// Drain the outstanding skip, then one success clears it.
	for p.skip > 0 {
		p.runProgress(func() error { return mxl.ErrInterrupted }, nil)
		if p.skip == 0 {
			break
		}
	}
	for {
		if p.runProgress(func() error { return nil }, nil) {
			break
		}
	}

	calls := 0
	for i := 0; i < 100; i++ {
		p.runProgress(func() error {
			calls++
			return nil
		}, nil)
	}
	assert.Equal(t, 100, calls, "a healthy call must clear the backoff outright")
}

func TestProgressThrottle_FatalFailureIsNotThrottled(t *testing.T) {
	// An endpoint-lost error is the reconciler's business, not the
	// throttle's: swallowing ticks would delay the report that leads to
	// a rebuild.
	assert.Equal(t, 100, callsOver(100, errors.New("endpoint is gone")))
}

func TestProgressThrottle_ReportsEveryFailureItCalls(t *testing.T) {
	// Throttling the call throttles the log with it; a call that is
	// made must still be reported, so the condition stays visible at
	// V(1) rather than disappearing entirely.
	var p progressThrottle
	reported := 0
	made := 0
	for i := 0; i < 500; i++ {
		if p.runProgress(func() error { return mxl.ErrInterrupted }, func(error) { reported++ }) {
			made++
		}
	}
	assert.Equal(t, made, reported)
}
