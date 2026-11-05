package mirror

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/go-mxl/fabrics"
	"github.com/qvest-digital/go-mxl/mxl"
)

// recordingDriver records which MakeProgress variant was called.
type recordingDriver struct {
	blocking    []time.Duration
	nonBlocking int
}

func (d *recordingDriver) MakeProgress(timeout time.Duration) error {
	d.blocking = append(d.blocking, timeout)
	return nil
}

func (d *recordingDriver) MakeProgressNonBlocking() error {
	d.nonBlocking++
	return nil
}

// The interface's capabilities decide the variant, as makeProgress in
// tools/mxl-fabrics-demo/demo.cpp does, and progressFuncFor takes no
// provider: efa blocks like every other provider once its interface
// reports MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS.
func TestProgressFuncFor_BlocksWhereTheInterfaceDoes(t *testing.T) {
	d := &recordingDriver{}
	require.NoError(t, progressFuncFor(d, fabrics.InterfaceCapRemoteWrite|fabrics.InterfaceCapBlockingOperations)())
	assert.Equal(t, []time.Duration{progressTimeout}, d.blocking)
	assert.Zero(t, d.nonBlocking)

	d = &recordingDriver{}
	require.NoError(t, progressFuncFor(d, fabrics.InterfaceCapRemoteWrite)())
	assert.Empty(t, d.blocking, "an interface without blocking operations is polled")
	assert.Equal(t, 1, d.nonBlocking)
}

// scriptedProgress answers MakeProgress from a list, then OK.
func scriptedProgress(answers ...error) (ProgressFunc, *int) {
	calls := 0
	return func() error {
		calls++
		if calls <= len(answers) {
			return answers[calls-1]
		}
		return nil
	}, &calls
}

// noPoll fails the test when awaitTransfer polls, which it does only
// after an interrupt.
func noPoll(t *testing.T) ProgressFunc {
	return func() error {
		t.Error("polled without an interrupt")
		return nil
	}
}

func TestAwaitTransfer_ProgressesUntilNotReadyStops(t *testing.T) {
	progress, calls := scriptedProgress(fabrics.ErrNotReady, fabrics.ErrNotReady, fabrics.ErrNotReady)
	late, err := awaitTransfer(context.Background(), progress, noPoll(t), time.Now(), time.Second)
	require.NoError(t, err)
	assert.False(t, late)
	assert.Equal(t, 4, *calls, "a transfer is complete only once MakeProgress stops returning not-ready")
}

func TestAwaitTransfer_ReportsAMissedDeadline(t *testing.T) {
	progress, _ := scriptedProgress(fabrics.ErrNotReady)
	late, err := awaitTransfer(context.Background(), progress, noPoll(t), time.Now().Add(-50*time.Millisecond), 20*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, late)
}

func TestAwaitTransfer_ZeroDeadlineIsNeverLate(t *testing.T) {
	progress, _ := scriptedProgress()
	late, err := awaitTransfer(context.Background(), progress, noPoll(t), time.Now().Add(-time.Second), 0)
	require.NoError(t, err)
	assert.False(t, late, "a flow without a usable rate has no deadline to miss")
}

// A signal interrupts a blocking progress call, and only the blocking
// call (blockOnCQ in RCInitiator.cpp and RDMInitiator.cpp). The poll
// that checks it is not interrupted by one, so the transfer goes on.
func TestAwaitTransfer_ChecksAnInterruptWithAPoll(t *testing.T) {
	progress, calls := scriptedProgress(mxl.ErrInterrupted, fabrics.ErrNotReady)
	poll, polls := scriptedProgress(fabrics.ErrNotReady)
	_, err := awaitTransfer(context.Background(), progress, poll, time.Now(), time.Second)
	require.NoError(t, err, "a signal is not a dropped target")
	assert.Equal(t, 1, *polls)
	assert.Equal(t, 3, *calls)
}

// A connection-oriented initiator that holds no target any more answers
// interrupted to the poll as well: RCInitiator::makeProgress throws
// "No more targets available".
func TestAwaitTransfer_AnInterruptedPollLosesTheTarget(t *testing.T) {
	progress, _ := scriptedProgress(mxl.ErrInterrupted)
	poll, polls := scriptedProgress(mxl.ErrInterrupted)
	_, err := awaitTransfer(context.Background(), progress, poll, time.Now(), time.Second)
	require.ErrorIs(t, err, errTargetLost)
	assert.Equal(t, 1, *polls)
}

func TestAwaitTransfer_EndpointErrorLosesTheTarget(t *testing.T) {
	progress, _ := scriptedProgress(errors.New("endpoint gone"))
	_, err := awaitTransfer(context.Background(), progress, noPoll(t), time.Now(), time.Second)
	require.ErrorIs(t, err, errTargetLost)
}

// RDMInitiator::makeProgress never reports a lost target, so on efa and
// shm the bound on an outstanding transfer is what finds it.
func TestAwaitTransfer_AbandonsATransferThatNeverCompletes(t *testing.T) {
	progress, _ := scriptedProgress(fabrics.ErrNotReady)
	_, err := awaitTransfer(context.Background(), progress, noPoll(t), time.Now().Add(-abandonAfter-time.Millisecond), time.Second)
	require.ErrorIs(t, err, errTargetLost)
}

func TestAwaitTransfer_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	progress, calls := scriptedProgress()
	_, err := awaitTransfer(ctx, progress, noPoll(t), time.Now(), time.Second)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, *calls)
}

func TestDefaultSamplePollInterval_48kHz(t *testing.T) {
	// 48000/1 with a 480-sample batch spans 10 ms, and the loop polls
	// twice within it.
	assert.Equal(t, 5*time.Millisecond, defaultSamplePollInterval(mxl.Rational{Num: 48000, Den: 1}, 480))
}

// A poll period equal to the batch period lets the two clocks drift
// against each other, so one poll finds nothing and the next finds two
// batches: every sample arrives, but in pairs at twice the interval,
// which a reader sees as gaps.
func TestDefaultSamplePollInterval_OversamplesTheCommitCadence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rate  mxl.Rational
		batch uint64
	}{
		{"48kHz/480", mxl.Rational{Num: 48000, Den: 1}, 480},
		{"48kHz/1024", mxl.Rational{Num: 48000, Den: 1}, 1024},
		{"96kHz/480", mxl.Rational{Num: 96000, Den: 1}, 480},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batchSpan := time.Duration(int64(time.Second) *
				int64(tc.rate.Den) * int64(tc.batch) / int64(tc.rate.Num))
			got := defaultSamplePollInterval(tc.rate, tc.batch)

			assert.Positive(t, got)
			assert.LessOrEqual(t, got*2, batchSpan, "the loop must poll at least twice per committed batch")
		})
	}
}

func TestDefaultSamplePollInterval_UnusableInputsFallBack(t *testing.T) {
	assert.Zero(t, defaultSamplePollInterval(mxl.Rational{Num: 48000, Den: 1}, 0))
	assert.Zero(t, defaultSamplePollInterval(mxl.Rational{}, 480))
}
