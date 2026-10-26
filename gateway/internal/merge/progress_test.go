package merge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A producer whose pod runs on but commits nothing keeps every object of
// its flow as it was; its head standing still is the only sign. A grain
// ring's head moves a grain at a time, a channel buffer's by the samples
// of a batch, and a late discrete commit steps it back: each is a move.
// Each change is reported once, for the gateway to write it once.
func TestProgressFindsAMemberWhoseHeadStandsStill(t *testing.T) {
	t0 := time.Unix(1000, 0)
	p := newProgress([]uint64{100, 48000}, t0)
	audio := func(i int) uint64 { return 48000 + uint64(i)*480 }
	at := func(i int) time.Time { return t0.Add(time.Duration(i) * 40 * time.Millisecond) }

	for i := 1; at(i).Before(t0.Add(StallAfter)); i++ {
		require.False(t, p.observe([]uint64{100, audio(i)}, at(i)), "a member counted as stalled before StallAfter")
	}
	require.Empty(t, p.stalled())

	n := int(StallAfter / (40 * time.Millisecond))
	require.True(t, p.observe([]uint64{100, audio(n)}, at(n)))
	require.Equal(t, []int{0}, p.stalled(), "the grain member stood still, the audio one moved")
	require.False(t, p.observe([]uint64{100, audio(n + 1)}, at(n+1)), "an unchanged stall was reported again")

	require.True(t, p.observe([]uint64{99, audio(n + 2)}, at(n+2)))
	require.Empty(t, p.stalled(), "a head stepped back was not counted as a move")

	later := at(n + 2).Add(StallAfter)
	require.True(t, p.observe([]uint64{99, audio(n + 2)}, later))
	require.Equal(t, []int{0, 1}, p.stalled(), "an audio member whose samples stopped was not found")
}
