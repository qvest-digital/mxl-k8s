package merge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ring simulates one member's ring: committed grains by index, with the
// slot showing the last index written there.
type ring struct {
	grains  uint
	head    uint64
	slots   map[uint]Grain
	partial bool // commit grains with half their slices
}

func newRing(grains uint) *ring { return &ring{grains: grains, slots: map[uint]Grain{}} }

func (r *ring) commit(index uint64) {
	g := Grain{Index: index, TotalSlices: 10, ValidSlices: 10}
	if r.partial {
		g.ValidSlices = 5
	}
	r.slots[uint(index%uint64(r.grains))] = g
	r.head = index
}

func (r *ring) member() Member {
	return Member{Head: r.head, Grain: func(slot uint) Grain { return r.slots[slot] }}
}

// drive runs both rings and the policy over [start, end), dropping what
// drop says, and returns, per published index, the source it showed.
func drive(t *testing.T, d *Discrete, rings []*ring, start, end uint64, drop func(m int, i uint64) bool) map[uint64]int {
	t.Helper()
	shown := map[uint64]int{}
	now := time.Unix(0, 0)
	for i := start; i < end; i++ {
		for m, r := range rings {
			if !drop(m, i) {
				r.commit(i)
			}
		}
		ms := make([]Member, len(rings))
		for m, r := range rings {
			ms[m] = r.member()
		}
		now = now.Add(20 * time.Millisecond)
		before := d.Head
		_, head, _ := d.Step(ms, now)
		for j := before + 1; j <= head; j++ {
			shown[j] = d.Current[uint(j%uint64(d.Grains))]
		}
	}
	return shown
}

func TestDiscreteServesEveryGrainSomeMemberHas(t *testing.T) {
	a, b := newRing(20), newRing(20)
	d := NewDiscrete(20, 2)
	d.Ordered, d.Skew = true, 2
	// Staggered outages: a misses [100,150), b misses [200,250).
	drop := func(m int, i uint64) bool {
		return (m == 0 && i >= 100 && i < 150) || (m == 1 && i >= 200 && i < 250)
	}
	shown := drive(t, d, []*ring{a, b}, 10, 400, drop)

	for i := uint64(20); i < 395; i++ {
		src, ok := shown[i]
		require.True(t, ok, "index %d was never published", i)
		require.NotEqual(t, 2, src, "index %d published INVALID though a member had it", i)
		require.False(t, drop(src, i), "index %d shown from member %d, which dropped it", i, src)
	}
}

func TestDiscretePublishesInvalidOnlyWhereEveryMemberDropped(t *testing.T) {
	a, b := newRing(20), newRing(20)
	d := NewDiscrete(20, 2)
	d.Ordered, d.Skew = true, 2
	drop := func(m int, i uint64) bool { return i == 120 || (m == 0 && i == 130) }
	shown := drive(t, d, []*ring{a, b}, 10, 200, drop)

	require.Equal(t, 2, shown[120], "an index both members dropped is shown as a member's grain")
	require.Equal(t, 1, shown[130])
}

// With both members healthy an ordered set never leaves its primary, so
// readers never pay a switch.
func TestDiscreteOrderedStaysOnThePrimary(t *testing.T) {
	a, b := newRing(20), newRing(20)
	d := NewDiscrete(20, 2)
	d.Ordered, d.Skew = true, 2
	switches := 0
	now := time.Unix(0, 0)
	for i := uint64(10); i < 300; i++ {
		// b commits first every other grain, which must not move the
		// set to b: the primary is only late if it misses the tolerance.
		if i%2 == 0 {
			b.commit(i)
		} else {
			a.commit(i)
		}
		now = now.Add(time.Millisecond)
		sel, _, _ := d.Step([]Member{a.member(), b.member()}, now)
		if i%2 == 0 {
			a.commit(i)
		} else {
			b.commit(i)
		}
		sel2, _, _ := d.Step([]Member{a.member(), b.member()}, now)
		switches += len(sel) + len(sel2)
	}
	require.LessOrEqual(t, switches, 20, "one select per slot at startup, none after")
}

// A data grain's valid slices are its length, so a committed data grain
// counts as complete; a video grain with half its slices does not.
func TestDiscreteJudgesDataGrainsByCommit(t *testing.T) {
	a := newRing(20)
	a.partial = true
	video := NewDiscrete(20, 1)
	video.Skew = 100
	data := NewDiscrete(20, 1)
	data.Data, data.Skew = true, 100
	for i := uint64(10); i < 30; i++ {
		a.commit(i)
	}
	_, vh, _ := video.Step([]Member{a.member()}, time.Unix(1, 0))
	_, dh, _ := data.Step([]Member{a.member()}, time.Unix(1, 0))
	require.Equal(t, uint64(28), vh, "a partial video grain was published")
	require.Equal(t, uint64(29), dh, "a committed data grain was held back")
}

func TestAudioServesEverySampleSomeMemberHoldsWhole(t *testing.T) {
	const batch = 480
	a := NewAudio(2, 38, 2)
	a.Skew = 2048
	held := [2]map[uint64]bool{{}, {}}
	// a misses batches 100-119, b misses batches 300-319.
	drop := func(m int, b uint64) bool {
		return (m == 0 && b >= 100 && b < 120) || (m == 1 && b >= 300 && b < 320)
	}
	shownPage := map[uint64]int{}
	now := time.Unix(0, 0)
	for b := uint64(10); b < 500; b++ {
		for m := 0; m < 2; m++ {
			if !drop(m, b) {
				a.Committed(m, b*batch, (b+1)*batch)
				for s := b * batch; s < (b+1)*batch; s++ {
					held[m][s] = true
				}
			}
		}
		now = now.Add(10 * time.Millisecond)
		before := a.Head
		_, head, _ := a.Step(now)
		for s := before; s < head; s++ {
			shownPage[s] = a.Current[(s/PageSamples)%a.Pages]
		}
	}
	for s := uint64(20 * batch); s < 480*batch; s++ {
		src, ok := shownPage[s]
		require.True(t, ok, "sample %d never published", s)
		require.NotEqual(t, 2, src, "sample %d published as silence though a member held its page", s)
		require.True(t, held[src][s], "sample %d shown from member %d, which lacks it", s, src)
	}
}

func TestAudioSilencesAPageNoMemberHoldsWhole(t *testing.T) {
	a := NewAudio(2, 38, 2)
	a.Skew = 2048
	// Page 50 has a gap in both members, at different samples.
	for m := 0; m < 2; m++ {
		for s := uint64(40 * PageSamples); s < 60*PageSamples; s += 256 {
			gap := (m == 0 && s == 50*PageSamples) || (m == 1 && s == 50*PageSamples+512)
			if !gap {
				a.Committed(m, s, s+256)
			}
		}
	}
	a.Step(time.Unix(1, 0))
	require.Equal(t, 2, a.Current[50%a.Pages])
	require.NotEqual(t, 2, a.Current[49%a.Pages])
}
