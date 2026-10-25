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

// Mirrors over a network deliver the members' grains several
// milliseconds apart, in either order. Within the skew budget that
// jitter must not move the set off its primary: each move remaps the
// slots in every reader. Only a primary late beyond the tolerance does.
func TestDiscreteRidesOutJitterWithinTheBudget(t *testing.T) {
	a, b := newRing(20), newRing(20)
	d := NewDiscrete(20, 2)
	d.Ordered, d.Skew, d.Tolerance = true, 2, ToleranceFor(40*time.Millisecond)
	now := time.Unix(0, 0)
	a.commit(9) // a arrives first once, and becomes the primary
	d.Step([]Member{a.member(), b.member()}, now)
	b.commit(9)
	switches := 0
	for i := uint64(10); i < 300; i++ {
		now = now.Add(20 * time.Millisecond)
		late := time.Duration(i%3) * 6 * time.Millisecond // a is 0, 6 or 12 ms behind b
		b.commit(i)
		sel, _, wait := d.Step([]Member{a.member(), b.member()}, now)
		switches += len(sel)
		// The engine wakes when the wait runs out, before a arrives.
		if wait > 0 && wait < late {
			sel, _, _ = d.Step([]Member{a.member(), b.member()}, now.Add(wait))
			switches += len(sel)
		}
		a.commit(i)
		sel, _, _ = d.Step([]Member{a.member(), b.member()}, now.Add(late))
		switches += len(sel)
	}
	require.Equal(t, 0, d.Primary)
	require.LessOrEqual(t, switches, 20, "one select per slot at startup, none after")

	// A primary that stops is replaced once the tolerance has passed.
	for i := uint64(300); i < 340; i++ {
		now = now.Add(20 * time.Millisecond)
		b.commit(i)
		if _, _, wait := d.Step([]Member{a.member(), b.member()}, now); wait > 0 {
			d.Step([]Member{a.member(), b.member()}, now.Add(wait))
		}
	}
	require.Equal(t, 1, d.Primary)
	require.Equal(t, uint64(339), d.Head)
}

// Mirrors over separate paths deliver the members a little apart. An
// ordered set starts on its first member, even when the second's grain
// is there first, as long as the first follows within the tolerance;
// starting on whichever arrived first left the set on the second member
// for as long as that one delivered.
func TestDiscreteOrderedStartsOnTheFirstMember(t *testing.T) {
	for name, tc := range map[string]struct {
		ordered bool
		late    time.Duration // how long after b's grain a's arrives
		want    int
	}{
		"ordered, first member within the tolerance": {true, 3 * time.Millisecond, 0},
		"ordered, first member beyond the tolerance": {true, 30 * time.Millisecond, 1},
		"first arrival": {false, 3 * time.Millisecond, 1},
	} {
		t.Run(name, func(t *testing.T) {
			a, b := newRing(20), newRing(20)
			d := NewDiscrete(20, 2)
			d.Ordered, d.Skew, d.Tolerance = tc.ordered, 2, ToleranceFor(40*time.Millisecond)
			now := time.Unix(0, 0)
			b.commit(10)
			_, _, wait := d.Step([]Member{a.member(), b.member()}, now)
			if wait > 0 && wait < tc.late {
				d.Step([]Member{a.member(), b.member()}, now.Add(wait))
			}
			a.commit(10)
			d.Step([]Member{a.member(), b.member()}, now.Add(tc.late))
			require.Equal(t, uint64(10), d.Head)
			require.Equal(t, tc.want, d.Current[10])
			if tc.ordered {
				require.Equal(t, tc.want, d.Primary)
			}
		})
	}
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

// The engine starts with whatever arrives first, mostly in the middle of
// a page. Every sample it publishes must still come from a member.
func TestAudioStartsAtAPageBoundaryWithoutSilence(t *testing.T) {
	const batch = 480
	a := NewAudio(1, 38, 1)
	a.Skew = 2048
	var shown []int
	first := uint64(0)
	for b := uint64(10); b < 60; b++ { // 4800 is in the middle of page 4
		a.Committed(0, b*batch, (b+1)*batch)
		before := a.Head
		_, head, _ := a.Step(time.Unix(0, 0))
		if first == 0 && head > before {
			first = before
		}
		for s := before; s < head; s++ {
			shown = append(shown, a.Current[(s/PageSamples)%a.Pages])
		}
	}
	require.NotEmpty(t, shown)
	require.Zero(t, first%PageSamples, "publishing started mid-page, at %d", first)
	require.NotContains(t, shown, 1, "a published sample was silence though the member held it")
}

// Audio starts on its first member in the same way, rather than on the
// member whose batch was reported first.
func TestAudioStartsOnTheFirstMember(t *testing.T) {
	for name, tc := range map[string]struct {
		late time.Duration
		want int
	}{
		"within the tolerance": {3 * time.Millisecond, 0},
		"beyond the tolerance": {30 * time.Millisecond, 1},
	} {
		t.Run(name, func(t *testing.T) {
			a := NewAudio(2, 38, 2)
			a.Skew, a.Tolerance = 2048, ToleranceFor(40*time.Millisecond)
			now := time.Unix(0, 0)
			a.Committed(1, 10*PageSamples, 11*PageSamples)
			if _, _, wait := a.Step(now); wait > 0 && wait < tc.late {
				a.Step(now.Add(wait))
			}
			a.Committed(0, 10*PageSamples, 11*PageSamples)
			a.Step(now.Add(tc.late))
			require.Equal(t, uint64(11*PageSamples), a.Head)
			require.Equal(t, tc.want, a.Current[10])
			require.Equal(t, tc.want, a.Primary)
		})
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

// Mirrored members commit a grain after its period, so in steady state the
// output's head sits one grain behind the index due. A member writes the
// due index next, into a slot the output no longer offers, so nothing may
// move: every select zaps the slot in every reader's mapping.
func TestDiscreteRetiresNothingWhileTheOutputTrailsTheClockByOne(t *testing.T) {
	const grains = 20
	a, b := newRing(grains), newRing(grains)
	d := NewDiscrete(grains, 2)
	d.Ordered, d.Skew = true, 8
	now := time.Unix(0, 0)
	for i := uint64(100); i < 160; i++ {
		a.commit(i)
		b.commit(i)
		ms := []Member{a.member(), b.member()}
		now = now.Add(20 * time.Millisecond)
		d.Step(ms, now)
		require.Equal(t, i, d.Head)
		require.Empty(t, d.Retire(ms, i+1), "an output one grain behind the clock moved slots at index %d", i)
	}
}

// While an index both members dropped holds the output back by up to the
// skew, the members keep writing, each grain into the slot of the index
// a ring period back. Every grain the output still offers its readers
// must stay the grain of that index through the members' next write.
func TestDiscreteRetiresSlotsTheMembersOverwriteWhileItLags(t *testing.T) {
	const grains = 20
	a, b := newRing(grains), newRing(grains)
	d := NewDiscrete(grains, 2)
	d.Ordered, d.Skew = true, 8
	now := time.Unix(0, 0)
	retired := 0
	for i := uint64(100); i < 160; i++ {
		// The members write at the clock, so the index due is their head.
		if i != 120 {
			a.commit(i)
			b.commit(i)
		}
		ms := []Member{a.member(), b.member()}
		now = now.Add(20 * time.Millisecond)
		d.Step(ms, now)
		sel := d.Retire(ms, i)
		if d.Head == i {
			require.Empty(t, sel, "an output keeping up with the clock moved slots at index %d", i)
		}
		retired += len(sel)

		// The engine passes at the boundary to i+1, then the members
		// write i+1, at the clock.
		now = now.Add(time.Microsecond)
		d.Step(ms, now)
		retired += len(d.Retire(ms, i+1))
		next := map[*ring]Grain{}
		for _, r := range []*ring{a, b} {
			next[r] = r.slots[uint((i+1)%grains)]
			r.slots[uint((i+1)%grains)] = Grain{Index: i + 1, TotalSlices: 10, ValidSlices: 10}
		}
		for j := max(d.Head+2, grains) - grains; j <= d.Head; j++ {
			src := d.Current[uint(j%grains)]
			if src == d.Placeholder {
				continue
			}
			g := []*ring{a, b}[src].slots[uint(j%grains)]
			require.Equal(t, j, g.Index, "after index %d the output offers index %d from member %d, whose slot holds %d",
				i, j, src, g.Index)
		}
		for r, g := range next {
			r.slots[uint((i+1)%grains)] = g
		}
	}
	require.Equal(t, uint64(159), d.Head)
	require.NotZero(t, retired, "the output never lagged")
}

// A member that commits each grain a period after its boundary is never
// about to write the index due. Taken for one, it retired the oldest
// grain readers trust on every grain, a remap in every reader each
// time. After a loss it writes on at its own lag, and the slot that
// write overwrites still moves first.
func TestDiscreteRetiresForAMemberBehindTheClockOnlyWhatItOverwrites(t *testing.T) {
	const grains = 20
	a, b := newRing(grains), newRing(grains)
	d := NewDiscrete(grains, 2)
	d.Ordered, d.Skew = true, 8
	now := time.Unix(0, 0)
	retired := 0
	for i := uint64(100); i < 160; i++ {
		// i is committed in the period of i+1, which wakes a pass.
		if i != 140 {
			a.commit(i)
			b.commit(i)
		}
		ms := []Member{a.member(), b.member()}
		now = now.Add(20 * time.Millisecond)
		d.Step(ms, now)
		sel := d.Retire(ms, i+1)
		// The pass before the boundary of i+2, after which i+1 is written.
		sel = append(sel, d.Retire(ms, i+2)...)
		if d.Head == i {
			require.Empty(t, sel, "an output keeping up with its members moved slots at index %d", i)
		}
		retired += len(sel)

		next := map[*ring]Grain{}
		for _, r := range []*ring{a, b} {
			next[r] = r.slots[uint((i+1)%grains)]
			r.slots[uint((i+1)%grains)] = Grain{Index: i + 1, TotalSlices: 10, ValidSlices: 10}
		}
		for j := max(d.Head+2, grains) - grains; j <= d.Head; j++ {
			src := d.Current[uint(j%grains)]
			if src == d.Placeholder {
				continue
			}
			g := []*ring{a, b}[src].slots[uint(j%grains)]
			require.Equal(t, j, g.Index, "after index %d the output offers index %d from member %d, whose slot holds %d",
				i, j, src, g.Index)
		}
		for r, g := range next {
			r.slots[uint((i+1)%grains)] = g
		}
	}
	require.Equal(t, uint64(159), d.Head)
	require.NotZero(t, retired, "the output never lagged")
}

// A member writing ahead of the clock overwrites sooner than one at the
// clock: a slot it serves moves to the member that still holds the index,
// and only to INVALID where both would overwrite it.
func TestDiscreteRetiresToTheMemberThatStillHoldsTheIndex(t *testing.T) {
	const grains = 20
	a, b := newRing(grains), newRing(grains)
	for i := uint64(90); i <= 128; i++ {
		if i != 120 {
			a.commit(i)
		}
	}
	for i := uint64(90); i <= 123; i++ {
		if i != 120 {
			b.commit(i)
		}
	}
	d := NewDiscrete(grains, 2)
	d.Ordered, d.Skew, d.Head, d.Primary = true, 10, 119, 0
	for i := range d.Current {
		d.Current[i] = 0
	}
	sel := d.Retire([]Member{a.member(), b.member()}, 123)

	// a writes 129 next, into the slot of 109; b writes 124, into 104's.
	want := map[uint]int{}
	for i := uint64(101); i <= 109; i++ {
		want[uint(i%grains)] = 1
		if i <= 104 {
			want[uint(i%grains)] = 2
		}
	}
	got := map[uint]int{}
	for _, s := range sel {
		got[s.Slot] = s.Source
	}
	require.Equal(t, want, got)
	for slot, src := range want {
		require.Equal(t, src, d.Current[slot])
	}
}

// Readers of an audio output trust half its buffer behind the head. With
// a skew budget past that, an output held back by a page both members
// broke lags members that keep writing, each sample over the one a
// buffer back. Every sample the output still offers must stay true
// through the members' next batch.
func TestAudioRetiresPagesTheMembersOverwriteWhileItLags(t *testing.T) {
	const (
		pages  = 8
		length = pages * PageSamples
		batch  = 256
	)
	a := NewAudio(2, pages, 2)
	a.Skew = 6000 // past the 4096 samples readers trust
	rings := [2][]uint64{make([]uint64, length), make([]uint64, length)}
	write := func(m int, start, end uint64) {
		for s := start; s < end; s++ {
			rings[m][s%length] = s
		}
	}
	drop := func(b uint64) bool { return b == 80 } // both members break page 20
	now := time.Unix(0, 0)
	retired := 0
	for b := uint64(32); b < 200; b++ {
		if !drop(b) {
			for m := 0; m < 2; m++ {
				write(m, b*batch, (b+1)*batch)
				a.Committed(m, b*batch, (b+1)*batch)
			}
		}
		now = now.Add(5 * time.Millisecond)
		a.Step(now)
		// The members write at the clock: what is due is what they wrote.
		sel := a.Retire((b + 1) * batch)
		if a.Head+length/2 >= (b+2)*batch {
			require.Empty(t, sel, "an output within half the buffer of the clock moved pages at batch %d", b)
		}
		retired += len(sel)

		// The members' next batch lands before the next pass.
		if !drop(b + 1) {
			for m := 0; m < 2; m++ {
				write(m, (b+1)*batch, (b+2)*batch)
			}
		}
		for s := a.Head - min(a.Head, length/2); s < a.Head; s++ {
			src := a.Current[(s/PageSamples)%pages]
			if src == a.Silence {
				continue
			}
			require.Equal(t, s, rings[src][s%length], "after batch %d the output offers sample %d from member %d, which holds %d there",
				b, s, src, rings[src][s%length])
		}
	}
	require.Equal(t, uint64(200*batch), a.Head)
	require.NotZero(t, retired, "the output never lagged")
}

// A member writing ahead of the clock overwrites sooner: a page it serves
// moves to the member that still holds it whole, and to silence once
// that one would overwrite it too.
func TestAudioRetiresToTheMemberThatStillHoldsThePage(t *testing.T) {
	const (
		pages = 8
		batch = 256
	)
	a := NewAudio(2, pages, 2)
	a.Skew = 1 << 20
	for b := uint64(32); b < 102; b++ {
		if b == 80 {
			continue // both members break page 20
		}
		a.Committed(0, b*batch, (b+1)*batch)
		if b < 86 {
			a.Committed(1, b*batch, (b+1)*batch)
		}
		a.Step(time.Unix(0, 0))
	}
	require.Equal(t, uint64(20*PageSamples), a.Head)
	require.Equal(t, 0, a.Primary)

	// a reaches sample 26112 + 256 - 8192 = 18176, into page 17; b, at
	// the clock, 14080. Readers trust from 16384, page 16.
	sel := a.Retire(86 * batch)
	require.ElementsMatch(t, []PageSelect{{Page: 16 % pages, Source: 1}, {Page: 17 % pages, Source: 1}}, sel)

	// With the clock where a is, b would overwrite them as well.
	sel = a.Retire(102 * batch)
	require.ElementsMatch(t, []PageSelect{{Page: 16 % pages, Source: 2}, {Page: 17 % pages, Source: 2}}, sel)
}

// A local member's head can jump over a gap in one pass. That one range
// says nothing about the batches that follow, and an output that keeps
// up must not keep retiring its oldest pages because of it.
func TestAudioRetireForgetsAJumpOnceTheBatchesResume(t *testing.T) {
	const batch = 480
	a := NewAudio(1, 8, 1)
	a.Skew, a.Ahead = 2048, 960
	end := uint64(40 * batch)
	commit := func(n uint64) []PageSelect {
		a.Committed(0, end, end+n)
		end += n
		a.Step(time.Unix(0, 0))
		return a.Retire(end)
	}
	for i := 0; i < 40; i++ {
		require.Empty(t, commit(batch), "an output keeping up moved pages")
	}
	commit(4096) // the head moved over a gap
	commit(batch)
	for i := 0; i < 40; i++ {
		require.Empty(t, commit(batch), "pages still move %d batches after the jump", i+2)
	}
}

// A restarted producer's new ring is put in place of its old one in every
// view, and holds none of the grains the old one served: a slot left on
// the member would show whatever the new ring has there. Every slot the
// member serves moves first, to a member that still holds its grain or to
// the INVALID grain, and the member is served again once its new ring
// delivers.
func TestDiscreteDropMovesAReplacedMembersSlotsToWhatStillHoldsThem(t *testing.T) {
	const grains = 20
	a, b := newRing(grains), newRing(grains)
	d := NewDiscrete(grains, 2)
	d.Ordered, d.Skew = true, 8
	now := time.Unix(0, 0)
	var head uint64
	for i := uint64(100); i <= 119; i++ {
		a.commit(i)
		if i < 110 || i > 112 {
			b.commit(i)
		}
		_, head, _ = d.Step([]Member{a.member(), b.member()}, now)
	}
	require.Equal(t, uint64(119), head)
	for slot, src := range d.Current {
		require.Equal(t, 0, src, "slot %d not on the primary", slot)
	}

	sel := d.Drop([]Member{a.member(), b.member()}, 0)
	require.Len(t, sel, grains, "a slot stayed on the replaced member")
	for _, s := range sel {
		require.Equal(t, uint64(s.Slot), s.Index%grains, "slot %d named for grain %d", s.Slot, s.Index)
		switch {
		case s.Index == 100: // as old as b may be overwriting
			require.Equal(t, 2, s.Source)
		case s.Index >= 110 && s.Index <= 112: // b skipped them
			require.Equal(t, 2, s.Source, "grain %d moved to a member without it", s.Index)
		default:
			require.Equal(t, 1, s.Source, "grain %d not moved to b, which holds it", s.Index)
		}
		require.Equal(t, s.Source, d.Current[s.Slot])
	}

	fresh := newRing(grains) // the restarted producer's ring
	for i := uint64(120); i <= 125; i++ {
		b.commit(i)
		if i >= 123 {
			fresh.commit(i)
		}
	}
	_, head, _ = d.Step([]Member{fresh.member(), b.member()}, now.Add(time.Second))
	require.Equal(t, uint64(125), head)
	from := map[uint64]int{}
	for i := uint64(120); i <= 125; i++ {
		from[i] = d.Current[i%grains]
	}
	require.Equal(t, map[uint64]int{120: 1, 121: 1, 122: 1, 123: 0, 124: 0, 125: 0}, from)
}

// The same for audio: every page the member serves moves to a member that
// holds it whole, or to silence, and what the member committed before is
// forgotten, so the new ring is not taken to hold it.
func TestAudioDropMovesAReplacedMembersPagesToWhatStillHoldsThem(t *testing.T) {
	a := NewAudio(2, 38, 2)
	a.Skew = 2048
	for n := uint64(10); n <= 40; n++ {
		a.Committed(0, n*PageSamples, (n+1)*PageSamples)
		switch n {
		case 30: // b skipped it
		case 25: // b holds half of it
			a.Committed(1, n*PageSamples, n*PageSamples+PageSamples/2)
		default:
			a.Committed(1, n*PageSamples, (n+1)*PageSamples)
		}
	}
	_, head, _ := a.Step(time.Unix(0, 0))
	require.Equal(t, uint64(41*PageSamples), head)
	// The old ring's last commits, which no pass has published yet.
	a.Committed(0, 41*PageSamples, 43*PageSamples)

	sel := a.Drop(0)
	require.Len(t, sel, 31, "a page stayed on the replaced member")
	for _, s := range sel {
		want := 1
		if s.Page == 30 || s.Page == 25 {
			want = 2 // b does not hold it whole
		}
		require.Equal(t, want, s.Source, "page slot %d", s.Page)
	}
	for p, src := range a.Current {
		require.NotEqual(t, 0, src, "page slot %d on the replaced member", p)
	}

	// The new ring commits from page 43 on; nothing before is taken from it.
	a.Committed(1, 41*PageSamples, 44*PageSamples)
	a.Committed(0, 43*PageSamples, 44*PageSamples)
	sel, head, _ = a.Step(time.Unix(1, 0))
	require.Equal(t, uint64(44*PageSamples), head)
	require.Equal(t, []PageSelect{{Page: 41 % 38, Source: 1}, {Page: 42 % 38, Source: 1}, {Page: 43 % 38, Source: 0}}, sel)
}

// The replaced member's head is forgotten with its pages. Left in place,
// the old ring's last commit would read as the member having skipped the
// pages after it, and its new ring would be passed over at once rather
// than waited for as a late primary.
func TestAudioDropWaitsForTheNewRingAsALatePrimary(t *testing.T) {
	a := NewAudio(2, 38, 2)
	a.Skew, a.Tolerance = 2048, 5*time.Millisecond
	for n := uint64(10); n <= 20; n++ {
		a.Committed(0, n*PageSamples, (n+1)*PageSamples)
		a.Committed(1, n*PageSamples, (n+1)*PageSamples)
	}
	now := time.Unix(0, 0)
	_, head, _ := a.Step(now)
	require.Equal(t, uint64(21*PageSamples), head)
	a.Committed(0, 21*PageSamples, 23*PageSamples) // the old ring's last, unpublished

	a.Drop(0)
	a.Committed(1, 21*PageSamples, 24*PageSamples)
	sel, head, wait := a.Step(now)
	require.Empty(t, sel)
	require.Equal(t, uint64(21*PageSamples), head)
	require.Equal(t, a.Tolerance, wait, "the restarted primary is taken to have skipped what its old ring wrote")

	_, head, _ = a.Step(now.Add(wait))
	require.Equal(t, uint64(24*PageSamples), head)
	require.Equal(t, 1, a.Primary)
}
