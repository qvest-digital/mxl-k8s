// Package merge serves an MxlFlowRedundancySet's output on this node: a
// flow on mxlfs whose grain files, or channels file, are views over the
// member flows' rings. Selecting a member for a slot or a page remaps the
// readers' pages onto that member's; no payload is copied.
//
// The policies here decide what to select and how far the output may be
// published; the engine applies them to the views and the output's
// header.
package merge

import "time"

// Grain is what a member's ring slot holds, as its grain header reads.
type Grain struct {
	Index       uint64
	Invalid     bool
	ValidSlices uint16
	TotalSlices uint16
}

// Member is one member as the discrete policy sees it.
type Member struct {
	Head  uint64                // the member flow's head index
	Grain func(slot uint) Grain // the header of a ring slot
}

// Discrete decides, grain by grain, which member a video or data slot
// shows. A member serves index i when it committed i (its head reached
// it, the slot carries i and is not invalid) and, for video, all slices
// are valid. A data grain's valid slices are its length, so a committed
// data grain is complete.
//
// The primary, at first the first member, serves every grain it has.
// Another member serves a grain the primary skipped at once, and one
// the primary is merely late with after Tolerance, and then becomes the
// primary. An index no member delivers within Skew grains of the
// leading member is published from the INVALID placeholder, source
// Placeholder.
type Discrete struct {
	Grains      uint   // ring depth
	Data        bool   // a data flow: committed means complete
	Skew        uint64 // in grains
	Tolerance   time.Duration
	Ordered     bool // keep a primary; false serves first arrival
	Placeholder int  // the source index of the INVALID grain

	Head    uint64 // published head; 0 until the first grain
	Primary int    // -1 until chosen; an ordered set starts on member 0
	Current []int  // source selected per slot

	waitingIndex uint64
	waitingSince time.Time

	seen []uint64 // per member, the head Retire last saw
	late []uint64 // per member, how far its head was behind due when it last moved
}

// ToleranceFor is how long a set with skew budget skew waits for a late
// primary before replacing it: half the budget, and no less than 2ms.
// Replacing the primary moves every slot it then serves, one remap per
// slot in every reader, so arrival jitter between members within the
// budget must not do it.
func ToleranceFor(skew time.Duration) time.Duration {
	return max(skew/2, 2*time.Millisecond)
}

// Select is one view slot to point at a source, for the grain of Index.
type Select struct {
	Slot   uint
	Source int
	Index  uint64
}

// NewDiscrete returns a policy for a ring of grains slots whose slots all
// start on the placeholder.
func NewDiscrete(grains uint, placeholder int) *Discrete {
	cur := make([]int, grains)
	for i := range cur {
		cur[i] = placeholder
	}
	return &Discrete{Grains: grains, Placeholder: placeholder, Primary: -1, Current: cur, Tolerance: 2 * time.Millisecond}
}

func (d *Discrete) serves(m Member, index uint64) bool {
	if m.Head < index || m.Head-index > uint64(d.Grains)-2 {
		return false // not committed yet, or already overwritten
	}
	g := m.Grain(uint(index % uint64(d.Grains)))
	if g.Index != index || g.Invalid {
		return false
	}
	return d.Data || g.ValidSlices >= g.TotalSlices
}

// Step advances the published head as far as the members allow at now.
// It returns the selects to apply, in index order, before the new head
// is published, and
// how long until a pending decision times out (0 when none waits).
func (d *Discrete) Step(members []Member, now time.Time) (selects []Select, head uint64, wait time.Duration) {
	if d.Ordered && d.Primary < 0 {
		d.Primary = 0
	}
	var lead uint64
	for _, m := range members {
		if m.Head > lead {
			lead = m.Head
		}
	}
	if d.Head == 0 && lead > 0 {
		d.Head = lead - 1 // start at the leading member's newest grain
	}
	for d.Head < lead {
		index := d.Head + 1
		slot := uint(index % uint64(d.Grains))
		pick, alt := -1, -1
		for i, m := range members {
			if !d.serves(m, index) {
				continue
			}
			if d.Ordered && i == d.Primary {
				pick = i
			} else if alt < 0 {
				alt = i
			}
		}
		if !d.Ordered && alt >= 0 && pick < 0 {
			pick = alt
		}
		if pick < 0 && alt >= 0 {
			if d.waitingIndex != index {
				d.waitingIndex, d.waitingSince = index, now
			}
			switch {
			case d.Primary < 0 || members[d.Primary].Head >= index:
				pick = alt // the primary skipped it
			case now.Sub(d.waitingSince) >= d.Tolerance:
				pick, d.Primary = alt, alt // the primary is late: replace it
			default:
				return selects, d.Head, d.Tolerance - now.Sub(d.waitingSince)
			}
		}
		if pick < 0 {
			if lead < index+d.Skew {
				break
			}
			pick = d.Placeholder
		}
		if d.Primary < 0 && pick != d.Placeholder {
			d.Primary = pick
		}
		if d.Current[slot] != pick {
			d.Current[slot] = pick
			selects = append(selects, Select{Slot: slot, Source: pick, Index: index})
		}
		d.Head = index
	}
	return selects, d.Head, 0
}

// Retire returns the selects that keep the output's oldest grains true
// while it lags its members. Readers trust every index from
// Head - Grains + 2 on, but a member writes index i + Grains into the
// slot of i. A slot whose source may do that before the next pass moves
// to a member that still holds i and will not, or to the placeholder,
// rather than show the next ring period's grain as i. due is the index
// the clock is at a guard past now. The engine passes that guard before
// every grain boundary, so a member writing at the clock writes nothing
// past due before the next pass. Nothing moves while the output keeps up
// with the members and the clock.
//
// A member may write the grain after its head, or, after a loss, the
// index it would have written had it lost none: due, less how far its
// head was behind due when it last moved. For a member writing at the
// clock that is due. One that commits each grain a period or more after
// its boundary is not taken to write due, or the oldest grain readers
// trust would be retired every grain while it keeps up.
func (d *Discrete) Retire(members []Member, due uint64) (selects []Select) {
	if len(d.seen) != len(members) {
		d.seen, d.late = make([]uint64, len(members)), make([]uint64, len(members))
	}
	for m, mb := range members {
		if mb.Head != d.seen[m] {
			d.seen[m], d.late[m] = mb.Head, due-min(due, mb.Head)
		}
	}
	// next is the highest index member m may write before the next pass.
	next := func(m int) uint64 { return max(members[m].Head+1, due-min(due, d.late[m])) }
	var reach uint64
	for m := range members {
		reach = max(reach, next(m))
	}
	from := uint64(1)
	if d.Head+2 > uint64(d.Grains) {
		from = d.Head + 2 - uint64(d.Grains)
	}
	for i := from; i <= d.Head && i+uint64(d.Grains) <= reach; i++ {
		slot := uint(i % uint64(d.Grains))
		cur := d.Current[slot]
		if cur == d.Placeholder || next(cur) < i+uint64(d.Grains) {
			continue
		}
		to := d.Placeholder
		for m := range members {
			if m != cur && d.serves(members[m], i) && next(m) < i+uint64(d.Grains) {
				to = m
				break
			}
		}
		d.Current[slot] = to
		selects = append(selects, Select{Slot: slot, Source: to, Index: i})
	}
	return selects
}

// Drop returns the selects that move every slot member m serves to
// another member that holds its grain, or to the placeholder, before m's
// ring is replaced by one that holds none of them. It forgets what Retire
// saw of m.
func (d *Discrete) Drop(members []Member, m int) (selects []Select) {
	for slot, cur := range d.Current {
		if cur != m {
			continue
		}
		// The index the slot holds below the published head.
		back := (d.Head%uint64(d.Grains) + uint64(d.Grains) - uint64(slot)) % uint64(d.Grains)
		to, index := d.Placeholder, uint64(0)
		if d.Head >= back {
			index = d.Head - back
			for o := range members {
				if o != m && d.serves(members[o], index) {
					to = o
					break
				}
			}
		}
		d.Current[slot] = to
		selects = append(selects, Select{Slot: uint(slot), Source: to, Index: index})
	}
	if m < len(d.seen) {
		d.seen[m], d.late[m] = 0, 0
	}
	return selects
}

// PageSamples is how many 32-bit samples one page of a channel holds.
const PageSamples = 4096 / 4

// coverage is what one member holds of the page in one ring slot.
type coverage struct {
	n      uint64 // absolute page number
	to     uint64 // samples [n*PageSamples, to) arrived without a gap
	broken bool   // a gap: the member does not hold the page whole
	valid  bool
}

// Audio decides, page by page of every channel's ring, which member a
// view page shows, from the sample ranges members are reported to have
// committed. A page takes the primary, at first the first member, while
// it holds the page without a gap, another member that does when the
// primary has a gap or is late beyond Tolerance, and the silence source
// when no member holds it whole within Skew samples of the leading
// member.
type Audio struct {
	Pages     uint64 // pages per channel ring
	Skew      uint64 // in samples
	Tolerance time.Duration
	Silence   int
	Ahead     uint64 // samples the clock advances between two passes

	Head    uint64 // published head: the exclusive end of the samples
	Primary int
	Current []int // source per page slot

	cov          [][]coverage
	evHead       []uint64
	batch        []uint64 // per member, the range it last committed at once
	started      bool
	waitingPage  uint64
	waitingSince time.Time
}

// NewAudio returns a policy for members rings of pages pages each.
func NewAudio(members int, pages uint64, silence int) *Audio {
	a := &Audio{Pages: pages, Silence: silence, Primary: 0, Tolerance: 2 * time.Millisecond,
		Current: make([]int, pages), cov: make([][]coverage, members), evHead: make([]uint64, members),
		batch: make([]uint64, members), waitingPage: ^uint64(0)}
	for i := range a.Current {
		a.Current[i] = silence
	}
	for m := range a.cov {
		a.cov[m] = make([]coverage, pages)
	}
	return a
}

// Committed records that member committed samples [start, end).
func (a *Audio) Committed(member int, start, end uint64) {
	if !a.started {
		// Publish from the first page boundary on: the page the first
		// arrival starts in is not known whole for any member, and would
		// be served as silence.
		a.Head, a.started = (start+PageSamples-1)/PageSamples*PageSamples, true
	}
	for n := start / PageSamples; n*PageSamples < end; n++ {
		c := &a.cov[member][n%a.Pages]
		lo, hi := max(start, n*PageSamples), min(end, (n+1)*PageSamples)
		if !c.valid || c.n != n {
			*c = coverage{n: n, to: n * PageSamples, valid: true}
		}
		if lo != c.to {
			c.broken = true
		} else if !c.broken {
			c.to = hi
		}
	}
	if end > a.evHead[member] {
		a.evHead[member] = end
	}
	// libmxl writes at most half the buffer at once; a longer range is a
	// local member's head moving over a gap.
	a.batch[member] = min(end-start, a.Pages*PageSamples/2)
}

// PageSelect is one page slot to point at a source in every channel.
type PageSelect struct {
	Page   uint64
	Source int
}

// Step advances the published head as far as the members allow at now.
func (a *Audio) Step(now time.Time) (selects []PageSelect, head uint64, wait time.Duration) {
	var lead uint64
	for _, h := range a.evHead {
		lead = max(lead, h)
	}
	for a.started && a.Head < lead {
		n := a.Head / PageSamples
		p := n % a.Pages
		upto := make([]uint64, len(a.cov)+1)
		pick, alt := -1, -1
		for m := range a.cov {
			c := a.cov[m][p]
			if c.valid && c.n == n && !c.broken {
				upto[m] = c.to
			}
			if upto[m] <= a.Head {
				continue
			}
			if m == a.Primary {
				pick = m
			} else if alt < 0 || upto[m] > upto[alt] {
				alt = m
			}
		}
		if pick < 0 && alt >= 0 {
			if a.waitingPage != n {
				a.waitingPage, a.waitingSince = n, now
			}
			gap := (a.cov[a.Primary][p].valid && a.cov[a.Primary][p].n == n && a.cov[a.Primary][p].broken) ||
				a.evHead[a.Primary] > a.Head
			switch {
			case gap:
				pick = alt
			case now.Sub(a.waitingSince) >= a.Tolerance:
				pick, a.Primary = alt, alt
			default:
				return selects, a.Head, a.Tolerance - now.Sub(a.waitingSince)
			}
		}
		if pick < 0 {
			if lead < a.Head+a.Skew {
				break
			}
			pick = a.Silence
			upto = append(upto[:0:0], make([]uint64, len(a.cov)+1)...)
			upto[pick] = min((n+1)*PageSamples, lead)
		}
		if a.Current[p] != pick {
			a.Current[p] = pick
			selects = append(selects, PageSelect{Page: p, Source: pick})
		}
		a.Head = upto[pick]
	}
	return selects, a.Head, 0
}

// Drop returns the page selects that move every page member m serves to
// another member that holds it whole, or to silence, before m's ring is
// replaced by one that holds none of them. It forgets what m committed.
func (a *Audio) Drop(m int) (selects []PageSelect) {
	for p, cur := range a.Current {
		if cur != m {
			continue
		}
		to := a.Silence
		if a.Head > 0 {
			// The page the slot holds below the published head.
			last := (a.Head - 1) / PageSamples
			if back := (last%a.Pages + a.Pages - uint64(p)) % a.Pages; last >= back {
				n := last - back
				for o := range a.cov {
					c := a.cov[o][p]
					if o != m && c.valid && c.n == n && !c.broken && c.to >= min((n+1)*PageSamples, a.Head) {
						to = o
						break
					}
				}
			}
		}
		a.Current[p] = to
		selects = append(selects, PageSelect{Page: uint64(p), Source: to})
	}
	a.cov[m] = make([]coverage, a.Pages)
	a.evHead[m], a.batch[m] = 0, 0
	return selects
}

// Retire returns the page selects that keep the output's oldest samples
// true while it lags its members by more than half the buffer. Readers
// trust samples from Head minus half the buffer on, but a member writing
// sample s overwrites s minus the buffer. A page whose source may write
// over its trusted samples before the next pass moves to a member that
// holds the page whole and will not, or to silence. A member is taken to
// write its next batch as long as its last one. due is the sample index
// the clock is at.
func (a *Audio) Retire(due uint64) (selects []PageSelect) {
	if !a.started {
		return nil
	}
	length := a.Pages * PageSamples
	// reach is the first sample member m does not overwrite before the
	// next pass: past its last batch, or past what is due by then.
	reach := func(m int) uint64 {
		w := max(a.evHead[m], due) + a.Ahead + a.batch[m]
		return w - min(w, length)
	}
	var most uint64
	for m := range a.cov {
		most = max(most, reach(m))
	}
	lo := a.Head - min(a.Head, length/2)
	for n := lo / PageSamples; ; n++ {
		first := max(lo, n*PageSamples)
		if first >= a.Head || first >= most {
			break
		}
		p := n % a.Pages
		cur := a.Current[p]
		if cur == a.Silence || first >= reach(cur) {
			continue
		}
		to := a.Silence
		for m := range a.cov {
			c := a.cov[m][p]
			if m != cur && c.valid && c.n == n && !c.broken && c.to >= min((n+1)*PageSamples, a.Head) && first >= reach(m) {
				to = m
				break
			}
		}
		a.Current[p] = to
		selects = append(selects, PageSelect{Page: p, Source: to})
	}
	return selects
}
