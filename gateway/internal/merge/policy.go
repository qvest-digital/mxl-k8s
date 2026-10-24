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
// The primary serves every grain it has. Another member serves a grain
// the primary skipped at once, and one the primary is merely late with
// after Tolerance, and then becomes the primary. An index no member
// delivers within Skew grains of the leading member is published from
// the INVALID placeholder, source Placeholder.
type Discrete struct {
	Grains      uint   // ring depth
	Data        bool   // a data flow: committed means complete
	Skew        uint64 // in grains
	Tolerance   time.Duration
	Ordered     bool // keep a primary; false serves first arrival
	Placeholder int  // the source index of the INVALID grain

	Head    uint64 // published head; 0 until the first grain
	Primary int    // -1 until chosen
	Current []int  // source selected per slot

	waitingIndex uint64
	waitingSince time.Time
}

// Select is one view slot to point at a source.
type Select struct {
	Slot   uint
	Source int
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
// It returns the selects to apply before the new head is published, and
// how long until a pending decision times out (0 when none waits).
func (d *Discrete) Step(members []Member, now time.Time) (selects []Select, head uint64, wait time.Duration) {
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
			selects = append(selects, Select{Slot: slot, Source: pick})
		}
		d.Head = index
	}
	return selects, d.Head, 0
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
// committed. A page takes the primary while it holds the page without a
// gap, another member that does when the primary has a gap or is late
// beyond Tolerance, and the silence source when no member holds it whole
// within Skew samples of the leading member.
type Audio struct {
	Pages     uint64 // pages per channel ring
	Skew      uint64 // in samples
	Tolerance time.Duration
	Silence   int

	Head    uint64 // published head: the exclusive end of the samples
	Primary int
	Current []int // source per page slot

	cov          [][]coverage
	evHead       []uint64
	started      bool
	waitingPage  uint64
	waitingSince time.Time
}

// NewAudio returns a policy for members rings of pages pages each.
func NewAudio(members int, pages uint64, silence int) *Audio {
	a := &Audio{Pages: pages, Silence: silence, Primary: -1, Tolerance: 2 * time.Millisecond,
		Current: make([]int, pages), cov: make([][]coverage, members), evHead: make([]uint64, members),
		waitingPage: ^uint64(0)}
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
		a.Head, a.started = start, true
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
			gap := a.Primary < 0 || (a.cov[a.Primary][p].valid && a.cov[a.Primary][p].n == n && a.cov[a.Primary][p].broken) ||
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
		if a.Primary < 0 && pick != a.Silence {
			a.Primary = pick
		}
		if a.Current[p] != pick {
			a.Current[p] = pick
			selects = append(selects, PageSelect{Page: p, Source: pick})
		}
		a.Head = upto[pick]
	}
	return selects, a.Head, 0
}
