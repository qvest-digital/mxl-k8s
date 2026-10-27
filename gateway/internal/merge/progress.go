package merge

import (
	"sync"
	"time"
)

// StallAfter is the least time a member's head may stand still, once it
// moved, before the member counts as stalled. A producer whose pod runs
// on but commits nothing, or a mirror that delivers nothing, leaves the
// output served from the others while every object of the member reads
// healthy.
const StallAfter = 2 * time.Second

// stallGrains is how many grain periods a member's head may stand still
// before it counts as stalled, where that is longer than StallAfter.
const stallGrains = 3

// StartGrace is how long a member whose head has not moved since the
// engine started may stand still before it counts as stalled. An absent
// member is a placeholder until its mirror fills it, which takes seconds,
// and an engine is started at every set start, node rejoin and gateway
// restart.
const StartGrace = 10 * time.Second

// RecoverAfter is how long a stalled member's head has to move steadily,
// never standing still for half its stall window, before the member
// counts as moving again. A producer behind a dead input commits a grain
// every two seconds or so; counting each commit as a recovery turned the
// member stalled and back at every one of them. The pause rule is what
// keeps such a member stalled; RecoverAfter only has to outlast the
// bursts it delivers between pauses.
//
// Measured from grains resuming steadily, a member is reported moving
// within RecoverAfter, or within stalledRecheck plus RecoverAfter, six
// seconds, when nothing raises an event on its set: a restarted producer,
// whose new ring is taken at the first recheck it can be opened at, or a
// report the set's queue had no room for. A member that moves again committed less than half its
// window before, so it stalls no sooner than that: it changes state at
// most twice in RecoverAfter plus half its stall window.
const RecoverAfter = 5 * time.Second

// stallWindow is how long a member of grains of period may stand still
// once it moved: StallAfter, or stallGrains periods of a slower flow.
func stallWindow(period time.Duration) time.Duration {
	return max(StallAfter, stallGrains*period)
}

// progress follows when each member's head last moved. A grain ring's
// head is a grain index, a channel buffer's a sample index; either moves
// with every commit. The engine's loop observes; anyone may ask.
type progress struct {
	mu     sync.Mutex
	window time.Duration
	start  time.Time
	head   []uint64
	moved  []time.Time // zero while the head has not moved since start
	steady []time.Time // since when a stalled member moves steadily; read only while it is stalled
	known  []bool      // moved, StartGrace passed, or reported stalled: stall is decided
	stall  []bool
}

// newProgress follows members whose heads are heads at now, stalled once
// a moved head stands still for window.
func newProgress(heads []uint64, now time.Time, window time.Duration) *progress {
	n := len(heads)
	return &progress{window: window, start: now, head: append([]uint64(nil), heads...),
		moved: make([]time.Time, n), steady: make([]time.Time, n), known: make([]bool, n), stall: make([]bool, n)}
}

// stalledFrom takes the members reported as stalled from the start, so
// that an engine rebuilt under one does not count its first commit as
// a recovery.
func (p *progress) stalledFrom(reported []bool) *progress {
	for i, r := range reported {
		p.known[i], p.stall[i] = r, r
	}
	return p
}

// observe records the members' heads at now and reports whether a member
// became stalled, moved again, or had its state decided. Any change of a
// head counts as a move: a late commit steps a discrete head backwards,
// and a replaced ring starts at its own. A nil progress, an engine whose
// members are not judged, observes nothing.
func (p *progress) observe(heads []uint64, now time.Time) (changed bool) {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	pause := p.window / 2
	for i, h := range heads {
		if h != p.head[i] {
			// A stalled member's moves count as steady from the first
			// one that follows a pause. Its first move after it stalled
			// does: it stood still for a window then, or never moved.
			if p.stall[i] && now.Sub(p.moved[i]) >= pause {
				p.steady[i] = now
			}
			p.head[i], p.moved[i] = h, now
		}
		known, stall := true, p.stall[i]
		switch {
		case p.known[i] && stall:
			// Moving again takes RecoverAfter of moves no pause apart.
			moving := !p.moved[i].IsZero() && now.Sub(p.moved[i]) < pause
			stall = !moving || now.Sub(p.steady[i]) < RecoverAfter
		case !p.moved[i].IsZero():
			stall = now.Sub(p.moved[i]) >= p.window
		case now.Sub(p.start) >= StartGrace:
			stall = true
		default:
			known = false
		}
		if known != p.known[i] || stall != p.stall[i] {
			p.known[i], p.stall[i], changed = known, stall, true
		}
	}
	return changed
}

// stalled returns, as of the last observe, the members stalled and those
// not yet decided: not moved since start, within StartGrace.
func (p *progress) stalled() (stalled, undecided []int) {
	if p == nil {
		return nil, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.stall {
		switch {
		case !p.known[i]:
			undecided = append(undecided, i)
		case p.stall[i]:
			stalled = append(stalled, i)
		}
	}
	return stalled, undecided
}
