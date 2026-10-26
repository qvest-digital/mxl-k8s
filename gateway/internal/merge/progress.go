package merge

import (
	"sync"
	"time"
)

// StallAfter is how long a member's head may stand still before the
// member counts as stalled. A producer whose pod runs on but commits
// nothing, or a mirror that delivers nothing, leaves the output served
// from the others while every object of the member reads healthy.
const StallAfter = 2 * time.Second

// progress follows when each member's head last moved. A grain ring's
// head is a grain index, a channel buffer's a sample index; either moves
// with every commit. The engine's loop observes; anyone may ask.
type progress struct {
	mu    sync.Mutex
	head  []uint64
	moved []time.Time
	stall []bool
}

func newProgress(heads []uint64, now time.Time) *progress {
	p := &progress{head: append([]uint64(nil), heads...), moved: make([]time.Time, len(heads)),
		stall: make([]bool, len(heads))}
	for i := range p.moved {
		p.moved[i] = now
	}
	return p
}

// observe records the members' heads at now and reports whether a member
// became stalled or moved again. Any change of a head counts as a move:
// a late commit steps a discrete head backwards, and a replaced ring
// starts at its own.
func (p *progress) observe(heads []uint64, now time.Time) (changed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, h := range heads {
		if h != p.head[i] {
			p.head[i], p.moved[i] = h, now
		}
		if s := now.Sub(p.moved[i]) >= StallAfter; s != p.stall[i] {
			p.stall[i], changed = s, true
		}
	}
	return changed
}

// stalled returns the members stalled at the last observe.
func (p *progress) stalled() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []int
	for i, s := range p.stall {
		if s {
			out = append(out, i)
		}
	}
	return out
}
