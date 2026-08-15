package mirror

// MakeProgress is called on every tick of a transfer loop, which is
// the right cadence while a mirror is delivering and the wrong one the
// moment it cannot.
//
// libmxl-fabrics throws "No more targets available while calling
// makeProgress" whenever the initiator holds no target, and logs it at
// error level on the way out. An initiator reaches that state without
// the mirror being gone: the peer gateway restarting has its endpoint
// evicted, and the source keeps its reader, its initiator and its loop
// until the reconciler observes the rotated targetInfo and rebuilds.
// For that whole window a 2ms tick emits ~500 error lines a second per
// mirror, which buries every other line in the gateway log at exactly
// the moment somebody is reading it.
//
// The throttle backs the call off while it keeps failing and restores
// the tick the moment it succeeds. Backing off a persistent failure is
// what the loop should do regardless of the logging: retrying an
// unchanged condition every 2ms is a busy-wait.
//
// Only fabricTransient is throttled. fabricIdle is MakeProgress's
// ordinary "still working" answer and must stay on the tick, or a
// mirror with queued transfers would drain them at the backoff rate.

// maxProgressBackoffTicks caps the throttle. At the default 2ms tick
// it holds the retry to roughly one call every 128ms, which keeps a
// targetless mirror at ~8 log lines a second instead of ~500 while
// still noticing recovery well inside one reconcile.
const maxProgressBackoffTicks = 64

// progressThrottle decides which ticks may call MakeProgress. The zero
// value calls on every tick, which is the state a healthy mirror never
// leaves.
type progressThrottle struct {
	// skip counts ticks still to be passed over before the next call.
	skip int
	// backoff is the number of ticks skipped after the next failure.
	// Doubles per consecutive failure, capped, and resets on success.
	backoff int
}

// allow reports whether this tick may call MakeProgress, consuming one
// tick of any outstanding backoff when it may not.
func (p *progressThrottle) allow() bool {
	if p.skip > 0 {
		p.skip--
		return false
	}
	return true
}

// record folds the outcome of a call into the throttle.
func (p *progressThrottle) record(kind fabricFailure) {
	if kind != fabricTransient {
		p.skip = 0
		p.backoff = 0
		return
	}
	if p.backoff == 0 {
		p.backoff = 1
	} else if p.backoff < maxProgressBackoffTicks {
		p.backoff *= 2
	}
	p.skip = p.backoff
}

// runProgress calls makeProgress on the ticks the throttle allows,
// reporting failures through report. Returns whether a call was made,
// which is what the tests assert against.
func (p *progressThrottle) runProgress(makeProgress ProgressFunc, report func(error)) bool {
	if !p.allow() {
		return false
	}
	err := makeProgress()
	p.record(classifyFabricError(err))
	if err != nil && report != nil {
		report(err)
	}
	return true
}
