package mirror

import (
	"errors"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
)

// retryExhaustAfter is how long a reliable connection keeps
// retransmitting to a peer that stopped acknowledging before it fails:
// seven retries of the RDMA-CM RoCE default ACK timeout, 4.096us x
// 2^17 = 537ms each. libfabric's verbs provider sets neither, so this
// is what every verbs connection runs with.
const retryExhaustAfter = 7 * 537 * time.Millisecond

// deliveryStallAfter is how long a connected target may go without a
// completed transfer, while its source head advances, before it is
// re-added on its own.
//
// Twice retryExhaustAfter, so a connection still retrying a lost
// packet is not torn down from under the retry. It is 7.5s with the
// default ACK timeout and follows retryExhaustAfter if that changes.
const deliveryStallAfter = 2 * retryExhaustAfter

// observeDelivery reads the initiator's counters for entry's target,
// takes delivery and bytes from them, and reports whether the target
// has stopped completing transfers: connected for longer than
// deliveryStallAfter, the source head advancing, and nothing completed
// in that time.
//
// Only that target is re-added. Its peers on the same initiator keep
// their connections and their windows; one target that stops
// acknowledging no longer holds anything of theirs.
func (r *SourceReconciler) observeDelivery(entry *sourceEntry, now time.Time) bool {
	if entry.info == nil || r.opener == nil {
		return false
	}
	st, err := r.opener.targetStats(entry.shared, entry.info)
	if err != nil {
		return false
	}
	entry.stats.Store(&st)
	entry.bytes.Store(st.BytesCompleted)
	if entry.recordDelivered(st.Completed, now) {
		return false
	}
	return deliveryStalled(now, entry, r.deliveryStallAfter())
}

// deliveryStallAfter returns the stall window, deliveryStallAfter
// unless a test shortened it.
func (r *SourceReconciler) deliveryStallAfter() time.Duration {
	if r.deliveryStall > 0 {
		return r.deliveryStall
	}
	return deliveryStallAfter
}

// deliveryStalled is the stall rule of observeDelivery on the entry's
// recorded timestamps.
func deliveryStalled(now time.Time, entry *sourceEntry, after time.Duration) bool {
	if !connectionOriented(entry.provider()) || !entry.connected.Load() {
		return false
	}
	since := entry.connectedAt.Load()
	if since == nil {
		return false
	}
	if sent := entry.lastSentAt.Load(); sent != nil && sent.After(*since) {
		since = sent
	}
	if now.Sub(*since) <= after {
		return false
	}
	// A producer that stopped writing leaves nothing to complete.
	head := entry.headAdvancedAt.Load()
	return head != nil && now.Sub(*head) <= after
}

// targetStats asks the shared initiator for one target's counters.
func (o *libmxlOpener) targetStats(s *sharedSource, info *fabrics.TargetInfo) (fabrics.TargetStats, error) {
	if s == nil || s.initiator == nil {
		return fabrics.TargetStats{}, errors.New("no initiator")
	}
	return s.initiator.TargetStats(info)
}
