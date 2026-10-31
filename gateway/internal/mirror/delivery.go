package mirror

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
)

// defaultAckTimeout is the ACK timeout exponent a verbs connection
// runs with when FI_VERBS_ACK_TIMEOUT does not set one: the RDMA-CM
// RoCE default, 4.096us x 2^17 = 537ms per retransmission.
const defaultAckTimeout = 17

// ackRetries is how often a reliable connection retransmits to a peer
// that stopped acknowledging before it fails; libfabric's verbs
// provider asks for 7 (the 3-bit maximum).
const ackRetries = 7

// ackTimeoutEnv is the variable libfabric's verbs provider reads the
// ACK timeout exponent from, in the builds that carry the option. -1,
// unset or out of range leaves the kernel default.
const ackTimeoutEnv = "FI_VERBS_ACK_TIMEOUT"

// tcpDeliveryStallAfter is the stall window of a tcp mirror, which
// has no ACK timeout of its own to derive one from. It is the verbs
// window at the default ACK timeout.
var tcpDeliveryStallAfter = deliveryStallAfter(fmt.Sprint(defaultAckTimeout))

// deliveryStallAfter is how long a connected verbs target may go
// without a completed transfer, while its source head advances, before
// it is re-added on its own: twice the time the connection retries an
// unacknowledged packet before it fails, so a connection still
// retrying is not torn down from under the retry. ackTimeout is the
// value of FI_VERBS_ACK_TIMEOUT: 7.5s at the default exponent 17,
// 0.94s at 14.
func deliveryStallAfter(ackTimeout string) time.Duration {
	exp := defaultAckTimeout
	if v, err := strconv.Atoi(strings.TrimSpace(ackTimeout)); err == nil && v >= 1 && v <= 31 {
		exp = v
	}
	perRetry := time.Duration(4096<<exp) * time.Nanosecond
	return 2 * ackRetries * perRetry
}

// observeDelivery reads the initiator's counters for entry's target,
// takes delivery and bytes from them, and reports whether the target
// has stopped completing transfers: connected for longer than its
// stall window (deliveryStallFor), the source head advancing, and
// nothing completed in that time.
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
	return deliveryStalled(now, entry, r.deliveryStallFor(entry.provider()))
}

// deliveryStallFor returns the stall window of a mirror on provider:
// derived from the configured ACK timeout for verbs, the default for
// tcp, and deliveryStall when a test set one.
func (r *SourceReconciler) deliveryStallFor(provider fabrics.Provider) time.Duration {
	if r.deliveryStall > 0 {
		return r.deliveryStall
	}
	if provider == fabrics.ProviderVerbs {
		return deliveryStallAfter(r.ackTimeout)
	}
	return tcpDeliveryStallAfter
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
