#!/usr/bin/env bash
# Assert audio samples keep flowing across the fabric at delivery
# rate after the target-side gateway pod bounces. Restarting the
# gateway on the reader's node rebuilds the mirror's target fabric
# side and rotates TargetInfo; the source initiator must reconnect
# (or be rebuilt by the starvation watchdog) and resume delivering
# continuous samples. A wedged initiator delivers a trickle: the
# reader's batch index stalls or crawls while every freshness-based
# signal on the mirror reports progress, which is the failure this
# case exists to catch. Sits between 45-samples-flowing and the
# reschedule cases so the cluster is still in its pristine two-node
# demo state here.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

READER_POD="${READER_POD:-mxl-tcp-demo-audio-reader}"
GATEWAY_DS="${GATEWAY_DS:-mxl-k8s-gateway}"
# Delivery window: two index samples this far apart must differ by at
# least MIN_WINDOW_SAMPLES. The demo flow is 48 kHz, so a healthy
# mirror delivers ~48000*WINDOW_SECS samples per window; the bound is
# roughly 40% of that, far above any trickle and below any healthy
# delivery once the resync settle is averaged in.
WINDOW_SECS="${AUDIO_RATE_WINDOW_SECS:-5}"
MIN_WINDOW_SAMPLES="${AUDIO_RATE_MIN_WINDOW_SAMPLES:-100000}"
# Ceiling for the whole post-bounce recovery before the rate check
# gives up: mirrors reconverge in ~15s normally; the starvation exit
# adds its own bounded window before the rebuild.
RECOVERY_TIMEOUT_SECS="${AUDIO_RATE_RECOVERY_TIMEOUT_SECS:-120}"

# Most recent sample-batch index from the reader, as in
# 45-samples-flowing: anchored on "frags=" lines so the resync log
# lines do not shadow it. The tail is wide enough to span the whole
# recovery window at 100 batches/s.
sample_idx() {
  "${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${READER_POD}" --tail=2000 2>/dev/null \
    | awk '
        /frags=/ && match($0, /idx=[0-9]+/) {
          last = substr($0, RSTART + 4, RLENGTH - 4)
        }
        END { if (last != "") print last }
      '
}

# The reader must already be flowing: 45-samples-flowing ran before
# this case, so an empty index here is itself a failure.
first_idx=$(sample_idx || true)
[ -n "$first_idx" ] || fail "${READER_POD} has no sample-batch index; 45-samples-flowing must run first"

# The reader's node is the mirror's target side; bounce only that
# node's gateway pod so the source side stays up and has to recover
# against the rotated endpoint.
reader_node=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${READER_POD}" \
                -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
[ -n "$reader_node" ] || fail "could not resolve ${READER_POD}'s node"
gateway_pod="$(daemonset_pod_on gateway "$reader_node")"
[ -n "$gateway_pod" ] || fail "no running gateway pod on ${reader_node}"
echo "  bouncing ${gateway_pod} on ${reader_node} (target side of the audio mirror)"

"${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${gateway_pod}" --wait=false \
  || fail "deleting ${gateway_pod} failed"

# The DaemonSet replaces the pod immediately; wait for the
# replacement to run on the same node before watching the index, so
# the recovery clock starts from a live target side.
deadline=$(( $(date +%s) + ROLLOUT_TIMEOUT_SECS ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if [ -n "$(daemonset_pod_on gateway "$reader_node")" ]; then
    break
  fi
  sleep 2
done
[ -n "$(daemonset_pod_on gateway "$reader_node")" ] \
  || fail "no gateway pod came back up on ${reader_node}"
echo "  replacement gateway running on ${reader_node}"

# Poll until a full window delivers at rate. Sampling the index twice
# per attempt bounds each check to WINDOW_SECS; the outer deadline
# absorbs the mirror reconvergence and the starvation rebuild.
deadline=$(( $(date +%s) + RECOVERY_TIMEOUT_SECS ))
attempt=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  attempt=$(( attempt + 1 ))
  a=$(sample_idx || true)
  [ -n "$a" ] || { sleep 2; continue; }
  sleep "$WINDOW_SECS"
  b=$(sample_idx || true)
  [ -n "$b" ] || { sleep 2; continue; }
  if [ "$b" -gt "$a" ] && [ $(( b - a )) -ge "$MIN_WINDOW_SAMPLES" ]; then
    echo "  samples resumed at rate after gateway bounce: idx ${a} -> ${b} over ${WINDOW_SECS}s (attempt ${attempt})"
    exit 0
  fi
  echo "  attempt ${attempt}: idx ${a:-?} -> ${b:-?} over ${WINDOW_SECS}s, below ${MIN_WINDOW_SAMPLES}; retrying"
  sleep 2
done

echo "  last reader logs:" >&2
"${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${READER_POD}" --tail=30 >&2 2>/dev/null || true
echo "  gateway pods on ${reader_node}:" >&2
"${KUBECTL[@]}" -n "$NAMESPACE" get pods -o wide --field-selector "spec.nodeName=${reader_node}" >&2 2>/dev/null || true
fail "audio samples did not resume at rate within ${RECOVERY_TIMEOUT_SECS}s of the target gateway bounce"
