#!/usr/bin/env bash
# Assert every MxlFlowMirror recovers after the fabrics gateway
# DaemonSet rolls. The DaemonSet runs both the source and target
# reconcilers; restarting it tears down all libmxl-fabrics
# initiators and targets at once. The mirror must reconverge to
# Ready without manual intervention.
#
# Recovery normally completes within ~15s. The outer 180s ceiling
# absorbs libmxl-fabrics' RCTarget reconnect cycles. The
# stuck-detector exits the wait early when the target reconciler
# stalls without errors (silent ErrNotReady), so a real hang is
# reported in tens of seconds with diagnostics instead of after the
# full timeout.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

GATEWAY_DS="${GATEWAY_DS:-mxl-k8s-gateway}"
RECOVERY_TIMEOUT_SECS="${RECOVERY_TIMEOUT_SECS:-180}"
# Phase=Degraded with status.lastGrainAt unchanged for this many
# seconds means no fresh grains are landing - the target side has
# stalled and waiting longer will not recover the mirror.
STUCK_SECS="${STUCK_SECS:-45}"
POLL_INTERVAL_SECS=3

# Confirm at least one mirror exists before the restart; otherwise
# the assertion below is vacuous.
names=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
        -o jsonpath='{range .items[*]}{.metadata.name} {end}')
[ -n "$names" ] || fail "no MxlFlowMirror resources found in namespace ${NAMESPACE}"
echo "  pre-restart mirrors: ${names}"

"${KUBECTL[@]}" -n "$NAMESPACE" rollout restart "ds/${GATEWAY_DS}" \
  || fail "rollout restart ds/${GATEWAY_DS} failed"

"${KUBECTL[@]}" -n "$NAMESPACE" rollout status "ds/${GATEWAY_DS}" \
    --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "ds/${GATEWAY_DS} rollout did not complete in ${ROLLOUT_TIMEOUT_SECS}s"
echo "  ds/${GATEWAY_DS}: rolled out"

# Emit a focused snapshot so the operator does not have to wait for
# the suite-end collect_diagnostics dump to triage a stuck mirror.
dump_recovery_diagnostics() {
  echo "--- mxlfm status ---" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm -o yaml >&2 2>/dev/null || true
  echo "--- recent events (mxl-system) ---" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" get events --sort-by=.lastTimestamp \
      2>/dev/null | tail -30 >&2 || true
  echo "--- gateway logs (last 80 lines per pod) ---" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" logs \
      -l "app.kubernetes.io/name=${GATEWAY_DS}" \
      --all-containers --prefix --tail=80 >&2 2>/dev/null || true
}

# Per-mirror stuck-state tracked via three parallel indexed arrays
# (bash 3.2 has no associative arrays). Indices align across
# mirror_names / mirror_last_at / mirror_frozen_secs.
mirror_names=()
mirror_last_at=()
mirror_frozen_secs=()

# One polling-friendly line:
#   <name>|<phase>|<lastGrainAt>|<targetProgressReason>;
# Reason is empty until the TargetProgress condition exists.
POLL_JSONPATH='{range .items[*]}{.metadata.name}|{.status.phase}|{.status.lastGrainAt}|{range .status.conditions[?(@.type=="TargetProgress")]}{.reason}{end};{end}'

deadline=$(( $(date +%s) + RECOVERY_TIMEOUT_SECS ))
last_snapshot=""

while [ "$(date +%s)" -lt "$deadline" ]; do
  snapshot=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
              -o jsonpath="$POLL_JSONPATH" 2>/dev/null || true)
  last_snapshot="$snapshot"

  # Success when every mirror entry has phase=Ready. The regex
  # anchors on (name|Ready|...;)+ to require every entry, not just
  # one, to be Ready.
  if echo "$snapshot" | grep -Eq '^([a-z0-9.-]+\|Ready\|[^|]*\|[^;]*;)+$'; then
    echo "  post-restart snapshot: ${snapshot}"
    exit 0
  fi

  # Walk entries; for any Degraded mirror whose lastGrainAt has not
  # advanced since the previous poll, accumulate dead time. Any
  # phase change or lastGrainAt change resets the counter.
  IFS=';' read -r -a entries <<<"$snapshot"
  for entry in "${entries[@]}"; do
    [ -z "$entry" ] && continue
    IFS='|' read -r name phase last_at reason <<<"$entry"

    idx=-1
    for i in "${!mirror_names[@]}"; do
      if [ "${mirror_names[$i]}" = "$name" ]; then
        idx=$i
        break
      fi
    done
    if [ "$idx" -lt 0 ]; then
      mirror_names+=("$name")
      mirror_last_at+=("$last_at")
      mirror_frozen_secs+=(0)
      continue
    fi

    if [ "$phase" = "Degraded" ] && [ "$last_at" = "${mirror_last_at[$idx]}" ]; then
      mirror_frozen_secs[$idx]=$(( mirror_frozen_secs[$idx] + POLL_INTERVAL_SECS ))
      if [ "${mirror_frozen_secs[$idx]}" -ge "$STUCK_SECS" ]; then
        echo "  current snapshot: ${snapshot}" >&2
        echo "  mirror ${name} Degraded with lastGrainAt frozen at '${last_at}' for ${mirror_frozen_secs[$idx]}s (reason=${reason:-unknown})" >&2
        dump_recovery_diagnostics
        fail "mirror ${name} stuck Degraded: no grain commits for >=${STUCK_SECS}s after rollout"
      fi
    else
      mirror_frozen_secs[$idx]=0
      mirror_last_at[$idx]="$last_at"
    fi
  done

  sleep "$POLL_INTERVAL_SECS"
done

echo "  current snapshot: ${last_snapshot}" >&2
dump_recovery_diagnostics
fail "not all MxlFlowMirrors returned to Ready within ${RECOVERY_TIMEOUT_SECS}s"
