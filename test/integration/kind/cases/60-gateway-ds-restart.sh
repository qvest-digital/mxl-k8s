#!/usr/bin/env bash
# Assert every MxlFlowMirror recovers after the fabrics gateway
# DaemonSet rolls. The DaemonSet runs both the source and target
# reconcilers; restarting it tears down all libmxl-fabrics
# initiators and targets at once. The mirror must reconverge to
# Ready without manual intervention.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

GATEWAY_DS="${GATEWAY_DS:-mxl-fabrics-gateway}"
RECOVERY_TIMEOUT_SECS="${RECOVERY_TIMEOUT_SECS:-60}"

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

# Poll every mirror until all phases are Ready. Same shape as
# 30-mirror-ready.sh: a single jsonpath joins name=phase pairs so
# the regex below only matches when every entry is Ready.
JSONPATH='{range .items[*]}{.metadata.name}={.status.phase};{end}'
phase=$(wait_phase mxlfm "$JSONPATH" \
        '^([a-z0-9-]+=Ready;)+$' \
        "$RECOVERY_TIMEOUT_SECS") \
  || {
    echo "current phases: $("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
            -o jsonpath="$JSONPATH" 2>/dev/null || true)" >&2
    fail "not all MxlFlowMirrors returned to Ready within ${RECOVERY_TIMEOUT_SECS}s"
  }
echo "  post-restart phases: ${phase}"
