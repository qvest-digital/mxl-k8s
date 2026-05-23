#!/usr/bin/env bash
# Assert the control-plane workloads finish rolling out and every
# selected pod reaches the Ready condition. Covers the operator
# Deployment and the agent + gateway DaemonSets.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

WORKLOADS=(
  deploy/mxl-operator
  ds/mxl-domain-agent
  ds/mxl-fabrics-gateway
)

for w in "${WORKLOADS[@]}"; do
  "${KUBECTL[@]}" -n "$NAMESPACE" rollout status "$w" \
      --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
    || fail "$w rollout did not complete in ${ROLLOUT_TIMEOUT_SECS}s"
  echo "  ${w}: rolled out"
done

"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready pod \
    -l 'app.kubernetes.io/name in (mxl-operator,mxl-domain-agent,mxl-fabrics-gateway)' \
    --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "control-plane pods did not all become Ready"
echo "  control-plane pods: Ready"
