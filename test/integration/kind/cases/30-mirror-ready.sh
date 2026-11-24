#!/usr/bin/env bash
# Assert at least one MxlFlowMirror exists in the demo namespace and
# every listed mirror reaches phase=Ready with its target descriptor
# published. The target-side gateway reconciler writes the descriptor
# into the MxlFlowMirrorTarget of the mirror's name after opening the
# libmxl Writer and the fabrics Target, then names it by hash in
# status.targetInfoHash; a missing or mismatching one means the target
# plumbing never materialised even if the phase string reads Ready.
#
# The status carries no progress time: both gateways renew a heartbeat
# Lease per mirror once grains move.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

JSONPATH='{range .items[*]}{.metadata.name}={.status.phase};{end}'

phase=$(wait_phase mxlflowmirrors "$JSONPATH" \
        '^([a-z0-9-]+=Ready;)+$' \
        "$MIRROR_TIMEOUT_SECS") \
  || {
    echo "current phases: $("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
            -o jsonpath="$JSONPATH" 2>/dev/null || true)" >&2
    fail "not all MxlFlowMirrors reached Ready in ${MIRROR_TIMEOUT_SECS}s"
  }

# Walk every mirror; require a published descriptor and heartbeats.
names=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
        -o jsonpath='{range .items[*]}{.metadata.name} {end}')
[ -n "$names" ] || fail "no MxlFlowMirror resources found in namespace ${NAMESPACE}"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi | cut -d' ' -f1
}

# heartbeat <side> <mirror>: whether that side's heartbeat Lease for the
# mirror exists.
heartbeat() {
  "${KUBECTL[@]}" -n "$LEASE_NAMESPACE" get leases -l "mxl.qvest-digital.com/mirror-side=$1" \
      -o jsonpath='{range .items[*]}{.metadata.annotations.mxl\.qvest-digital\.com/mirror}{"\n"}{end}' \
    | grep -Fxq "${NAMESPACE}/$2"
}

for name in $names; do
  hash=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${name}" \
        -o jsonpath='{.status.targetInfoHash}')
  [ -n "$hash" ] \
    || fail "MxlFlowMirror/${name} has empty status.targetInfoHash (target side never opened)"
  obj=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirrortarget/${name}" \
        -o jsonpath='{.spec.targetInfo}') \
    || fail "MxlFlowMirror/${name} has no MxlFlowMirrorTarget"
  [ "$(printf '%s' "$obj" | sha256)" = "$hash" ] \
    || fail "MxlFlowMirror/${name} status.targetInfoHash does not name its MxlFlowMirrorTarget"

  deadline=$(( $(date +%s) + 30 ))
  until heartbeat source "$name" && heartbeat target "$name"; do
    [ "$(date +%s)" -lt "$deadline" ] \
      || fail "MxlFlowMirror/${name} has no source and target heartbeat Lease after 30s"
    sleep 2
  done
  echo "  ${name}: Ready, descriptor published, heartbeats renewed"
done

echo "  phases: ${phase}"
