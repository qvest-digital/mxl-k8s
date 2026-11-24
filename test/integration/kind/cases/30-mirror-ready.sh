#!/usr/bin/env bash
# Assert at least one MxlFlowMirror exists in the demo namespace and
# every listed mirror reaches phase=Ready with a non-empty
# status.targetInfo. status.targetInfo is set by the target-side
# gateway reconciler after opening the libmxl Writer and the
# fabrics Target; an empty value means the target plumbing never
# materialised even if the phase string happens to read Ready.
#
# The descriptor is also published in the MxlFlowMirrorTarget of the
# mirror's name, which status.targetInfoHash must name, and both
# gateways renew a heartbeat Lease per mirror once grains move.

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

# Walk every mirror; require status.targetInfo to be non-empty.
names=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
        -o jsonpath='{range .items[*]}{.metadata.name} {end}')
[ -n "$names" ] || fail "no MxlFlowMirror resources found in namespace ${NAMESPACE}"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi | cut -d' ' -f1
}

# heartbeat <side> <mirror>: whether that side's heartbeat Lease for the
# mirror exists.
heartbeat() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get leases -l "mxl.qvest-digital.com/mirror-side=$1" \
      -o jsonpath='{range .items[*]}{.metadata.annotations.mxl\.qvest-digital\.com/mirror}{"\n"}{end}' \
    | grep -Fxq "${NAMESPACE}/$2"
}

for name in $names; do
  ti=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${name}" \
        -o jsonpath='{.status.targetInfo}')
  if [ -z "$ti" ]; then
    fail "MxlFlowMirror/${name} has empty status.targetInfo (target side never opened)"
  fi
  hash=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${name}" \
        -o jsonpath='{.status.targetInfoHash}')
  obj=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirrortarget/${name}" \
        -o jsonpath='{.spec.targetInfo}') \
    || fail "MxlFlowMirror/${name} has no MxlFlowMirrorTarget"
  [ "$(printf '%s' "$obj" | sha256)" = "$hash" ] \
    || fail "MxlFlowMirror/${name} status.targetInfoHash does not name its MxlFlowMirrorTarget"
  [ "$obj" = "$ti" ] \
    || fail "MxlFlowMirror/${name} carries another descriptor in status.targetInfo than in its MxlFlowMirrorTarget"

  deadline=$(( $(date +%s) + 30 ))
  until heartbeat source "$name" && heartbeat target "$name"; do
    [ "$(date +%s)" -lt "$deadline" ] \
      || fail "MxlFlowMirror/${name} has no source and target heartbeat Lease after 30s"
    sleep 2
  done
  echo "  ${name}: Ready, descriptor published, heartbeats renewed"
done

echo "  phases: ${phase}"
