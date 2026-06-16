#!/usr/bin/env bash
# Assert at least one MxlFlowMirror exists in the demo namespace and
# every listed mirror reaches phase=Ready with a non-empty
# status.targetInfo. status.targetInfo is set by the target-side
# gateway reconciler after opening the libmxl Writer and the
# fabrics Target; an empty value means the target plumbing never
# materialised even if the phase string happens to read Ready.

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

for name in $names; do
  ti=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${name}" \
        -o jsonpath='{.status.targetInfo}')
  if [ -z "$ti" ]; then
    fail "MxlFlowMirror/${name} has empty status.targetInfo (target side never opened)"
  fi
  echo "  ${name}: Ready, targetInfo set"
done

echo "  phases: ${phase}"
