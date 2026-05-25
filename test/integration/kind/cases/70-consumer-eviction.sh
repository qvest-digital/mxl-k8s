#!/usr/bin/env bash
# Assert the operator garbage-collects the demo MxlFlowMirror when
# the consumer pod is removed. The demo reader is a bare Pod;
# deleting it leaves the MxlReceiver pointing at a podRef that no
# longer resolves, which the operator's pod watch must translate
# into a mirror GC. The reader is re-applied at the end so the
# rest of the suite (notably 80-origin-lease-expiry) still has a
# materialized mirror to inspect.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

READER_POD="${READER_POD:-mxl-tcp-demo-reader}"
READER_MANIFEST="${READER_MANIFEST:-${PWD}/examples/tcp-demo/21-reader.yaml}"
GC_TIMEOUT_SECS="${GC_TIMEOUT_SECS:-30}"

# Establish baseline: at least one mirror exists so the assertion
# is not trivially satisfied.
pre=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
      -o jsonpath='{range .items[*]}{.metadata.name} {end}')
[ -n "$pre" ] || fail "no MxlFlowMirror in namespace ${NAMESPACE} before deletion"
echo "  pre-deletion mirrors: ${pre}"

"${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${READER_POD}" \
    --grace-period=0 --force --ignore-not-found \
  || fail "delete pod/${READER_POD} failed"

# Wait for the pod to actually be gone; the operator's resolver
# returns nil targets only on a NotFound, not on Terminating.
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete \
    "pod/${READER_POD}" --timeout=30s >/dev/null 2>&1 || true

deadline=$(( $(date +%s) + GC_TIMEOUT_SECS ))
remaining=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  remaining=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
              -o jsonpath='{range .items[*]}{.metadata.name} {end}' 2>/dev/null || true)
  # Trim trailing whitespace so an empty list compares equal to "".
  remaining="$(echo "$remaining" | sed 's/[[:space:]]*$//')"
  [ -z "$remaining" ] && break
  sleep 2
done
if [ -n "$remaining" ]; then
  fail "MxlFlowMirror(s) not GC'd within ${GC_TIMEOUT_SECS}s after reader deletion: ${remaining}"
fi
echo "  mirrors GC'd within budget"

# Restore the reader so subsequent cases see a converged demo. A
# per-pod apply (not the all-in-one tcp-demo kustomization) leaves
# the rest of the cluster untouched and avoids spawning legacy
# DaemonSets that the kustomize-only manifest set would.
"${KUBECTL[@]}" -n "$NAMESPACE" apply -f "$READER_MANIFEST" >/dev/null \
  || fail "re-apply ${READER_MANIFEST} failed"
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
    "pod/${READER_POD}" --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "${READER_POD} did not return to Ready after restore"

# Wait until the operator re-creates the mirror so cleanup is
# observable, not just declarative.
JSONPATH='{range .items[*]}{.metadata.name}={.status.phase};{end}'
phase=$(wait_phase mxlfm "$JSONPATH" \
        '^([a-z0-9-]+=Ready;)+$' \
        "$MIRROR_TIMEOUT_SECS") \
  || fail "mirror did not return to Ready after reader restore"
echo "  post-restore phases: ${phase}"
