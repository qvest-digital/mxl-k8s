#!/usr/bin/env bash
# Assert the demo MxlFlowMirror reconverges after the producer pod
# churns. The producer in examples/tcp-demo is a bare Pod (not a
# Deployment); the rollout equivalent is delete + re-apply. After
# the new writer reaches Ready, the agent's PublishVanished must
# have demoted the old Origin (or released its Lease) and the
# new writer's agent must have republished a fresh Origin so the
# operator can keep mxlfm.spec.sourceNode pointing at a live node
# and the mirror returns to Ready.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

WRITER_POD="${WRITER_POD:-mxl-tcp-demo-writer}"
REAPPLY_DIR="${REAPPLY_DIR:-${PWD}/examples/kind/demo}"
RECOVERY_TIMEOUT_SECS="${RECOVERY_TIMEOUT_SECS:-90}"

mirror=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
          -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[ -n "$mirror" ] || fail "no MxlFlowMirror found in namespace ${NAMESPACE}"

orig_source=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
              -o jsonpath='{.spec.sourceNode}')
[ -n "$orig_source" ] || fail "mxlfm/${mirror} has empty spec.sourceNode"
orig_uid=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${WRITER_POD}" \
            -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
echo "  mirror=${mirror} sourceNode=${orig_source} writer.uid=${orig_uid}"

"${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${WRITER_POD}" \
    --grace-period=0 --force --ignore-not-found \
  || fail "delete pod/${WRITER_POD} failed"

# Re-apply the demo kustomization to recreate the writer pod.
# apply is idempotent for the other manifests.
"${KUBECTL[@]}" apply -k "$REAPPLY_DIR" >/dev/null \
  || fail "re-apply ${REAPPLY_DIR} failed"

"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
    "pod/${WRITER_POD}" --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "${WRITER_POD} did not become Ready after restart"

new_uid=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${WRITER_POD}" \
          -o jsonpath='{.metadata.uid}')
[ "$new_uid" != "$orig_uid" ] \
  || fail "writer pod UID did not change after delete + apply (uid=${new_uid})"
new_node=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${WRITER_POD}" \
            -o jsonpath='{.spec.nodeName}')
echo "  writer recreated uid=${new_uid} on node=${new_node}"

# Wait for the mirror to return to Ready. The mirror's name is
# stable as long as the consumer pod stays on the same target
# node, so polling mxlfm/${mirror} is safe in the typical case
# where the writer returns to the original worker.
phase=$(wait_phase "mxlfm/${mirror}" '{.status.phase}' '^Ready$' \
          "$RECOVERY_TIMEOUT_SECS") \
  || fail "mxlfm/${mirror} did not return to Ready within ${RECOVERY_TIMEOUT_SECS}s (last=${phase})"
echo "  mxlfm/${mirror} phase=${phase}"

new_source=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
              -o jsonpath='{.spec.sourceNode}')
[ -n "$new_source" ] || fail "mxlfm/${mirror} has empty spec.sourceNode after recovery"
echo "  mxlfm sourceNode after recovery: ${new_source}"
