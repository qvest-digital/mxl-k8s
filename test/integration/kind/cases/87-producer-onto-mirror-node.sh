#!/usr/bin/env bash
# 87-producer-onto-mirror-node.sh -- a producer lands on the node
# that already mirrors its flow.
#
# This is the one relocation that leaves no trace for the agent to
# read. libmxl builds a new flow in a temporary directory and renames
# it into place, so a producer creating its flow raises a rename the
# fanotify watch sees. A producer that finds the directory already
# there -- because a mirror materialized it for a local consumer --
# takes openFlow(..., READ_WRITE) instead and raises nothing. The node
# keeps the Ready it published for the mirrored copy, and once the
# previous origin is pruned the flow has no Origin location anywhere.
# resolveSourceNode has no answer from that point on, so no mirror can
# be repointed and no further consumer can materialize the flow.
#
# The case reproduces it by pinning the writer onto the mirror's own
# target node, and asserts the two halves of the recovery: the shim
# reports the attach so the agent claims Origin, and the mirror that
# now describes a transfer from the node to itself is removed.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -uo pipefail

# shellcheck source=../lib.sh
. "${KIND_TEST_LIB:?KIND_TEST_LIB not set}"

WRITER_POD="${WRITER_POD:-mxl-tcp-demo-writer}"
WRITER_MANIFEST="${WRITER_MANIFEST:-${PWD}/examples/tcp-demo/10-writer.yaml}"
READER_POD="${READER_POD:-mxl-tcp-demo-reader}"
READER_MANIFEST="${READER_MANIFEST:-${PWD}/examples/tcp-demo/21-reader.yaml}"
CLAIM_TIMEOUT_SECS="${CLAIM_TIMEOUT_SECS:-90}"
SELF_MIRROR_TIMEOUT_SECS="${SELF_MIRROR_TIMEOUT_SECS:-90}"
SURVIVAL_SETTLE_SECS="${SURVIVAL_SETTLE_SECS:-10}"

pinned=0

# Put the writer back where the manifest wants it. A writer left
# pinned to the consumer's node would strand every later case with a
# flow that never needs a mirror.
restore_writer() {
  [ "$pinned" -eq 1 ] || return 0
  pinned=0
  "${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${WRITER_POD}" \
      --grace-period=0 --force --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" apply -f "$WRITER_MANIFEST" >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
      "pod/${WRITER_POD}" --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null 2>&1 \
    || echo "WARN: ${WRITER_POD} did not return to Ready after unpinning" >&2
}
trap restore_writer EXIT

# phase_for <flowID> <node> -- the phase that flow records for the
# node, empty when the node is absent from its locations.
phase_for() {
  local flow="$1" want="$2" pairs pair
  pairs=$("${KUBECTL[@]}" get mxlflow "$flow" -o \
    'jsonpath={range .status.locations[*]}{.nodeName}{"="}{.phase}{","}{end}' 2>/dev/null) || return 1
  IFS=','
  for pair in $pairs; do
    case "$pair" in
      "${want}="*) unset IFS; echo "${pair#*=}"; return 0 ;;
    esac
  done
  unset IFS
  echo ""
}

# Establish the precondition rather than inherit it. The cases that
# run before this one evict the consumer and cycle nodes, and the
# reader is a bare Pod, so it does not come back on its own and the
# intent GC takes its mirror with it. Re-applying here keeps the case
# runnable on its own and in any suite order.
mirror=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
          -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -z "$mirror" ]; then
  echo "  no mirror present; restoring ${READER_POD} to create one"
  "${KUBECTL[@]}" -n "$NAMESPACE" apply -f "$READER_MANIFEST" >/dev/null \
    || fail "re-apply ${READER_MANIFEST} failed"
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
      "pod/${READER_POD}" --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
    || fail "${READER_POD} did not become Ready"
  wait_phase mxlfm '{range .items[*]}{.metadata.name}={.status.phase};{end}' \
      '^([a-z0-9-]+=Ready;)+$' "$MIRROR_TIMEOUT_SECS" >/dev/null \
    || fail "no MxlFlowMirror reached Ready after restoring ${READER_POD}"
  mirror=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
fi
[ -n "$mirror" ] || fail "no MxlFlowMirror found in namespace ${NAMESPACE}"

flow=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
        -o jsonpath='{.spec.flowID}')
target=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
          -o jsonpath='{.spec.targetNode}')
source_node=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
          -o jsonpath='{.spec.sourceNode}')
[ -n "$flow" ] && [ -n "$target" ] && [ -n "$source_node" ] \
  || fail "mxlfm/${mirror} is missing flowID, targetNode or sourceNode"

[ "$target" != "$source_node" ] \
  || fail "mxlfm/${mirror} already sources from its own target; nothing to relocate onto"

before_target="$(phase_for "$flow" "$target")"
echo "  mirror=${mirror} flow=${flow}"
echo "  source=${source_node} target=${target} target.phase=${before_target:-<none>}"

[ "$before_target" = "Ready" ] \
  || fail "expected the mirror target to record Ready before the producer moves, got '${before_target:-<none>}'"

# Pin the writer onto the mirror's target node. nodeName bypasses the
# scheduler, which is what makes the collision deterministic rather
# than a matter of which worker the scheduler happens to pick.
#
# awk rather than a sed newline substitution: GNU sed accepts \n in
# the replacement, BSD sed does not, and the suite has to read the
# same on either.
pinned_manifest="$(mktemp)"
awk -v node="$target" '
  /^spec:$/ && !done { print; print "  nodeName: " node; done = 1; next }
  { print }
' "$WRITER_MANIFEST" > "$pinned_manifest"
grep -q "nodeName: ${target}" "$pinned_manifest" \
  || fail "could not pin the writer manifest to ${target}"

"${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${WRITER_POD}" \
    --grace-period=0 --force --ignore-not-found >/dev/null \
  || fail "delete pod/${WRITER_POD} failed"
pinned=1

"${KUBECTL[@]}" -n "$NAMESPACE" apply -f "$pinned_manifest" >/dev/null \
  || fail "apply writer pinned to ${target} failed"
rm -f "$pinned_manifest"

"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
    "pod/${WRITER_POD}" --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "${WRITER_POD} did not become Ready on ${target}"

landed=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${WRITER_POD}" \
          -o jsonpath='{.spec.nodeName}')
[ "$landed" = "$target" ] \
  || fail "writer landed on ${landed}, expected ${target}"
echo "  writer pinned onto ${target}, the node already mirroring ${flow}"

# The attach notification is the only signal for this transition, so
# a node still reporting Ready means the shim never reported it or the
# agent never acted on it.
deadline=$(( $(date +%s) + CLAIM_TIMEOUT_SECS ))
phase=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  phase="$(phase_for "$flow" "$target")"
  [ "$phase" = "Origin" ] && break
  sleep 3
done
[ "$phase" = "Origin" ] \
  || fail "${target} still records '${phase:-<none>}' for ${flow} after ${CLAIM_TIMEOUT_SECS}s; the producer attach was never claimed"
echo "  ${target} claimed Origin for ${flow}"

# Only the Origin renews the Lease, so the claim has to bring one with
# it or consumers read the new Origin as stale.
lease="mxl-flow-${flow}-${target}"
"${KUBECTL[@]}" -n "$NAMESPACE" get "lease/${lease}" >/dev/null 2>&1 \
  || fail "no origin Lease ${lease} after ${target} claimed Origin"
echo "  origin Lease ${lease} present"

# A mirror from the node to itself describes no transfer. Removing it
# is safe while the producer holds the flow: libmxl deletes a flow on
# release only when the departing writer can take an exclusive flock,
# which the producer's own shared lock denies.
deadline=$(( $(date +%s) + SELF_MIRROR_TIMEOUT_SECS ))
gone=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  if ! "${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" >/dev/null 2>&1; then
    gone=1
    break
  fi
  sleep 3
done
if [ "$gone" -ne 1 ]; then
  "${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
    -o 'jsonpath={.spec.sourceNode}{" -> "}{.spec.targetNode}{" "}{.status.phase}{"\n"}' >&2 || true
  fail "mxlfm/${mirror} still exists ${SELF_MIRROR_TIMEOUT_SECS}s after its origin moved onto ${target}"
fi
echo "  mxlfm/${mirror} removed once its origin and target became the same node"

# The flow directory must survive that removal. Tearing the mirror
# down closes the gateway's FlowWriter, and libmxl deletes a flow on
# release only when the departing writer can take an exclusive flock,
# which the local producer's shared lock denies. Had it been deleted,
# the agent's fanotify watch would demote this node to Stale, so the
# settle window is what gives that demotion time to land and makes the
# assertion mean something.
sleep "$SURVIVAL_SETTLE_SECS"
still="$(phase_for "$flow" "$target")"
[ "$still" = "Origin" ] \
  || fail "${target} records '${still:-<none>}' for ${flow} ${SURVIVAL_SETTLE_SECS}s after the mirror was removed; the local flow did not survive the teardown"
echo "  ${flow} still Origin on ${target} after mirror teardown"

restore_writer
