#!/usr/bin/env bash
# 86-node-drain.sh -- cordon and drain are not departure.
#
# The control plane treats one node signal as terminal: the Node
# object being gone. Cordon and drain both leave it in place, and the
# distinction is load-bearing.
#
# Cordon only blocks new scheduling. DaemonSet pods tolerate
# node.kubernetes.io/unschedulable, so the agent keeps publishing and
# the gateway keeps serving; nothing about the flow changes.
#
# Drain evicts the workload but skips DaemonSets, which is why
# --ignore-daemonsets exists. The agent survives to settle its own
# locations: Stale while a flow directory is still on disk with nothing
# feeding it, removed once the directory is gone. The operator has no
# part in it, because the node is still there -- that is what separates
# a drained node from a departed one, whose entries the operator prunes
# and records as LocationsPruned.
#
# The case runs the demo writer on the node it drains, so there is a
# live Origin for the drain to take away: the cases before it leave no
# producer behind, and a node listing nothing proves nothing. The
# writer is put back as it was found, running unpinned or absent.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -uo pipefail

# shellcheck source=../lib.sh
. "${KIND_TEST_LIB:?KIND_TEST_LIB not set}"

DRAIN_TIMEOUT_SECS="${DRAIN_TIMEOUT_SECS:-120}"
SETTLE_TIMEOUT_SECS="${SETTLE_TIMEOUT_SECS:-120}"
VIDEO_FLOW="${VIDEO_FLOW:-5fbec3b1-1b0f-417d-9059-8b94a47197ed}"
WRITER_POD="${WRITER_POD:-mxl-tcp-demo-writer}"
WRITER_MANIFEST="${WRITER_MANIFEST:-$(demo_manifest "${PWD}/examples/tcp-demo/10-writer.yaml")}"

victim=""
uncordon_done=0
had_writer=0
"${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${WRITER_POD}" >/dev/null 2>&1 && had_writer=1

# Uncordon, then put the writer back as it was found. Always make the
# node schedulable again: the demo's podAntiAffinity needs both
# workers, so a cordon left behind would strand a pod Pending for every
# case that follows.
restore() {
  [ "$uncordon_done" -eq 0 ] || return 0
  uncordon_done=1
  [ -n "$victim" ] && "${KUBECTL[@]}" uncordon "$victim" >/dev/null 2>&1
  "${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${WRITER_POD}" \
      --grace-period=0 --force --ignore-not-found >/dev/null 2>&1
  if [ "$had_writer" -eq 1 ]; then
    "${KUBECTL[@]}" -n "$NAMESPACE" apply -f "$WRITER_MANIFEST" >/dev/null 2>&1
  fi
  return 0
}
trap restore EXIT

# flow_phase_on <node> -- the phase the demo video flow records for the
# node, empty when it lists none.
flow_phase_on() {
  "${KUBECTL[@]}" get mxlflow "$VIDEO_FLOW" \
      -o 'jsonpath={range .status.locations[*]}{.nodeName}{"="}{.phase}{"\n"}{end}' 2>/dev/null \
    | awk -F= -v n="$1" '$1 == n { print $2 }'
}

victim="$("${KUBECTL[@]}" get nodes -l '!node-role.kubernetes.io/control-plane' \
  -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' | head -n 1)"
[ -n "$victim" ] || fail "no worker node to drain"

# The writer, pinned to the victim. nodeName bypasses the scheduler, so
# where it runs is not up to the anti-affinity with the reader.
pinned="$(mktemp)"
awk -v node="$victim" '
  /^spec:$/ && !done { print; print "  nodeName: " node; done = 1; next }
  { print }
' "$WRITER_MANIFEST" > "$pinned"
grep -q "nodeName: ${victim}" "$pinned" || fail "could not pin the writer manifest to ${victim}"
"${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${WRITER_POD}" \
    --grace-period=0 --force --ignore-not-found >/dev/null 2>&1
"${KUBECTL[@]}" -n "$NAMESPACE" apply -f "$pinned" >/dev/null || fail "apply writer pinned to ${victim} failed"
rm -f "$pinned"
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
    "pod/${WRITER_POD}" --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null \
  || fail "${WRITER_POD} did not become Ready on ${victim}"

deadline=$(( $(date +%s) + SETTLE_TIMEOUT_SECS ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  [ "$(flow_phase_on "$victim")" = "Origin" ] && break
  sleep 3
done
[ "$(flow_phase_on "$victim")" = "Origin" ] \
  || fail "${VIDEO_FLOW} records '$(flow_phase_on "$victim")' on ${victim} with its writer running there, not Origin"

before_count="$(location_count "$victim")"
echo "  victim=${victim} locations=${before_count}"

agent_before="$(daemonset_pod_on agent "$victim")"
gateway_before="$(daemonset_pod_on gateway "$victim")"
[ -n "$agent_before" ] || fail "no running agent pod on ${victim} to begin with"
[ -n "$gateway_before" ] || fail "no running gateway pod on ${victim} to begin with"

# --- cordon ------------------------------------------------------
"${KUBECTL[@]}" cordon "$victim" >/dev/null || fail "could not cordon ${victim}"
sleep 5

after_cordon="$(location_count "$victim")"
[ "$after_cordon" -eq "$before_count" ] \
  || fail "cordon changed the location count for ${victim}: ${before_count} -> ${after_cordon}"
[ -n "$(daemonset_pod_on agent "$victim")" ] \
  || fail "agent pod left ${victim} on cordon; DaemonSets tolerate unschedulable"
echo "  cordoned: ${after_cordon} location(s) intact, agent and gateway still running"

# --- drain -------------------------------------------------------
# Second precision, as the event timestamps compared against it.
drain_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
"${KUBECTL[@]}" drain "$victim" \
  --ignore-daemonsets --delete-emptydir-data --force \
  --timeout="${DRAIN_TIMEOUT_SECS}s" >/dev/null 2>&1 \
  || fail "drain of ${victim} did not complete within ${DRAIN_TIMEOUT_SECS}s"

# Drain must not have taken the agent or the gateway with it: without
# them the node could not report its own state, which is precisely
# the condition that makes a departure unrecoverable.
[ -n "$(daemonset_pod_on agent "$victim")" ] \
  || fail "agent pod gone from ${victim} after drain --ignore-daemonsets"
[ -n "$(daemonset_pod_on gateway "$victim")" ] \
  || fail "gateway pod gone from ${victim} after drain --ignore-daemonsets"

# Nothing on the drained node may stay published as a copy a consumer
# can be sent to. What is left is Stale or gone, and only the agent
# moves it.
deadline=$(( $(date +%s) + SETTLE_TIMEOUT_SECS ))
non_stale=-1
while [ "$(date +%s)" -lt "$deadline" ]; do
  non_stale="$(location_phases "$victim" | grep -cv '^Stale$' || true)"
  [ "$non_stale" -eq 0 ] && break
  sleep 3
done

if [ "$non_stale" -ne 0 ]; then
  echo "phases still recorded for ${victim}:" >&2
  location_phases "$victim" >&2
  fail "after ${SETTLE_TIMEOUT_SECS}s, ${non_stale} location(s) on drained ${victim} are still neither Stale nor removed"
fi

# The node is still registered, so the operator's departed-node prune
# has no business touching its entries. The case before this one
# departs a node, possibly this one, so only events since the drain
# count.
need python3
pruned="$("${KUBECTL[@]}" get events -A --field-selector reason=LocationsPruned -o json 2>/dev/null \
  | python3 -c '
import json, sys
since, node = sys.argv[1], sys.argv[2]
for e in json.load(sys.stdin).get("items", []):
    at = e.get("lastTimestamp") or e.get("eventTime") or e["metadata"]["creationTimestamp"]
    msg = e.get("message", "")
    # "Dropped locations for departed nodes: [a b]". Matched by name:
    # one kind worker name is a prefix of the next.
    names = msg[msg.find("[") + 1:msg.rfind("]")].split()
    if at[:19] >= since[:19] and node in names:
        print(msg)
' "$drain_start" "$victim")" || fail "could not list LocationsPruned events"
[ -z "$pruned" ] \
  || fail "the operator pruned locations of drained ${victim} as departed: ${pruned}"

remaining="$(location_count "$victim")"
echo "  drained: ${before_count} -> ${remaining} location(s), every one left Stale, none pruned by the operator, DaemonSets kept"

# The drain evicted the writer, so its Origin is what had to go.
case "$(flow_phase_on "$victim")" in
  ''|Stale) ;;
  *) fail "${VIDEO_FLOW} still records '$(flow_phase_on "$victim")' on ${victim} after its writer was drained" ;;
esac
echo "  ${VIDEO_FLOW} on ${victim}: Origin -> '$(flow_phase_on "$victim")' (empty: removed)"

# --- uncordon ----------------------------------------------------
restore
echo "  ${victim} uncordoned, writer restored as found"
