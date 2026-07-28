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
# --ignore-daemonsets exists. The agent survives to notice its flow
# directory vanish and demote its own location to Stale. The entry
# stays on the flow, because the node is still there -- that is what
# separates a drained node from a departed one, whose entry the
# operator removes outright.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -uo pipefail

# shellcheck source=../lib.sh
. "${KIND_TEST_LIB:?KIND_TEST_LIB not set}"

DRAIN_TIMEOUT_SECS="${DRAIN_TIMEOUT_SECS:-120}"
SETTLE_TIMEOUT_SECS="${SETTLE_TIMEOUT_SECS:-120}"

victim=""
uncordon_done=0

uncordon_node() {
  [ -n "$victim" ] || return 0
  [ "$uncordon_done" -eq 0 ] || return 0
  uncordon_done=1
  "${KUBECTL[@]}" uncordon "$victim" >/dev/null 2>&1 || true
}

# Always make the node schedulable again: the demo's podAntiAffinity
# needs both workers, so a cordon left behind would strand a pod
# Pending for every case that follows.
trap uncordon_node EXIT

for node in $("${KUBECTL[@]}" get nodes \
    -l '!node-role.kubernetes.io/control-plane' \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'); do
  if [ "$(location_count "$node")" -gt 0 ]; then
    victim="$node"
    break
  fi
done
[ -n "$victim" ] || fail "no worker node carries an MxlFlow location; nothing to observe"

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

# The entry must survive: the node is still registered, so the
# operator's departed-node prune has no business touching it. Only
# its phase moves, and only the agent moves it.
deadline=$(( $(date +%s) + SETTLE_TIMEOUT_SECS ))
non_stale=-1
while [ "$(date +%s)" -lt "$deadline" ]; do
  non_stale="$(location_phases "$victim" | grep -cv '^Stale$' || true)"
  [ "$non_stale" -eq 0 ] && break
  sleep 3
done

remaining="$(location_count "$victim")"
[ "$remaining" -eq "$before_count" ] \
  || fail "drain removed location entries for ${victim} (${before_count} -> ${remaining}); a drained node is not a departed one"

if [ "$non_stale" -ne 0 ]; then
  echo "phases still recorded for ${victim}:" >&2
  location_phases "$victim" >&2
  fail "after ${SETTLE_TIMEOUT_SECS}s, ${non_stale} location(s) on drained ${victim} are still not Stale"
fi
echo "  drained: ${remaining} location(s) retained and demoted to Stale, DaemonSets kept"

# --- uncordon ----------------------------------------------------
uncordon_node
echo "  ${victim} uncordoned"
