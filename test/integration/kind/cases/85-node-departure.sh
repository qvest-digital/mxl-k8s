#!/usr/bin/env bash
# 85-node-departure.sh -- a node leaves the cluster and rejoins.
#
# Node removal is the one lifecycle event no agent can report for
# itself: the agent dies with the node, so whatever it published on
# MxlFlow.status.locations outlives it. On a cluster that recycles
# capacity (spot instances, autoscaler consolidation) those entries
# accumulate, and an Origin among them reads as a live producer to
# anything that does not also consult the Lease.
#
# The case drives a real departure and a real rejoin. A KIND node is
# a container, so stopping it and deleting the Node object reproduces
# reclamation exactly as the API server sees it, and starting the
# container again lets the kubelet re-register under the same name --
# a full leave/join cycle without recreating the cluster.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -uo pipefail

# shellcheck source=../lib.sh
. "${KIND_TEST_LIB:?KIND_TEST_LIB not set}"

RUNTIME="${CONTAINER_RUNTIME:-docker}"
need "$RUNTIME"

PRUNE_TIMEOUT_SECS="${PRUNE_TIMEOUT_SECS:-90}"
REJOIN_TIMEOUT_SECS="${REJOIN_TIMEOUT_SECS:-180}"

# flows_on_node <node> -- names of MxlFlows carrying a location for
# the node, one per line. MxlFlow is cluster-scoped.
#
# The filter that reads naturally here,
# .items[?(@.status.locations[*].nodeName=='x')], cannot be used:
# kubectl's jsonpath engine rejects a comparison whose left side
# expands to more than one element ("can only compare one element at
# a time"), and any flow with a second location does exactly that.
# Emitting the node list per flow and matching in shell sidesteps it.
#
# stderr is deliberately not discarded. A jsonpath or connection
# error must not read as "this node carries no locations".
flows_on_node() {
  local want="$1" name nodes
  "${KUBECTL[@]}" get mxlflow -o \
    'jsonpath={range .items[*]}{.metadata.name}{"|"}{range .status.locations[*]}{.nodeName}{","}{end}{"\n"}{end}' \
  | while IFS='|' read -r name nodes; do
      [ -n "$name" ] || continue
      case ",${nodes}" in
        *",${want},"*) echo "$name" ;;
      esac
    done
}

# location_count <node> -- how many flows still list the node.
location_count() {
  flows_on_node "$1" | grep -c . || true
}

# mirrors_sourced_from <node> -- "<name>=<phase>" per mirror whose
# spec.sourceNode is the node. Same range-and-match shape as above.
mirrors_sourced_from() {
  local want="$1" name src phase
  "${KUBECTL[@]}" get mxlflowmirror -A -o \
    'jsonpath={range .items[*]}{.metadata.name}{"|"}{.spec.sourceNode}{"|"}{.status.phase}{"\n"}{end}' \
  | while IFS='|' read -r name src phase; do
      [ -n "$name" ] || continue
      [ "$src" = "$want" ] && echo "${name}=${phase}"
    done
}

victim=""
restore_done=0

restore_node() {
  [ -n "$victim" ] || return 0
  [ "$restore_done" -eq 0 ] || return 0
  restore_done=1
  "$RUNTIME" start "$victim" >/dev/null 2>&1 || true
  local deadline
  deadline=$(( $(date +%s) + REJOIN_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if "${KUBECTL[@]}" get node "$victim" \
        -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null \
        | grep -qx True; then
      echo "  ${victim} rejoined and is Ready"
      "${KUBECTL[@]}" -n "$NAMESPACE" rollout status ds/mxl-k8s-gateway \
        --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null 2>&1 || true
      "${KUBECTL[@]}" -n "$NAMESPACE" rollout status ds/mxl-k8s-agent \
        --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null 2>&1 || true
      return 0
    fi
    sleep 3
  done
  echo "WARN: ${victim} did not rejoin within ${REJOIN_TIMEOUT_SECS}s" >&2
}

# Always put the node back, including on a mid-case failure: a
# missing worker would sabotage any case that runs after this one.
trap restore_node EXIT

for node in $("${KUBECTL[@]}" get nodes \
    -l '!node-role.kubernetes.io/control-plane' \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'); do
  if [ "$(location_count "$node")" -gt 0 ]; then
    victim="$node"
    break
  fi
done
[ -n "$victim" ] || fail "no worker node carries an MxlFlow location; nothing to observe"

before="$(location_count "$victim")"
echo "  victim=${victim} locations=${before}"

if ! "$RUNTIME" inspect "$victim" >/dev/null 2>&1; then
  fail "no ${RUNTIME} container named ${victim}; is this a KIND cluster?"
fi

"$RUNTIME" stop "$victim" >/dev/null || fail "could not stop container ${victim}"
"${KUBECTL[@]}" delete node "$victim" --wait=true >/dev/null \
  || fail "could not delete Node ${victim}"

deadline=$(( $(date +%s) + PRUNE_TIMEOUT_SECS ))
remaining=-1
while [ "$(date +%s)" -lt "$deadline" ]; do
  remaining="$(location_count "$victim")"
  [ "$remaining" -eq 0 ] && break
  sleep 3
done

if [ "$remaining" -ne 0 ]; then
  echo "flows still listing ${victim}:" >&2
  flows_on_node "$victim" >&2
  fail "after ${PRUNE_TIMEOUT_SECS}s, ${remaining} flow(s) still list departed node ${victim}"
fi
echo "  ${victim} stopped, Node deleted, ${before} location(s) pruned"

# No mirror may keep claiming Ready while sourcing from the node that
# just left. Anything still Ready there is reporting a producer that
# cannot exist.
stale_ready="$(mirrors_sourced_from "$victim" | grep -c '=Ready$' || true)"
[ "$stale_ready" -eq 0 ] \
  || fail "${stale_ready} mirror(s) still report Ready with sourceNode=${victim}"
echo "  no mirror reports Ready with sourceNode=${victim}"

restore_node

deadline=$(( $(date +%s) + REJOIN_TIMEOUT_SECS ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  "${KUBECTL[@]}" get node "$victim" >/dev/null 2>&1 && break
  sleep 3
done
"${KUBECTL[@]}" get node "$victim" >/dev/null 2>&1 \
  || fail "${victim} never came back; later cases would run a worker short"

