#!/usr/bin/env bash
# Assert that an MxlFlow, an MxlFlowMirror and an origin Lease nothing
# justifies are actually collected, and that the live ones the demo is
# built on are not.
#
# The flow's grace period is not waited out. It is measured from the
# transition time of the condition that turned, so the case backdates
# that timestamp and lets the resulting write wake the reconciler -- the
# same trick the unit tests use, and the reason the clock was moved onto
# the object in the first place. Waiting five minutes would put this
# case past the suite's whole runtime; shortening the grace on the
# cluster would test a configuration nothing runs.
#
# What only a live cluster can prove is that the collectors have the
# RBAC to delete. A missing verb on mxlflows, mxlflowmirrors or
# coordination.k8s.io/leases fails nothing at startup and nothing in a
# unit test: the objects simply stay, which is indistinguishable from
# the state this whole change set exists to fix.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

need python3

GC_FLOW="fbfbfbfb-0000-4000-8000-00000000000c"
GC_MIRROR="${GC_FLOW}--gc-test"
GC_UNCLAIMED="${GC_FLOW}--gc-unclaimed"
GC_LEASE="mxl-flow-fbfbfbfb-0000-4000-8000-00000000000d-node-that-left"

cleanup() {
  "${KUBECTL[@]}" delete mxlflow "$GC_FLOW" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete mxlflowmirror "$GC_MIRROR" "$GC_UNCLAIMED" \
    --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n mxl-system delete lease "$GC_LEASE" \
    --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# gone <kubectl-args...> -- polls until the object is absent.
gone() {
  local deadline=$(( $(date +%s) + 90 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    "${KUBECTL[@]}" get "$@" >/dev/null 2>&1 || return 0
    sleep 2
  done
  return 1
}

# condition <reason-path> -- polls until a condition of the given type
# reads the expected status, printing its reason.
# usage: cond_reason <kubectl-args...> <type>
cond_reason() {
  local type="${*: -1}"
  local args=("${@:1:$#-1}")
  "${KUBECTL[@]}" get "${args[@]}" -o json 2>/dev/null | python3 -c '
import json, sys
want = sys.argv[1]
o = json.load(sys.stdin)
for c in (o.get("status") or {}).get("conditions", []):
    if c.get("type") == want:
        print(c.get("status", "") + "/" + c.get("reason", ""))
        break
' "$type"
}

wait_cond() {
  local want="$1"; shift
  local deadline=$(( $(date +%s) + 90 )) got=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    got=$(cond_reason "$@" || true)
    [ "$got" = "$want" ] && { echo "$got"; return 0; }
    sleep 2
  done
  echo "wanted $want, last saw '${got:-<none>}'" >&2
  return 1
}

# backdate <kubectl-args...> -- rewrites every operator-owned condition
# on the object so its grace period has already elapsed. Server-side
# apply preserves a transition time whose status has not changed, so
# the operator reads this value back rather than restamping it.
backdate() {
  local past
  past=$(python3 -c '
import datetime
print((datetime.datetime.now(datetime.timezone.utc)
       - datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
  local patch
  patch=$("${KUBECTL[@]}" get "$@" -o json 2>/dev/null | python3 -c '
import json, sys
past = sys.argv[1]
owned = {"Live", "Claimed", "Sourceable"}
o = json.load(sys.stdin)
conds = []
for c in (o.get("status") or {}).get("conditions", []):
    if c.get("type") in owned:
        c = dict(c, lastTransitionTime=past)
    conds.append(c)
print(json.dumps({"status": {"conditions": conds}}))
' "$past")
  "${KUBECTL[@]}" patch "$@" --subresource=status --type=merge -p "$patch" >/dev/null
}

# --- the demo's own objects are judged justified ----------------------

# The collectors run against everything on the cluster, so the first
# thing to establish is that they leave the working set alone.
mirrors=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o name 2>/dev/null)
[ -n "$mirrors" ] || fail "no MxlFlowMirror in ${NAMESPACE} to check"

for m in $mirrors; do
  claimed=$(cond_reason -n "$NAMESPACE" "$m" Claimed)
  sourceable=$(cond_reason -n "$NAMESPACE" "$m" Sourceable)
  echo "  ${m#mxlflowmirror.mxl.qvest-digital.com/}: Claimed=${claimed:-<unset>} Sourceable=${sourceable:-<unset>}"
  case "$claimed" in
    True/*) ;;
    *) fail "$m has Claimed=${claimed:-<unset>}; a mirror a running consumer asks for must read claimed, or the collector will take it" ;;
  esac
  case "$sourceable" in
    True/*) ;;
    *) fail "$m has Sourceable=${sourceable:-<unset>}; its flow has a live origin, so the collector must see one" ;;
  esac
done

# --- a claimed mirror with no source waits, then goes ----------------

# A flow with no locations at all: nothing holds a copy, which is what
# a producer's node being reclaimed leaves behind once the departed
# location has been pruned.
"${KUBECTL[@]}" apply -f - <<EOF >/dev/null
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlFlow
metadata:
  name: $GC_FLOW
spec:
  id: $GC_FLOW
  definition:
    id: $GC_FLOW
EOF

node=$("${KUBECTL[@]}" get nodes -o jsonpath='{.items[0].metadata.name}')
[ -n "$node" ] || fail "could not read a node name"

# Claimed by a pod that really is running, so the mirror survives long
# enough for its conditions to be read. Any pod will do: the collector
# asks whether spec.requestor resolves, not what the pod is.
claimant=$("${KUBECTL[@]}" -n mxl-system get pods \
  -l app.kubernetes.io/component=agent \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
claimant_uid=$("${KUBECTL[@]}" -n mxl-system get "pod/${claimant}" \
  -o jsonpath='{.metadata.uid}' 2>/dev/null)
[ -n "$claimant" ] && [ -n "$claimant_uid" ] || fail "no agent pod to claim the test mirror"

"${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlFlowMirror
metadata:
  name: $GC_MIRROR
spec:
  flowID: $GC_FLOW
  sourceNode: $node
  targetNode: $node
  provider: tcp
  requestor:
    name: $claimant
    namespace: mxl-system
    uid: $claimant_uid
EOF

wait_cond "True/RequestorLive" -n "$NAMESPACE" "mxlflowmirror/$GC_MIRROR" Claimed \
  >/dev/null || fail "the operator did not publish Claimed=True on a mirror whose requestor pod is running"
wait_cond "False/OriginUnresolved" -n "$NAMESPACE" "mxlflowmirror/$GC_MIRROR" Sourceable \
  >/dev/null || fail "the operator did not publish Sourceable=False/OriginUnresolved on a mirror whose flow names no origin"
echo "  claimed mirror with no source: waiting out the grace period"

# Still there: a source that has gone gets the grace, because the flow
# may be mid-republish and tearing down a working consumer's mirror for
# a producer that is restarting costs it a re-materialization.
"${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/$GC_MIRROR" >/dev/null 2>&1 \
  || fail "a claimed mirror was collected before its grace period elapsed"

backdate -n "$NAMESPACE" "mxlflowmirror/$GC_MIRROR"
gone -n "$NAMESPACE" "mxlflowmirror/$GC_MIRROR" \
  || fail "a mirror whose flow names no origin survived its grace period. Check the operator's delete verb on mxlflowmirrors"
echo "  unsourceable mirror collected after its grace period"

# --- an unclaimed mirror goes at once --------------------------------

# No owner reference and no requestor. Under the collectors this
# replaces, a mirror carrying neither creator label either was owned by
# none of them and would have outlived the cluster.
"${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlFlowMirror
metadata:
  name: $GC_UNCLAIMED
spec:
  flowID: $GC_FLOW
  sourceNode: $node
  targetNode: $node
  provider: tcp
EOF

# No condition to observe: a claim names its claimant, so the claimant
# being gone is unambiguous and the mirror is deleted in the same pass
# that judges it.
gone -n "$NAMESPACE" "mxlflowmirror/$GC_UNCLAIMED" \
  || fail "an unclaimed MxlFlowMirror survived. Check the operator's delete verb on mxlflowmirrors"
echo "  unclaimed mirror collected at once"

# --- and then the flow it was the last reference to -------------------

# The flow was kept alive by that mirror, so this is also the assertion
# that the two are no longer each other's justification: the mirror
# citing the flow and the flow citing the mirror's target copy is the
# cycle in which neither was ever collected.
wait_cond "False/NoLiveCopy" "mxlflow/$GC_FLOW" Live \
  >/dev/null || fail "the operator did not publish Live=False/NoLiveCopy on a flow with no origin and no mirror"

backdate "mxlflow/$GC_FLOW"
gone "mxlflow/$GC_FLOW" \
  || fail "an MxlFlow with no live copy survived its grace period. Check the operator's delete verb on mxlflows"
echo "  flow with no live copy collected"

# --- an orphaned origin Lease is collected ----------------------------

# Expired, and naming a node that is not in the cluster. The agent that
# wrote it is the only other thing that deletes one, and a node removed
# from the cluster takes its agent with it -- which is how a showcase
# cluster accumulated eight of these over a week.
"${KUBECTL[@]}" -n mxl-system apply -f - <<EOF >/dev/null
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: $GC_LEASE
  namespace: mxl-system
spec:
  holderIdentity: node-that-left
  leaseDurationSeconds: 30
  renewTime: "$(python3 -c '
import datetime
print((datetime.datetime.now(datetime.timezone.utc)
       - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000000Z"))')"
EOF

gone -n mxl-system "lease/$GC_LEASE" \
  || fail "an expired origin Lease naming a node that left the cluster survived. Check the operator's delete verb on coordination.k8s.io/leases"
echo "  orphaned origin Lease collected"

# --- and the live ones are still there --------------------------------

# The collectors are cluster-wide, so the last thing to establish is
# that the pass which removed three objects removed only those three.
live=$("${KUBECTL[@]}" -n mxl-system get leases -o name 2>/dev/null | grep -c '^lease.coordination.k8s.io/mxl-flow-' || true)
[ "${live:-0}" -gt 0 ] \
  || fail "no origin Lease left in mxl-system; the demo's producers hold flows, so their Leases must survive"
echo "  ${live} live origin Lease(s) untouched"

"${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o name 2>/dev/null | grep -q . \
  || fail "no MxlFlowMirror left in ${NAMESPACE}; the demo's mirrors are claimed and sourceable and must survive"

echo "  flow, mirror and lease collection all reached the API"
