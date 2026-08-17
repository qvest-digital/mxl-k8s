#!/usr/bin/env bash
# Assert that the flow and mirror lifecycle is observable from the API:
# events reach the apiserver, and the location timestamps mean what they
# claim.
#
# Deliberately read-only, and placed after the cases that already drive
# the transitions it inspects. Mirrors reaching Ready (30) produce the
# phase and target-progress events; a flow acquiring an Origin produces
# the origin record. Provoking that churn again would double it for no
# extra coverage, so this case adds reads and no waits.
#
# Two of these properties can only fail on a live cluster. Events are
# dropped silently when the emitting component lacks RBAC on events, so
# a unit test that stubs the recorder proves nothing about whether they
# land. And lastObserved outrunning appearedAt is what keeps the source
# gateway from reading every agent rescan as a producer restart; it
# holds only when the agent, the CRD schema and the gateway agree.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

need python3

# --- events reached the apiserver -------------------------------------

# All namespaces, not just the demo one. An event lands in the involved
# object's namespace, and MxlFlow and MxlNodeCapabilities are cluster
# scoped, so theirs go to default while MxlFlowMirror and MxlReceiver
# events go to mxl-system. Querying one namespace finds half of them and
# reads as a wiring failure. kubectl describe resolves both, which is
# why the split stays invisible until something greps for it.
#
# One query, filtered client-side: a fieldSelector per reason would be a
# round trip each.
events_json=$("${KUBECTL[@]}" get events -A -o json 2>/dev/null) \
  || fail "could not list events"

reasons=$(printf '%s' "$events_json" | python3 -c '
import json, sys
d = json.load(sys.stdin)
seen = {}
for e in d.get("items", []):
    kind = (e.get("involvedObject") or {}).get("kind", "")
    if kind.startswith("Mxl"):
        key = kind + "/" + e.get("reason", "")
        seen[key] = seen.get(key, 0) + 1
for k in sorted(seen):
    print(k, seen[k])
')

echo "  mxl events observed:"
if [ -n "$reasons" ]; then
  printf '%s\n' "$reasons" | sed 's/^/    /'
else
  echo "    (none)"
fi

have() { printf '%s\n' "$reasons" | grep -q "^$1 "; }

# Every mirror reached Ready, and nothing reaches Ready without a
# transition into it. An absent event means the recorder is unwired or
# the gateway lacks RBAC on events -- both silent at runtime.
have "MxlFlowMirror/PhaseChanged" \
  || fail "no PhaseChanged event on any MxlFlowMirror; every mirror reached Ready, so a transition was recorded. Check the gateway's events RBAC and Recorder wiring"

# OriginMoved covers first establishment as well as a later move, so a
# flow carrying an Origin has produced one.
have "MxlFlow/OriginMoved" \
  || fail "no OriginMoved event on any MxlFlow; flows carry an Origin, so establishment was recorded. Check the operator's events RBAC and the flow reconciler's Recorder"

# --- the origin record and the timestamp split ------------------------

# Via a script file rather than python3 -c: the checks below need both
# quote characters, and nesting them inside a shell-quoted -c argument
# is how the first version of this case broke.
check=$(mktemp)
trap 'rm -f "$check"' EXIT
cat >"$check" <<'PYEOF'
import datetime
import json
import sys

d = json.load(sys.stdin)
items = d.get("items", [])
if not items:
    sys.exit("no MxlFlow objects")

now = datetime.datetime.now(datetime.timezone.utc)


def ts(v):
    return datetime.datetime.fromisoformat(v.replace("Z", "+00:00")) if v else None


checked = 0
for f in items:
    name = f["metadata"]["name"][:18]
    st = f.get("status", {})
    locs = st.get("locations", [])
    origins = [l for l in locs if l.get("phase") == "Origin"]
    if not origins:
        continue
    checked += 1
    o = origins[0]

    # The record the objects previously could not answer: which node
    # holds it, and since when.
    if st.get("originNode") != o["nodeName"]:
        sys.exit("%s: status.originNode=%r does not match the Origin location %r"
                 % (name, st.get("originNode"), o["nodeName"]))
    if not st.get("originChangedAt"):
        sys.exit("%s: an Origin is recorded but originChangedAt is unset, so "
                 "nothing says when it got there" % name)

    # appearedAt is the source gateway's rotation baseline. A live
    # location without one leaves a reader unable to detect any later
    # rotation for its whole lifetime; a Stale one that kept its stamp
    # stops the next appearance reading as a rotation at all.
    for l in locs:
        node, phase = l["nodeName"], l.get("phase")
        if phase == "Stale":
            if l.get("appearedAt"):
                sys.exit("%s: Stale location on %s still carries appearedAt"
                         % (name, node))
            continue
        if not l.get("appearedAt"):
            sys.exit("%s: location on %s phase=%s has no appearedAt"
                     % (name, node, phase))

    # The split under test: the heartbeat advances, the appearance does
    # not. Were these still one field, the refresh would read to the
    # gateway as a producer restart once per rescan, forever.
    lo, ap = ts(o.get("lastObserved")), ts(o.get("appearedAt"))
    if lo is None:
        sys.exit("%s: Origin on %s has no lastObserved" % (name, o["nodeName"]))
    if lo < ap:
        sys.exit("%s: lastObserved %s predates appearedAt %s" % (name, lo, ap))
    age = (now - lo).total_seconds()
    # The agent rescans every 30s; three windows is generous and still
    # catches a refresh that never runs at all.
    if age > 95:
        sys.exit("%s: Origin lastObserved is %.0fs old, so the agent is not "
                 "confirming the copy -- the refresh pass is not running"
                 % (name, age))
    print("    %s... origin=%s lastObserved=%.0fs ago appearedAt=%.0fs ago"
          % (name, o["nodeName"], age, (now - ap).total_seconds()))

if checked == 0:
    sys.exit("no MxlFlow carries an Origin location")
print("    checked %d flow(s) with an Origin" % checked)
PYEOF

"${KUBECTL[@]}" get mxlflows -o json 2>/dev/null | python3 "$check" \
  || fail "flow origin record or location timestamps are wrong"

echo "  lifecycle events reach the API and the origin record is consistent"
