#!/usr/bin/env bash
# 56-origin-lease-gate.sh -- only a flow's Origin renews its Lease.
#
# The Lease is what lets a consumer reject an Origin whose producer
# died: resolveSourceNode skips an Origin location whose Lease has
# gone stale. That only works while the Lease means "this flow's
# producer is alive". A mirror target renewing one too turns it into
# "some node holds a copy of this flow", which every target satisfies,
# and leaves nothing to tell a live producer from a mirrored copy of a
# dead one.
#
# The assertion is over whatever the suite has already built, so the
# case costs one API round trip per location and adds no cluster
# churn. It runs while the demo is converged: the cases that cycle
# nodes take the producer's bare Pod with them, and a run with no
# producer left has no Origin to assert against.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -uo pipefail

# shellcheck source=../lib.sh
. "${KIND_TEST_LIB:?KIND_TEST_LIB not set}"

LEASES_NAMESPACE="${LEASES_NAMESPACE:-$NAMESPACE}"

# One "<flowID>|<node>=<phase>,..." per line. Written to a file rather
# than piped so the counters below survive: a pipeline would run the
# loop in a subshell and discard them. The flow name is emitted
# outside the locations range because kubectl's jsonpath cannot reach
# back to the parent from inside one.
locations="$(mktemp)"
trap 'rm -f "$locations"' EXIT

"${KUBECTL[@]}" get mxlflow -o \
  'jsonpath={range .items[*]}{.metadata.name}{"|"}{range .status.locations[*]}{.nodeName}{"="}{.phase}{","}{end}{"\n"}{end}' \
  2>/dev/null > "$locations" || fail "could not list MxlFlow locations"

[ -s "$locations" ] || fail "no MxlFlow carries a location; nothing to assert"

checked=0
origins=0
violations=0

while IFS='|' read -r flow locs; do
  [ -n "$flow" ] || continue
  # Split the comma-joined "<node>=<phase>" list without disturbing
  # the outer read's IFS.
  locs="$(echo "$locs" | tr ',' '\n')"
  while read -r pair; do
    [ -n "$pair" ] || continue
    node="${pair%%=*}"
    phase="${pair#*=}"
    [ -n "$node" ] || continue

    lease="mxl-flow-${flow}-${node}"
    if "${KUBECTL[@]}" -n "$LEASES_NAMESPACE" get "lease/${lease}" >/dev/null 2>&1; then
      held=yes
    else
      held=no
    fi
    checked=$(( checked + 1 ))

    case "$phase" in
      Origin)
        origins=$(( origins + 1 ))
        if [ "$held" = no ]; then
          echo "  VIOLATION ${flow} on ${node} is Origin with no Lease" >&2
          violations=$(( violations + 1 ))
        fi
        ;;
      Ready)
        # A mirror target holding a Lease is the defect this gates: it
        # makes the Lease answer a question no consumer asked.
        if [ "$held" = yes ]; then
          echo "  VIOLATION ${flow} on ${node} is Ready but renews an origin Lease" >&2
          violations=$(( violations + 1 ))
        fi
        ;;
    esac
  done <<EOF
$locs
EOF
done < "$locations"

echo "  checked ${checked} location(s), ${origins} of them Origin"

[ "$origins" -gt 0 ] \
  || fail "no Origin location found; the assertion would pass vacuously"
[ "$violations" -eq 0 ] \
  || fail "${violations} location(s) disagree with their origin Lease"

echo "  every Origin holds a Lease and no mirror target holds one"
