#!/usr/bin/env bash
# Checks the Grafana dashboards the chart ships:
#
#   - each file is valid JSON and ASCII only;
#   - each has a uid, and no two share one;
#   - panel ids, row panels' children included, are unique per board;
#   - every datasource names the uid "prometheus", the placeholder the
#     chart's ConfigMap template rewrites to dashboards.datasourceUid;
#   - every mxl_* metric a query or variable names is defined in the Go
#     sources of this repository, so a renamed metric fails here rather
#     than as an empty panel. A name ending in an underscore is the
#     prefix of a __name__ regex and is not checked.
#
# Runs on bash 3.2. Needs jq.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
dir="$root/charts/mxl-k8s/dashboards"
fail=0

err() {
  echo "check-dashboards: $*" >&2
  fail=1
}

# Metric names defined in Go, test files excluded. A histogram's series
# carry suffixes its definition does not.
defined=$(cd "$root" && grep -rhoE --include='*.go' --exclude='*_test.go' '"mxl_[a-z0-9_]+"' . | tr -d '"' | sort -u)

uids=""
for f in "$dir"/*.json; do
  name=$(basename "$f")
  if ! jq -e . "$f" >/dev/null 2>&1; then
    err "$name: not valid JSON"
    continue
  fi
  if LC_ALL=C grep -q '[^ -~	]' "$f"; then
    err "$name: non-ASCII characters"
  fi

  uid=$(jq -r '.uid // ""' "$f")
  if [ -z "$uid" ]; then
    err "$name: no uid"
  fi
  case " $uids " in
  *" $uid "*) err "$name: uid $uid used by another dashboard" ;;
  esac
  uids="$uids $uid"

  dups=$(jq -r '[.panels[] | ., (.panels // [])[]] | map(.id) | group_by(.) | map(select(length > 1) | .[0]) | .[]' "$f")
  if [ -n "$dups" ]; then
    err "$name: duplicate panel ids: $(echo $dups)"
  fi

  bad=$(jq -r '[.. | objects | select(has("datasource")) | .datasource | objects | .uid] | unique | map(select(. != "prometheus")) | .[]' "$f")
  if [ -n "$bad" ]; then
    err "$name: datasource uid other than the placeholder: $(echo $bad)"
  fi

  used=$(jq -r '[.. | objects | (.expr?, .definition?, (.query? | strings), (.query?.query? // empty))] | map(strings) | .[]' "$f" |
    grep -oE 'mxl_[a-z0-9_]+' | grep -v '_$' | sed -E 's/_(bucket|count|sum)$//' | sort -u)
  for m in $used; do
    if ! echo "$defined" | grep -qx "$m"; then
      err "$name: $m is not defined in the Go sources"
    fi
  done
done

exit "$fail"
