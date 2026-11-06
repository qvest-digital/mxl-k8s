#!/usr/bin/env bash
# Assert the gateways record the demo's video mirror through
# libmxl-monitoring on both nodes: the source gateway's handle counts
# OK reads that grow, the target gateway's counts OK writes, and both
# report the mirror under the same flow label, <namespace>/<name>.
#
# Numbered with the steady-state cases: from 50 on the suite reschedules
# producers and evicts consumers, and the demo pods this addresses by
# name no longer exist by then.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

VIDEO_FLOW="${VIDEO_FLOW:-5fbec3b1-1b0f-417d-9059-8b94a47197ed}"
WRITER_POD="${WRITER_POD:-mxl-tcp-demo-writer}"
READER_POD="${READER_POD:-mxl-tcp-demo-reader}"
METRICS_PORT="${GATEWAY_METRICS_PORT:-8080}"
SAMPLE_WINDOW_SECS="${REPLICATION_WINDOW_SECS:-6}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# metric_value <file> <metric> <label-substring>...
# First sample of <metric> whose label set contains every substring.
metric_value() {
  local file="$1" metric="$2"
  shift 2
  awk -v m="$metric" -v w="$*" '
    BEGIN { n = split(w, want, " ") }
    index($0, m "{") == 1 {
      for (i = 1; i <= n; i++) if (index($0, want[i]) == 0) next
      print $NF; exit
    }
  ' "$file"
}

scrape() {
  local pod="$1" out="$2" code
  code=$(port_forward_fetch "$pod" "$METRICS_PORT" "metrics" "$out" || true)
  [ "$code" = "200" ] || fail "gateway ${pod} /metrics returned HTTP ${code}"
  [ -s "$out" ] || fail "gateway ${pod} /metrics returned an empty body"
}

node_of_pod() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get "pod/$1" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true
}

for pod in "$WRITER_POD" "$READER_POD"; do
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/${pod}" \
      --timeout="${MIRROR_TIMEOUT_SECS}s" || fail "${pod} did not become Ready"
done
writer_node="$(node_of_pod "$WRITER_POD")"
reader_node="$(node_of_pod "$READER_POD")"
[ -n "$writer_node" ] && [ -n "$reader_node" ] || fail "demo pods have no node"

mirror="$("${KUBECTL[@]}" get mxlflowmirrors -A -o \
  'jsonpath={range .items[*]}{.spec.flowID} {.spec.targetNode} {.metadata.namespace}/{.metadata.name}{"\n"}{end}' \
  | awk -v f="$VIDEO_FLOW" -v n="$reader_node" '$1 == f && $2 == n { print $3; exit }')"
[ -n "$mirror" ] || fail "no MxlFlowMirror of ${VIDEO_FLOW} to ${reader_node}"
echo "  mirror ${mirror}: ${writer_node} -> ${reader_node}"

source_gw="$(daemonset_pod_on gateway "$writer_node")"
target_gw="$(daemonset_pod_on gateway "$reader_node")"
[ -n "$source_gw" ] || fail "no gateway pod on ${writer_node}"
[ -n "$target_gw" ] || fail "no gateway pod on ${reader_node}"

flow="flow=\"${mirror}\""

scrape "$source_gw" "$WORK_DIR/source-1"
first="$(metric_value "$WORK_DIR/source-1" mxl_gateway_source_reads_total "$flow" 'result="ok"')"
[ -n "$first" ] && [ "${first%.*}" -gt 0 ] || fail "source gateway ${source_gw} counted no OK read for ${mirror}"
state="$(metric_value "$WORK_DIR/source-1" mxl_gateway_source_flow_state "$flow" 'state="open"')"
[ "$state" = "1" ] || fail "source handle of ${mirror} is not open"

sleep "$SAMPLE_WINDOW_SECS"
scrape "$source_gw" "$WORK_DIR/source-2"
second="$(metric_value "$WORK_DIR/source-2" mxl_gateway_source_reads_total "$flow" 'result="ok"')"
[ "${second%.*}" -gt "${first%.*}" ] \
  || fail "OK reads of ${mirror} did not grow over ${SAMPLE_WINDOW_SECS}s (${first} -> ${second})"
echo "  source: read ${first} -> ${second}"

scrape "$target_gw" "$WORK_DIR/target"
writes="$(metric_value "$WORK_DIR/target" mxl_gateway_target_writes_total "$flow" 'result="ok"')"
[ -n "$writes" ] && [ "${writes%.*}" -gt 0 ] || fail "target gateway ${target_gw} counted no OK write for ${mirror}"
echo "  target: ${writes} grains written"
