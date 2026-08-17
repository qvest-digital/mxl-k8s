#!/usr/bin/env bash
# Assert the exporter serves metrics that describe the flows the demo
# is actually producing, on the node they live on.
#
# The demo writer and reader sit on different workers, so this also
# pins down the property a per-pod exporter could not have: the series
# are identified by node, the producing node reports its flow as
# Origin, and the consuming node reports the same flow as a mirror.
#
# Head index movement cannot be read from one sample, so the assertions
# about it span two scrapes a window apart. Activity needs no such
# window: the exporter judges it against the flow's own write clock and
# reports it on the first scrape that finds the flow.
#
# Numbered with the steady-state cases: from 50 on the suite reschedules
# producers, drains nodes and evicts consumers, and the demo pods this
# addresses by name no longer exist by then.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

VIDEO_FLOW="${VIDEO_FLOW:-5fbec3b1-1b0f-417d-9059-8b94a47197ed}"
WRITER_POD="${WRITER_POD:-mxl-tcp-demo-writer}"
READER_POD="${READER_POD:-mxl-tcp-demo-reader}"
METRICS_PORT="${EXPORTER_METRICS_PORT:-8080}"
SAMPLE_WINDOW_SECS="${EXPORTER_WINDOW_SECS:-6}"
DISCOVER_TIMEOUT_SECS="${EXPORTER_DISCOVER_TIMEOUT_SECS:-60}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# metric_value <file> <metric> <label-substring>
# First sample of <metric> whose label set contains the substring.
# Substring rather than regex: flow ids and paths carry characters a
# pattern would have to escape.
metric_value() {
  awk -v m="$2" -v w="$3" '
    index($0, m "{") == 1 && index($0, w) > 0 { print $NF; exit }
  ' "$1"
}

# metric_line <file> <metric> <label-substring>
# The whole exposition line, for assertions about several labels at
# once. Labels render in alphabetical order, so matching them in
# sequence with one pattern is order-dependent and brittle.
metric_line() {
  awk -v m="$2" -v w="$3" '
    index($0, m "{") == 1 && index($0, w) > 0 { print; exit }
  ' "$1"
}

# has_label <line> <label=value>
has_label() {
  case "$1" in
    *"$2"*) return 0 ;;
    *)      return 1 ;;
  esac
}

# scrape <pod> <outfile> -- fetch /metrics, failing the case on a
# non-200 or an empty body.
scrape() {
  local pod="$1" out="$2" code
  code=$(port_forward_fetch "$pod" "$METRICS_PORT" "metrics" "$out" || true)
  [ "$code" = "200" ] || fail "exporter ${pod} /metrics returned HTTP ${code}"
  [ -s "$out" ] || fail "exporter ${pod} /metrics returned an empty body"
}

node_of_pod() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get "pod/$1" \
      -o jsonpath='{.spec.nodeName}' 2>/dev/null || true
}

for pod in "$WRITER_POD" "$READER_POD"; do
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
      "pod/${pod}" --timeout="${MIRROR_TIMEOUT_SECS}s" \
    || fail "${pod} did not become Ready"
done

writer_node=$(node_of_pod "$WRITER_POD")
reader_node=$(node_of_pod "$READER_POD")
[ -n "$writer_node" ] || fail "could not resolve the node of ${WRITER_POD}"
[ -n "$reader_node" ] || fail "could not resolve the node of ${READER_POD}"
echo "-> writer on ${writer_node}, reader on ${reader_node}"

writer_exp=$(daemonset_pod_on exporter "$writer_node")
[ -n "$writer_exp" ] || fail "no exporter pod running on ${writer_node}"
echo "-> exporter on the writer node: ${writer_exp}"

# Wait for the flow to be both present and moving before measuring
# anything. The exporter re-lists the domain on a timer, so a flow
# created just before this case runs may not be in the first scrape;
# and a writer that has only just started leaves its flow directory in
# place before it produces, so present alone would let the head-index
# window below open against a stalled flow.
deadline=$(( $(date +%s) + DISCOVER_TIMEOUT_SECS ))
present=""
active=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  scrape "$writer_exp" "$WORK_DIR/first.txt"
  present=$(metric_value "$WORK_DIR/first.txt" mxl_flow_present "$VIDEO_FLOW")
  active=$(metric_value "$WORK_DIR/first.txt" mxl_flow_active "$VIDEO_FLOW")
  [ "$present" = "1" ] && [ "$active" = "1" ] && break
  sleep 3
done
[ "$present" = "1" ] \
  || fail "mxl_flow_present for ${VIDEO_FLOW} never reached 1 on ${writer_node} within ${DISCOVER_TIMEOUT_SECS}s"
[ "$active" = "1" ] \
  || fail "mxl_flow_active for ${VIDEO_FLOW} never reached 1 on ${writer_node} within ${DISCOVER_TIMEOUT_SECS}s; the writer is present but not producing"
echo "   mxl_flow_present: 1, mxl_flow_active: 1"

# Every series carries the node the exporter runs on. Without it two
# nodes' samples for one flow would be indistinguishable.
grep -q "node=\"${writer_node}\"" "$WORK_DIR/first.txt" \
  || fail "no series on ${writer_exp} carries node=\"${writer_node}\""
echo "   node label: ${writer_node}"

flows=$(metric_value "$WORK_DIR/first.txt" mxl_domain_num_flows "domain=")
case "$flows" in
  ''|0|0.*) fail "mxl_domain_num_flows on ${writer_node} is '${flows}', expected at least 1" ;;
esac
echo "   mxl_domain_num_flows: ${flows}"

# Config read through go-mxl, not just the directory listing.
rate_num=$(metric_value "$WORK_DIR/first.txt" mxl_flow_rate_num "$VIDEO_FLOW")
case "$rate_num" in
  ''|0|0.*) fail "mxl_flow_rate_num for ${VIDEO_FLOW} is '${rate_num}', expected the flow's grain rate" ;;
esac
echo "   mxl_flow_rate_num: ${rate_num}"

# flow_def.json and the flow header parsed into labels.
meta=$(metric_line "$WORK_DIR/first.txt" mxl_flow_metadata "$VIDEO_FLOW")
[ -n "$meta" ] || fail "no mxl_flow_metadata series for ${VIDEO_FLOW}"
has_label "$meta" 'flow_data_type="video"' \
  || fail "mxl_flow_metadata for ${VIDEO_FLOW} carries no flow_data_type=\"video\": ${meta}"
has_label "$meta" 'flow_media_type="video/v210"' \
  || fail "mxl_flow_metadata for ${VIDEO_FLOW} carries no flow_media_type from flow_def.json: ${meta}"
echo "   mxl_flow_metadata: flow_data_type=video, flow_media_type=video/v210"

first_head=$(metric_value "$WORK_DIR/first.txt" mxl_flow_head_index_grains "$VIDEO_FLOW")
[ -n "$first_head" ] || fail "no mxl_flow_head_index_grains sample for ${VIDEO_FLOW}"

sleep "$SAMPLE_WINDOW_SECS"
scrape "$writer_exp" "$WORK_DIR/second.txt"

last_head=$(metric_value "$WORK_DIR/second.txt" mxl_flow_head_index_grains "$VIDEO_FLOW")
[ -n "$last_head" ] || fail "mxl_flow_head_index_grains disappeared for ${VIDEO_FLOW} mid-window"

# awk, not shell arithmetic: the head index is a Prometheus float and
# can be rendered in exponent form once it is large enough.
advanced=$(awk -v a="$first_head" -v b="$last_head" 'BEGIN { print (b+0 > a+0) ? "yes" : "no" }')
[ "$advanced" = "yes" ] \
  || fail "head index did not advance over ${SAMPLE_WINDOW_SECS}s: first=${first_head} last=${last_head}"
echo "   head index advanced: ${first_head} -> ${last_head}"

still_active=$(metric_value "$WORK_DIR/second.txt" mxl_flow_active "$VIDEO_FLOW")
[ "$still_active" = "1" ] \
  || fail "mxl_flow_active for ${VIDEO_FLOW} fell to '${still_active}' across the window, expected it to stay 1"
echo "   mxl_flow_active: still 1"

# The MXL clock is the exporter's own, so the age has to be a small
# positive number rather than a wall-clock difference against a scrape
# timestamp.
age=$(metric_value "$WORK_DIR/second.txt" mxl_flow_last_write_age_seconds "$VIDEO_FLOW")
[ -n "$age" ] || fail "no mxl_flow_last_write_age_seconds sample for ${VIDEO_FLOW}"
sane=$(awk -v a="$age" 'BEGIN { print (a+0 >= 0 && a+0 < 5) ? "yes" : "no" }')
[ "$sane" = "yes" ] \
  || fail "mxl_flow_last_write_age_seconds for a flowing stream is ${age}s, expected under 5s"
echo "   mxl_flow_last_write_age_seconds: ${age}"

# Topology: the producing node reports Origin for the flow it writes.
loc=$(metric_line "$WORK_DIR/second.txt" mxl_flow_location_info "$VIDEO_FLOW")
[ -n "$loc" ] || fail "no mxl_flow_location_info series for ${VIDEO_FLOW} on ${writer_node}"
has_label "$loc" "node=\"${writer_node}\"" \
  || fail "mxl_flow_location_info for ${VIDEO_FLOW} is not labelled ${writer_node}: ${loc}"
has_label "$loc" 'phase="Origin"' \
  || fail "the writer's node does not report phase=Origin for ${VIDEO_FLOW}: ${loc}"
echo "   mxl_flow_location_info: Origin on ${writer_node}"

# ... and the consuming node reports the same flow as a mirror, which
# is what separates the two sides without a role label anywhere.
if [ "$reader_node" = "$writer_node" ]; then
  echo "   reader shares the writer's node; skipping the mirror-side assertion"
else
  reader_exp=$(daemonset_pod_on exporter "$reader_node")
  [ -n "$reader_exp" ] || fail "no exporter pod running on ${reader_node}"
  scrape "$reader_exp" "$WORK_DIR/reader.txt"

  rloc=$(metric_line "$WORK_DIR/reader.txt" mxl_flow_location_info "$VIDEO_FLOW")
  [ -n "$rloc" ] || fail "no mxl_flow_location_info for ${VIDEO_FLOW} on ${reader_node}"
  phase=$(echo "$rloc" | sed -n 's/.*phase="\([A-Za-z]*\)".*/\1/p')
  case "$phase" in
    Ready|Mirroring) echo "   mxl_flow_location_info: ${phase} on ${reader_node}" ;;
    Origin) fail "${reader_node} reports Origin for ${VIDEO_FLOW}; only the writer's node may" ;;
    *)      fail "unexpected mxl_flow_location_info phase '${phase}' on ${reader_node}" ;;
  esac

  # Each exporter reports its own node only. Both reporting every
  # location would put the cluster's flows on every node's series.
  if grep -q "node=\"${writer_node}\"" "$WORK_DIR/reader.txt"; then
    fail "${reader_exp} exported a series for ${writer_node}; each exporter must report only its own node"
  fi
  echo "   ${reader_exp} reports no series for ${writer_node}"
fi
