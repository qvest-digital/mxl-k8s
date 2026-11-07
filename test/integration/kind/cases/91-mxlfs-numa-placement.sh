#!/usr/bin/env bash
# Assert flow placement on a cluster brought up with MXLFS=1 and
# MXLFS_NUMA=flow:
#
#   1. the ring of the demo's mirrored video flow is held on its source
#      node while the mirror runs (mxl_flow_ring_held 1), and the
#      gateway's first acquire found it on the NICs' node (result
#      already: a KIND host's nodes share the host's NUMA nodes, and a
#      laptop or runner has one) or moved it there;
#   2. a gateway restart drops the holds with the process and the
#      mirror's next attach takes them again;
#   3. with an mxlfs module that has no placement ioctls the mirror is
#      Ready and the gateway counts the domain unsupported.
#
# Moving a ring between NUMA nodes needs a host of two and is not
# covered here. Skipped when the agent runs without --mxlfs or the
# gateway without --mxlfs-numa-placement-mode=flow.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

VIDEO_FLOW="${VIDEO_FLOW:-5fbec3b1-1b0f-417d-9059-8b94a47197ed}"
WRITER_POD="${WRITER_POD:-mxl-tcp-demo-writer}"
METRICS_PORT=8080
WAIT_SECS="${NUMA_WAIT_SECS:-60}"

if ! agent_runs_mxlfs; then
  echo "  agent runs without --mxlfs; cluster is up without MXLFS=1"
  exit 0
fi
if ! "${KUBECTL[@]}" -n "$NAMESPACE" get daemonset mxl-k8s-gateway \
      -o 'jsonpath={.spec.template.spec.containers[0].args}' \
    | grep -q -- '--mxlfs-numa-placement-mode=flow'; then
  echo "  gateway runs without flow placement; cluster is up without MXLFS_NUMA=flow"
  exit 0
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# value <file> <metric> <label-substring>... -- first sample of metric
# whose labels contain every substring, empty when none does.
value() {
  local file="$1" metric="$2"
  shift 2
  awk -v m="$metric" -v want="$*" '
    BEGIN { n = split(want, w, " ") }
    index($0, m "{") == 1 {
      for (i = 1; i <= n; i++) if (index($0, w[i]) == 0) next
      print $NF; exit
    }
  ' "$file"
}

scrape() {
  local pod="$1" out="$2" code
  code=$(port_forward_fetch "$pod" "$METRICS_PORT" "metrics" "$out" || true)
  [ "$code" = "200" ] || fail "${pod} /metrics returned HTTP ${code}"
}

node=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${WRITER_POD}" -o jsonpath='{.spec.nodeName}')
[ -n "$node" ] || fail "could not resolve the node of ${WRITER_POD}"

# The demo mirror of the video flow runs from the writer's node.
wait_ready_mirror() {
  local deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS )) m
  while [ "$(date +%s)" -lt "$deadline" ]; do
    m=$(mirrors_sourced_from "$node" | awk -F= '$2 == "Ready" { print $1; exit }')
    [ -n "$m" ] && { echo "$m"; return 0; }
    sleep 3
  done
  return 1
}
mirror=$(wait_ready_mirror) || fail "no Ready mirror sourced from ${node}"
echo "  ${mirror} is Ready, sourced from ${node}"

gateway=$(daemonset_pod_on gateway "$node")
[ -n "$gateway" ] || fail "no gateway pod running on ${node}"
scrape "$gateway" "$WORK_DIR/gateway.txt"

if [ "$(value "$WORK_DIR/gateway.txt" mxl_gateway_numa_supported)" = "0" ]; then
  unsupported=$(value "$WORK_DIR/gateway.txt" mxl_gateway_numa_moves_total 'result="unsupported"')
  [ -n "$unsupported" ] || fail "the gateway reports the module unsupported without counting it"
  echo "  the mxlfs module has no placement ioctls: mirror Ready, result unsupported"
  exit 0
fi

placed=$(value "$WORK_DIR/gateway.txt" mxl_gateway_numa_moves_total 'result="already"')
[ -n "$placed" ] || placed=$(value "$WORK_DIR/gateway.txt" mxl_gateway_numa_moves_total 'result="moved"')
[ -n "$placed" ] || fail "the gateway counted no first acquire as already or moved"
echo "  gateway placed the ring: ${placed} first acquire(s) already or moved"

exporter=$(daemonset_pod_on exporter "$node")
[ -n "$exporter" ] || fail "no exporter pod running on ${node}"

# held_becomes <want> -- waits for the exporter, which reads on its scan
# period, to report the video ring's hold as want.
held_becomes() {
  local deadline=$(( $(date +%s) + WAIT_SECS )) got=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    exporter=$(daemonset_pod_on exporter "$node")
    if [ -n "$exporter" ]; then
      scrape "$exporter" "$WORK_DIR/exporter.txt"
      got=$(value "$WORK_DIR/exporter.txt" mxl_flow_ring_held "flow_id=\"${VIDEO_FLOW}\"")
      [ "$got" = "$1" ] && return 0
    fi
    sleep 2
  done
  echo "  mxl_flow_ring_held for ${VIDEO_FLOW}: '${got}', want $1" >&2
  return 1
}
held_becomes 1 || fail "the video ring is not held while its mirror runs"
echo "  the video ring is held"

"${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$gateway" --wait=true >/dev/null \
  || fail "delete ${gateway}"
"${KUBECTL[@]}" -n "$NAMESPACE" rollout status ds/mxl-k8s-gateway \
    --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null \
  || fail "the gateway did not come back"
mirror=$(wait_ready_mirror) || fail "no Ready mirror sourced from ${node} after the gateway restart"
held_becomes 1 || fail "the restarted gateway did not hold the video ring again"

gateway=$(daemonset_pod_on gateway "$node")
[ -n "$gateway" ] || fail "no gateway pod running on ${node} after the restart"
scrape "$gateway" "$WORK_DIR/gateway.txt"
placed=$(value "$WORK_DIR/gateway.txt" mxl_gateway_numa_moves_total 'result="already"')
[ -n "$placed" ] || placed=$(value "$WORK_DIR/gateway.txt" mxl_gateway_numa_moves_total 'result="moved"')
[ -n "$placed" ] || fail "the restarted gateway counted no first acquire"
echo "  the restarted gateway holds the ring again"
