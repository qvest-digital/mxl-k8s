#!/usr/bin/env bash
# Assert the node domains are served from mxlfs end to end, on a cluster
# brought up with MXLFS=1:
#
#   1. the domain on each node is an mxlfs and the agent wrote its ready
#      marker;
#   2. a producer's flow is read on its own node without a mirror;
#   3. the same flow is read on another node by a consumer that carries
#      no shim and has no MxlReceiver: on mxlfs the open of the absent
#      flow waits, the agent sees it wanted and a mirror is created;
#   4. a redundancy set over members on both nodes is read on one of
#      them, and its reader keeps reading without reopening the output
#      when the local member's producer goes away.
#
# Skipped when the agent runs without --mxlfs, which is the case on any
# cluster brought up without MXLFS=1.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

TOOLS_IMAGE="${TOOLS_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
ATTEMPTS=10
SET_WINDOW_SECS="${MXLFS_SET_WINDOW_SECS:-10}"
FLOW="90a0e5a1-0000-4000-8000-000000000001"
MEMBER1="90a0e5a1-0000-4000-8000-000000000011"
MEMBER2="90a0e5a1-0000-4000-8000-000000000012"
OUTPUT="90a0e5a1-0000-4000-8000-000000000010"
MXLFS_MAGIC=6d786c66
SET=mxl-mxlfs-set
CM=mxl-mxlfs-flows
PODS="mxl-mxlfs-writer mxl-mxlfs-member1 mxl-mxlfs-member2 mxl-mxlfs-consumer mxl-mxlfs-set-reader"

if ! agent_runs_mxlfs; then
  echo "  agent runs without --mxlfs; cluster is up without MXLFS=1"
  exit 0
fi

cleanup() {
  # shellcheck disable=SC2086
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod $PODS --wait=false --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" delete mxlflowredundancyset "$SET" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1 || true
  for f in "$FLOW" "$MEMBER1" "$MEMBER2" "$OUTPUT"; do
    "${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o name 2>/dev/null \
      | grep "$f" | xargs -r "${KUBECTL[@]}" -n "$NAMESPACE" delete --wait=false >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT
cleanup
# shellcheck disable=SC2086
for p in $PODS; do
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete "pod/${p}" --timeout=60s >/dev/null 2>&1 || true
done

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=mxl-k8s-agent --field-selector=status.phase=Running \
    -o 'jsonpath={range .items[*]}{.spec.nodeName}{" "}{end}')
read -r node_a node_b _ <<<"$nodes" || true
[ -n "${node_b:-}" ] || fail "need two agent nodes"
echo "   ${node_a} produces and reads locally, ${node_b} reads across nodes"

base=$("${KUBECTL[@]}" -n "$NAMESPACE" get configmap mxl-demo-flow-config \
         -o 'jsonpath={.data.flow-video-v210\.json}')
[ -n "$base" ] || fail "demo flow definition missing"
with_id() { echo "$base" | sed -E "s/\"id\": *\"[0-9a-f-]+\"/\"id\": \"$1\"/"; }
"${KUBECTL[@]}" -n "$NAMESPACE" create configmap "$CM" \
    --from-literal=flow.json="$(with_id "$FLOW")" \
    --from-literal=member1.json="$(with_id "$MEMBER1")" \
    --from-literal=member2.json="$(with_id "$MEMBER2")" >/dev/null

# pod <name> <node> <read|write> <command...>: a demo-tools pod with the
# domain through the agent's CSI volume, and the node's /run/mxl read-only
# for the agent's ready marker.
pod() {
  local name="$1" node="$2" access="$3" cmd
  shift 3
  cmd=$(printf '"%s", ' "$@")
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
spec:
  nodeName: ${node}
  restartPolicy: Never
  terminationGracePeriodSeconds: 2
  containers:
    - name: tools
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: [${cmd%, }]
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-run, mountPath: /run/mxl, readOnly: true}
        - {name: mxl-domain, mountPath: /run/mxl/domain}
        - {name: flows, mountPath: /flows}
  volumes:
    - name: mxl-run
      hostPath: {path: /run/mxl, type: Directory}
    - name: mxl-domain
      csi: {driver: mxl.qvest-digital.com, volumeAttributes: {access: ${access}}}
    - name: flows
      configMap: {name: ${CM}}
---
EOF
}
writer() { pod "$1" "$2" write /usr/local/bin/write-grain -domain /run/mxl/domain -flow-def "/flows/$3.json"; }

{
  writer mxl-mxlfs-writer "$node_a" flow
  writer mxl-mxlfs-member1 "$node_a" member1
  writer mxl-mxlfs-member2 "$node_b" member2
  pod mxl-mxlfs-consumer "$node_b" read /bin/sh -c "sleep infinity"
} | "${KUBECTL[@]}" -n "$NAMESPACE" apply -f - >/dev/null

for p in mxl-mxlfs-writer mxl-mxlfs-member1 mxl-mxlfs-member2 mxl-mxlfs-consumer; do
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/${p}" \
      --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null || fail "${p} did not become Ready"
done

in_pod() { local p="$1"; shift; "${KUBECTL[@]}" -n "$NAMESPACE" exec "$p" -- "$@"; }

echo "-> the domain is an mxlfs on both nodes"
for p in mxl-mxlfs-writer mxl-mxlfs-consumer; do
  magic=$(in_pod "$p" stat -f -c %t /run/mxl/domain)
  [ "$magic" = "$MXLFS_MAGIC" ] || fail "/run/mxl/domain seen from ${p} has f_type ${magic}, not mxlfs"
  in_pod "$p" test -e /run/mxl/mxlfs.ready || fail "no mxlfs.ready on the node of ${p}"
done

for f in "$FLOW" "$MEMBER1"; do
  wait_phase "mxlflow/${f}" "{.status.locations[?(@.nodeName==\"${node_a}\")].phase}" \
    '^Origin$' 60 >/dev/null || fail "flow ${f} did not become Origin on ${node_a}"
done
wait_phase "mxlflow/${MEMBER2}" "{.status.locations[?(@.nodeName==\"${node_b}\")].phase}" \
  '^Origin$' 60 >/dev/null || fail "flow ${MEMBER2} did not become Origin on ${node_b}"

# read_in <pod> <flow>: read a few grains, retrying while a mirror is set up.
read_in() {
  local p="$1" flow="$2" out=""
  for _ in $(seq 1 "$ATTEMPTS"); do
    if out=$(in_pod "$p" /usr/local/bin/read-grain -domain /run/mxl/domain \
               -flow "$flow" -count 5 2>&1) \
       && echo "$out" | grep -q 'idx=.*invalid=false'; then
      return 0
    fi
    sleep 3
  done
  echo "$out" >&2
  return 1
}

echo "-> a reader on the producer's node"
read_in mxl-mxlfs-writer "$FLOW" || fail "${FLOW} could not be read on ${node_a}"
mirrors=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
            -o 'jsonpath={range .items[*]}{.spec.flowID}{"|"}{.spec.targetNode}{"\n"}{end}' \
          | grep -c "^${FLOW}|${node_a}$" || true)
[ "$mirrors" = 0 ] || fail "a local read of ${FLOW} created a mirror to its own node"

echo "-> a reader on another node, with no shim and no MxlReceiver"
read_in mxl-mxlfs-consumer "$FLOW" || fail "${FLOW} could not be read on ${node_b}"
phase=$("${KUBECTL[@]}" get mxlflow "$FLOW" \
  -o "jsonpath={.status.locations[?(@.nodeName==\"${node_b}\")].phase}")
case "$phase" in Ready|Mirroring) ;; *) fail "${FLOW} not mirrored to ${node_b} (phase '${phase}')" ;; esac
echo "   ${FLOW} on ${node_b}: ${phase}"

echo "-> a redundancy set over members on ${node_a} and ${node_b}, read on ${node_a}"
"${KUBECTL[@]}" apply -f - >/dev/null <<EOF
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlFlowRedundancySet
metadata:
  name: ${SET}
spec:
  members:
    - flowID: ${MEMBER1}
    - flowID: ${MEMBER2}
  output:
    flowID: ${OUTPUT}
EOF
# The reader opens the output once and prints every grain; a second
# "flow format=" line would mean it had to open the output again.
pod mxl-mxlfs-set-reader "$node_a" read /bin/sh -c \
    "until /usr/local/bin/read-grain -domain /run/mxl/domain -flow ${OUTPUT}; do sleep 1; done" \
  | "${KUBECTL[@]}" -n "$NAMESPACE" apply -f - >/dev/null
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready pod/mxl-mxlfs-set-reader \
    --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null || fail "mxl-mxlfs-set-reader did not become Ready"

valid_idx() {
  "${KUBECTL[@]}" -n "$NAMESPACE" logs pod/mxl-mxlfs-set-reader --tail=50 2>/dev/null \
    | awk 'match($0, /idx=[0-9]+/) && /invalid=false/ { last = substr($0, RSTART + 4, RLENGTH - 4) }
           END { if (last != "") print last }'
}
opens() {
  "${KUBECTL[@]}" -n "$NAMESPACE" logs pod/mxl-mxlfs-set-reader 2>/dev/null | grep -c 'flow format=' || true
}

deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
first=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  first=$(valid_idx || true)
  [ -n "$first" ] && break
  sleep 2
done
[ -n "$first" ] || fail "the output of ${SET} delivered no valid grain on ${node_a} in ${MIRROR_TIMEOUT_SECS}s"
for c in Valid MembersReady OutputReady; do
  wait_phase "mxlflowredundancyset/${SET}" "{.status.conditions[?(@.type==\"${c}\")].status}" \
    '^True$' 60 >/dev/null || fail "${SET} condition ${c} is not True"
done
"${KUBECTL[@]}" get mxlflowredundancyset "$SET" -o 'jsonpath={.status.nodes}' | grep -q "\"${node_a}\"" \
  || fail "${SET} does not list ${node_a} in status.nodes"
[ "$(opens)" = 1 ] || fail "the set reader opened the output $(opens) times before any member went away"
echo "   output delivering at idx ${first}"

echo "-> the local member's producer goes away"
"${KUBECTL[@]}" -n "$NAMESPACE" delete pod mxl-mxlfs-member1 --wait=true >/dev/null
killed=$(valid_idx)
sleep "$SET_WINDOW_SECS"
last=$(valid_idx || true)
if [ -z "$last" ] || [ "$last" -le "$killed" ]; then
  fail "the output stopped delivering after ${MEMBER1} went away: idx ${killed} -> '${last}'"
fi
[ "$(opens)" = 1 ] || fail "the set reader had to open the output again after ${MEMBER1} went away"
state=$("${KUBECTL[@]}" -n "$NAMESPACE" get pod mxl-mxlfs-set-reader -o 'jsonpath={.status.phase}')
[ "$state" = Running ] || fail "the set reader is ${state}"
echo "   output kept delivering from ${MEMBER2}: idx ${killed} -> ${last}, opened once"
