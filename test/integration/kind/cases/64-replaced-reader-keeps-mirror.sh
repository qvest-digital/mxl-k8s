#!/usr/bin/env bash
# Assert an on-demand mirror outlives the reader that asked for it when
# another reader on the same node is still using it.
#
# A reader replaced on its node opens the flow while the mirrored copy
# is still there, so its open succeeds, nothing asks for the mirror, and
# the mirror stays claimed by the pod it replaced. Collecting it when
# that pod goes leaves the new reader on a ring nothing writes. The
# agent on the target node names the pod still mapping the flow as the
# requestor, and the operator keeps the same mirror object throughout.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

TOOLS_IMAGE="${TOOLS_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
NODE_SHIM="${NODE_SHIM:-/run/mxl/libmxl-intent.so}"
FLOW="${HANDOVER_FLOW_ID:-64a1d1e0-0000-4000-8000-000000000001}"
HANDOVER_TIMEOUT_SECS="${HANDOVER_TIMEOUT_SECS:-60}"
# Past the operator's handover window, so a mirror it was going to
# collect is gone by the time it is looked at again.
HOLD_SECS="${HANDOVER_HOLD_SECS:-45}"
WRITER=mxl-handover-writer
FIRST=mxl-handover-reader-a
SECOND=mxl-handover-reader-b
CM=mxl-handover-flow

cleanup() {
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$WRITER" "$FIRST" "$SECOND" \
      --wait=false --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o name 2>/dev/null \
    | grep "$FLOW" | xargs -r "${KUBECTL[@]}" -n "$NAMESPACE" delete --wait=false >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete pod/"$WRITER" pod/"$FIRST" pod/"$SECOND" \
      --timeout=60s >/dev/null 2>&1 || true
  "${KUBECTL[@]}" delete mxlflow "$FLOW" --wait=false --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=mxl-k8s-agent --field-selector=status.phase=Running \
    -o 'jsonpath={range .items[*]}{.spec.nodeName}{" "}{end}')
read -r src_node tgt_node _ <<<"$nodes" || true
[ -n "${tgt_node:-}" ] || fail "need two agent nodes, one to write and one to read"
echo "   writer on ${src_node}, readers on ${tgt_node}"

base=$("${KUBECTL[@]}" -n "$NAMESPACE" get configmap mxl-demo-flow-config \
         -o 'jsonpath={.data.flow-video-v210\.json}')
[ -n "$base" ] || fail "demo flow definition missing"
"${KUBECTL[@]}" -n "$NAMESPACE" create configmap "$CM" \
    --from-literal=flow.json="$(echo "$base" | sed -E "s/\"id\": *\"[0-9a-f-]+\"/\"id\": \"${FLOW}\"/")" >/dev/null

# reader <name>: a pod on the target node reading the flow until it is
# deleted, through the shim the agent leaves on the node, as a consumer
# preloading it does.
reader() {
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
spec:
  nodeName: ${tgt_node}
  terminationGracePeriodSeconds: 1
  containers:
    - name: reader
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/usr/local/bin/read-grain", "-domain", "/run/mxl/domain", "-flow", "${FLOW}"]
      env:
        - {name: LD_PRELOAD, value: "${NODE_SHIM}"}
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-run, mountPath: /run/mxl}
  volumes:
    - name: mxl-run
      hostPath: {path: /run/mxl, type: Directory}
EOF
}

"${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${WRITER}
spec:
  nodeName: ${src_node}
  terminationGracePeriodSeconds: 1
  containers:
    - name: writer
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/usr/local/bin/write-grain", "-domain", "/run/mxl/domain", "-flow-def", "/flows/flow.json"]
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-run, mountPath: /run/mxl}
        - {name: flows, mountPath: /flows}
  volumes:
    - name: mxl-run
      hostPath: {path: /run/mxl, type: Directory}
    - name: flows
      configMap: {name: ${CM}}
EOF
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/${WRITER}" \
    --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null || fail "${WRITER} did not become Ready"
wait_phase "mxlflow/${FLOW}" "{.status.locations[?(@.nodeName==\"${src_node}\")].phase}" \
  '^Origin$' 60 >/dev/null || fail "flow ${FLOW} did not become Origin on ${src_node}"

mirror=$(echo "${FLOW}--${tgt_node}" | tr '[:upper:]' '[:lower:]')

# reading <pod>: the pod has printed a grain.
reading() {
  "${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/$1" 2>/dev/null | grep -q 'idx='
}
wait_reading() {
  local deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    reading "$1" && return 0
    sleep 2
  done
  return 1
}

reader "$FIRST" | "${KUBECTL[@]}" -n "$NAMESPACE" apply -f - >/dev/null
wait_reading "$FIRST" || fail "${FIRST} never read a grain of ${FLOW} on ${tgt_node}"
wait_phase "mxlflowmirror/${mirror}" '{.spec.requestor.name}/{.status.phase}' \
  "^${FIRST}/Ready\$" "$MIRROR_TIMEOUT_SECS" >/dev/null \
  || fail "mirror ${mirror} not Ready with ${FIRST} as its requestor"
uid=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${mirror}" -o 'jsonpath={.metadata.uid}')
echo "   ${FIRST} asked for ${mirror}"

# The second reader finds the mirrored copy in place, so it asks for
# nothing: the mirror is still claimed by the first.
reader "$SECOND" | "${KUBECTL[@]}" -n "$NAMESPACE" apply -f - >/dev/null
wait_reading "$SECOND" || fail "${SECOND} never read a grain of ${FLOW} on ${tgt_node}"
echo "   ${SECOND} reading the same copy"

"${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$FIRST" --grace-period=0 --force >/dev/null 2>&1 \
  || fail "delete pod/${FIRST} failed"
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete "pod/${FIRST}" --timeout=30s >/dev/null 2>&1 || true

# same_mirror: the mirror is the object the first reader asked for, not
# a fresh one re-created after a collection.
same_mirror() {
  [ "$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${mirror}" \
        -o 'jsonpath={.metadata.uid}' 2>/dev/null || true)" = "$uid" ]
}

deadline=$(( $(date +%s) + HANDOVER_TIMEOUT_SECS ))
handed=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  same_mirror || fail "mirror ${mirror} was collected when ${FIRST} went, though ${SECOND} still reads it"
  handed=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${mirror}" \
             -o 'jsonpath={.spec.requestor.name}' 2>/dev/null || true)
  [ "$handed" = "$SECOND" ] && break
  sleep 2
done
[ "$handed" = "$SECOND" ] \
  || fail "the agent on ${tgt_node} did not hand ${mirror} to ${SECOND} within ${HANDOVER_TIMEOUT_SECS}s (requestor '${handed}')"
echo "   ${mirror} handed to ${SECOND}"

sleep "$HOLD_SECS"
same_mirror || fail "mirror ${mirror} was collected after it was handed to ${SECOND}"
wait_phase "mxlflowmirror/${mirror}" \
  '{.status.conditions[?(@.type=="Claimed")].reason}/{.status.phase}' \
  '^RequestorLive/Ready$' 30 >/dev/null \
  || fail "mirror ${mirror} is not claimed by a live requestor and Ready after the handover"
"${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${SECOND}" -o 'jsonpath={.status.phase}' | grep -q Running \
  || fail "${SECOND} stopped reading"
since=$(date +%s)
before=$("${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${SECOND}" 2>/dev/null | grep -c 'idx=' || true)
sleep 3
after=$("${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${SECOND}" 2>/dev/null | grep -c 'idx=' || true)
[ "$after" -gt "$before" ] \
  || fail "${SECOND} read no grain in the $(( $(date +%s) - since ))s after the handover"
echo "   ${SECOND} still reading through the same mirror"
