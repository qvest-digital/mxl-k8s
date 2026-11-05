#!/usr/bin/env bash
# Assert an audio flow whose producer sets a sync batch above libmxl's
# 10 ms default is mirrored in the producer's batches.
#
# libmxl takes a flow's batch size hints from the options of the writer
# that creates it. libmxl-fabrics sizes an audio target's bounce-buffer
# entries from the target flow's sync hint and rejects a larger batch
# for good ("too large for the bounce buffer entry size"). The writer
# here creates the flow with a sync hint of 960 samples at 48 kHz,
# twice the default. The origin gateway has to publish the hints on the
# MxlFlow, the target gateway has to create its copy with them and
# publish the copy's hint on the mirror, and samples have to arrive at
# delivery rate with no rejected batch.
#
# The writer needs a demo-tools image whose write-samples takes -options.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

TOOLS_IMAGE="${TOOLS_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
FLOW="${FLOW:-65a0e5a1-0000-4000-8000-000000000001}"
SYNC_HINT=960
COMMIT_HINT=480
# As in 46-audio-mirror-target-gateway-bounce: about 40% of what a
# healthy 48 kHz mirror delivers in the window.
WINDOW_SECS="${AUDIO_RATE_WINDOW_SECS:-5}"
MIN_WINDOW_SAMPLES="${AUDIO_RATE_MIN_WINDOW_SAMPLES:-100000}"
WRITER=mxl-hints-audio-writer
READER=mxl-hints-audio-reader
CM=mxl-hints-audio-flow

cleanup() {
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$WRITER" "$READER" \
      --wait=false --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o name 2>/dev/null \
    | grep "$FLOW" | xargs -r "${KUBECTL[@]}" -n "$NAMESPACE" delete --wait=false >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete pod/"$WRITER" pod/"$READER" \
      --timeout=60s >/dev/null 2>&1 || true
  "${KUBECTL[@]}" delete mxlflow "$FLOW" --wait=false --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=mxl-k8s-agent --field-selector=status.phase=Running \
    -o 'jsonpath={range .items[*]}{.spec.nodeName}{" "}{end}')
read -r node_a node_b _ <<<"$nodes" || true
[ -n "${node_b:-}" ] || fail "need two agent nodes, one to write and one to read"
cleanup
echo "   writer on ${node_a}, reader on ${node_b}"

base=$("${KUBECTL[@]}" -n "$NAMESPACE" get configmap mxl-demo-flow-config \
         -o 'jsonpath={.data.flow-audio-f32\.json}')
[ -n "$base" ] || fail "demo audio flow definition missing"
"${KUBECTL[@]}" -n "$NAMESPACE" create configmap "$CM" \
    --from-literal=flow.json="$(echo "$base" | sed -E "s/\"id\": *\"[0-9a-f-]+\"/\"id\": \"${FLOW}\"/")" >/dev/null

since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${WRITER}
spec:
  nodeName: ${node_a}
  containers:
    - name: writer
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/usr/local/bin/write-samples", "-domain", "/run/mxl/domain", "-flow-def", "/flows/flow.json",
                "-batch", "${COMMIT_HINT}",
                "-options", "{\"maxSyncBatchSizeHint\":${SYNC_HINT},\"maxCommitBatchSizeHint\":${COMMIT_HINT}}"]
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-domain, mountPath: /run/mxl/domain}
        - {name: flows, mountPath: /flows}
  volumes:
    - name: mxl-domain
      hostPath: {path: /run/mxl/domain, type: Directory}
    - name: flows
      configMap: {name: ${CM}}
---
apiVersion: v1
kind: Pod
metadata:
  name: ${READER}
spec:
  nodeName: ${node_b}
  containers:
    - name: reader
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c"]
      args:
        - until /usr/local/bin/read-samples -domain /run/mxl/domain -flow ${FLOW}; do sleep 1; done
      env:
        - {name: LD_PRELOAD, value: /run/mxl/libmxl-intent.so}
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-run, mountPath: /run/mxl}
  volumes:
    - name: mxl-run
      hostPath: {path: /run/mxl, type: Directory}
EOF

"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/${WRITER}" \
    --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null || fail "${WRITER} did not become Ready"

# The origin gateway reads the hints out of the producer's flow.
wait_phase "mxlflow/${FLOW}" '{.status.maxSyncBatchSizeHint}/{.status.maxCommitBatchSizeHint}' \
  "^${SYNC_HINT}/${COMMIT_HINT}\$" 90 >/dev/null \
  || fail "MxlFlow ${FLOW} does not publish the producer's hints ${SYNC_HINT}/${COMMIT_HINT}"
echo "   MxlFlow publishes ${SYNC_HINT}/${COMMIT_HINT}"

mirror=""
deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
while [ -z "$mirror" ] && [ "$(date +%s)" -lt "$deadline" ]; do
  mirror=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
             -o 'jsonpath={range .items[*]}{.metadata.name}{" "}{.spec.flowID}{" "}{.spec.targetNode}{"\n"}{end}' \
           | awk -v f="$FLOW" -v n="$node_b" '$2 == f && $3 == n { print $1; exit }')
  [ -n "$mirror" ] || sleep 2
done
[ -n "$mirror" ] || fail "no mirror of ${FLOW} onto ${node_b}"
wait_phase "mxlflowmirror/${mirror}" '{.status.phase}' '^Ready$' "$MIRROR_TIMEOUT_SECS" >/dev/null \
  || fail "mirror ${mirror} did not become Ready"

# The target's copy carries the producer's sync hint, so its
# bounce-buffer entries hold one producer batch.
target_hint=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${mirror}" \
                -o 'jsonpath={.status.targetMaxSyncBatchSizeHint}')
[ "$target_hint" = "$SYNC_HINT" ] \
  || fail "target copy of ${FLOW} has sync hint '${target_hint}', the producer's is ${SYNC_HINT}"
echo "   target copy has sync hint ${target_hint}"

sample_idx() {
  "${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${READER}" --tail=200 2>/dev/null \
    | awk '/frags=/ && match($0, /idx=[0-9]+/) { last = substr($0, RSTART + 4, RLENGTH - 4) }
           END { if (last != "") print last }'
}
deadline=$(( $(date +%s) + 60 ))
first=""
while [ -z "$first" ] && [ "$(date +%s)" -lt "$deadline" ]; do
  first=$(sample_idx || true)
  [ -n "$first" ] || sleep 1
done
[ -n "$first" ] || fail "${READER} read no samples within 60s"
sleep "$WINDOW_SECS"
last=$(sample_idx || true)
delivered=$(( ${last:-0} - first ))
[ "$delivered" -ge "$MIN_WINDOW_SAMPLES" ] \
  || fail "${delivered} samples delivered in ${WINDOW_SECS}s, want >= ${MIN_WINDOW_SAMPLES}"
echo "   ${delivered} samples delivered in ${WINDOW_SECS}s"

gateway_pod="$(daemonset_pod_on gateway "$node_a")"
[ -n "$gateway_pod" ] || fail "no running gateway pod on ${node_a}"
if "${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${gateway_pod}" --since-time="$since" 2>/dev/null \
     | grep -q "too large for the bounce buffer"; then
  fail "the source gateway on ${node_a} sent a batch the target's entry cannot hold"
fi
progress=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlflowmirror/${mirror}" \
             -o 'jsonpath={.status.conditions[?(@.type=="SourceProgress")].message}')
case "$progress" in
  *"too large"*) fail "mirror ${mirror} SourceProgress reports a rejected batch: ${progress}" ;;
esac
echo "   no batch rejected"
