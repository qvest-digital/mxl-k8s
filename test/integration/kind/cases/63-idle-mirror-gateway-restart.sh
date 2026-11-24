#!/usr/bin/env bash
# Assert a mirror whose producer has paused rides out a gateway restart
# on either side and carries grains again once the producer resumes.
#
# A paused producer keeps its flow open and writes nothing, so neither
# gateway sees a grain while the source reader is reopened and while
# the source gateway, and then the target gateway, restart. The target
# keeps listening on the address it published, so on the source side
# the target must not be set up again: that closes the flow's writer on
# the target node, which invalidates every reader of it there, and
# publishes a new descriptor for a connection that already reached the
# old one. After a target restart the new target publishes its own
# address and the source has to follow it with no grain to prompt it.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

TOOLS_IMAGE="${TOOLS_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
FLOW="${IDLE_FLOW_ID:-63a1d1e0-0000-4000-8000-000000000001}"
# Longer than the source's reader-stall window, so the reader has been
# reopened against the paused flow before the gateway goes.
PAUSE_SETTLE_SECS="${IDLE_PAUSE_SETTLE_SECS:-30}"
RESUME_TIMEOUT_SECS="${IDLE_RESUME_TIMEOUT_SECS:-60}"
WRITER=mxl-idle-writer
ANCHOR=mxl-idle-anchor
RECEIVER=mxl-idle
CM=mxl-idle-flow

resume_writer() {
  "${KUBECTL[@]}" -n "$NAMESPACE" exec "$WRITER" -- sh -c 'kill -CONT "$(cat /tmp/writer.pid)"'
}
pause_writer() {
  "${KUBECTL[@]}" -n "$NAMESPACE" exec "$WRITER" -- sh -c 'kill -STOP "$(cat /tmp/writer.pid)"'
}

cleanup() {
  resume_writer >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete mxlreceiver "$RECEIVER" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$WRITER" "$ANCHOR" \
      --wait=false --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete pod/"$WRITER" pod/"$ANCHOR" \
      --timeout=60s >/dev/null 2>&1 || true
  "${KUBECTL[@]}" delete mxlflow "$FLOW" --wait=false --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=mxl-k8s-agent --field-selector=status.phase=Running \
    -o 'jsonpath={range .items[*]}{.spec.nodeName}{" "}{end}')
read -r src_node tgt_node _ <<<"$nodes" || true
[ -n "${tgt_node:-}" ] || fail "need two agent nodes, one to write and one to mirror to"
cleanup
echo "   writer on ${src_node}, mirror to ${tgt_node}"

base=$("${KUBECTL[@]}" -n "$NAMESPACE" get configmap mxl-demo-flow-config \
         -o 'jsonpath={.data.flow-video-v210\.json}')
[ -n "$base" ] || fail "demo flow definition missing"
"${KUBECTL[@]}" -n "$NAMESPACE" create configmap "$CM" \
    --from-literal=flow.json="$(echo "$base" | sed -E "s/\"id\": *\"[0-9a-f-]+\"/\"id\": \"${FLOW}\"/")" >/dev/null

# The writer runs under a shell so it can be stopped and continued: a
# container's init process ignores SIGSTOP sent from inside its own PID
# namespace.
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
      command: ["/bin/sh", "-c", "/usr/local/bin/write-grain -domain /run/mxl/domain -flow-def /flows/flow.json & echo \$! > /tmp/writer.pid; wait \$!"]
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
---
apiVersion: v1
kind: Pod
metadata:
  name: ${ANCHOR}
spec:
  nodeName: ${tgt_node}
  terminationGracePeriodSeconds: 1
  containers:
    - name: anchor
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c", "sleep infinity"]
---
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlReceiver
metadata:
  name: ${RECEIVER}
spec:
  flowID: "${FLOW}"
  provider: tcp
  podRef:
    name: ${ANCHOR}
    namespace: ${NAMESPACE}
EOF

for p in "$WRITER" "$ANCHOR"; do
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/${p}" \
      --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null || fail "${p} did not become Ready"
done

mirror="mxlflowmirror/${FLOW}--${tgt_node}"
wait_phase "$mirror" '{.status.phase}' '^Ready$' "$MIRROR_TIMEOUT_SECS" >/dev/null \
  || fail "${mirror} did not reach Ready"

# target_heartbeat: the renewTime of the mirror's target heartbeat Lease,
# cut to whole seconds, empty without one. The target renews it while
# grains commit; the mirror's status carries no progress time.
target_heartbeat() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get leases -l mxl.qvest-digital.com/mirror-side=target \
      -o jsonpath='{range .items[*]}{.metadata.annotations.mxl\.qvest-digital\.com/mirror}{" "}{.spec.renewTime}{"\n"}{end}' \
    | awk -v m="${NAMESPACE}/${mirror#mxlflowmirror/}" '$1 == m { print substr($2, 1, 19) "Z" }'
}

deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
until [ -n "$(target_heartbeat)" ]; do
  [ "$(date +%s)" -lt "$deadline" ] || fail "${mirror} did not carry a grain"
  sleep 2
done

target_info() { "${KUBECTL[@]}" -n "$NAMESPACE" get "$mirror" -o 'jsonpath={.status.targetInfoHash}'; }

# bounce <node>: replace the gateway pod on the node and wait for the
# replacement to run.
bounce() {
  local old new deadline
  old="$(daemonset_pod_on gateway "$1")"
  [ -n "$old" ] || fail "no running gateway pod on $1"
  "${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${old}" --wait=false >/dev/null \
    || fail "deleting ${old} failed"
  deadline=$(( $(date +%s) + ROLLOUT_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    new="$(daemonset_pod_on gateway "$1")"
    if [ -n "$new" ] && [ "$new" != "$old" ]; then
      echo "   ${old} -> ${new}"
      return 0
    fi
    sleep 2
  done
  fail "no replacement gateway pod came up on $1"
}


# resumes <what>: continue the writer and require a grain committed on
# the target after the resume. The heartbeat is RFC 3339 in UTC at
# second precision, so a later stamp compares greater as a string.
resumes() {
  local since deadline last
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  resume_writer || fail "could not continue the writer"
  deadline=$(( $(date +%s) + RESUME_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    last=$(target_heartbeat)
    if [[ "$last" > "$since" ]]; then
      echo "   grains land again after $1: target heartbeat ${last}, resumed ${since}"
      return 0
    fi
    sleep 2
  done
  "${KUBECTL[@]}" -n "$NAMESPACE" get "$mirror" -o yaml >&2 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" get leases -l mxl.qvest-digital.com/mirror-side -o yaml >&2 || true
  fail "no grain reached ${tgt_node} within ${RESUME_TIMEOUT_SECS}s of resuming after $1"
}

echo "-> producer paused, source reader reopened, source gateway restarted"
before="$(target_info)"
[ -n "$before" ] || fail "${mirror} names no target descriptor"
pause_writer || fail "could not stop the writer"
sleep "$PAUSE_SETTLE_SECS"
bounce "$src_node"
sleep 15
after="$(target_info)"
[ "$after" = "$before" ] \
  || fail "the target was set up again for a mirror whose producer only paused (descriptor changed)"
echo "   target descriptor unchanged"
resumes "the source gateway restart"

echo "-> producer paused, target gateway restarted"
pause_writer || fail "could not stop the writer"
sleep "$PAUSE_SETTLE_SECS"
before="$(target_info)"
bounce "$tgt_node"
# The status keeps the old hash until the new target publishes, so
# only a different one says the restarted target set itself up.
deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
until after="$(target_info)" && [ -n "$after" ] && [ "$after" != "$before" ]; do
  [ "$(date +%s)" -lt "$deadline" ] || fail "the restarted target published no new descriptor"
  sleep 2
done
echo "   target descriptor replaced"
sleep 10
resumes "the target gateway restart"
