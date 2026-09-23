#!/usr/bin/env bash
# Assert on-demand materialization for a consumer that is told what to
# read after it started -- the way a function is re-targeted through its
# own API or an IS-05 activation -- and that the intent protocol holds
# across shim releases.
#
# The consumer pod starts with nothing to read. Flows are then opened
# from inside it on a node where they do not exist:
#
#   1. through the previous release's shim, as a pod that copied it
#      before an agent upgrade still carries it;
#   2. through the shim the running agent leaves on the node;
#   3. in a second MxlDomain, which the agent materialises but does not
#      mirror: the open must fail cleanly, the pod must keep running,
#      and the agent must say why.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

OLD_SHIM_IMAGE="${OLD_SHIM_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/shim:v1.1.0-rc.19}"
TOOLS_IMAGE="${TOOLS_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
NODE_SHIM="${NODE_SHIM:-/run/mxl/libmxl-intent.so}"
FLOW_OLD="${FLOW_OLD:-57a0e5a1-0000-4000-8000-000000000001}"
FLOW_NEW="${FLOW_NEW:-57a0e5a1-0000-4000-8000-000000000002}"
SCRATCH_DOMAIN="${SCRATCH_DOMAIN:-kind-runtime-scratch}"
SCRATCH_ID="${SCRATCH_ID:-57a0e5a1-0000-4000-8000-0000000000ff}"
ATTEMPTS="${ATTEMPTS:-6}"
WRITER=mxl-runtime-intent-writer
CONSUMER=mxl-runtime-intent-consumer
CM=mxl-runtime-intent-flows

cleanup() {
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$WRITER" "$CONSUMER" \
      --wait=false --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" delete mxldomain "$SCRATCH_DOMAIN" --ignore-not-found >/dev/null 2>&1 || true
  for f in "$FLOW_OLD" "$FLOW_NEW"; do
    "${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o name 2>/dev/null \
      | grep "$f" | xargs -r "${KUBECTL[@]}" -n "$NAMESPACE" delete --wait=false >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT
cleanup
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete pod/"$WRITER" pod/"$CONSUMER" \
    --timeout=60s >/dev/null 2>&1 || true

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=mxl-k8s-agent --field-selector=status.phase=Running \
    -o 'jsonpath={range .items[*]}{.spec.nodeName}{" "}{end}')
read -r node_a node_b _ <<<"$nodes" || true
[ -n "${node_b:-}" ] || fail "need two agent nodes, one to write and one to read"
echo "   writer on ${node_a}, consumer on ${node_b}"

base=$("${KUBECTL[@]}" -n "$NAMESPACE" get configmap mxl-demo-flow-config \
         -o 'jsonpath={.data.flow-video-v210\.json}')
[ -n "$base" ] || fail "demo flow definition missing"
"${KUBECTL[@]}" -n "$NAMESPACE" create configmap "$CM" \
    --from-literal=old.json="$(echo "$base" | sed -E "s/\"id\": *\"[0-9a-f-]+\"/\"id\": \"${FLOW_OLD}\"/")" \
    --from-literal=new.json="$(echo "$base" | sed -E "s/\"id\": *\"[0-9a-f-]+\"/\"id\": \"${FLOW_NEW}\"/")" \
    >/dev/null

"${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${WRITER}
spec:
  nodeName: ${node_a}
  containers:
$(for f in old new; do cat <<C
    - name: write-${f}
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/usr/local/bin/write-grain", "-domain", "/run/mxl/domain", "-flow-def", "/flows/${f}.json"]
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-run, mountPath: /run/mxl}
        - {name: flows, mountPath: /flows}
C
done)
  volumes:
    - name: mxl-run
      hostPath: {path: /run/mxl, type: Directory}
    - name: flows
      configMap: {name: ${CM}}
---
apiVersion: v1
kind: Pod
metadata:
  name: ${CONSUMER}
spec:
  nodeName: ${node_b}
  initContainers:
    - name: old-shim
      image: ${OLD_SHIM_IMAGE}
      imagePullPolicy: IfNotPresent
      volumeMounts:
        - {name: old-shim, mountPath: /shared}
  containers:
    - name: consumer
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      # Started with nothing to read, as a function waiting to be told.
      command: ["/bin/sh", "-c", "sleep infinity"]
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-run, mountPath: /run/mxl}
        - {name: old-shim, mountPath: /opt/old-shim}
  volumes:
    - name: mxl-run
      hostPath: {path: /run/mxl, type: Directory}
    - name: old-shim
      emptyDir: {}
EOF

for p in "$WRITER" "$CONSUMER"; do
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/${p}" \
      --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null || fail "${p} did not become Ready"
done

# The flows must be known cluster-wide before a consumer can ask for them.
for f in "$FLOW_OLD" "$FLOW_NEW"; do
  wait_phase "mxlflow/${f}" "{.status.locations[?(@.nodeName==\"${node_a}\")].phase}" \
    '^Origin$' 60 >/dev/null || fail "flow ${f} did not become Origin on ${node_a}"
done
old_shim=$("${KUBECTL[@]}" -n "$NAMESPACE" exec "$CONSUMER" -- sh -c 'ls /opt/old-shim/*.so' | head -1)
[ -n "$old_shim" ] || fail "${OLD_SHIM_IMAGE} delivered no shim"

# read_through <shim> <domain dir> <flow>: open the flow from the running
# pod, retrying as a function re-targeted at runtime would.
read_through() {
  local shim="$1" dir="$2" flow="$3" i out
  for i in $(seq 1 "$ATTEMPTS"); do
    if out=$("${KUBECTL[@]}" -n "$NAMESPACE" exec "$CONSUMER" -- env LD_PRELOAD="$shim" \
          /usr/local/bin/read-grain -domain "$dir" -flow "$flow" -count 3 2>&1) \
       && echo "$out" | grep -q 'idx='; then
      return 0
    fi
    sleep 3
  done
  echo "$out" >&2
  return 1
}

echo "-> previous release's shim (${OLD_SHIM_IMAGE}) against this agent"
read_through "$old_shim" /run/mxl/domain "$FLOW_OLD" \
  || fail "a consumer carrying the previous shim could not materialize ${FLOW_OLD} at runtime"

echo "-> the shim the agent leaves on the node"
read_through "$NODE_SHIM" /run/mxl/domain "$FLOW_NEW" \
  || fail "a consumer re-targeted at runtime could not materialize ${FLOW_NEW}"

for f in "$FLOW_OLD" "$FLOW_NEW"; do
  phase=$("${KUBECTL[@]}" get mxlflow "$f" \
    -o "jsonpath={.status.locations[?(@.nodeName==\"${node_b}\")].phase}")
  case "$phase" in Ready|Mirroring) ;; *) fail "${f} not mirrored to ${node_b} (phase '${phase}')" ;; esac
  echo "   ${f} on ${node_b}: ${phase}"
done

echo "-> a flow in a second MxlDomain, which is materialised but not mirrored"
"${KUBECTL[@]}" apply -f - <<EOF >/dev/null
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlDomain
metadata:
  name: ${SCRATCH_DOMAIN}
spec:
  id: ${SCRATCH_ID}
  directory: scratch
EOF
wait_phase "mxldomain/${SCRATCH_DOMAIN}" \
  "{.status.nodes[?(@.nodeName==\"${node_b}\")].ready}/{.status.nodes[?(@.nodeName==\"${node_b}\")].mirrored}" \
  '^true/false$' 90 >/dev/null || fail "${SCRATCH_DOMAIN} not materialised unmirrored on ${node_b}"

since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if "${KUBECTL[@]}" -n "$NAMESPACE" exec "$CONSUMER" -- env LD_PRELOAD="$NODE_SHIM" \
     /usr/local/bin/read-grain -domain /run/mxl/scratch -flow "$FLOW_OLD" -count 1 >/dev/null 2>&1; then
  fail "a flow in an unmirrored domain was read on a node it was never written on"
fi
"${KUBECTL[@]}" -n "$NAMESPACE" get pod "$CONSUMER" -o 'jsonpath={.status.phase}' | grep -q Running \
  || fail "the refused open took the consumer pod down"

agent=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods -l app.kubernetes.io/name=mxl-k8s-agent \
          --field-selector "spec.nodeName=${node_b}" -o 'jsonpath={.items[0].metadata.name}')
"${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${agent}" --since-time="$since" 2>/dev/null \
  | grep -q 'does not mirror' \
  || fail "the agent on ${node_b} did not say why the open was refused"
echo "   refused cleanly; consumer still Running"
