#!/usr/bin/env bash
# Assert the operator demotes a receiver when the origin's agent
# stops renewing its per-flow Lease. The agent on the origin node
# is taken out of service by patching the agent DaemonSet to a
# nodeSelector that the origin node does not satisfy; the DS
# controller then evicts the local pod and refuses to reschedule
# it there. After the 30s Lease window elapses, MxlFlow's
# OriginFresh condition must flip to False and the MxlReceiver
# must drop to Pending. The DaemonSet and node labels are
# restored on exit so subsequent reconciliation returns to a
# healthy demo.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

AGENT_DS="${AGENT_DS:-mxl-k8s-agent}"
RECEIVER_NAME="${RECEIVER_NAME:-tcp-demo}"
GATE_LABEL_KEY="${GATE_LABEL_KEY:-mxl-test-agent-eligible}"
GATE_LABEL_VAL="${GATE_LABEL_VAL:-yes}"
LEASE_EXPIRY_TIMEOUT_SECS="${LEASE_EXPIRY_TIMEOUT_SECS:-60}"

mirror=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
          -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[ -n "$mirror" ] || fail "no MxlFlowMirror in namespace ${NAMESPACE}"

origin_node=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
              -o jsonpath='{.spec.sourceNode}')
[ -n "$origin_node" ] || fail "mxlfm/${mirror} has empty spec.sourceNode"

flow_id=$("${KUBECTL[@]}" -n "$NAMESPACE" get "mxlfm/${mirror}" \
          -o jsonpath='{.spec.flowID}')
[ -n "$flow_id" ] || fail "mxlfm/${mirror} has empty spec.flowID"
echo "  origin_node=${origin_node} flow_id=${flow_id} mirror=${mirror}"

# Snapshot the list of nodes other than the origin so they can be
# labeled to keep their agent pods running while the origin agent
# is evicted. Build via a plain awk filter; bash 3.2 has no array
# difference primitive.
all_nodes=$("${KUBECTL[@]}" get nodes \
            -o jsonpath='{range .items[*]}{.metadata.name} {end}')
other_nodes=$(echo "$all_nodes" | awk -v drop="$origin_node" '
  {
    for (i = 1; i <= NF; i++) if ($i != drop) printf "%s ", $i
  }')
[ -n "$other_nodes" ] || fail "no nodes other than ${origin_node} to keep the agent on"

cleanup() {
  # Roll back the DS patch and label additions so the rest of the
  # suite (and any hand-driven inspection) sees the original state.
  "${KUBECTL[@]}" -n "$NAMESPACE" patch "ds/${AGENT_DS}" --type=json \
      -p='[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]' \
      >/dev/null 2>&1 || true
  for n in $other_nodes; do
    "${KUBECTL[@]}" label node "$n" "${GATE_LABEL_KEY}-" \
        >/dev/null 2>&1 || true
  done
  "${KUBECTL[@]}" -n "$NAMESPACE" rollout status "ds/${AGENT_DS}" \
      --timeout="${ROLLOUT_TIMEOUT_SECS}s" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for n in $other_nodes; do
  "${KUBECTL[@]}" label node "$n" \
      "${GATE_LABEL_KEY}=${GATE_LABEL_VAL}" --overwrite >/dev/null \
    || fail "label node ${n} failed"
done

"${KUBECTL[@]}" -n "$NAMESPACE" patch "ds/${AGENT_DS}" --type=merge \
    -p="{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":{\"${GATE_LABEL_KEY}\":\"${GATE_LABEL_VAL}\"}}}}}" \
    >/dev/null \
  || fail "patch ds/${AGENT_DS} nodeSelector failed"

# Wait for the agent pod on the origin node to be deleted. The DS
# controller does this once the template no longer selects the
# origin. Field-selector keeps the query cheap.
deadline=$(( $(date +%s) + 60 ))
agent_pod=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  agent_pod=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
                -l "app.kubernetes.io/name=${AGENT_DS}" \
                --field-selector="spec.nodeName=${origin_node}" \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -z "$agent_pod" ] && break
  sleep 2
done
[ -z "$agent_pod" ] \
  || fail "agent pod ${agent_pod} on ${origin_node} was not evicted within 60s"
echo "  agent on ${origin_node} evicted"

lease_name="mxl-flow-${flow_id}-${origin_node}"

# Park until the Lease window plus a small grace has elapsed. The
# agent writes Leases with LeaseDurationSeconds=30 and renews every
# 10s, so 40s past the last renewal is comfortably stale. Doing the
# wait up front avoids a race where the receiver reconciles before
# the Lease has actually expired and decides OriginFresh is still
# True.
sleep 40

# The receiver controller does not watch Leases, so a passive
# expiry does not by itself trigger a reconcile. Touch the
# receiver with a benign annotation bump to enqueue one; the
# resolver will then read the now-stale Lease and patch the
# OriginFresh condition to False.
"${KUBECTL[@]}" -n "$NAMESPACE" annotate "mxlr/${RECEIVER_NAME}" \
    "mxl-test-poke=$(date +%s)" --overwrite >/dev/null \
  || fail "annotate mxlr/${RECEIVER_NAME} failed"

# Poll the OriginFresh condition on the cluster-scoped MxlFlow.
# The condition is owned by the operator's receiver reconciler and
# flips to False once every Origin location's Lease is past its
# renewal window.
JSONPATH='{range .status.conditions[?(@.type=="OriginFresh")]}{.status}{end}'
deadline=$(( $(date +%s) + LEASE_EXPIRY_TIMEOUT_SECS ))
cond_status=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  cond_status=$("${KUBECTL[@]}" get "mxlflow/${flow_id}" \
                -o jsonpath="$JSONPATH" 2>/dev/null || true)
  [ "$cond_status" = "False" ] && break
  sleep 2
done
if [ "$cond_status" != "False" ]; then
  echo "  lease ${lease_name} state:" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" get "lease/${lease_name}" -o yaml >&2 \
      2>/dev/null || true
  fail "MxlFlow/${flow_id} OriginFresh did not become False within ${LEASE_EXPIRY_TIMEOUT_SECS}s (last=${cond_status})"
fi
echo "  MxlFlow/${flow_id} OriginFresh=False"

# The MxlReceiver should drop to Pending once the resolver can no
# longer find a fresh Origin. Reuse wait_phase for the namespaced
# resource.
recv_phase=$(wait_phase "mxlr/${RECEIVER_NAME}" '{.status.phase}' \
              '^Pending$' "$LEASE_EXPIRY_TIMEOUT_SECS") \
  || fail "MxlReceiver/${RECEIVER_NAME} did not drop to Pending (last=${recv_phase})"
echo "  MxlReceiver/${RECEIVER_NAME} phase=${recv_phase}"
