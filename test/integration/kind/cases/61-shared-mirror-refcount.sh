#!/usr/bin/env bash
# Assert two same-flow, same-namespace MxlReceivers whose consumer
# pods sit on the same target node share one MxlFlowMirror and
# co-own it via metadata.ownerReferences. The pre-refcount design
# used a single-value LabelCreatedByReceiver, which two co-resident
# receivers would ping-pong on every reconcile. Owner refs let the
# apiserver garbage-collect the mirror only after the last owning
# receiver is gone, so a single receiver deletion no longer
# flap-deletes the mirror under any sibling still consuming it.
#
# Test runs ad-hoc receivers/pods in a separate namespace so it
# does not interfere with the demo's mxl-tcp-demo-reader.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

TEST_NS="${TEST_NS:-mxl-refcount-test}"
TEST_FLOW="${TEST_FLOW:-61bbccdd-eeff-1122-3344-556677889900}"
TARGET_NODE="${TARGET_NODE:-}"
SOURCE_NODE="${SOURCE_NODE:-}"
POLL_INTERVAL_SECS=2
POLL_TIMEOUT_SECS="${POLL_TIMEOUT_SECS:-60}"
GC_TIMEOUT_SECS="${GC_TIMEOUT_SECS:-90}"

cleanup() {
  "${KUBECTL[@]}" delete namespace "$TEST_NS" --wait=false --ignore-not-found \
      >/dev/null 2>&1 || true
}
trap cleanup EXIT

dump_diagnostics() {
  echo "--- mxlfm in $TEST_NS ---" >&2
  "${KUBECTL[@]}" -n "$TEST_NS" get mxlfm -o yaml >&2 2>/dev/null || true
  echo "--- mxlreceiver in $TEST_NS ---" >&2
  "${KUBECTL[@]}" -n "$TEST_NS" get mxlreceiver -o yaml >&2 2>/dev/null || true
  echo "--- recent events in $TEST_NS ---" >&2
  "${KUBECTL[@]}" -n "$TEST_NS" get events --sort-by=.lastTimestamp \
      2>/dev/null | tail -30 >&2 || true
  echo "--- operator logs (last 80 lines per pod) ---" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" logs \
      -l app.kubernetes.io/name=mxl-k8s-operator \
      --all-containers --prefix --tail=80 >&2 2>/dev/null || true
}

# Pick two distinct kind worker nodes from the cluster; the demo
# writer hosts the flow's Origin, so pin the test consumer pods to
# the non-writer worker. SOURCE_NODE/TARGET_NODE can be overridden.
if [ -z "$SOURCE_NODE" ] || [ -z "$TARGET_NODE" ]; then
  nodes=$("${KUBECTL[@]}" get nodes -o jsonpath='{.items[*].metadata.name}')
  set -- $nodes
  if [ "$#" -lt 2 ]; then
    fail "case 61 requires at least 2 nodes; got $#: $*"
  fi
  SOURCE_NODE="${SOURCE_NODE:-$1}"
  shift
  TARGET_NODE="${TARGET_NODE:-$1}"
fi
echo "  source node: $SOURCE_NODE; target node: $TARGET_NODE"

"${KUBECTL[@]}" create namespace "$TEST_NS" >/dev/null \
  || fail "create namespace $TEST_NS failed"

# Two consumer pods on the same target node so the receivers
# resolve to the same (flowID, targetNode) mirror key. pause image
# avoids pulling demo tooling -- this test does not need libmxl
# inside the consumer; only the operator's reconciler is exercised.
"${KUBECTL[@]}" -n "$TEST_NS" apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: consumer-a
  labels:
    app.kubernetes.io/name: refcount-consumer-a
spec:
  nodeName: $TARGET_NODE
  containers:
    - name: c
      image: registry.k8s.io/pause:3.10
---
apiVersion: v1
kind: Pod
metadata:
  name: consumer-b
  labels:
    app.kubernetes.io/name: refcount-consumer-b
spec:
  nodeName: $TARGET_NODE
  containers:
    - name: c
      image: registry.k8s.io/pause:3.10
EOF

"${KUBECTL[@]}" -n "$TEST_NS" wait --for=condition=PodScheduled \
    "pod/consumer-a" "pod/consumer-b" --timeout=30s >/dev/null \
  || fail "consumer pods did not schedule in 30s"

# A standalone MxlFlow with the test flow ID, status.locations
# pinned to SOURCE_NODE as the Origin. The receivers below carry
# this flow ID; the operator resolves the source from the flow's
# status. The flow has cluster scope and is deleted via the
# trap (status.locations is set via subresource patch).
"${KUBECTL[@]}" apply -f - <<EOF >/dev/null
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlFlow
metadata:
  name: $TEST_FLOW
spec:
  id: $TEST_FLOW
  definition:
    id: $TEST_FLOW
EOF
trap '"${KUBECTL[@]}" delete mxlflow "$TEST_FLOW" --ignore-not-found >/dev/null 2>&1 || true; cleanup' EXIT

"${KUBECTL[@]}" patch mxlflow "$TEST_FLOW" --subresource=status --type=merge -p \
  "{\"status\":{\"locations\":[{\"nodeName\":\"$SOURCE_NODE\",\"phase\":\"Origin\"}]}}" \
  >/dev/null || fail "patch flow status.locations failed"

"${KUBECTL[@]}" -n "$TEST_NS" apply -f - <<EOF >/dev/null
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlReceiver
metadata:
  name: recv-a
spec:
  flowID: $TEST_FLOW
  provider: tcp
  podRef:
    name: consumer-a
    namespace: $TEST_NS
---
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlReceiver
metadata:
  name: recv-b
spec:
  flowID: $TEST_FLOW
  provider: tcp
  podRef:
    name: consumer-b
    namespace: $TEST_NS
EOF

# Poll for exactly one mirror to appear in $TEST_NS with two owner
# references. The mirror's status.phase is not required to reach
# Ready -- the gateway side may fail to actually open libfabric
# against the synthetic test pods, but the operator's owner-ref
# bookkeeping must converge regardless.
deadline=$(( $(date +%s) + POLL_TIMEOUT_SECS ))
mirror_name=""
owner_count=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  count=$("${KUBECTL[@]}" -n "$TEST_NS" get mxlfm \
            -o jsonpath='{range .items[*]}{.metadata.name} {end}' 2>/dev/null \
            | wc -w | tr -d ' ')
  if [ "$count" = "1" ]; then
    mirror_name=$("${KUBECTL[@]}" -n "$TEST_NS" get mxlfm \
                    -o jsonpath='{.items[0].metadata.name}')
    owner_count=$("${KUBECTL[@]}" -n "$TEST_NS" get \
                    "mxlfm/${mirror_name}" \
                    -o jsonpath='{.metadata.ownerReferences[*].uid}' \
                  | wc -w | tr -d ' ')
    if [ "$owner_count" = "2" ]; then
      break
    fi
  fi
  sleep "$POLL_INTERVAL_SECS"
done

if [ -z "$mirror_name" ] || [ "$owner_count" != "2" ]; then
  echo "  expected one mirror with two owner refs; got mirror='${mirror_name}' owners=${owner_count}" >&2
  dump_diagnostics
  fail "co-resident receivers did not converge on one mirror with two owners in ${POLL_TIMEOUT_SECS}s"
fi
echo "  mirror $mirror_name: $owner_count owner refs"

# Both owner refs must be non-controller and non-blocking; that is
# the invariant that lets receiver delete proceed without waiting
# on the shared mirror finalising.
ctrl_flags=$("${KUBECTL[@]}" -n "$TEST_NS" get "mxlfm/${mirror_name}" \
              -o jsonpath='{.metadata.ownerReferences[*].controller}')
block_flags=$("${KUBECTL[@]}" -n "$TEST_NS" get "mxlfm/${mirror_name}" \
                -o jsonpath='{.metadata.ownerReferences[*].blockOwnerDeletion}')
case "$ctrl_flags" in
  *true*) fail "owner ref carries controller=true; co-ownership requires controller=false (got: $ctrl_flags)";;
esac
case "$block_flags" in
  *true*) fail "owner ref carries blockOwnerDeletion=true; receiver delete must not wait on the co-owned mirror (got: $block_flags)";;
esac
echo "  owner refs: controller=${ctrl_flags:-false} blockOwnerDeletion=${block_flags:-false}"

# Delete recv-a; the mirror must stay, with one owner ref pointing
# at recv-b.
recv_b_uid=$("${KUBECTL[@]}" -n "$TEST_NS" get mxlreceiver/recv-b \
              -o jsonpath='{.metadata.uid}')
"${KUBECTL[@]}" -n "$TEST_NS" delete mxlreceiver/recv-a --wait=true \
    --timeout=60s >/dev/null \
  || { dump_diagnostics; fail "delete recv-a hung; the finalizer must release on owner-ref removal"; }

deadline=$(( $(date +%s) + POLL_TIMEOUT_SECS ))
ok=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  count=$("${KUBECTL[@]}" -n "$TEST_NS" get mxlfm \
            -o jsonpath='{range .items[*]}{.metadata.name} {end}' 2>/dev/null \
            | wc -w | tr -d ' ')
  if [ "$count" = "1" ]; then
    uid=$("${KUBECTL[@]}" -n "$TEST_NS" get "mxlfm/${mirror_name}" \
            -o jsonpath='{.metadata.ownerReferences[*].uid}' \
            2>/dev/null)
    if [ "$uid" = "$recv_b_uid" ]; then
      ok=1
      break
    fi
  fi
  sleep "$POLL_INTERVAL_SECS"
done
if [ "$ok" != "1" ]; then
  echo "  expected mirror $mirror_name with surviving owner UID $recv_b_uid" >&2
  dump_diagnostics
  fail "after recv-a deletion the mirror must retain only recv-b's owner ref"
fi
echo "  recv-a removed; mirror retains recv-b as sole owner"

# Delete recv-b; first confirm the operator removed the last owner
# ref (DeletionTimestamp non-empty as apiserver GC accepts the
# delete), then wait for the mirror to disappear via foreground
# propagation. 60s is comfortable for kube-controller-manager's GC
# loop on kind.
"${KUBECTL[@]}" -n "$TEST_NS" delete mxlreceiver/recv-b --wait=true \
    --timeout=60s >/dev/null \
  || { dump_diagnostics; fail "delete recv-b hung; the finalizer must release on owner-ref removal"; }

# DeletionTimestamp check: bounded 5s window; the operator's last
# owner-ref removal is in-process, GC reaction is in the kube-
# controller-manager which polls.
del_deadline=$(( $(date +%s) + 5 ))
del_observed=0
while [ "$(date +%s)" -lt "$del_deadline" ]; do
  dt=$("${KUBECTL[@]}" -n "$TEST_NS" get "mxlfm/${mirror_name}" \
        -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true)
  if [ -n "$dt" ]; then
    del_observed=1
    break
  fi
  # The mirror may have already been fully GC'd in the 5s window.
  if ! "${KUBECTL[@]}" -n "$TEST_NS" get "mxlfm/${mirror_name}" \
        >/dev/null 2>&1; then
    del_observed=1
    break
  fi
  sleep 1
done
if [ "$del_observed" != "1" ]; then
  echo "  no deletionTimestamp on mirror within 5s of recv-b delete" >&2
  dump_diagnostics
  fail "operator did not release last owner ref so GC could mark the mirror for deletion"
fi

deadline=$(( $(date +%s) + GC_TIMEOUT_SECS ))
gone=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  if ! "${KUBECTL[@]}" -n "$TEST_NS" get "mxlfm/${mirror_name}" \
        >/dev/null 2>&1; then
    gone=1
    break
  fi
  sleep "$POLL_INTERVAL_SECS"
done
if [ "$gone" != "1" ]; then
  echo "  mirror $mirror_name still present after ${GC_TIMEOUT_SECS}s" >&2
  dump_diagnostics
  fail "apiserver garbage collector did not remove the mirror within ${GC_TIMEOUT_SECS}s of the last owner ref disappearing"
fi
echo "  mirror $mirror_name garbage-collected after last owner removed"
