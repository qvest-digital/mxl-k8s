#!/usr/bin/env bash
# Assert mirrors are placed on, and bound to, a pair of fabric
# interfaces, and that a second mirror between the same two nodes is
# placed on the other pair.
#
# kind-up.sh attaches every node to a second network and runs the
# gateways without a bind address (FABRIC_PORTS=2), so each node offers
# two tcp interfaces in two subnets. The second mirror copies the demo
# flow into another namespace, which is how two consumer namespaces on
# one node each get a mirror of their own, so both carry real grains.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

PLACEMENT_TIMEOUT_SECS="${PLACEMENT_TIMEOUT_SECS:-60}"
SECOND_NS=mxl-placement-test

tcp_interfaces() {
  "${KUBECTL[@]}" get "mxlnodecapabilities/$1" \
    -o jsonpath='{range .status.providers[?(@.name=="tcp")].interfaces[*]}{.address}{"\n"}{end}' 2>/dev/null \
    | grep -c . || true
}

subnet_of() {
  "${KUBECTL[@]}" get "mxlnodecapabilities/$1" \
    -o jsonpath="{.status.providers[?(@.name==\"tcp\")].interfaces[?(@.address==\"$2\")].subnet}"
}

mirror_field() {
  "${KUBECTL[@]}" -n "$1" get "mxlflowmirror/$2" -o jsonpath="$3" 2>/dev/null || true
}

# placed <namespace> <mirror>: waits for Placed=True/Placed and for both
# gateways to report the interfaces the placement names, and prints
# "<source interface> <target interface>".
placed() {
  local ns="$1" m="$2" deadline src tgt bsrc btgt reason
  deadline=$(( $(date +%s) + PLACEMENT_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    reason=$(mirror_field "$ns" "$m" '{.status.conditions[?(@.type=="Placed")].reason}')
    src=$(mirror_field "$ns" "$m" '{.spec.placement.sourceInterface}')
    tgt=$(mirror_field "$ns" "$m" '{.spec.placement.targetInterface}')
    bsrc=$(mirror_field "$ns" "$m" '{.status.sourceInterface}')
    btgt=$(mirror_field "$ns" "$m" '{.status.targetInterface}')
    if [ "$reason" = "Placed" ] && [ -n "$src" ] && [ "$src" = "$bsrc" ] && [ -n "$tgt" ] && [ "$tgt" = "$btgt" ]; then
      echo "$src $tgt"
      return 0
    fi
    sleep 2
  done
  echo "   ${ns}/${m}: reason='${reason}' placed='${src}->${tgt}' bound='${bsrc}->${btgt}'" >&2
  "${KUBECTL[@]}" -n "$ns" get "mxlflowmirror/$m" -o yaml >&2 || true
  return 1
}

# The demo's video mirror: the first tcp mirror in the demo namespace.
demo=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
  -o jsonpath='{range .items[?(@.spec.provider=="tcp")]}{.metadata.name}{"\n"}{end}' | head -n1)
[ -n "$demo" ] || fail "no tcp MxlFlowMirror in ${NAMESPACE}"
src_node=$(mirror_field "$NAMESPACE" "$demo" '{.spec.sourceNode}')
tgt_node=$(mirror_field "$NAMESPACE" "$demo" '{.spec.targetNode}')

if [ "$(tcp_interfaces "$src_node")" -lt 2 ] || [ "$(tcp_interfaces "$tgt_node")" -lt 2 ]; then
  echo "  ${src_node} or ${tgt_node} offers fewer than two tcp interfaces (FABRIC_PORTS=1); skipping"
  exit 0
fi

echo "-> ${NAMESPACE}/${demo} (${src_node} -> ${tgt_node})"
pair=$(placed "$NAMESPACE" "$demo") || fail "the demo mirror was never placed and bound"
read -r first_src first_tgt <<<"$pair"
echo "   placed and bound on ${first_src} -> ${first_tgt}"
[ "$(subnet_of "$src_node" "$first_src")" = "$(subnet_of "$tgt_node" "$first_tgt")" ] \
  || fail "the demo mirror pairs interfaces of two subnets"

# Captured first: under pipefail, a reader closing the pipe early fails
# kubectl with SIGPIPE.
listing=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o wide)
case "${listing%%$'\n'*}" in
  *PLACEMENT*) ;;
  *) fail "kubectl get mxlflowmirrors shows no placement column: ${listing%%$'\n'*}" ;;
esac

# Waited for, so the cases after this one find the demo's mirrors only.
cleanup() {
  "${KUBECTL[@]}" delete namespace "$SECOND_NS" --ignore-not-found --timeout=120s >/dev/null 2>&1 || true
}
trap cleanup EXIT

reader=$(mirror_field "$NAMESPACE" "$demo" '{.spec.requestor.name}')
reader_ns=$(mirror_field "$NAMESPACE" "$demo" '{.spec.requestor.namespace}')
reader_uid=$(mirror_field "$NAMESPACE" "$demo" '{.spec.requestor.uid}')
if [ -z "$reader" ]; then
  # A receiver-owned demo mirror names no pod; any Running pod on the
  # target node claims the second one for the length of this case.
  reader_ns=$NAMESPACE
  reader=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods --field-selector="spec.nodeName=${tgt_node},status.phase=Running" \
    -o jsonpath='{.items[0].metadata.name}')
  reader_uid=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${reader}" -o jsonpath='{.metadata.uid}')
fi
flow=$(mirror_field "$NAMESPACE" "$demo" '{.spec.flowID}')

"${KUBECTL[@]}" create namespace "$SECOND_NS" >/dev/null
"${KUBECTL[@]}" apply -f - >/dev/null <<EOF
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlFlowMirror
metadata:
  name: second
  namespace: ${SECOND_NS}
spec:
  flowID: ${flow}
  sourceNode: ${src_node}
  targetNode: ${tgt_node}
  provider: tcp
  requestor:
    namespace: ${reader_ns}
    name: ${reader}
    uid: ${reader_uid}
EOF

echo "-> ${SECOND_NS}/second"
pair=$(placed "$SECOND_NS" second) || fail "the second mirror was never placed and bound"
read -r second_src second_tgt <<<"$pair"
echo "   placed and bound on ${second_src} -> ${second_tgt}"
[ "$second_src" != "$first_src" ] || fail "the second mirror leaves ${src_node} on the demo mirror's interface ${second_src}"
[ "$second_tgt" != "$first_tgt" ] || fail "the second mirror arrives on ${tgt_node} at the demo mirror's interface ${second_tgt}"

wait_phase_in() {
  local deadline value
  deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    value=$(mirror_field "$SECOND_NS" second '{.status.phase}')
    [ "$value" = "Ready" ] && return 0
    sleep 2
  done
  echo "   phase '${value}'" >&2
  return 1
}
wait_phase_in || fail "the second mirror never moved grains over the other interface pair"
echo "   Ready over the second pair"

# A cleared placement is chosen again, and the gateways follow it.
"${KUBECTL[@]}" -n "$SECOND_NS" patch mxlflowmirror second --type=merge -p '{"spec":{"placement":null}}' >/dev/null
pair=$(placed "$SECOND_NS" second) || fail "a cleared placement was not chosen again"
echo "   placed again on ${pair}"

# A placement set by hand onto the pair the demo mirror uses is kept, and
# both gateways move the live mirror onto it: the target rebuilds its
# fabric side on the new address, the source re-adds the new target.
"${KUBECTL[@]}" -n "$SECOND_NS" patch mxlflowmirror second --type=merge \
  -p "{\"spec\":{\"placement\":{\"sourceInterface\":\"${first_src}\",\"targetInterface\":\"${first_tgt}\"}}}" >/dev/null
deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  bsrc=$(mirror_field "$SECOND_NS" second '{.status.sourceInterface}')
  btgt=$(mirror_field "$SECOND_NS" second '{.status.targetInterface}')
  phase=$(mirror_field "$SECOND_NS" second '{.status.phase}')
  [ "$bsrc" = "$first_src" ] && [ "$btgt" = "$first_tgt" ] && [ "$phase" = "Ready" ] && break
  sleep 2
done
if [ "$bsrc" != "$first_src" ] || [ "$btgt" != "$first_tgt" ] || [ "$phase" != "Ready" ]; then
  fail "a hand-set placement was not followed: bound '${bsrc}->${btgt}', phase '${phase}'"
fi
echo "   rebound by hand onto ${first_src} -> ${first_tgt}, Ready"
