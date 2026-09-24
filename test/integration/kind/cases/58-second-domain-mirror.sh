#!/usr/bin/env bash
# Assert a flow in a second MxlDomain is mirrored between nodes within
# that domain.
#
# The domain is created without a directory, so it lives at
# domains/<id> below the runtime root. A writer on one node writes a
# flow there whose id a writer also uses in the primary domain; a
# consumer on another node opens the second domain's flow at runtime
# through the shim. The mirror must carry the domain and land in the
# second domain's directory, and the primary domain's flow of the same
# id must stay where it was written: the same id in two domains is two
# flows. Both the running agent's shim and the previous release's are
# exercised, since the domain is read from the path the shim reports.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

OLD_SHIM_IMAGE="${OLD_SHIM_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/shim:v1.1.0-rc.19}"
TOOLS_IMAGE="${TOOLS_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
NODE_SHIM="${NODE_SHIM:-/run/mxl/libmxl-intent.so}"
DOMAIN="${DOMAIN:-kind-second-domain}"
DOMAIN_ID="${DOMAIN_ID:-58a0e5a1-0000-4000-8000-0000000000dd}"
FLOW_SHARED="${FLOW_SHARED:-58a0e5a1-0000-4000-8000-000000000001}"
FLOW_OLD_SHIM="${FLOW_OLD_SHIM:-58a0e5a1-0000-4000-8000-000000000002}"
ATTEMPTS="${ATTEMPTS:-6}"
DOMAIN_DIR="/run/mxl/domains/${DOMAIN_ID}"
WRITER=mxl-second-domain-writer
CONSUMER=mxl-second-domain-consumer
CM=mxl-second-domain-flows

cleanup() {
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$WRITER" "$CONSUMER" \
      --wait=false --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1 || true
  for f in "$FLOW_SHARED" "$FLOW_OLD_SHIM"; do
    "${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors -o name 2>/dev/null \
      | grep "$f" | xargs -r "${KUBECTL[@]}" -n "$NAMESPACE" delete --wait=false >/dev/null 2>&1 || true
  done
  "${KUBECTL[@]}" delete mxldomain "$DOMAIN" --ignore-not-found >/dev/null 2>&1 || true
  # The agent never removes a domain directory -- flows may be in it.
  if [ -n "${nodes:-}" ]; then
    for n in $nodes; do
      "${KUBECTL[@]}" -n "$NAMESPACE" run "mxl-domain-rm-${n}" --restart=Never --rm -i \
        --image="$TOOLS_IMAGE" --image-pull-policy=IfNotPresent --quiet \
        --overrides="{\"spec\":{\"nodeName\":\"${n}\",\"volumes\":[{\"name\":\"r\",\"hostPath\":{\"path\":\"/run/mxl\"}}],\"containers\":[{\"name\":\"rm\",\"image\":\"${TOOLS_IMAGE}\",\"command\":[\"rm\",\"-rf\",\"${DOMAIN_DIR}\"],\"volumeMounts\":[{\"name\":\"r\",\"mountPath\":\"/run/mxl\"}]}]}}" \
        >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=mxl-k8s-agent --field-selector=status.phase=Running \
    -o 'jsonpath={range .items[*]}{.spec.nodeName}{" "}{end}')
read -r node_a node_b _ <<<"$nodes" || true
[ -n "${node_b:-}" ] || fail "need two agent nodes, one to write and one to read"
cleanup
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete pod/"$WRITER" pod/"$CONSUMER" \
    --timeout=60s >/dev/null 2>&1 || true
echo "   writer on ${node_a}, consumer on ${node_b}"

"${KUBECTL[@]}" apply -f - <<EOF >/dev/null
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlDomain
metadata:
  name: ${DOMAIN}
spec:
  id: ${DOMAIN_ID}
EOF
for n in "$node_a" "$node_b"; do
  wait_phase "mxldomain/${DOMAIN}" \
    "{.status.nodes[?(@.nodeName==\"${n}\")].ready}/{.status.nodes[?(@.nodeName==\"${n}\")].mirrored}" \
    '^true/true$' 90 >/dev/null || fail "${DOMAIN} not materialised and mirrored on ${n}"
done
echo "   ${DOMAIN} at ${DOMAIN_DIR}, mirrored on both nodes"

base=$("${KUBECTL[@]}" -n "$NAMESPACE" get configmap mxl-demo-flow-config \
         -o 'jsonpath={.data.flow-video-v210\.json}')
[ -n "$base" ] || fail "demo flow definition missing"
flow_def() { echo "$base" | sed -E "s/\"id\": *\"[0-9a-f-]+\"/\"id\": \"$1\"/"; }
"${KUBECTL[@]}" -n "$NAMESPACE" create configmap "$CM" \
    --from-literal=shared.json="$(flow_def "$FLOW_SHARED")" \
    --from-literal=old.json="$(flow_def "$FLOW_OLD_SHIM")" >/dev/null

# write-<name> <domain dir> <definition>
writer() {
  cat <<C
    - name: write-$1
      image: ${TOOLS_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/usr/local/bin/write-grain", "-domain", "$2", "-flow-def", "/flows/$3"]
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - {name: mxl-run, mountPath: /run/mxl}
        - {name: flows, mountPath: /flows}
C
}

"${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${WRITER}
spec:
  nodeName: ${node_a}
  containers:
$(writer primary /run/mxl/domain shared.json)
$(writer second "$DOMAIN_DIR" shared.json)
$(writer second-old "$DOMAIN_DIR" old.json)
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

# The same id in two domains is two MxlFlows: the bare id in the primary
# domain, <domain>.<id> in the second.
for f in "$FLOW_SHARED" "${DOMAIN}.${FLOW_SHARED}" "${DOMAIN}.${FLOW_OLD_SHIM}"; do
  wait_phase "mxlflow/${f}" "{.status.locations[?(@.nodeName==\"${node_a}\")].phase}" \
    '^Origin$' 60 >/dev/null || fail "flow ${f} did not become Origin on ${node_a}"
done
domain=$("${KUBECTL[@]}" get mxlflow "${DOMAIN}.${FLOW_SHARED}" -o 'jsonpath={.spec.domain}')
[ "$domain" = "$DOMAIN" ] || fail "second domain's MxlFlow names domain '${domain}'"

old_shim=$("${KUBECTL[@]}" -n "$NAMESPACE" exec "$CONSUMER" -- sh -c 'ls /opt/old-shim/*.so' | head -1)
[ -n "$old_shim" ] || fail "${OLD_SHIM_IMAGE} delivered no shim"

# read_through <shim> <flow>: open a flow of the second domain from the
# running pod, retrying as a function re-targeted at runtime would.
read_through() {
  local shim="$1" flow="$2" i out
  for i in $(seq 1 "$ATTEMPTS"); do
    if out=$("${KUBECTL[@]}" -n "$NAMESPACE" exec "$CONSUMER" -- env LD_PRELOAD="$shim" \
          /usr/local/bin/read-grain -domain "$DOMAIN_DIR" -flow "$flow" -count 3 2>&1) \
       && echo "$out" | grep -q 'idx='; then
      return 0
    fi
    sleep 3
  done
  echo "$out" >&2
  return 1
}

echo "-> the shim the agent leaves on the node"
read_through "$NODE_SHIM" "$FLOW_SHARED" \
  || fail "a consumer could not read ${FLOW_SHARED} of ${DOMAIN} on ${node_b}"

echo "-> previous release's shim (${OLD_SHIM_IMAGE})"
read_through "$old_shim" "$FLOW_OLD_SHIM" \
  || fail "a consumer carrying the previous shim could not read ${FLOW_OLD_SHIM} of ${DOMAIN}"

for f in "$FLOW_SHARED" "$FLOW_OLD_SHIM"; do
  mirror=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
    -o "jsonpath={range .items[?(@.spec.flowID==\"${f}\")]}{.metadata.name} {.spec.domain} {.spec.targetNode}{\"\n\"}{end}")
  echo "$mirror" | grep -q " ${DOMAIN} ${node_b}\$" \
    || fail "no mirror of ${f} in ${DOMAIN} towards ${node_b} (got '${mirror}')"
  phase=$("${KUBECTL[@]}" get mxlflow "${DOMAIN}.${f}" \
    -o "jsonpath={.status.locations[?(@.nodeName==\"${node_b}\")].phase}")
  case "$phase" in Ready|Mirroring) ;; *) fail "${DOMAIN}.${f} not mirrored to ${node_b} (phase '${phase}')" ;; esac
  echo "   ${DOMAIN}.${f} on ${node_b}: ${phase}"
done

# Nothing asked for the primary domain's flow on node B, so neither a
# mirror of it nor a location there may exist: the second domain's
# consumer must not have been served the primary's flow of the same id.
primary_mirror=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
  -o "jsonpath={range .items[?(@.spec.flowID==\"${FLOW_SHARED}\")]}{.spec.domain}{\"|\"}{end}")
case "|${primary_mirror}" in *"||"*) fail "a primary-domain mirror of ${FLOW_SHARED} was created" ;; esac
phase=$("${KUBECTL[@]}" get mxlflow "$FLOW_SHARED" \
  -o "jsonpath={.status.locations[?(@.nodeName==\"${node_b}\")].phase}")
[ -z "$phase" ] || fail "the primary domain's ${FLOW_SHARED} appeared on ${node_b} (phase '${phase}')"

"${KUBECTL[@]}" -n "$NAMESPACE" exec "$CONSUMER" -- \
  test -d "${DOMAIN_DIR}/${FLOW_SHARED}.mxl-flow" \
  || fail "the mirror did not write into ${DOMAIN_DIR} on ${node_b}"
if "${KUBECTL[@]}" -n "$NAMESPACE" exec "$CONSUMER" -- \
     test -e "/run/mxl/domain/${FLOW_SHARED}.mxl-flow"; then
  fail "the second domain's mirror wrote into the primary domain on ${node_b}"
fi
echo "   written into ${DOMAIN_DIR}; primary domain untouched"
