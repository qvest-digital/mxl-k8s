#!/usr/bin/env bash
# Assert every agent node carries the default MxlDomain as BCP-007-03
# requires: a domain_def.json at the root of the domain directory
# whose id is the MxlDomain's, the same on every node. A media
# function reads that file to learn the mxl_domain_id it publishes,
# so a node without it, or with another id, has a function publishing
# null or a different domain than the one mxl-k8s mirrors.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

DOMAIN_NAME="${DOMAIN_NAME:-default}"
DOMAIN_DIR="${DOMAIN_DIR:-/run/mxl/domain}"
CHECK_IMAGE="${CHECK_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
CHECK_TIMEOUT_SECS="${CHECK_TIMEOUT_SECS:-90}"
SYNC_TIMEOUT_SECS="${SYNC_TIMEOUT_SECS:-60}"
POD_PREFIX=mxl-domain-identity-check

cleanup() {
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod -l "app.kubernetes.io/name=${POD_PREFIX}" \
      --wait=false --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT
"${KUBECTL[@]}" -n "$NAMESPACE" delete pod -l "app.kubernetes.io/name=${POD_PREFIX}" \
    --ignore-not-found --force --grace-period=0 >/dev/null 2>&1 || true

id=$("${KUBECTL[@]}" get mxldomain "$DOMAIN_NAME" -o 'jsonpath={.spec.id}') \
  || fail "no MxlDomain ${DOMAIN_NAME}; the chart renders it by default"
echo "$id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[1-9a-f][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' \
  || fail "MxlDomain ${DOMAIN_NAME} id ${id} is not an MXL UUID"
echo "   MxlDomain ${DOMAIN_NAME} id ${id}"

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
          -l app.kubernetes.io/name=mxl-k8s-agent \
          --field-selector=status.phase=Running \
          -o 'jsonpath={range .items[*]}{.spec.nodeName}{"\n"}{end}')
[ -n "$nodes" ] || fail "no Running agent pods; nothing materialises the domain"

# Each agent reports its node; the status is what a consumer such as an
# NMOS controller reads to find a domain it can publish.
deadline=$(( $(date +%s) + SYNC_TIMEOUT_SECS ))
for node in $nodes; do
  while :; do
    entry=$("${KUBECTL[@]}" get mxldomain "$DOMAIN_NAME" -o \
      "jsonpath={.status.nodes[?(@.nodeName==\"${node}\")].ready}/{.status.nodes[?(@.nodeName==\"${node}\")].mirrored}")
    [ "$entry" = "true/true" ] && break
    [ "$(date +%s)" -lt "$deadline" ] \
      || fail "${node} did not report ${DOMAIN_NAME} ready and mirrored (got '${entry}')"
    sleep 2
  done
done

for node in $nodes; do
  pod="${POD_PREFIX}-${node}"
  echo "-> ${node}"
  "${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  labels:
    app.kubernetes.io/name: ${POD_PREFIX}
spec:
  nodeName: ${node}
  restartPolicy: Never
  securityContext:
    runAsUser: 65534
  containers:
    - name: read
      image: ${CHECK_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c", "cat /domain/domain_def.json"]
      volumeMounts:
        - name: domain
          mountPath: /domain
          readOnly: true
  volumes:
    - name: domain
      hostPath:
        path: ${DOMAIN_DIR}
        type: Directory
EOF
  phase=$(wait_phase "pod/${pod}" '{.status.phase}' '^(Succeeded|Failed)$' \
            "$CHECK_TIMEOUT_SECS") \
    || fail "${pod} did not finish in ${CHECK_TIMEOUT_SECS}s"
  out=$("${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${pod}" 2>&1 || true)
  [ "$phase" = "Succeeded" ] \
    || fail "an unprivileged reader on ${node} cannot read ${DOMAIN_DIR}/domain_def.json: ${out}"

  # All four keys the schema requires, and the MxlDomain's id.
  for key in id label description tags; do
    echo "$out" | grep -q "\"${key}\"" || fail "domain_def.json on ${node} lacks ${key}: ${out}"
  done
  echo "$out" | grep -q "\"id\": \"${id}\"" \
    || fail "domain_def.json on ${node} does not carry ${id}: ${out}"
  echo "   domain_def.json carries ${id}"
done
