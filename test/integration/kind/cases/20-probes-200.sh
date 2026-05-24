#!/usr/bin/env bash
# Assert /healthz and /readyz on each control-plane component's probe
# container port (8081) answer HTTP 200. Uses kubectl port-forward
# so no curl-capable image needs to live inside the cluster.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

# Parallel arrays: workload label / probe port. Kept index-aligned
# so bash 3.2 (no associative arrays) can iterate them.
WORKLOADS=(
  mxl-k8s-operator
  mxl-k8s-agent
  mxl-k8s-gateway
)
PORTS=(
  8081
  8081
  8081
)

probe_failed=0
i=0
while [ "$i" -lt "${#WORKLOADS[@]}" ]; do
  label="${WORKLOADS[$i]}"
  port="${PORTS[$i]}"
  i=$(( i + 1 ))

  pod=$(resolve_pod "$label") || fail "no Running pod found for app.kubernetes.io/name=${label}"
  echo "-> ${label} (pod/${pod}) :${port}"

  for endpoint in healthz readyz; do
    code=$(port_forward_probe "$pod" "$port" "$endpoint" || true)
    if [ "$code" = "200" ]; then
      echo "   /${endpoint}: 200"
    else
      echo "   /${endpoint}: HTTP ${code} (expected 200)" >&2
      probe_failed=1
    fi
  done
done

[ "$probe_failed" -eq 0 ] || fail "one or more probe endpoints did not return 200"
