#!/usr/bin/env bash
# kind-status.sh — quick at-a-glance view of the demo's state.

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"
KUBECTL=(kubectl --context "kind-${CLUSTER_NAME}")

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "KIND cluster ${CLUSTER_NAME} not present. Run: make kind-up"
  exit 1
fi

printf '\n=== Nodes ===\n'
"${KUBECTL[@]}" get nodes

printf '\n=== Pods ===\n'
"${KUBECTL[@]}" -n mxl-system get pods -o wide

printf '\n=== Domains / Flows / Capabilities ===\n'
"${KUBECTL[@]}" -n mxl-system get mxldomains,mxlflows,mxlnodecapabilities -o wide

printf '\n=== Receivers / Mirrors ===\n'
"${KUBECTL[@]}" -n mxl-system get mxlreceivers,mxlflowmirrors -o wide
