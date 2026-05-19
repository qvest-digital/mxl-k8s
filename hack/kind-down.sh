#!/usr/bin/env bash
# kind-down.sh -- delete the demo KIND cluster.

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "Deleting KIND cluster ${CLUSTER_NAME}"
  kind delete cluster --name "$CLUSTER_NAME"
else
  echo "KIND cluster ${CLUSTER_NAME} not present; nothing to do"
fi
