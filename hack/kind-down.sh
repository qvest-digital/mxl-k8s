#!/usr/bin/env bash
# kind-down.sh -- delete the demo KIND cluster.

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-docker}"

if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
  export KIND_EXPERIMENTAL_PROVIDER=podman
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "Deleting KIND cluster ${CLUSTER_NAME}"
  kind delete cluster --name "$CLUSTER_NAME"
else
  echo "KIND cluster ${CLUSTER_NAME} not present; nothing to do"
fi

# The second fabric network kind-up.sh attaches the nodes to outlives
# the cluster otherwise.
fabric_net="${CLUSTER_NAME}-fabric-b"
if $CONTAINER_RUNTIME network inspect "$fabric_net" >/dev/null 2>&1; then
  echo "Removing network ${fabric_net}"
  $CONTAINER_RUNTIME network rm "$fabric_net" >/dev/null
fi
