#!/usr/bin/env bash
# kind-up.sh — bring up a KIND cluster, build & load the mxl-k8s
# images, apply examples/tcp-demo, and wait for the MxlFlowMirror
# to reach Ready.
#
# Idempotent: re-running the script reuses an existing cluster,
# rebuilds the images (Docker caching keeps unchanged layers fast),
# reloads them, and re-applies the demo. Pair with `hack/kind-down.sh`
# to start clean.
#
# Requires: docker, kind ≥ 0.20, kubectl, a Linux kernel ≥ 5.17 on
# the host (KIND nodes share it; the agent's fanotify needs
# FAN_REPORT_DFID_NAME).

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KIND_CONFIG="${REPO_ROOT}/hack/kind-config.yaml"
KUBECTL=(kubectl --context "kind-${CLUSTER_NAME}")

MIRROR_TIMEOUT_SECS="${MIRROR_TIMEOUT_SECS:-180}"
ROLLOUT_TIMEOUT_SECS="${ROLLOUT_TIMEOUT_SECS:-180}"

declare -A IMAGES=(
  [docker/operator.Dockerfile]=local/mxl-operator:dev
  [docker/agent.Dockerfile]=local/mxl-domain-agent:dev
  [docker/gateway.Dockerfile]=local/mxl-fabrics-gateway:dev
  [docker/shim.Dockerfile]=local/mxl-shim:dev
  [docker/demo-tools.Dockerfile]=local/mxl-demo-tools:dev
)

log()  { printf '\n=== %s ===\n' "$*" >&2; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }; }

need docker
need kind
need kubectl

log "Building images"
cd "$REPO_ROOT"
for dockerfile in "${!IMAGES[@]}"; do
  tag="${IMAGES[$dockerfile]}"
  echo "  -> ${tag}"
  docker build -q -f "${dockerfile}" -t "${tag}" . > /dev/null
done

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  log "Creating KIND cluster ${CLUSTER_NAME}"
  kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 60s
else
  log "KIND cluster ${CLUSTER_NAME} already exists; reusing"
fi

log "Loading images into the cluster"
for tag in "${IMAGES[@]}"; do
  echo "  -> ${tag}"
  kind load docker-image --name "$CLUSTER_NAME" "$tag"
done

log "Installing CRDs"
# Apply the CRDs in their own pass first so the demo's resources
# (MxlReceiver / MxlFlow / etc.) can be discovered when the next
# apply hits them. kubectl's discovery cache won't refresh inside
# a single apply.
"${KUBECTL[@]}" apply -k "${REPO_ROOT}/config/crd/"
"${KUBECTL[@]}" wait --for=condition=Established --timeout=60s crd \
  mxldomains.mxl.qvest-digital.com \
  mxlflows.mxl.qvest-digital.com \
  mxlflowmirrors.mxl.qvest-digital.com \
  mxlreceivers.mxl.qvest-digital.com \
  mxlnodecapabilities.mxl.qvest-digital.com

log "Applying examples/tcp-demo"
"${KUBECTL[@]}" apply -k "${REPO_ROOT}/examples/tcp-demo/"

# kubelet caches images by tag, so re-loading a :dev image doesn't
# get picked up by existing pods. Force a rollout restart of the
# DaemonSets / Deployment, and replace the bare demo Pods so the
# whole demo runs against the freshly-loaded images.
if "${KUBECTL[@]}" -n mxl-system get deploy/mxl-operator >/dev/null 2>&1; then
  log "Rolling out latest images"
  "${KUBECTL[@]}" -n mxl-system rollout restart deploy/mxl-operator ds/mxl-domain-agent ds/mxl-fabrics-gateway || true
fi
# Wait for the deletes to complete — re-applying while a pod is
# still Terminating leaves the new pod in limbo (apply observes
# the live object and treats it as a no-op).
"${KUBECTL[@]}" -n mxl-system delete pod mxl-tcp-demo-writer mxl-tcp-demo-reader --ignore-not-found
"${KUBECTL[@]}" apply -k "${REPO_ROOT}/examples/tcp-demo/"

log "Waiting for control-plane workloads (timeout ${ROLLOUT_TIMEOUT_SECS}s)"
"${KUBECTL[@]}" -n mxl-system rollout status deploy/mxl-operator         --timeout="${ROLLOUT_TIMEOUT_SECS}s"
"${KUBECTL[@]}" -n mxl-system rollout status ds/mxl-domain-agent         --timeout="${ROLLOUT_TIMEOUT_SECS}s"
"${KUBECTL[@]}" -n mxl-system rollout status ds/mxl-fabrics-gateway      --timeout="${ROLLOUT_TIMEOUT_SECS}s"

log "Waiting for MxlFlowMirror to reach Ready (timeout ${MIRROR_TIMEOUT_SECS}s)"
deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
phase=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  phase=$("${KUBECTL[@]}" -n mxl-system get mxlflowmirrors \
      -o jsonpath='{range .items[*]}{.metadata.name}={.status.phase} {end}' 2>/dev/null || true)
  if [[ "$phase" == *=Ready* ]]; then
    log "Mirror Ready: ${phase}"
    break
  fi
  sleep 2
done

if [[ "$phase" != *=Ready* ]]; then
  log "Mirror did not reach Ready in time."
  echo "Current state:"
  "${KUBECTL[@]}" -n mxl-system get mxlflowmirrors -o wide || true
  "${KUBECTL[@]}" -n mxl-system describe mxlflowmirrors || true
  echo
  echo "Recent gateway logs:"
  "${KUBECTL[@]}" -n mxl-system logs ds/mxl-fabrics-gateway --tail=80 || true
  exit 1
fi

cat <<EOF

KIND cluster '${CLUSTER_NAME}' is up and the demo is converged.

  Status:    make kind-status
  Logs:      kubectl --context kind-${CLUSTER_NAME} -n mxl-system logs ds/mxl-fabrics-gateway
  Reader:    kubectl --context kind-${CLUSTER_NAME} -n mxl-system logs pod/mxl-tcp-demo-reader
  Tear down: make kind-down
EOF
