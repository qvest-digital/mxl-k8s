#!/usr/bin/env bash
# kind-up.sh -- bring up a KIND cluster, build & load the mxl-k8s
# images, install the mxl-k8s Helm chart, apply the demo workload
# from examples/kind/demo, and wait for the MxlFlowMirror to reach
# Ready.
#
# Idempotent: re-running the script reuses an existing cluster,
# rebuilds the images (Docker caching keeps unchanged layers fast),
# reloads them, and re-installs the chart. Pair with
# `hack/kind-down.sh` to start clean.
#
# Requires: docker or podman, kind >= 0.20, kubectl, a Linux kernel
# >= 5.17 on the host (KIND nodes share it; the agent's fanotify needs
# FAN_REPORT_DFID_NAME).
#
# Set CONTAINER_RUNTIME=podman (or pass via the Makefile) to use
# Podman instead of Docker.
<<<<<<< improvement-mxl-stability
=======
#
# Set BUILD=<tag> to skip the local image build and use CI-produced
# images instead. With BUILD unset or BUILD=local (the default) the
# script builds the five component images locally as
# ${IMAGE_REGISTRY}/<component>:dev. With BUILD=<tag> the script
# pulls ${IMAGE_REGISTRY}/<component>:<tag> for every component and
# kind-loads it. Local and tag modes produce identically-shaped
# refs; only the tag differs.
#
# Set IMAGE_REGISTRY=<prefix> to override the registry prefix.
# Default is ghcr.io/qvest-digital/mxl-k8s.
>>>>>>> main

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-docker}"
<<<<<<< improvement-mxl-stability
=======
BUILD="${BUILD-local}"
IMAGE_REGISTRY="${IMAGE_REGISTRY:-ghcr.io/qvest-digital/mxl-k8s}"
>>>>>>> main
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../" && pwd)"
KIND_CONFIG="${REPO_ROOT}/hack/kind-config.yaml"
KUBECTL=(kubectl --context "kind-${CLUSTER_NAME}")

<<<<<<< improvement-mxl-stability
=======
# Validate BUILD before any side effects. Empty string is rejected;
# "local" enables the local-build path; anything else is treated as
# a CI image tag.
case "$BUILD" in
  "")
    echo "ERROR: BUILD must be 'local' or a non-empty image tag" >&2
    exit 2
    ;;
  local)
    BUILD_MODE=local
    TAG=dev
    ;;
  *)
    BUILD_MODE=tag
    BUILD_TAG="$BUILD"
    TAG="$BUILD_TAG"
    ;;
esac

>>>>>>> main
# When using podman, tell KIND to use the podman provider.
if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
  export KIND_EXPERIMENTAL_PROVIDER=podman
fi

MIRROR_TIMEOUT_SECS="${MIRROR_TIMEOUT_SECS:-180}"
ROLLOUT_TIMEOUT_SECS="${ROLLOUT_TIMEOUT_SECS:-300}"

# Parallel arrays: Dockerfile / CI component name. Kept index-aligned
# so bash 3.2 (no associative arrays) can iterate them. The image
# reference for each component is always ${IMAGE_REGISTRY}/<comp>:${TAG}.
IMAGE_DOCKERFILES=(
  docker/operator.Dockerfile
  docker/agent.Dockerfile
  docker/gateway.Dockerfile
  docker/shim.Dockerfile
  docker/demo-tools.Dockerfile
)
IMAGE_COMPONENTS=(
  operator
  agent
  gateway
  shim
  demo-tools
)

IMAGE_TAGS=()
for comp in "${IMAGE_COMPONENTS[@]}"; do
  IMAGE_TAGS+=("${IMAGE_REGISTRY}/${comp}:${TAG}")
done

log()  { printf '\n=== %s ===\n' "$*" >&2; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }; }

need "$CONTAINER_RUNTIME"
need kind
need kubectl
need helm

# Podman preflight: KIND requires a rootful machine with enough RAM.
if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
  if podman machine inspect 2>/dev/null | grep -q '"Rootful": false'; then
    echo "ERROR: podman machine is not rootful. KIND requires rootful mode." >&2
    echo "  Fix:  podman machine stop && podman machine set --rootful && podman machine start" >&2
    exit 1
  fi
  mem=$(podman machine inspect 2>/dev/null | sed -n 's/.*"Memory": \([0-9]*\).*/\1/p' || true)
  if [[ -n "$mem" ]] && (( mem < 4096 )); then
    echo "WARNING: podman machine has ${mem} MB RAM; 4096+ MB recommended for a 3-node KIND cluster." >&2
    echo "  Fix:  podman machine stop && podman machine set --memory 4096 && podman machine start" >&2
  fi
fi
<<<<<<< improvement-mxl-stability

log "Building images"
cd "$REPO_ROOT"
for i in "${!IMAGE_DOCKERFILES[@]}"; do
  dockerfile="${IMAGE_DOCKERFILES[$i]}"
  tag="${IMAGE_TAGS[$i]}"
  echo "  -> ${tag}"
  $CONTAINER_RUNTIME build -q -f "${dockerfile}" -t "${tag}" . > /dev/null
done

=======

if [[ "$BUILD_MODE" == "local" ]]; then
  log "Building images"
  cd "$REPO_ROOT"
  for i in "${!IMAGE_DOCKERFILES[@]}"; do
    dockerfile="${IMAGE_DOCKERFILES[$i]}"
    tag="${IMAGE_TAGS[$i]}"
    echo "  -> ${tag}"
    $CONTAINER_RUNTIME build -q -f "${dockerfile}" -t "${tag}" . > /dev/null
  done
else
  log "Pulling CI images (BUILD=${BUILD_TAG})"
  cd "$REPO_ROOT"
  for tag in "${IMAGE_TAGS[@]}"; do
    echo "  -> ${tag}"
    $CONTAINER_RUNTIME pull "${tag}"
  done
fi

>>>>>>> main
CLUSTER_REUSED=false
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  # Verify the existing cluster's nodes are actually running.
  if ! $CONTAINER_RUNTIME ps --filter "label=io.x-k8s.kind.cluster=$CLUSTER_NAME" --format '{{.Status}}' 2>/dev/null | grep -qi "up\|running"; then
    log "KIND cluster ${CLUSTER_NAME} exists but nodes are not running; recreating"
    kind delete cluster --name "$CLUSTER_NAME"
    kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 60s
  else
    log "KIND cluster ${CLUSTER_NAME} already exists; reusing"
    CLUSTER_REUSED=true
  fi
else
  log "Creating KIND cluster ${CLUSTER_NAME}"
  kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 60s
fi

load_image() {
  local tag="$1"
  if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
    # Podman stores unqualified images under localhost/, but Kubernetes
    # resolves them as docker.io/. Re-tag so containerd inside the
    # KIND nodes finds them under the name the pods actually request.
    # Skip the docker.io re-tag for already-qualified references (CI
    # images already carry a registry component).
    local canonical
    case "$tag" in
      */*/*) canonical="$tag" ;;
      *)     canonical="docker.io/${tag}" ;;
    esac
    if [[ "$canonical" != "$tag" ]]; then
      $CONTAINER_RUNTIME tag "$tag" "$canonical" 2>/dev/null || true
    fi
    local tmptar
    tmptar="$(mktemp "${TMPDIR:-/tmp}/kind-image-XXXXXX")"
    $CONTAINER_RUNTIME save -o "$tmptar" "$canonical"
    kind load image-archive --name "$CLUSTER_NAME" "$tmptar"
    rm -f "$tmptar"
  else
    kind load docker-image --name "$CLUSTER_NAME" "$tag"
  fi
}

log "Loading images into the cluster"
for tag in "${IMAGE_TAGS[@]}"; do
  echo "  -> ${tag}"
<<<<<<< improvement-mxl-stability
  if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
    # Podman stores unqualified images under localhost/, but Kubernetes
    # resolves them as docker.io/. Re-tag so containerd inside the
    # KIND nodes finds them under the name the pods actually request.
    canonical="docker.io/${tag}"
    $CONTAINER_RUNTIME tag "$tag" "$canonical" 2>/dev/null || true
    tmptar="$(mktemp "${TMPDIR:-/tmp}/kind-image-XXXXXX")"
    $CONTAINER_RUNTIME save -o "$tmptar" "$canonical"
    kind load image-archive --name "$CLUSTER_NAME" "$tmptar"
    rm -f "$tmptar"
  else
    kind load docker-image --name "$CLUSTER_NAME" "$tag"
  fi
=======
  load_image "$tag"
>>>>>>> main
done

# The demo workload (writer/reader) and the shim init-container in
# examples/tcp-demo/ pin shim and demo-tools to :dev. In local mode
# that matches what we just loaded; in tag mode we also need a :dev
# alias so the demo pods schedule against the freshly-loaded image.
if [[ "$TAG" != "dev" ]]; then
  log "Aliasing demo images to :dev"
  for comp in shim demo-tools; do
    src="${IMAGE_REGISTRY}/${comp}:${TAG}"
    dst="${IMAGE_REGISTRY}/${comp}:dev"
    echo "  -> ${dst}"
    $CONTAINER_RUNTIME tag "$src" "$dst"
    load_image "$dst"
  done
fi

log "Installing the mxl-k8s Helm chart"
# Helm 3 installs charts/mxl-k8s/crds/ on first install automatically
# and leaves them in place on upgrades. No separate kubectl apply -k
# config/crd/ pass is needed.
helm upgrade --install mxl-k8s "${REPO_ROOT}/charts/mxl-k8s" \
  --kube-context "kind-${CLUSTER_NAME}" \
  --namespace mxl-system --create-namespace \
  -f "${REPO_ROOT}/examples/kind/values.yaml" \
  --set operator.image.tag="$TAG" \
  --set agent.image.tag="$TAG" \
  --set gateway.image.tag="$TAG" \
  --wait --timeout="${ROLLOUT_TIMEOUT_SECS}s"

apply_demo() {
  # --load-restrictor=LoadRestrictionsNone lets the overlay reference
  # the writer/receiver/reader manifests in ../../tcp-demo without
  # duplicating them under examples/kind/demo/.
  "${KUBECTL[@]}" kustomize --load-restrictor=LoadRestrictionsNone \
      "${REPO_ROOT}/examples/kind/demo/" \
    | "${KUBECTL[@]}" apply -f -
}

log "Waiting for CRDs to establish"
"${KUBECTL[@]}" wait --for=condition=Established --timeout=60s crd \
  mxldomains.mxl.qvest-digital.com \
  mxlflows.mxl.qvest-digital.com \
  mxlflowmirrors.mxl.qvest-digital.com \
  mxlreceivers.mxl.qvest-digital.com \
  mxlnodecapabilities.mxl.qvest-digital.com

log "Applying the demo workload"
apply_demo

# On re-runs the kubelet caches images by tag, so re-loading a :dev
<<<<<<< improvement-mxl-stability
# image doesn't get picked up by existing pods. Force a rollout
# restart and replace bare demo pods so everything runs against the
# freshly-loaded images. Skip this on a brand-new cluster where the
# first apply already schedules the correct images.
if [[ "$CLUSTER_REUSED" == "true" ]]; then
  if "${KUBECTL[@]}" -n mxl-system get deploy/mxl-operator >/dev/null 2>&1; then
    log "Rolling out latest images"
    "${KUBECTL[@]}" -n mxl-system rollout restart deploy/mxl-operator ds/mxl-domain-agent ds/mxl-fabrics-gateway || true
  fi
  # Wait for the deletes to complete -- re-applying while a pod is
  # still Terminating leaves the new pod in limbo (apply observes
  # the live object and treats it as a no-op).
  "${KUBECTL[@]}" -n mxl-system delete pod mxl-tcp-demo-writer mxl-tcp-demo-reader --ignore-not-found --force --grace-period=0
  "${KUBECTL[@]}" apply -k "${REPO_ROOT}/examples/tcp-demo/"
fi

log "Waiting for control-plane workloads (timeout ${ROLLOUT_TIMEOUT_SECS}s)"
"${KUBECTL[@]}" -n mxl-system rollout status deploy/mxl-operator         --timeout="${ROLLOUT_TIMEOUT_SECS}s"
"${KUBECTL[@]}" -n mxl-system rollout status ds/mxl-domain-agent         --timeout="${ROLLOUT_TIMEOUT_SECS}s"
"${KUBECTL[@]}" -n mxl-system rollout status ds/mxl-fabrics-gateway      --timeout="${ROLLOUT_TIMEOUT_SECS}s"
=======
# image doesn't get picked up by existing demo pods. Force the bare
# pods to be recreated so they pick up the freshly-loaded images.
# The chart's --wait above already covers the operator/agent/gateway
# rollout on re-runs.
if [[ "$CLUSTER_REUSED" == "true" ]]; then
  # Wait for the deletes to complete -- re-applying while a pod is
  # still Terminating leaves the new pod in limbo (apply observes
  # the live object and treats it as a no-op).
  "${KUBECTL[@]}" -n mxl-system delete pod \
    mxl-tcp-demo-writer mxl-tcp-demo-reader \
    mxl-tcp-demo-audio-writer mxl-tcp-demo-audio-reader \
    --ignore-not-found --force --grace-period=0
  apply_demo
fi
>>>>>>> main

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
  "${KUBECTL[@]}" -n mxl-system logs ds/mxl-k8s-gateway --tail=80 || true
  exit 1
fi

cat <<EOF

KIND cluster '${CLUSTER_NAME}' is up and the demo is converged.

  Status:    make kind-status
  Logs:      kubectl --context kind-${CLUSTER_NAME} -n mxl-system logs ds/mxl-k8s-gateway
  Reader:    kubectl --context kind-${CLUSTER_NAME} -n mxl-system logs pod/mxl-tcp-demo-reader
  Tear down: make kind-down
EOF
