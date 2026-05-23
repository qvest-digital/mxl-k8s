#!/usr/bin/env bash
# kind-up.sh -- bring up a KIND cluster, build & load the mxl-k8s
# images, apply examples/tcp-demo, and wait for the MxlFlowMirror
# to reach Ready.
#
# Idempotent: re-running the script reuses an existing cluster,
# rebuilds the images (Docker caching keeps unchanged layers fast),
# reloads them, and re-applies the demo. Pair with `hack/kind-down.sh`
# to start clean.
#
# Requires: docker or podman, kind >= 0.20, kubectl, a Linux kernel
# >= 5.17 on the host (KIND nodes share it; the agent's fanotify needs
# FAN_REPORT_DFID_NAME).
#
# Set CONTAINER_RUNTIME=podman (or pass via the Makefile) to use
# Podman instead of Docker.
#
# Set BUILD=<tag> to skip the local image build and use CI-produced
# images instead. With BUILD unset or BUILD=local (the default) the
# script builds the five component images locally. With
# BUILD=<tag> the script pulls
# ghcr.io/<owner>/mxl-k8s/<component>:<tag> for every component,
# kind-loads it, and rewrites the demo manifests to reference it
# via a generated kustomize overlay.
#
# Set IMAGE_REGISTRY=<prefix> to override the registry prefix used
# in BUILD=<tag> mode. Default is ghcr.io/qvest-digital/mxl-k8s.

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-docker}"
BUILD="${BUILD-local}"
IMAGE_REGISTRY="${IMAGE_REGISTRY:-ghcr.io/qvest-digital/mxl-k8s}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../" && pwd)"
KIND_CONFIG="${REPO_ROOT}/hack/kind-config.yaml"
KUBECTL=(kubectl --context "kind-${CLUSTER_NAME}")

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
    ;;
  *)
    BUILD_MODE=tag
    BUILD_TAG="$BUILD"
    ;;
esac

# When using podman, tell KIND to use the podman provider.
if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
  export KIND_EXPERIMENTAL_PROVIDER=podman
fi

MIRROR_TIMEOUT_SECS="${MIRROR_TIMEOUT_SECS:-180}"
ROLLOUT_TIMEOUT_SECS="${ROLLOUT_TIMEOUT_SECS:-300}"

# Parallel arrays: Dockerfile / local dev tag / CI component name /
# image reference as it appears in examples/*-demo manifests. Kept
# index-aligned so bash 3.2 (no associative arrays) can iterate them.
IMAGE_DOCKERFILES=(
  docker/operator.Dockerfile
  docker/agent.Dockerfile
  docker/gateway.Dockerfile
  docker/shim.Dockerfile
  docker/demo-tools.Dockerfile
)
IMAGE_LOCAL_TAGS=(
  local/mxl-operator:dev
  local/mxl-domain-agent:dev
  local/mxl-fabrics-gateway:dev
  local/mxl-shim:dev
  local/mxl-demo-tools:dev
)
IMAGE_COMPONENTS=(
  operator
  agent
  gateway
  shim
  demo-tools
)

# Resolve the image-tag list this run actually loads into KIND.
# In local mode this is IMAGE_LOCAL_TAGS verbatim. In tag mode it
# becomes ghcr.io/<owner>/mxl-k8s/<component>:<BUILD_TAG>.
IMAGE_TAGS=()
if [[ "$BUILD_MODE" == "local" ]]; then
  for tag in "${IMAGE_LOCAL_TAGS[@]}"; do
    IMAGE_TAGS+=("$tag")
  done
else
  for comp in "${IMAGE_COMPONENTS[@]}"; do
    IMAGE_TAGS+=("${IMAGE_REGISTRY}/${comp}:${BUILD_TAG}")
  done
fi

log()  { printf '\n=== %s ===\n' "$*" >&2; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }; }

need "$CONTAINER_RUNTIME"
need kind
need kubectl

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

log "Loading images into the cluster"
for tag in "${IMAGE_TAGS[@]}"; do
  echo "  -> ${tag}"
  if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
    # Podman stores unqualified images under localhost/, but Kubernetes
    # resolves them as docker.io/. Re-tag so containerd inside the
    # KIND nodes finds them under the name the pods actually request.
    # Skip the docker.io re-tag for already-qualified references (CI
    # images already carry a registry component).
    case "$tag" in
      */*/*) canonical="$tag" ;;
      *)     canonical="docker.io/${tag}" ;;
    esac
    if [[ "$canonical" != "$tag" ]]; then
      $CONTAINER_RUNTIME tag "$tag" "$canonical" 2>/dev/null || true
    fi
    tmptar="$(mktemp "${TMPDIR:-/tmp}/kind-image-XXXXXX")"
    $CONTAINER_RUNTIME save -o "$tmptar" "$canonical"
    kind load image-archive --name "$CLUSTER_NAME" "$tmptar"
    rm -f "$tmptar"
  else
    kind load docker-image --name "$CLUSTER_NAME" "$tag"
  fi
done

# In BUILD=<tag> mode the demo manifests still reference local/...:dev.
# Render examples/tcp-demo once with kustomize, then rewrite each
# component image reference to the resolved CI image. The rendered
# manifest is applied below in place of the kustomization directory.
# In local mode the kustomization is applied directly.
RENDERED_MANIFEST=""
if [[ "$BUILD_MODE" == "tag" ]]; then
  RENDERED_MANIFEST="$(mktemp "${TMPDIR:-/tmp}/mxl-kind-demo-XXXXXX.yaml")"
  trap 'rm -f "$RENDERED_MANIFEST"' EXIT
  "${KUBECTL[@]}" kustomize "${REPO_ROOT}/examples/tcp-demo/" > "${RENDERED_MANIFEST}.in"
  # Stream-replace local/<name>:dev with the resolved CI reference.
  # IMAGE_LOCAL_TAGS[i] always maps to IMAGE_TAGS[i] by index.
  sed_args=()
  for i in "${!IMAGE_LOCAL_TAGS[@]}"; do
    src="${IMAGE_LOCAL_TAGS[$i]}"
    dst="${IMAGE_TAGS[$i]}"
    # Escape '/' and '&' for sed's replacement field.
    src_esc=$(printf '%s' "$src" | sed -e 's/[\/&]/\\&/g')
    dst_esc=$(printf '%s' "$dst" | sed -e 's/[\/&]/\\&/g')
    sed_args+=("-e" "s/${src_esc}/${dst_esc}/g")
  done
  sed "${sed_args[@]}" "${RENDERED_MANIFEST}.in" > "$RENDERED_MANIFEST"
  rm -f "${RENDERED_MANIFEST}.in"
  # Sanity check: every IMAGE_LOCAL_TAGS reference must be gone from
  # the rendered output. If one remains, the demo would silently pull
  # a local/...:dev image that isn't loaded.
  for src in "${IMAGE_LOCAL_TAGS[@]}"; do
    if grep -q "$src" "$RENDERED_MANIFEST"; then
      echo "ERROR: image ${src} still present in rendered demo manifest" >&2
      exit 1
    fi
  done
fi

apply_demo() {
  if [[ "$BUILD_MODE" == "tag" ]]; then
    "${KUBECTL[@]}" apply -f "$RENDERED_MANIFEST"
  else
    "${KUBECTL[@]}" apply -k "${REPO_ROOT}/examples/tcp-demo/"
  fi
}

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
apply_demo

# On re-runs the kubelet caches images by tag, so re-loading a :dev
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
  apply_demo
fi

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
