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

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-docker}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../" && pwd)"
KIND_CONFIG="${REPO_ROOT}/hack/kind-config.yaml"
KUBECTL=(kubectl --context "kind-${CLUSTER_NAME}")

# When BUILD is set, use externally-built images under
# ${IMAGE_REPO}/<name>:${BUILD} instead of rebuilding `local/mxl-*:dev`
# locally. CI sets IMAGE_TARS=<dir> so each image loads from
# <dir>/kind-image-<name>.tar (no GHCR round-trip); without IMAGE_TARS
# the script falls back to pulling from the registry. The demo
# manifests still reference `local/mxl-*:dev` in tree; the BUILD path
# patches the live workloads with `kubectl set image` after apply.
BUILD="${BUILD:-}"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/qvest-digital/mxl-k8s}"
IMAGE_TARS="${IMAGE_TARS:-}"

# When using podman, tell KIND to use the podman provider.
if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
  export KIND_EXPERIMENTAL_PROVIDER=podman
fi

MIRROR_TIMEOUT_SECS="${MIRROR_TIMEOUT_SECS:-180}"
ROLLOUT_TIMEOUT_SECS="${ROLLOUT_TIMEOUT_SECS:-300}"

IMAGE_DOCKERFILES=(
  docker/operator.Dockerfile
  docker/agent.Dockerfile
  docker/gateway.Dockerfile
  docker/shim.Dockerfile
  docker/demo-tools.Dockerfile
)
IMAGE_TAGS=(
  local/mxl-operator:dev
  local/mxl-domain-agent:dev
  local/mxl-fabrics-gateway:dev
  local/mxl-shim:dev
  local/mxl-demo-tools:dev
)
# Parallel to IMAGE_TAGS. Names match the kind-image-<name>.tar
# artefacts the images.yml workflow uploads and the components in
# the meta script's `all` list.
IMAGE_CI_NAMES=(
  operator
  agent
  gateway
  shim
  demo-tools
)

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

log "Preparing images"
cd "$REPO_ROOT"
if [[ -z "$BUILD" ]]; then
  # Local developer path: build the `local/mxl-*:dev` set from the
  # working tree.
  for i in "${!IMAGE_DOCKERFILES[@]}"; do
    dockerfile="${IMAGE_DOCKERFILES[$i]}"
    tag="${IMAGE_TAGS[$i]}"
    echo "  -> ${tag} (building)"
    $CONTAINER_RUNTIME build -q -f "${dockerfile}" -t "${tag}" . > /dev/null
  done
else
  # CI path: every image already exists as ${IMAGE_REPO}/<name>:${BUILD}.
  # With IMAGE_TARS set, load each one from a `docker save` archive
  # (the artefact the images.yml build job uploaded); otherwise pull
  # the manifest list off the registry. The build manifest deliberately
  # mirrors the per-component naming the meta script in images.yml
  # uses (operator / agent / gateway / shim / demo-tools).
  for i in "${!IMAGE_CI_NAMES[@]}"; do
    name="${IMAGE_CI_NAMES[$i]}"
    ref="${IMAGE_REPO}/${name}:${BUILD}"
    if [[ -n "$IMAGE_TARS" ]]; then
      tar="${IMAGE_TARS}/kind-image-${name}.tar"
      if [[ ! -f "$tar" ]]; then
        echo "ERROR: BUILD=${BUILD} but ${tar} is missing." >&2
        exit 1
      fi
      echo "  -> ${ref} (loading ${tar})"
      $CONTAINER_RUNTIME load -i "$tar" >/dev/null
    else
      echo "  -> ${ref} (pulling)"
      $CONTAINER_RUNTIME pull "$ref" >/dev/null
    fi
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
if [[ -z "$BUILD" ]]; then
  REFS_TO_LOAD=("${IMAGE_TAGS[@]}")
else
  REFS_TO_LOAD=()
  for name in "${IMAGE_CI_NAMES[@]}"; do
    REFS_TO_LOAD+=("${IMAGE_REPO}/${name}:${BUILD}")
  done
fi
for ref in "${REFS_TO_LOAD[@]}"; do
  echo "  -> ${ref}"
  if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
    # Podman stores unqualified images under localhost/, but Kubernetes
    # resolves them as docker.io/. Re-tag so containerd inside the
    # KIND nodes finds them under the name the pods actually request.
    # ghcr.io/* refs are already fully qualified and skip this step.
    if [[ "$ref" != *"/"*"/"* ]]; then
      canonical="docker.io/${ref}"
      $CONTAINER_RUNTIME tag "$ref" "$canonical" 2>/dev/null || true
    else
      canonical="$ref"
    fi
    tmptar="$(mktemp "${TMPDIR:-/tmp}/kind-image-XXXXXX")"
    $CONTAINER_RUNTIME save -o "$tmptar" "$canonical"
    kind load image-archive --name "$CLUSTER_NAME" "$tmptar"
    rm -f "$tmptar"
  else
    kind load docker-image --name "$CLUSTER_NAME" "$ref"
  fi
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
# When BUILD is set, redirect the demo's image references via a
# throwaway kustomize overlay. kustomize's `images:` transformer
# rewrites every container that references `local/mxl-*:dev` to the
# externally-built ${IMAGE_REPO}/<name>:${BUILD} ref, including bare
# Pods (`kubectl set image` does not support `kind: Pod`). The overlay
# lives under the shell's temp dir so it never enters the repo.
APPLY_DIR="${REPO_ROOT}/examples/tcp-demo/"
DEMO_OVERLAY=""
if [[ -n "$BUILD" ]]; then
  DEMO_OVERLAY="$(mktemp -d "${TMPDIR:-/tmp}/kind-demo-overlay-XXXXXX")"
  # kustomize rejects absolute paths in `resources`; symlink the
  # in-tree base into the overlay so the relative path resolves.
  ln -s "${REPO_ROOT}/examples/tcp-demo" "${DEMO_OVERLAY}/base"
  cat > "${DEMO_OVERLAY}/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - base
images:
  - name: local/mxl-operator
    newName: ${IMAGE_REPO}/operator
    newTag: ${BUILD}
  - name: local/mxl-domain-agent
    newName: ${IMAGE_REPO}/agent
    newTag: ${BUILD}
  - name: local/mxl-fabrics-gateway
    newName: ${IMAGE_REPO}/gateway
    newTag: ${BUILD}
  - name: local/mxl-shim
    newName: ${IMAGE_REPO}/shim
    newTag: ${BUILD}
  - name: local/mxl-demo-tools
    newName: ${IMAGE_REPO}/demo-tools
    newTag: ${BUILD}
EOF
  APPLY_DIR="$DEMO_OVERLAY"
  log "Using BUILD overlay at ${DEMO_OVERLAY}"
fi
"${KUBECTL[@]}" apply -k "$APPLY_DIR"

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
  "${KUBECTL[@]}" apply -k "$APPLY_DIR"
fi

# Clean up the BUILD overlay temp dir if we created one.
if [[ -n "$DEMO_OVERLAY" ]]; then
  rm -rf "$DEMO_OVERLAY"
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
