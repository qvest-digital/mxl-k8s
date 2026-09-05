#!/usr/bin/env bash
# Run mxl-flow-probe against a flow on a specific node.
#
#   hack/flow-probe.sh <node> <flow-uuid> [probe flags...]
#   hack/flow-probe.sh n07 aea7b9e9-... -duration 30s
#   hack/flow-probe.sh n06 aea7b9e9-... -json
#
# The probe needs libmxl, so it runs inside a scratch pod built from the
# go-mxl runtime image with the node's MXL domain mounted. The pod is
# created once per node and reused; it carries no state and is safe to
# delete at any time. Set KEEP=0 to remove it when the probe finishes.
#
# The binary is compiled locally in the go-mxl builder image and piped
# into the pod, so this works against any cluster without publishing an
# image first.
#
# Exit status is the probe's: 0 only for a SMOOTH verdict.
set -euo pipefail

NS=${NS:-mxl-system}
KEEP=${KEEP:-1}
GO_MXL_TAG=${GO_MXL_TAG:-1.1.0-rc.2}
BUILDER=ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG}
RUNTIME=ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}
DOMAIN=${DOMAIN:-/run/mxl/domain}

if [ $# -lt 2 ]; then
    sed -n '2,18p' "$0" >&2
    exit 2
fi
node=$1
flow=$2
shift 2

repo=$(cd "$(dirname "$0")/.." && pwd)
bin=${BIN:-$repo/.probe-bin/mxl-flow-probe}

if [ ! -x "$bin" ]; then
    echo "==> building mxl-flow-probe" >&2
    mkdir -p "$(dirname "$bin")"
    docker run --rm \
        -v "$repo":/w -v "$(dirname "$bin")":/out \
        -w /w/gateway -e GOWORK=off -e GOFLAGS=-buildvcs=false \
        "$BUILDER" \
        sh -c 'go build -trimpath -o /out/mxl-flow-probe ./cmd/mxl-flow-probe' >&2
    chmod 0755 "$bin"
fi

pod=mxl-flow-probe-$node
if ! kubectl -n "$NS" get pod "$pod" >/dev/null 2>&1; then
    echo "==> creating $pod on $node" >&2
    kubectl -n "$NS" apply -f - >&2 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  labels: {app.kubernetes.io/name: mxl-flow-probe}
spec:
  nodeName: $node
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  containers:
  - name: probe
    image: $RUNTIME
    command: ["sleep", "infinity"]
    securityContext:
      runAsNonRoot: false
      allowPrivilegeEscalation: false
      capabilities: {drop: ["ALL"], add: ["DAC_OVERRIDE", "FOWNER"]}
    resources:
      requests: {cpu: 100m, memory: 64Mi}
    volumeMounts:
    - {name: mxl-domain, mountPath: $DOMAIN}
    - {name: work, mountPath: /work}
  volumes:
  - name: mxl-domain
    hostPath: {path: $DOMAIN, type: DirectoryOrCreate}
  - name: work
    emptyDir: {}
EOF
    kubectl -n "$NS" wait --for=condition=Ready "pod/$pod" --timeout=120s >&2
fi

# Upload once; the emptyDir keeps it for the pod's lifetime.
if ! kubectl -n "$NS" exec "$pod" -- test -x /work/mxl-flow-probe 2>/dev/null; then
    kubectl -n "$NS" exec -i "$pod" -- \
        sh -c 'cat > /work/mxl-flow-probe && chmod 0755 /work/mxl-flow-probe' < "$bin"
fi

set +e
kubectl -n "$NS" exec "$pod" -- \
    /work/mxl-flow-probe -domain "$DOMAIN" -flow "$flow" -label "$node" "$@"
rc=$?
set -e

[ "$KEEP" = "0" ] && kubectl -n "$NS" delete pod "$pod" --wait=false >/dev/null 2>&1
exit $rc
