#!/usr/bin/env bash
# Assert the MxlFlow follows a producer that comes back in another format.
#
# A writer publishes a flow at 1280x720, 25/1 and exits; libmxl removes
# the flow directory with the last writer. A writer with the same flow id
# at 1920x1080, 50/1 then publishes a new directory under the same name.
# The MxlFlow outlived the first directory, so it has to take the new
# definition rather than keep describing a format nothing produces.
set -euo pipefail
. "$KIND_TEST_LIB"
need python3

FLOW="59595959-0000-4000-8000-000000000001"
POD="mxl-format-change-writer"
CM="mxl-format-change-defs"
BASE="${PWD}/examples/tcp-demo/flow-video-v210.json"
TIMEOUT="${FORMAT_CHANGE_TIMEOUT_SECS:-60}"

cleanup() {
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1 || true
  "${KUBECTL[@]}" delete mxlflow "$FLOW" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# flow_def <width> <height> <rate> -- the demo video definition under
# this case's flow id, in another geometry and rate.
flow_def() {
  python3 - "$BASE" "$FLOW" "$@" <<'PY'
import json, sys
base, flow, w, h, rate = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
d = json.load(open(base))
d.update(id=flow, frame_width=w, frame_height=h, grain_rate={"numerator": rate, "denominator": 1})
for c in d["components"]:
    c.update(width=w if c["name"] == "Y" else w // 2, height=h)
print(json.dumps(d))
PY
}

# writer <definition file> [node] -- the demo writer on this case's flow.
writer() {
  local node_line=""
  [ -n "${2:-}" ] && node_line="  nodeName: $2"
  "${KUBECTL[@]}" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NAMESPACE}
spec:
${node_line}
  initContainers:
    - name: install-intent-shim
      image: ghcr.io/qvest-digital/mxl-k8s/shim:dev
      imagePullPolicy: IfNotPresent
      volumeMounts:
        - { name: intent-shim, mountPath: /shared }
  containers:
    - name: writer
      image: ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev
      imagePullPolicy: IfNotPresent
      command: ["/usr/local/bin/write-grain"]
      args: ["-domain", "/run/mxl/domain", "-flow-def", "/etc/mxl-format/$1"]
      env:
        - { name: LD_PRELOAD, value: /opt/mxl-intent/libmxl-intent.so }
      securityContext:
        capabilities: { add: ["IPC_LOCK", "SYS_RESOURCE"] }
      volumeMounts:
        - { name: mxl-run, mountPath: /run/mxl }
        - { name: defs, mountPath: /etc/mxl-format, readOnly: true }
        - { name: intent-shim, mountPath: /opt/mxl-intent, readOnly: true }
  volumes:
    - name: mxl-run
      hostPath: { path: /run/mxl, type: DirectoryOrCreate }
    - name: defs
      configMap: { name: ${CM} }
    - name: intent-shim
      emptyDir: {}
EOF
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/${POD}" \
      --timeout="${ROLLOUT_TIMEOUT_SECS:-120}s" >/dev/null \
    || fail "${POD} ($1) did not become Ready"
}

# geometry -- "<width>x<height>@<rate numerator>" the MxlFlow describes.
geometry() {
  "${KUBECTL[@]}" get mxlflow "$FLOW" -o \
    'jsonpath={.spec.definition.frame_width}x{.spec.definition.frame_height}@{.spec.definition.grain_rate.numerator}' \
    2>/dev/null || true
}

wait_geometry() {
  local want="$1" got="" deadline=$(( $(date +%s) + TIMEOUT ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    got=$(geometry)
    [ "$got" = "$want" ] && { echo "$got"; return 0; }
    sleep 2
  done
  echo "$got"
  return 1
}

"${KUBECTL[@]}" -n "$NAMESPACE" create configmap "$CM" \
    --from-literal=a.json="$(flow_def 1280 720 25)" \
    --from-literal=b.json="$(flow_def 1920 1080 50)" >/dev/null \
  || fail "create configmap ${CM}"

writer a.json
node=$("${KUBECTL[@]}" -n "$NAMESPACE" get "pod/${POD}" -o jsonpath='{.spec.nodeName}')
got=$(wait_geometry "1280x720@25") \
  || fail "MxlFlow ${FLOW} never described the first producer (last=${got})"
echo "  first producer on ${node}: MxlFlow ${got}"

# A graceful exit lets the last writer remove the flow directory.
"${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${POD}" --wait=true >/dev/null \
  || fail "delete ${POD}"
"${KUBECTL[@]}" get mxlflow "$FLOW" >/dev/null 2>&1 \
  || fail "MxlFlow ${FLOW} is gone after the writer left; the case needs it to outlive the directory"

writer b.json "$node"
got=$(wait_geometry "1920x1080@50") \
  || fail "MxlFlow ${FLOW} still describes ${got} after the producer came back at 1920x1080@50"
echo "  second producer on ${node}: MxlFlow ${got}"
