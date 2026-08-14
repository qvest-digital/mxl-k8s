#!/usr/bin/env bash
# Assert the agent delivers libmxl-intent.so on every node it runs
# on, and that a consumer pod can preload the delivered copy. The
# copy is what a consumer that cannot template the carrier image's
# initContainer into its own pod spec preloads, so "the file is
# there" is not enough: it has to be readable by the consumer's uid
# and loadable by the dynamic linker. The check pod greps its own
# /proc/self/maps, which is only non-empty once ld.so has actually
# mapped the library.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

SHIM_PATH="${SHIM_PATH:-/run/mxl/libmxl-intent.so}"
CHECK_IMAGE="${CHECK_IMAGE:-ghcr.io/qvest-digital/mxl-k8s/demo-tools:dev}"
CHECK_TIMEOUT_SECS="${CHECK_TIMEOUT_SECS:-90}"
POD_PREFIX=mxl-shim-delivery-check

cleanup() {
  "${KUBECTL[@]}" -n "$NAMESPACE" delete pod -l "app.kubernetes.io/name=${POD_PREFIX}" \
      --wait=false --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# A leftover from an earlier run has to be gone before the same name
# is applied again: applying against a Terminating pod leaves the new
# one unscheduled.
"${KUBECTL[@]}" -n "$NAMESPACE" delete pod -l "app.kubernetes.io/name=${POD_PREFIX}" \
    --ignore-not-found --force --grace-period=0 >/dev/null 2>&1 || true

nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
          -l app.kubernetes.io/name=mxl-k8s-agent \
          --field-selector=status.phase=Running \
          -o 'jsonpath={range .items[*]}{.spec.nodeName}{"\n"}{end}')
[ -n "$nodes" ] || fail "no Running agent pods; nothing delivers the shim"

for node in $nodes; do
  pod="${POD_PREFIX}-${node}"
  echo "-> ${node}"

  "${KUBECTL[@]}" -n "$NAMESPACE" apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  labels:
    app.kubernetes.io/name: ${POD_PREFIX}
spec:
  nodeName: ${node}
  restartPolicy: Never
  containers:
    - name: preload
      image: ${CHECK_IMAGE}
      imagePullPolicy: IfNotPresent
      env:
        - name: LD_PRELOAD
          value: ${SHIM_PATH}
      command: ["/bin/sh", "-c"]
      args:
        - |
          set -e
          ls -l ${SHIM_PATH}
          grep -F $(basename "$SHIM_PATH") /proc/self/maps
      volumeMounts:
        - name: mxl-run
          mountPath: $(dirname "$SHIM_PATH")
  volumes:
    - name: mxl-run
      hostPath:
        path: $(dirname "$SHIM_PATH")
        type: Directory
EOF

  phase=$(wait_phase "pod/${pod}" '{.status.phase}' '^(Succeeded|Failed)$' \
            "$CHECK_TIMEOUT_SECS") \
    || fail "${pod} did not finish in ${CHECK_TIMEOUT_SECS}s"

  out=$("${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${pod}" 2>&1 || true)
  if [ "$phase" != "Succeeded" ]; then
    echo "$out" >&2
    fail "${pod} did not reach Succeeded (${phase}); ${SHIM_PATH} missing or not loadable on ${node}"
  fi

  echo "$out" | grep -q -- '-rw-r--r--' \
    || fail "${SHIM_PATH} on ${node} is not mode 0644: ${out}"
  echo "$out" | grep -q "r-xp .*$(basename "$SHIM_PATH")" \
    || fail "${SHIM_PATH} on ${node} was not mapped executable by ld.so: ${out}"

  echo "   preloaded from the node: $(echo "$out" | head -1)"
done
