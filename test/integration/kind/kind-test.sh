#!/usr/bin/env bash
# kind-test.sh -- integration test for an already-running mxl-k8s KIND
# cluster. Asserts the control-plane workloads are Ready, their
# /healthz and /readyz probe endpoints answer 200, and the demo's
# MxlFlowMirror has reached phase=Ready.
#
# Driven from `make kind-test` after `make kind` (or `make kind-up`)
# has converged the cluster. Designed to be invoked from the
# kind-integration GitHub Actions job; runs in well under 5 minutes
# against an already-converged cluster, and dumps diagnostics to
# ${KIND_DIAG_DIR:-./kind-diagnostics} on failure so the workflow can
# upload them as an artifact.
#
# Required tools on PATH: kubectl, kind, curl.

set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-mxl-k8s-demo}"
KUBECTL=(kubectl --context "kind-${CLUSTER_NAME}")
NAMESPACE="${MXL_NAMESPACE:-mxl-system}"
ROLLOUT_TIMEOUT_SECS="${ROLLOUT_TIMEOUT_SECS:-180}"
MIRROR_TIMEOUT_SECS="${MIRROR_TIMEOUT_SECS:-180}"
PROBE_TIMEOUT_SECS="${PROBE_TIMEOUT_SECS:-30}"
DIAG_DIR="${KIND_DIAG_DIR:-${PWD}/kind-diagnostics}"

# Component probe topology: each entry is "kind/name probe-port".
# probe port is the container's `name: probe` (8081) on every
# component, see examples/tcp-demo/{02-agent,03-gateway,04-operator}.yaml.
COMPONENTS=(
  "deploy/mxl-operator         8081"
  "ds/mxl-domain-agent         8081"
  "ds/mxl-fabrics-gateway      8081"
)

log()  { printf '\n=== %s ===\n' "$*" >&2; }
fail() { echo "FAIL: $*" >&2; collect_diagnostics; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"; }

collect_diagnostics() {
  mkdir -p "$DIAG_DIR"
  log "Collecting diagnostics into $DIAG_DIR"
  "${KUBECTL[@]}" get all -A -o wide                > "$DIAG_DIR/get-all.txt"        2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" describe pods     > "$DIAG_DIR/describe-pods.txt"  2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" get events --sort-by=.lastTimestamp \
                                                    > "$DIAG_DIR/events.txt"        2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" get mxldomains,mxlflows,mxlflowmirrors,mxlreceivers,mxlnodecapabilities -o yaml \
                                                    > "$DIAG_DIR/mxl-resources.yaml" 2>&1 || true
  for ws in mxl-operator mxl-domain-agent mxl-fabrics-gateway; do
    "${KUBECTL[@]}" -n "$NAMESPACE" logs "${ws#*/}" --all-containers --prefix --tail=400 -l app.kubernetes.io/name="$ws" \
                                                    > "$DIAG_DIR/${ws}.log"         2>&1 || true
  done
}

cleanup_pf() {
  # Kill any background port-forwarders we spawned. Trap is set per
  # forward so this only runs if one leaked.
  if [[ -n "${PF_PID:-}" ]] && kill -0 "$PF_PID" 2>/dev/null; then
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
  fi
}
trap cleanup_pf EXIT

need kubectl
need kind
need curl

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  fail "KIND cluster ${CLUSTER_NAME} not present (run 'make kind' first)"
fi

log "Waiting for CRDs"
for crd in \
    mxldomains.mxl.qvest-digital.com \
    mxlflows.mxl.qvest-digital.com \
    mxlflowmirrors.mxl.qvest-digital.com \
    mxlreceivers.mxl.qvest-digital.com \
    mxlnodecapabilities.mxl.qvest-digital.com; do
  "${KUBECTL[@]}" wait --for=condition=Established --timeout=60s "crd/$crd" \
    || fail "CRD $crd not Established"
done

log "Waiting for control-plane rollouts (timeout ${ROLLOUT_TIMEOUT_SECS}s)"
"${KUBECTL[@]}" -n "$NAMESPACE" rollout status deploy/mxl-operator         --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "deploy/mxl-operator rollout did not complete"
"${KUBECTL[@]}" -n "$NAMESPACE" rollout status ds/mxl-domain-agent         --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "ds/mxl-domain-agent rollout did not complete"
"${KUBECTL[@]}" -n "$NAMESPACE" rollout status ds/mxl-fabrics-gateway      --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "ds/mxl-fabrics-gateway rollout did not complete"

log "Waiting for control-plane pods Ready"
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready pod \
  -l 'app.kubernetes.io/name in (mxl-operator,mxl-domain-agent,mxl-fabrics-gateway)' \
  --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "control-plane pods did not all become Ready"

# Pick one pod per component and probe its /healthz and /readyz via
# `kubectl port-forward`. The probe ports are not Service-exposed --
# port-forward speaks directly to the pod, so a passing curl proves
# the endpoint answers from inside the cluster's pod network without
# requiring a curl-capable image inside the cluster.
probe_one() {
  local resource="$1" port="$2"
  local kind name pod local_port
  kind="${resource%%/*}"
  name="${resource##*/}"

  # Resolve to a concrete Ready pod.
  case "$kind" in
    deploy)
      pod=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
              -l "app.kubernetes.io/name=${name}" \
              --field-selector=status.phase=Running \
              -o jsonpath='{.items[0].metadata.name}')
      ;;
    ds)
      pod=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
              -l "app.kubernetes.io/name=${name}" \
              --field-selector=status.phase=Running \
              -o jsonpath='{.items[0].metadata.name}')
      ;;
    *) fail "unknown resource kind: $kind" ;;
  esac
  [[ -n "$pod" ]] || fail "no Running pod found for $resource"

  # Bind to a kernel-allocated free port (:0) and learn it from the
  # port-forward's stdout. Avoids races with other parallel jobs.
  local pf_log; pf_log="$(mktemp)"
  "${KUBECTL[@]}" -n "$NAMESPACE" port-forward "pod/${pod}" "0:${port}" \
      > "$pf_log" 2>&1 &
  PF_PID=$!

  local deadline=$(( $(date +%s) + PROBE_TIMEOUT_SECS ))
  while [[ "$(date +%s)" -lt "$deadline" ]]; do
    local_port=$(sed -n 's/.*Forwarding from 127\.0\.0\.1:\([0-9]*\) ->.*/\1/p' "$pf_log" | head -1)
    [[ -n "$local_port" ]] && break
    sleep 0.2
  done
  if [[ -z "$local_port" ]]; then
    cat "$pf_log" >&2
    kill "$PF_PID" 2>/dev/null || true
    rm -f "$pf_log"
    fail "port-forward to $pod did not start"
  fi

  local rc=0
  for endpoint in healthz readyz; do
    local code
    code=$(curl -sS -o /dev/null -w '%{http_code}' \
                --max-time 5 "http://127.0.0.1:${local_port}/${endpoint}" || echo "000")
    if [[ "$code" != "200" ]]; then
      echo "  $pod /${endpoint}: HTTP $code (expected 200)" >&2
      rc=1
    else
      echo "  $pod /${endpoint}: 200" >&2
    fi
  done

  kill "$PF_PID" 2>/dev/null || true
  wait "$PF_PID" 2>/dev/null || true
  PF_PID=""
  rm -f "$pf_log"
  return "$rc"
}

log "Probing /healthz and /readyz on each control-plane component"
probe_failed=0
for entry in "${COMPONENTS[@]}"; do
  # shellcheck disable=SC2086
  set -- $entry
  resource="$1"; port="$2"
  echo "-> $resource :$port"
  probe_one "$resource" "$port" || probe_failed=1
done
(( probe_failed == 0 )) || fail "one or more probe endpoints did not return 200"

log "Waiting for MxlFlowMirror phase=Ready (timeout ${MIRROR_TIMEOUT_SECS}s)"
deadline=$(( $(date +%s) + MIRROR_TIMEOUT_SECS ))
phase=""
while [[ "$(date +%s)" -lt "$deadline" ]]; do
  phase=$("${KUBECTL[@]}" -n "$NAMESPACE" get mxlflowmirrors \
            -o jsonpath='{range .items[*]}{.metadata.name}={.status.phase} {end}' 2>/dev/null || true)
  # require at least one mirror, and all listed ones Ready.
  if [[ -n "$phase" && "$phase" != *"=Pending"* && "$phase" != *"=Failed"* && "$phase" == *"=Ready"* ]]; then
    # ensure every listed mirror is Ready (no non-Ready phases present).
    if ! grep -Eo '=[A-Za-z]+' <<<"$phase" | grep -vq '=Ready'; then
      log "MxlFlowMirror Ready: ${phase}"
      break
    fi
  fi
  sleep 2
done

if [[ -z "$phase" ]]; then
  fail "no MxlFlowMirror resources found in namespace ${NAMESPACE}"
fi
if grep -Eo '=[A-Za-z]+' <<<"$phase" | grep -vq '=Ready' ; then
  echo "current phases: $phase" >&2
  fail "not all MxlFlowMirrors reached Ready in ${MIRROR_TIMEOUT_SECS}s"
fi

log "PASS: control-plane Ready, probes 200, mirror Ready"
