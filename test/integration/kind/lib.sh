# lib.sh -- shared helpers for the KIND integration suite. Sourced by
# run.sh and by each case under cases/.
#
# Exported state:
#   KIND_CLUSTER          name of the running KIND cluster
#   NAMESPACE             namespace the demo is applied to
#   KUBECTL               array; expands to `kubectl --context kind-...`
#   KIND_DIAG_DIR         directory where collect_diagnostics writes
#   ROLLOUT_TIMEOUT_SECS  per-rollout-status wait
#   MIRROR_TIMEOUT_SECS   per-MxlFlowMirror Ready wait
#   PROBE_TIMEOUT_SECS    per-port-forward startup wait

KIND_CLUSTER="${KIND_CLUSTER:-mxl-k8s-demo}"
NAMESPACE="${MXL_NAMESPACE:-mxl-system}"
KUBECTL=(kubectl --context "kind-${KIND_CLUSTER}")
KIND_DIAG_DIR="${KIND_DIAG_DIR:-${PWD}/kind-diagnostics}"
ROLLOUT_TIMEOUT_SECS="${ROLLOUT_TIMEOUT_SECS:-180}"
MIRROR_TIMEOUT_SECS="${MIRROR_TIMEOUT_SECS:-180}"
PROBE_TIMEOUT_SECS="${PROBE_TIMEOUT_SECS:-30}"

log()  { printf '\n=== %s ===\n' "$*" >&2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"; }

# wait_phase <resource> <jsonpath> <regex> <timeout-secs>
# Polls the resource until the jsonpath output matches the regex, or
# the timeout elapses. On success prints the last observed value to
# stdout; on timeout returns non-zero with the last value on stderr.
wait_phase() {
  local resource="$1" jsonpath="$2" regex="$3" timeout="$4"
  local deadline value
  deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    value=$("${KUBECTL[@]}" -n "$NAMESPACE" get "$resource" \
              -o jsonpath="$jsonpath" 2>/dev/null || true)
    if echo "$value" | grep -Eq "$regex"; then
      echo "$value"
      return 0
    fi
    sleep 2
  done
  echo "wait_phase: $resource $jsonpath did not match /$regex/ in ${timeout}s; last='$value'" >&2
  return 1
}

# port_forward_probe <pod> <port> <path>
# Opens a kubectl port-forward to pod:port, curls /<path>, and prints
# the HTTP status code on stdout. Returns 0 iff the code is 200.
port_forward_probe() {
  port_forward_fetch "$1" "$2" "$3" /dev/null
}

# port_forward_fetch <pod> <port> <path> [body-file]
# Opens a kubectl port-forward to pod:port, GETs /<path>, writes the
# response body to body-file when one is named, and prints the HTTP
# status code on stdout. Returns 0 iff the code is 200.
# Random-binds locally (`0:port`) and learns the assigned port from
# kubectl's stdout, avoiding clashes with parallel forwards.
port_forward_fetch() {
  local pod="$1" port="$2" path="$3" body="${4:-/dev/null}"
  local pf_log local_port code rc deadline pf_pid
  pf_log="$(mktemp)"
  "${KUBECTL[@]}" -n "$NAMESPACE" port-forward "pod/${pod}" "0:${port}" \
      > "$pf_log" 2>&1 &
  pf_pid=$!

  deadline=$(( $(date +%s) + PROBE_TIMEOUT_SECS ))
  local_port=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    local_port=$(sed -n 's/.*Forwarding from 127\.0\.0\.1:\([0-9]*\) ->.*/\1/p' "$pf_log" | head -1)
    [ -n "$local_port" ] && break
    sleep 0.2
  done
  if [ -z "$local_port" ]; then
    cat "$pf_log" >&2
    kill "$pf_pid" 2>/dev/null || true
    wait "$pf_pid" 2>/dev/null || true
    rm -f "$pf_log"
    echo "000"
    return 1
  fi

  code=$(curl -sS -o "$body" -w '%{http_code}' \
              --max-time 10 "http://127.0.0.1:${local_port}/${path}" || echo "000")
  rc=0
  [ "$code" = "200" ] || rc=1

  kill "$pf_pid" 2>/dev/null || true
  wait "$pf_pid" 2>/dev/null || true
  rm -f "$pf_log"
  echo "$code"
  return "$rc"
}

# resolve_pod <app.kubernetes.io/name label value>
# Prints the name of one Running pod with the given label, or returns
# non-zero. Used by cases that need to address a concrete pod.
resolve_pod() {
  local label="$1" pod
  pod=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
          -l "app.kubernetes.io/name=${label}" \
          --field-selector=status.phase=Running \
          -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -n "$pod" ] || return 1
  echo "$pod"
}

# collect_diagnostics
# Dumps cluster state and component logs into $KIND_DIAG_DIR. Called
# by run.sh once at the end of the suite if any case failed, so the
# CI job can upload the directory as a workflow artifact.
collect_diagnostics() {
  mkdir -p "$KIND_DIAG_DIR"
  "${KUBECTL[@]}" get all -A -o wide \
      > "$KIND_DIAG_DIR/get-all.txt"        2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" describe pods \
      > "$KIND_DIAG_DIR/describe-pods.txt"  2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" get events --sort-by=.lastTimestamp \
      > "$KIND_DIAG_DIR/events.txt"         2>&1 || true
  "${KUBECTL[@]}" -n "$NAMESPACE" get \
      mxldomains,mxlflows,mxlflowmirrors,mxlreceivers,mxlnodecapabilities \
      -o yaml \
      > "$KIND_DIAG_DIR/mxl-resources.yaml" 2>&1 || true
  for app in mxl-k8s-operator mxl-k8s-agent mxl-k8s-gateway mxl-k8s-exporter; do
    "${KUBECTL[@]}" -n "$NAMESPACE" logs \
        -l "app.kubernetes.io/name=${app}" \
        --all-containers --prefix --tail=400 \
        > "$KIND_DIAG_DIR/${app}.log"       2>&1 || true
  done
  for pod in mxl-tcp-demo-writer mxl-tcp-demo-reader \
             mxl-tcp-demo-audio-writer mxl-tcp-demo-audio-reader; do
    "${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${pod}" \
        --all-containers --prefix --tail=400 \
        > "$KIND_DIAG_DIR/${pod}.log"       2>&1 || true
  done
}

# --- MxlFlow location helpers -------------------------------------
#
# The filter that reads naturally for these, e.g.
# .items[?(@.status.locations[*].nodeName=='x')], cannot be used:
# kubectl's jsonpath engine rejects a comparison whose left side
# expands to more than one element ("can only compare one element at
# a time"), and any flow with a second location does exactly that.
# Each helper emits one line per object and matches in shell instead.
#
# None of them discard stderr: a jsonpath or connection error must
# not read as "this node carries nothing".

# flows_on_node <node> -- names of MxlFlows carrying a location for
# the node, one per line. MxlFlow is cluster-scoped.
flows_on_node() {
  local want="$1" name nodes
  "${KUBECTL[@]}" get mxlflow -o \
    'jsonpath={range .items[*]}{.metadata.name}{"|"}{range .status.locations[*]}{.nodeName}{","}{end}{"\n"}{end}' \
  | while IFS='|' read -r name nodes; do
      [ -n "$name" ] || continue
      case ",${nodes}" in
        *",${want},"*) echo "$name" ;;
      esac
    done
}

# location_count <node> -- how many flows list the node.
location_count() {
  flows_on_node "$1" | grep -c . || true
}

# location_phases <node> -- the phase each flow records for the node,
# one per line. Empty when the node is listed by no flow.
location_phases() {
  local want="$1" name pairs pair
  "${KUBECTL[@]}" get mxlflow -o \
    'jsonpath={range .items[*]}{.metadata.name}{"|"}{range .status.locations[*]}{.nodeName}{"="}{.phase}{","}{end}{"\n"}{end}' \
  | while IFS='|' read -r name pairs; do
      [ -n "$name" ] || continue
      IFS=','
      for pair in $pairs; do
        case "$pair" in
          "${want}="*) echo "${pair#*=}" ;;
        esac
      done
      unset IFS
    done
}

# mirrors_sourced_from <node> -- "<name>=<phase>" per MxlFlowMirror
# whose spec.sourceNode is the node.
mirrors_sourced_from() {
  local want="$1" name src phase
  "${KUBECTL[@]}" get mxlflowmirror -A -o \
    'jsonpath={range .items[*]}{.metadata.name}{"|"}{.spec.sourceNode}{"|"}{.status.phase}{"\n"}{end}' \
  | while IFS='|' read -r name src phase; do
      [ -n "$name" ] || continue
      [ "$src" = "$want" ] && echo "${name}=${phase}"
    done
}

# daemonset_pod_on <ds-label-value> <node> -- name of the running
# DaemonSet pod for the component on the node, empty when none.
daemonset_pod_on() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get pods \
    --field-selector "spec.nodeName=$2,status.phase=Running" -o \
    'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' \
  | grep "^mxl-k8s-$1-" | head -1
}
