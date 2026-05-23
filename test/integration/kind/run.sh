#!/usr/bin/env bash
# run.sh -- KIND integration suite runner. Sources lib.sh, iterates
# cases/*.sh in lexicographic order, prints a pass/fail summary, and
# exits non-zero if any case failed. Per-case stdout/stderr land
# under $KIND_DIAG_DIR/cases/; cluster-level diagnostics under
# $KIND_DIAG_DIR/ are collected once at the end on any failure.

set -uo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KIND_TEST_LIB="${SUITE_DIR}/lib.sh"

# shellcheck source=lib.sh
. "$KIND_TEST_LIB"

need kubectl
need kind
need curl

if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  fail "KIND cluster ${KIND_CLUSTER} not present (run 'make kind-up' first)"
fi

mkdir -p "$KIND_DIAG_DIR/cases"

# Resolve the case list. CASE_GLOB lets a developer filter; defaults
# to every executable .sh file under cases/.
CASE_GLOB="${CASE_GLOB:-${SUITE_DIR}/cases/*.sh}"
cases=()
for path in $CASE_GLOB; do
  [ -f "$path" ] || continue
  cases+=("$path")
done

if [ "${#cases[@]}" -eq 0 ]; then
  fail "no cases matched ${CASE_GLOB}"
fi

# Parallel arrays (bash 3.2 has no associative arrays): case path /
# result label / wall-clock duration. Index-aligned.
results_path=()
results_label=()
results_secs=()
any_failed=0

for case_path in "${cases[@]}"; do
  case_name="$(basename "$case_path" .sh)"
  case_log="${KIND_DIAG_DIR}/cases/${case_name}.log"
  log "case: ${case_name}"
  start=$(date +%s)
  if bash "$case_path" > "$case_log" 2>&1; then
    label=PASS
  else
    label=FAIL
    any_failed=1
  fi
  end=$(date +%s)
  secs=$(( end - start ))

  results_path+=("$case_path")
  results_label+=("$label")
  results_secs+=("$secs")

  # Stream the case's output back to the runner's stdout so the CI
  # log shows it inline; the file under cases/ keeps the raw copy.
  sed 's/^/    /' "$case_log" >&2
  printf '  [%s] %s (%ss)\n' "$label" "$case_name" "$secs" >&2
done

log "summary"
i=0
while [ "$i" -lt "${#results_path[@]}" ]; do
  printf '  %-4s %-32s %ss\n' \
      "${results_label[$i]}" \
      "$(basename "${results_path[$i]}" .sh)" \
      "${results_secs[$i]}" >&2
  i=$(( i + 1 ))
done

if [ "$any_failed" -ne 0 ]; then
  log "collecting diagnostics into ${KIND_DIAG_DIR}"
  collect_diagnostics
  exit 1
fi

log "PASS"
