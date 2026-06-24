#!/usr/bin/env bash
# run-nmos-tests.sh -- Integration test harness for NMOS IS-04/IS-05 endpoints.
#
# Usage:
#   ./test/nmos/run-nmos-tests.sh [options]
#
# Options:
#   --server-url URL   Use an already-running NMOS server (required if no
#                      agent binary is available to run locally).
#   --skip-amwa        Skip AMWA nmos-testing even if installed.
#   --help             Show this help.
#
# Environment variables:
#   NMOS_SERVER_URL    HTTP base URL of a running NMOS server.
#                      Overrides --server-url. Default: http://127.0.0.1:8080
#   GOFLAGS            Extra flags passed to `go test`.
#
# Prerequisites:
#   - A running MXL agent with NMOS server enabled, OR
#   - An external NMOS server with IS-04 v1.3 and IS-05 v1.2 endpoints.
#
# The script:
#   1. Verifies the NMOS server is reachable.
#   2. Runs Go integration tests against it.
#   3. Optionally runs AMWA nmos-testing if installed.
#   4. Reports results and known limitations.
#
# Exit codes:
#   0 - All tests passed.
#   1 - One or more tests failed.
#   2 - Setup/connectivity failure.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
SERVER_URL=""
SKIP_AMWA="false"

usage() {
  sed -n '/^# Usage:/,/^# Exit/p' "$0" | sed 's/^# \?//'
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-url)  SERVER_URL="$2"; shift 2 ;;
    --skip-amwa)   SKIP_AMWA="true"; shift ;;
    --help|-h)     usage ;;
    *)             echo "Unknown option: $1"; usage ;;
  esac
done

# Override from env
if [[ -n "${NMOS_SERVER_URL:-}" ]]; then
  SERVER_URL="$NMOS_SERVER_URL"
fi

if [[ -z "$SERVER_URL" ]]; then
  SERVER_URL="http://127.0.0.1:8080"
fi

log() {
  echo "[run-nmos-tests] $*"
}

# -----------------------------------------------------------------------
# Step 1: Verify server is reachable
# -----------------------------------------------------------------------
log "Checking NMOS server at $SERVER_URL..."
if ! curl -sf -o /dev/null -m 5 "$SERVER_URL/x-nmos/node/" 2>/dev/null; then
  log "ERROR: NMOS server not reachable at $SERVER_URL"
  log ""
  log "Start an MXL agent with NMOS server enabled, or provide --server-url"
  log "pointing at a running instance."
  exit 2
fi
log "Server is reachable"

# -----------------------------------------------------------------------
# Step 2: Run Go integration tests
# -----------------------------------------------------------------------
log "Running integration tests..."
export NMOS_SERVER_URL="$SERVER_URL"

cd "$SCRIPT_DIR"
TEST_EXIT=0
if GOWORK=off go test -tags integration -v -count=1 \
    -timeout 120s \
    ${GOFLAGS:-} \
    . 2>&1; then
  log "Integration tests PASSED"
else
  TEST_EXIT=$?
  log "Integration tests FAILED (exit code: $TEST_EXIT)"
fi

# -----------------------------------------------------------------------
# Step 3: Optional AMWA nmos-testing
# -----------------------------------------------------------------------
AMWA_EXIT="skipped"
if [[ "$SKIP_AMWA" == "false" ]] && command -v nmos-testing >/dev/null 2>&1; then
  log "Running AMWA nmos-testing..."
  HOST=$(echo "$SERVER_URL" | sed 's|http://||' | cut -d: -f1)
  PORT=$(echo "$SERVER_URL" | sed 's|http://||' | cut -d: -f2 | cut -d/ -f1)

  # IS-04-01: Node API test suite
  log "Running IS-04-01 (Node API)..."
  AMWA_EXIT=0
  nmos-testing --host "$HOST" --port "$PORT" --version v1.3 --selector "test_01" 2>&1 || AMWA_EXIT=$?
  if [[ $AMWA_EXIT -ne 0 ]]; then
    log "IS-04-01: Some tests reported failures (exit $AMWA_EXIT)"
    log "  Note: Receiver-related failures are expected (not implemented)"
  fi

  # IS-05-01: Connection API sender test suite
  log "Running IS-05-01 (Connection API - Sender)..."
  IS05_EXIT=0
  nmos-testing --host "$HOST" --port "$PORT" --version v1.2 --selector "test_01" 2>&1 || IS05_EXIT=$?
  if [[ $IS05_EXIT -ne 0 ]]; then
    log "IS-05-01: Some tests reported failures (exit $IS05_EXIT)"
    log "  Note: Receiver endpoint failures are expected (not implemented)"
  fi
else
  if [[ "$SKIP_AMWA" == "true" ]]; then
    log "AMWA nmos-testing skipped (--skip-amwa)"
  else
    log "AMWA nmos-testing not installed; skipping"
    log "  Install: pip install nmos-testing"
    log "  See: https://specs.amwa.org/nmos-testing"
  fi
fi

# -----------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------
log "========================================"
log "NMOS Integration Test Summary"
log "  Server URL:              $SERVER_URL"
log "  Go integration tests:    exit $TEST_EXIT"
log "  AMWA nmos-testing:       $AMWA_EXIT"
log "========================================"
log ""
log "Known limitations:"
log "  - Receiver resources not implemented (IS-05 Receiver endpoints empty)"
log "  - Sender staged PATCH is read-only (always-active sender model)"
log "  - BCP-007-03 DNS-SD not tested here (requires network stack)"
log "  - AMWA nmos-testing requires pip install and a NMOS Registry"
log "========================================"

exit $TEST_EXIT
