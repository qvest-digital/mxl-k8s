#!/usr/bin/env bash
# run-nmos-tests.sh -- Integration test harness for NMOS IS-04/IS-05 endpoints.
#
# Usage:
#   ./test/nmos/run-nmos-tests.sh [options]
#
# Options:
#   --server-url URL   Use an already-running NMOS server (required if no
#                      agent binary is available to run locally).
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
#   3. Reports results and known limitations.
#
# Exit codes:
#   0 - All tests passed.
#   1 - One or more tests failed.
#   2 - Setup/connectivity failure.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
SERVER_URL=""

usage() {
  sed -n '/^# Usage:/,/^# Exit/p' "$0" | sed 's/^# \?//'
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-url)  SERVER_URL="$2"; shift 2 ;;
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
# Summary
# -----------------------------------------------------------------------
log "========================================"
log "NMOS Integration Test Summary"
log "  Server URL:              $SERVER_URL"
log "  Go integration tests:    exit $TEST_EXIT"
log "========================================"
log ""
log "Known limitations:"
log "  - Receiver resources not implemented (IS-05 Receiver endpoints empty)"
log "  - Sender staged PATCH is read-only (always-active sender model)"
log "  - BCP-007-03 DNS-SD not tested here (requires network stack)"
log "========================================"

exit $TEST_EXIT
