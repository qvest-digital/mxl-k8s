# NMOS Integration Tests

End-to-end integration tests for the NMOS IS-04 (Node API) and IS-05 (Connection
API) sender proxy implementation.

## Files

| File | Purpose |
|------|---------|
| `nmos_integration_test.go` | Go test suite validating IS-04/IS-05 endpoints |
| `run-nmos-tests.sh` | Shell script: build, start, test, teardown |
| `README.md` | This file |

## Quick start

```bash
# Run against a locally running NMOS server:
./test/nmos/run-nmos-tests.sh

# Run against an existing server:
./test/nmos/run-nmos-tests.sh --server-url http://nmos-host:port

# Skip AMWA tests:
./test/nmos/run-nmos-tests.sh --skip-amwa

# Run Go tests directly (requires a running server):
NMOS_SERVER_URL=http://127.0.0.1:8080 go test -tags integration -v ./test/nmos/...
```

## Test coverage

### IS-04 Node API (v1.3)
- Version discovery (`/x-nmos/node/`)
- Self resource with required BCP-007-03 fields
- Device, Source, Flow, Sender, Receiver listing
- Empty receiver list (Receiver not implemented)

### IS-05 Connection API (v1.2)
- Version discovery (`/x-nmos/connection/`)
- Sender active resource retrieval
- Sender staged resource (read-only, controller-compatible)
- Sender transport file parameters
- Sender constraints (concrete enum values)
- Unknown sender returns 404

### HTTP conformance
- CORS headers on all endpoints
- OPTIONS returns 204
- Disallowed methods return 405
- Unknown paths return 404
- Error responses have standard format with code/error/debug fields

### BCP-007-03 Sender Proxy
- All resource types accessible (self, devices, sources, flows, senders, receivers)
- Senders expose transport file via IS-05
- Receiver not implemented (empty list)

### AMWA nmos-testing (optional)
- IS-04-01: Node API conformance
- IS-05-01: Connection API sender tests
- IS-05-02: Transport file tests
- Runs only if `nmos-testing` is installed; skipped otherwise

## Known limitations

| Limitation | Rationale |
|-----------|-----------|
| Receiver not implemented | Sender proxy scope; IS-05 Receiver endpoints return empty |
| Sender staged is read-only | Senders are always-active; PATCH accepted for controller compatibility |
| No DNS-SD testing | BCP-007-03 DNS-SD advertisement requires network stack testing outside this suite |
| AMWA nmos-testing optional | Requires pip install nmos-testing and a NMOS Registry |
| No kind cluster tests | Kubernetes-based testing deferred to CI pipeline |

## Requirements

- Go 1.21+
- curl
- Optional: AMWA nmos-testing (`pip install nmos-testing`)
- Optional: Running MXL agent with NMOS server enabled
