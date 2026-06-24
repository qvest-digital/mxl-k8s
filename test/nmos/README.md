# NMOS Integration Tests

End-to-end integration tests for the NMOS IS-04 (Node API) and IS-05 (Connection
API) sender proxy implementation.

## Files

| File | Purpose |
|------|---------|
| `nmos_integration_test.go` | Go test suite validating IS-04/IS-05 endpoints |
| `run-nmos-tests.sh` | Shell script: verify server, run tests, report |
| `README.md` | This file |

## Quick start

```bash
# Run against a locally running NMOS server:
./test/nmos/run-nmos-tests.sh

# Run against an existing server:
./test/nmos/run-nmos-tests.sh --server-url http://nmos-host:port

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

## Known limitations

| Limitation | Rationale |
|-----------|-----------|
| Receiver not implemented | Sender proxy scope; IS-05 Receiver endpoints return empty |
| Sender staged is read-only | Senders are always-active; PATCH accepted for controller compatibility |
| No DNS-SD testing | BCP-007-03 DNS-SD advertisement requires network stack testing outside this suite |
| No kind cluster tests | Kubernetes-based testing deferred to CI pipeline |

## AMWA nmos-testing

AMWA [nmos-testing](https://github.com/AMWA-TV/nmos-testing) is a separate
semi-automated test tool. Its test suites are not integrated into this test
path. Non-interactive suites from nmos-testing may be incorporated later as a
separate effort.

## Requirements

- Go 1.21+
- curl
- Running MXL agent with NMOS server enabled
