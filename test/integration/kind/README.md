# KIND integration test

`test/integration/kind/kind-test.sh` validates a KIND cluster that
has already been brought up by `make kind` (or `make kind-up`). It
asserts:

- The five `mxl.qvest-digital.com` CRDs reach `Established`.
- `deploy/mxl-operator`, `ds/mxl-domain-agent`, `ds/mxl-fabrics-gateway`
  in the `mxl-system` namespace finish rolling out and every pod
  reports `Ready`.
- `/healthz` and `/readyz` on each component's `probe` container port
  (8081) return HTTP 200. The check uses `kubectl port-forward` so
  no curl-capable image needs to live inside the cluster.
- At least one `MxlFlowMirror` exists and every listed one reaches
  `phase=Ready`.

Failure paths dump `kubectl get all -A`, `describe` for the
namespace's pods, namespace events, the full `mxl.*` resource set,
and the last 400 lines of each component's logs into
`${KIND_DIAG_DIR:-./kind-diagnostics}` so the kind-integration
GitHub Actions job can upload them as a workflow artifact.

## Usage

```sh
make kind          # converge the cluster (BUILD=<tag> in CI)
make kind-test     # run the integration test
```

Tunables (all optional, environment variables):

| Name | Default | Purpose |
| --- | --- | --- |
| `KIND_CLUSTER` | `mxl-k8s-demo` | Cluster name; must match `make kind` |
| `MXL_NAMESPACE` | `mxl-system` | Namespace where the demo is applied |
| `ROLLOUT_TIMEOUT_SECS` | `180` | Per-rollout-status timeout |
| `MIRROR_TIMEOUT_SECS` | `180` | MxlFlowMirror Ready wait |
| `PROBE_TIMEOUT_SECS` | `30` | Per-port-forward startup timeout |
| `KIND_DIAG_DIR` | `$PWD/kind-diagnostics` | Where failure diagnostics land |

## Runtime

Against an already-converged cluster the test completes in well
under a minute. Total budget including cluster bring-up in the
kind-integration CI job is < 5 min.
