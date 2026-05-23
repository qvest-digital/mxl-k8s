# KIND integration suite

End-to-end smoke tests that run against a converged KIND cluster
brought up by `make kind-up`. The suite verifies the control plane
reaches a healthy state and that frames are actually flowing from
the demo writer pod to the demo reader pod across the
MxlFlowMirror.

## Layout

```
test/integration/kind/
  lib.sh           shared helpers (KUBECTL, wait_phase, port_forward_probe, collect_diagnostics, ...)
  run.sh           orchestrator: iterates cases/*.sh, prints a pass/fail summary
  cases/           one assertion per file, executed in lexicographic order
    00-crds-established.sh
    10-rollouts-ready.sh
    20-probes-200.sh
    30-mirror-ready.sh
    40-frames-flowing.sh
```

## Usage

```sh
make kind-up      # build images, create cluster, apply examples/tcp-demo
make kind-test    # run this suite against the cluster
```

Filter to a subset:

```sh
CASE_GLOB='test/integration/kind/cases/30-*.sh' make kind-test
```

Tunables (all optional, environment variables):

| Name | Default | Purpose |
| --- | --- | --- |
| `KIND_CLUSTER` | `mxl-k8s-demo` | Cluster name; must match `make kind-up` |
| `MXL_NAMESPACE` | `mxl-system` | Namespace the demo is applied to |
| `ROLLOUT_TIMEOUT_SECS` | `180` | Per-rollout-status wait |
| `MIRROR_TIMEOUT_SECS` | `180` | MxlFlowMirror Ready / reader Ready wait |
| `PROBE_TIMEOUT_SECS` | `30` | Per-port-forward startup wait |
| `FRAMES_WINDOW_SECS` | `5` | Case 40 sample window |
| `KIND_DIAG_DIR` | `$PWD/kind-diagnostics` | Where failure diagnostics land |
| `CASE_GLOB` | `cases/*.sh` | Case selection glob |

## Diagnostics

On any case failure, `run.sh` calls `collect_diagnostics` and writes
into `$KIND_DIAG_DIR`:

- `get-all.txt` -- `kubectl get all -A -o wide`
- `describe-pods.txt` -- `kubectl describe pods` in the demo namespace
- `events.txt` -- namespace events sorted by `lastTimestamp`
- `mxl-resources.yaml` -- full `mxl.qvest-digital.com` resource dump
- `<component>.log` -- last 400 lines per control-plane component
- `cases/<case>.log` -- raw stdout/stderr of each case

The `kind-integration` GitHub Actions job uploads the directory as
an artifact named `kind-diagnostics-<run-id>` on failure.

## Writing a new case

Drop a new `<NN>-<name>.sh` into `cases/`. The runner discovers it
automatically. A case is just a bash script that:

1. Sources `lib.sh` via `. "$KIND_TEST_LIB"`.
2. Calls `fail "<reason>"` on a failed assertion (exits non-zero).
3. Exits 0 on success.

Available helpers (see `lib.sh` for signatures):

- `KUBECTL` -- array; expands to `kubectl --context kind-<cluster>`.
- `NAMESPACE` -- demo namespace.
- `log "msg"`, `fail "msg"`, `need "<cmd>"`.
- `wait_phase <resource> <jsonpath> <regex> <timeout-secs>`.
- `port_forward_probe <pod> <port> <path>` -- prints HTTP status,
  returns 0 iff 200.
- `resolve_pod <app.kubernetes.io/name label>`.

Cases run in lexicographic order; the runner does **not** stop on
the first failure, so a single broken case does not suppress the
remaining diagnostics. Keep the `NN-` prefix in sync with the
intended ordering (e.g. probe assertions after rollout assertions).
