# mxl-k8s

Kubernetes control plane for MXL ([Media eXchange Layer][mxl]) domains.
This is how the moving parts fit together at runtime:

![System context](docs/architecture/diagrams/01-system-context.drawio.svg)

The repo is a Go workspace with five modules:

| Module | Path | Purpose |
| --- | --- | --- |
| `api` | `github.com/qvest-digital/mxl-k8s/api` | CRD types (`MxlDomain`, `MxlFlow`, `MxlFlowMirror`, `MxlReceiver`, `MxlNodeCapabilities`). |
| `ipc` | `github.com/qvest-digital/mxl-k8s/ipc` | gRPC contract between agent ↔ gateway and gateway ↔ gateway. |
| `operator` | `github.com/qvest-digital/mxl-k8s/operator` | Cluster operator: reconciles the CRDs. |
| `agent` | `github.com/qvest-digital/mxl-k8s/agent` | Per-node DaemonSet: watches the MXL domain via `fanotify`, publishes flow state, gates consumer opens. Links `libmxl` via [`go-mxl`][go-mxl]. |
| `gateway` | `github.com/qvest-digital/mxl-k8s/gateway` | Per-node DaemonSet: cross-node grain transport. Links `libmxl-fabrics` via [`go-mxl/fabrics`][go-mxl]. |

See [`docs/architecture/`](docs/architecture/) for the full architecture
walkthrough and [`docs/BUILD.md`](docs/BUILD.md) for local-build instructions.

[mxl]: https://github.com/dmf-mxl/mxl
[go-mxl]: https://github.com/qvest-digital/go-mxl
