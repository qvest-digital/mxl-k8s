# mxl-k8s

Kubernetes control plane for [MXL][mxl]. mxl-k8s is the
cluster-side complement to [`dmf-mxl`][mxl]: it links upstream
`libmxl` and `libmxl-fabrics` and turns MXL's cross-node transport
into a cluster feature so individual media functions don't have to
carry it themselves.

## Why

MXL gives media functions intra-node, zero-copy flow exchange
on tmpfs. The moment two functions land on different machines,
crossing the wire is a separate stack: discover where the flow
lives, open a libmxl-fabrics Target on the consumer node, run
an Initiator on the producer node, exchange addresses and
memory keys, register memory regions, drive a transfer loop,
recover from drops. That is a lot to ask of a function whose
job is to produce or consume grains.

mxl-k8s lifts that stack into the cluster. A producer or
consumer pod keeps the same shape it has on a single node: it
reads or writes against its local `/run/mxl/domain` and never
mentions libmxl-fabrics. The cluster makes sure the flow shows
up there.

## How

![System context](docs/architecture/diagrams/01-system-context.drawio.svg)

Four pieces, all running inside the cluster:

- A per-node **agent** DaemonSet watches `/run/mxl/domain` via
  `fanotify` and publishes each flow on the Kubernetes API.
- A per-node **gateway** DaemonSet owns the libmxl-fabrics
  handles: FlowReader on producer nodes, FlowWriter on consumer
  nodes, Initiator/Target on the fabric, and the per-grain
  transfer loop.
- A cluster-scoped **operator** reconciles `MxlReceiver` intent
  ("this pod wants to consume that flow") into one
  `MxlFlowMirror` per (flow, target-node). Multiple consumers on
  the same node share a single mirror.
- An **LD_PRELOAD shim** in consumer pods turns the first
  libmxl probe (`access`, `stat`, `open`, ...) for a not-yet-
  materialised flow into a synchronous wait on the local agent.
  Consumer code calls `mxlCreateFlowReader` the same way it does
  on a single node.

The CRDs (`MxlFlow`, `MxlReceiver`, `MxlFlowMirror`,
`MxlDomain`, `MxlNodeCapabilities`) describe flows, who wants
them, and what each node can carry.

For the full architecture walkthrough (per-node anatomy,
control-plane and data-plane sequences, lifecycle diagrams) see
[`docs/architecture/`](docs/architecture/).

## Scope

mxl-k8s lives in the Media Exchange row of the
[EBU Dynamic Media Facility Reference Architecture (V2.0,
April 2026)](https://tech.ebu.ch/publications/white-paper-2026-04-15).
It covers the cross-node flow lifecycle: discovery and
registration, the per-mirror libmxl-fabrics handshake, and recovery
on writer or gateway restarts. Container orchestration, identity, and
per-function on-node behaviour stay with Kubernetes, the cluster's
identity provider, and upstream `dmf-mxl` respectively.

![DMF coverage map](docs/diagrams/dmf-coverage.drawio.svg)

`libmxl` and `libmxl-fabrics` are linked from
[`dmf-mxl/mxl`](https://github.com/dmf-mxl/mxl) through the
[`go-mxl`](https://github.com/qvest-digital/go-mxl) bindings.
FlowReader / FlowWriter semantics, grain layout, and the shape
of `flow_def.json` remain upstream's design; mxl-k8s is the
cluster orchestration around them.

See [`ROADMAP.md`](ROADMAP.md) for the feature roadmap.

## What you do not have to write

For media-function authors:

- No `libmxl-fabrics` link, no `libfabric` link, no headers.
- No TargetInfo handshake, no memory-region registration, no
  per-grain transfer loop.
- No provider choice at code time. The fabric provider
  (`tcp`, `verbs`, `efa`, `shm`) is a YAML knob on the
  `MxlReceiver`.
- No reconnect path for producer or consumer restarts. The
  gateway rebuilds the fabric side and republishes the new
  address on its own; the function keeps reading from its local
  domain.

For cluster operators:

- One DaemonSet per node owns the fabric. Bandwidth scaling and
  failure isolation follow the standard Kubernetes affordances.
- Provider rollout and host-side prerequisites
  (`/dev/infiniband`, `IPC_LOCK`, RoCEv2 PFC/DSCP, EFA AMI
  configuration) are documented under
  [`docs/RDMA.md`](docs/RDMA.md).
- Container images publish to
  `ghcr.io/qvest-digital/mxl-k8s/<component>` for every PR,
  every push to `main` (`:dev` plus `:sha-<short>`), and every
  per-component release tag (`:vX.Y.Z` plus `:latest` or
  `:pre`).

## Install via Helm

The control plane (operator, agent, gateway, CRDs, RBAC) ships as a
Helm chart at
`oci://ghcr.io/qvest-digital/mxl-k8s/charts/mxl-k8s`.

```sh
helm install mxl oci://ghcr.io/qvest-digital/mxl-k8s/charts/mxl-k8s \
  --version 1.0.0-rc.2 \
  --namespace mxl-system --create-namespace
```

FluxCD users point an `OCI`-typed `HelmRepository` at the same URL.
See [`charts/mxl-k8s/README.md`](charts/mxl-k8s/README.md) for the
full values reference, override examples (RDMA, private registry,
IRSA), and the FluxCD `HelmRelease` snippet.

## Run the demo locally

The repo ships a KIND cluster that runs an end-to-end TCP flow
across two worker nodes. Requires Docker, [`kind`][kind] >= 0.20,
and `kubectl`. Linux host with a kernel >= 5.17 (the agent's
`fanotify` needs `FAN_REPORT_DFID_NAME`).

For docker run:

```sh
make kind-up
```

or for podman:

```sh
make kind-up CONTAINER_RUNTIME=podman
```

That builds every component image locally, brings up a
three-node KIND cluster (control plane plus two workers),
applies the [`examples/tcp-demo`](examples/tcp-demo/) bundle, and
waits for the `MxlFlowMirror` to reach `Ready`. After about a
minute the writer pod is producing grains on one worker and the
reader pod is consuming them on the other.

```sh
kubectl --context kind-mxl-k8s-demo -n mxl-system logs pod/mxl-tcp-demo-reader
```

The reader prints one line per grain (`idx=... size=... slices=.../...`).
Use `make kind-status` for the converged state of the CRDs and
pods, `make kind-down` to tear the cluster down.

[`docs/KIND.md`](docs/KIND.md) walks through what each step does
and what to look at if convergence stalls.

## Repository layout

The repo is a Go workspace with five modules:

| Module | Path | Purpose |
| --- | --- | --- |
| `api` | `github.com/qvest-digital/mxl-k8s/api` | CRD types. |
| `ipc` | `github.com/qvest-digital/mxl-k8s/ipc` | gRPC contract between agent and gateway. |
| `operator` | `github.com/qvest-digital/mxl-k8s/operator` | Cluster operator that reconciles the CRDs. |
| `agent` | `github.com/qvest-digital/mxl-k8s/agent` | Per-node DaemonSet. Links libmxl via [`go-mxl`][go-mxl]. |
| `gateway` | `github.com/qvest-digital/mxl-k8s/gateway` | Per-node DaemonSet. Links libmxl-fabrics via [`go-mxl/fabrics`][go-mxl]. |

[`docs/USAGE.md`](docs/USAGE.md) covers the prerequisites for a
media function (container, libmxl link, capabilities) and how to
integrate it as a producer or consumer.
[`docs/BUILD.md`](docs/BUILD.md) covers local-build prerequisites
and the cgo lane for `agent` and `gateway`.
[`CLAUDE.md`](CLAUDE.md) carries the contributor rules.

[mxl]: https://github.com/dmf-mxl/mxl
[go-mxl]: https://github.com/qvest-digital/go-mxl
[kind]: https://kind.sigs.k8s.io/
