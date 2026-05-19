# Using mxl-k8s

`mxl-k8s` is the cluster-side complement to
[`dmf-mxl`](https://github.com/dmf-mxl/mxl), the EBU/AMWA Media
eXchange Layer that gives media functions intra-node, zero-copy flow
exchange over tmpfs. `mxl-k8s` turns MXL's cross-node transport
(`libmxl-fabrics`) into a cluster feature so individual media
functions do not have to carry it themselves.

This page shows what a media function needs in order to participate
and what the cluster needs in order to host the control plane. For
project scope and the DMF coverage map see the
[root README](../README.md); for forward-looking features see
[`ROADMAP.md`](../ROADMAP.md).

## Prerequisites

Three independent sets of requirements. Each item is a hard
prerequisite, not a suggestion.

### The media function itself

- Runs as a Linux process, packaged as an OCI container image
  (`linux/amd64` or `linux/arm64`). No host installer, no service
  manager inside the container, no Windows-only binaries.
- Links `libmxl` directly via the C ABI, via the Go binding
  [`go-mxl`](https://github.com/qvest-digital/go-mxl), or any other
  language binding upstream ships. The container base image must
  have a glibc compatible with the libmxl build.
- Runs with the `IPC_LOCK` and `SYS_RESOURCE` Linux capabilities
  (granted by the pod manifest). libmxl uses `mlock` to pin the
  grain ring into RAM so libmxl-fabrics can register it for
  zero-copy transport. The entrypoint must not drop these
  capabilities, and the container must not execute setuid / setcap
  binaries that would reset them.
- Lifecycle: long-lived foreground process. Honour `SIGTERM` so
  libmxl FlowWriter can close cleanly (closing removes the
  on-disk flow files) and consumer FlowReader handles can release
  before the pod ends.
- Logs to stdout / stderr so `kubectl logs` and any cluster log
  aggregator work without extra plumbing.
- Reads its configuration from environment variables or a mounted
  file. The flow UUID a producer creates, and the flow UUID a
  consumer opens, must be set at startup. No interactive prompts.
- Consumer-side only: must tolerate `LD_PRELOAD` injection through
  the consumer pod's initContainer pattern. The kernel strips
  `LD_PRELOAD` for setuid / fully statically linked binaries, and
  any entrypoint that wipes the environment defeats the shim.
- Container image is published to a registry the cluster's nodes
  can pull from (public, or private with `imagePullSecrets`
  configured out-of-band).

### The Kubernetes cluster

- Kubernetes v1.28 or later (the Helm chart's `kubeVersion`).
- Admission / Pod Security policy permits, on the pods that opt
  into MXL, all of: `hostPath` volumes, `IPC_LOCK` and
  `SYS_RESOURCE` capabilities, and `hostNetwork: true` for the
  gateway DaemonSet.
- Helm CLI 3.8 or later on the install host (OCI chart pull
  requires it).
- Cluster nodes can pull from
  `ghcr.io/qvest-digital/mxl-k8s/...`, either directly or through
  a registry mirror.

### Each node host

- Linux kernel 5.17 or later. The agent's `fanotify` watcher needs
  `FAN_REPORT_DFID_NAME`, added in 5.17.
- `/run/mxl/domain` available on the host filesystem. The Helm
  chart does not provision this today. The KIND demo relies on
  Kubernetes `hostPath` with `DirectoryOrCreate`. A production
  deployment should back the directory with a `tmpfs` mount, via
  `systemd-tmpfiles.d(5)` or a small node-init DaemonSet.
- Routable inter-node networking on whatever interface the
  libfabric endpoints bind to. The gateway runs `hostNetwork`, so
  the address it publishes in `MxlFlowMirror.status.targetInfo` is
  the node's own IP.
- Time synchronised across nodes via NTP, or PTP for tight media
  timing. `mxl-k8s` does not configure clocks; missing sync leaves
  inconsistent timestamps in grain metadata.
- Optional: RDMA hardware, kernel modules, and `/dev/infiniband`
  if you plan to use the `verbs` provider, or EFA hardware for
  `efa`. The host setup for those lives in
  [`docs/RDMA.md`](./RDMA.md).

## Install the control plane

The control plane (operator, agent, gateway, CRDs, RBAC) is one
Helm chart published as an OCI artefact:

```sh
helm install mxl oci://ghcr.io/qvest-digital/mxl-k8s/charts/mxl-k8s \
  --version 1.0.0-rc.2 \
  --namespace mxl-system --create-namespace
```

That installs:

- the `mxl-operator` Deployment (cluster-scoped reconciler);
- the `mxl-domain-agent` and `mxl-fabrics-gateway` DaemonSets,
  one Pod per node;
- the five CRDs (`MxlFlow`, `MxlReceiver`, `MxlFlowMirror`,
  `MxlDomain`, `MxlNodeCapabilities`);
- ClusterRoles and ClusterRoleBindings for the above.

See [`charts/mxl-k8s/README.md`](../charts/mxl-k8s/README.md) for the
full values reference, override examples (RDMA, private registry,
IRSA), and the FluxCD `HelmRelease` snippet.

A FluxCD-driven cluster points an `OCI`-typed `HelmRepository` at the
same URL.

## Integrating a producer

A producer is any media function whose `libmxl` FlowWriter writes
flows into `/run/mxl/domain`. The pod manifest needs:

- A container that links `libmxl` (directly or through `go-mxl`).
- A `hostPath` volume mount of `/run/mxl/domain` so the
  container's libmxl sees the same tmpfs as the node's agent and
  gateway.
- `IPC_LOCK` and `SYS_RESOURCE` in `securityContext.capabilities.
  add`.

Minimal manifest:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-producer
  namespace: my-namespace
spec:
  containers:
    - name: producer
      image: registry.example.com/my-producer:1.2.3
      args: ["--domain", "/run/mxl/domain", "--flow-def", "/etc/flow.json"]
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - name: mxl-domain
          mountPath: /run/mxl/domain
  volumes:
    - name: mxl-domain
      hostPath:
        path: /run/mxl/domain
        type: DirectoryOrCreate
```

No producer-side CR is required. Once `libmxl` creates the flow
directory, the node's agent observes it via `fanotify` and publishes
an `MxlFlow` resource that names the flow's UUID and origin node.

## Integrating a consumer

A consumer is any media function whose `libmxl` FlowReader reads a
flow from `/run/mxl/domain`. mxl-k8s offers two integration paths.

### Declarative `MxlReceiver`

Apply an `MxlReceiver` alongside the consumer pod. The operator
turns it into one `MxlFlowMirror` per distinct target node; the
gateway pair sets up the libmxl-fabrics transport; the flow appears
locally before the consumer pod starts reading.

```yaml
apiVersion: mxl.qvest-digital.com/v1alpha1
kind: MxlReceiver
metadata:
  name: my-receiver
  namespace: my-namespace
spec:
  flowID: "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
  provider: tcp
  podRef:
    name: my-consumer
    namespace: my-namespace
```

Use `podSelector` instead of `podRef` to bind a receiver to every
pod matching a label inside the namespace.

The consumer pod itself needs a `hostPath` mount of
`/run/mxl/domain` and the `IPC_LOCK` + `SYS_RESOURCE` capabilities,
the same shape as the producer pod above.

### On-demand intent (LD_PRELOAD shim)

If the consumer should open the flow lazily, without an
`MxlReceiver` applied ahead of time, inject the
`libmxl-intent.so` LD_PRELOAD shim into the consumer pod. The
shim intercepts the first `openat` for `/run/mxl/domain/<flowID>.
mxl-flow/flow_def.json`, blocks on the agent's UDS until the
mirror has materialised, and then lets the call complete.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-consumer
  namespace: my-namespace
spec:
  initContainers:
    - name: install-intent-shim
      image: ghcr.io/qvest-digital/mxl-k8s/shim:v1.0.0-rc.2
      volumeMounts:
        - name: intent-shim
          mountPath: /shared
  containers:
    - name: consumer
      image: registry.example.com/my-consumer:1.2.3
      env:
        - name: LD_PRELOAD
          value: /opt/mxl-intent/libmxl-intent.so
      securityContext:
        capabilities:
          add: ["IPC_LOCK", "SYS_RESOURCE"]
      volumeMounts:
        - name: mxl-run
          mountPath: /run/mxl
        - name: intent-shim
          mountPath: /opt/mxl-intent
          readOnly: true
  volumes:
    - name: mxl-run
      hostPath:
        path: /run/mxl
        type: DirectoryOrCreate
    - name: intent-shim
      emptyDir: {}
```

The volume mount targets `/run/mxl` rather than just
`/run/mxl/domain` so both the domain directory and the agent's
`/run/mxl/agent.sock` are visible inside the pod, even if the
socket is created after the pod schedules.

### Which path to use

The declarative path keeps the cluster's intent visible: anyone
can `kubectl get mxlreceiver` and see which pods want which flows.
The intent path keeps the consumer's container manifest fully
self-contained at the cost of hiding intent inside the pod's
runtime. Both paths converge on the same `MxlFlowMirror` because
the receiver reconciler and the agent's intent dispatcher use the
identical name derivation, so the two paths cannot create
duplicate mirrors.

### Discovering the flow UUID

Today the consumer must know the producer's flow UUID at startup,
typically baked into the consumer's configuration. A cluster-wide
flow listing UX (kubectl plugin, printer columns) is on the near
roadmap (see below).

## Picking a transport provider

`MxlReceiver.spec.provider` (and `MxlFlowMirror.spec.provider`)
selects the libmxl-fabrics provider for the cross-node transfer:

- `tcp`: plain TCP. Default-exercised; works on any normal node
  IP. No special host setup.
- `verbs`: Linux verbs / `librdmacm` / RoCE / InfiniBand. Needs
  RDMA hardware, kernel modules, and `/dev/infiniband`. See
  [`docs/RDMA.md`](./RDMA.md).
- `efa`: AWS Elastic Fabric Adapter. Needs EFA-capable instances.
- `shm`: host-local shared memory. Used between gateway endpoints
  that share a node.
- `auto`: let libmxl-fabrics pick a provider at runtime.

Only `tcp` is part of routine CI today. The non-`tcp` providers
work in code but have not been exercised continuously.

## See also

- [Root README](../README.md): project scope and the DMF coverage
  map.
- [`ROADMAP.md`](../ROADMAP.md): current / next / future features.
- [`docs/architecture/`](./architecture/): runtime walkthrough of
  the operator, agent, gateway, and shim.
- [`docs/BUILD.md`](./BUILD.md): local-build prerequisites and the
  cgo lane for `agent` and `gateway`.
- [`docs/RDMA.md`](./RDMA.md): host setup for the `verbs` and
  `efa` providers.
- [`docs/KIND.md`](./KIND.md): KIND demo walkthrough.
