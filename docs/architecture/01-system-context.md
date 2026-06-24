# System context

![System context](./diagrams/01-system-context.drawio.svg)

mxl-k8s is a control plane that wraps two upstream shared libraries:

- **libmxl** -- a shared-memory media SDK. A producer process calls
  `mxlCreateFlowWriter` and a consumer process calls
  `mxlCreateFlowReader`. Both expect to see the same domain directory
  on the local filesystem; flows materialise as
  `<flowID>.mxl-flow/{flow_def.json, data, grains/}` under that path.
- **libmxl-fabrics** -- a cross-node transport for libmxl flows. It
  registers libmxl's shared-memory regions with libfabric, lets a
  remote initiator RDMA grain payload + header into them, and
  coordinates the handshake through a serialised `TargetInfo` blob
  (libfabric address + memory keys).

The control plane's job is to take a user's intent ("this consumer
pod wants this flow"), make sure libmxl-fabrics has a target running
on the consumer's node and an initiator running on the producer's
node, and then get out of the way of the data path.

## What lives where

- **Inside the cluster**
  - One operator Deployment, cluster-scoped, leader-elected.
  - One DaemonSet pair (`agent` + `gateway`) on every node where MXL
    flows might land.
  - Whatever producer / consumer pods the user runs.
  - The Kubernetes API server, which is the only piece of state
    shared across nodes.
- **Per node, outside the pod sandboxes**
  - `tmpfs` mounted at `/run/mxl/domain`, the libmxl domain
    directory. It is `hostPath`-mounted into every pod that needs to
    open a libmxl handle on the node.
  - The libfabric provider (the in-demo path is `tcp`; verbs/EFA/shm
    code paths are accepted by the API but not exercised in routine
    CI).
- **External, linked in**
  - `libmxl.so` is linked into the gateway, the writer pod, and the
    consumer pod.
  - `libmxl-fabrics.so` is linked into the gateway only.

## Actors

- **Writer pod** -- a user's producer. Calls libmxl to create the
  flow files on the producer node's tmpfs domain. mxl-k8s does not
  spawn or schedule it.
- **Consumer pod** -- a user's consumer. Calls libmxl to open a
  FlowReader. It opts into on-demand materialization by `LD_PRELOAD`-
  ing `libmxl-intent.so`, which intercepts the libc calls libmxl
  makes (`openat`, `open`, `access`, `stat`, `lstat`) against
  anything under a `.mxl-flow/` directory and asks the local agent
  to materialize the flow before retrying.
- **Operator** (`operator/cmd/mxl-operator`) -- single Deployment,
  cluster-scoped. Its only active reconciler is
  `receiver.Reconciler`, which turns each `MxlReceiver` into one
  `MxlFlowMirror` per distinct target node. The other reconcilers
  it registers (`flow`, `mirror`, `domain`, `nodecaps`) are observer-
  only -- they log events and write nothing.
- **Agent** (`agent/cmd/mxl-domain-agent`) -- DaemonSet. Watches the
  node's tmpfs domain with `fanotify`, publishes `MxlFlow.status.
  locations` and `MxlDomain.status` for this node, serves the
  intent UDS at `/run/mxl/agent.sock`, and runs an NMOS IS-04/IS-05
  HTTP server that exposes MXL senders as NMOS nodes, devices,
  sources, flows, senders, and receivers for broadcast controllers.
- **Gateway** (`gateway/cmd/mxl-fabrics-gateway`) -- DaemonSet, runs
  with `hostNetwork: true`. Hosts the source and target
  `MxlFlowMirror` reconcilers and the capabilities publisher in one
  controller-runtime Manager.
- **Kubernetes API server** -- the only cluster-wide state. Every
  cross-node coordination message (TargetInfo handoff, origin
  publication, capability advertisement) is a CR update.

## Why these architectural choices

- **hostNetwork on the gateway.** libfabric MSG/RC endpoints must
  bind to a routable node IP. `TargetInfo` carries `host:port`, and
  any peer gateway in the cluster will dial it directly; a CNI
  overlay address would not carry the connection.
- **hostPath for `/run/mxl/domain`.** libmxl's domain is per-node
  (one tmpfs per node, shared by every libmxl handle on that node).
  Pods that link libmxl mount the domain in.
- **Cluster-state-only cross-node coordination.** There is no
  gateway-to-gateway RPC.

## Provider coverage

`api/v1alpha1/common.go` declares the enum: `auto | tcp | verbs | efa
| shm`. The end-to-end demo path under `examples/tcp-demo` exercises
`tcp`. The `examples/rdma-demo` and [`docs/RDMA.md`](../RDMA.md)
cover the verbs-provider host setup, but those code paths are not
part of routine CI. Treat anything beyond `tcp` as developer-only
until it grows tests.
