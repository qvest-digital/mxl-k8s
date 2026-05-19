# rdma-demo

Same end-to-end slice as `tcp-demo`, configured for the
`libmxl-fabrics` **verbs** provider. Grains move between gateway
pods over RDMA via libibverbs / librdmacm -- typically RoCEv2 on
ethernet NICs, native InfiniBand on IB fabrics, with EFA as a
close cousin (see `docs/RDMA.md` for AWS specifics).

## What changes vs. tcp-demo

- `mxl-fabrics-gateway` runs with `--providers=verbs`.
- The gateway DaemonSet bind-mounts `/dev/infiniband` from the host
  so libibverbs can reach the userspace RDMA character devices.
- The gateway container adds `IPC_LOCK` (`mlock(2)` for pinning
  tmpfs grain pages) and `SYS_RESOURCE` (raise
  `RLIMIT_MEMLOCK`) on top of the tcp-demo's set.
- `MxlReceiver.spec.provider` is `verbs`.

The writer and reader pods are identical to tcp-demo's apart from
names; libmxl itself doesn't know which fabric is moving the
grains. The on-demand `libmxl-intent.so` shim works unchanged.

## Prerequisites

The cluster must actually have RDMA-capable NICs and the matching
host-side drivers. See `docs/RDMA.md` for the full prerequisite
list; quick summary:

- Linux kernel >= 5.17 (same as tcp-demo) on the worker nodes.
- `ibverbs`, `rdma-core`, and the NIC-specific kernel modules
  (e.g. `mlx5_ib` for Mellanox / NVIDIA ConnectX, `bnxt_re` for
  Broadcom, `irdma` for Intel E810) loaded on the host.
- `RLIMIT_MEMLOCK` raised system-wide (often `infinity` via systemd
  or `ulimit -l unlimited` in container runtime config); the
  `SYS_RESOURCE` capability lets the container do this itself when
  the host default is small.
- A network configuration where the gateway pod's host network can
  carry RDMA traffic. For RoCEv2 this typically means both nodes
  see each other on the same VLAN with the right DSCP/PFC settings.

If your RDMA setup demands a dedicated network attachment per pod
(SR-IOV via Multus, dedicated VFs), add the right
`k8s.v1.cni.cncf.io/networks` annotation on the gateway DaemonSet
template and adjust `hostNetwork`/`POD_IP` accordingly.

## Build and apply

```sh
# Build images (same set as tcp-demo):
docker build -f docker/operator.Dockerfile -t local/mxl-operator:dev .
docker build -f docker/agent.Dockerfile    -t local/mxl-domain-agent:dev .
docker build -f docker/gateway.Dockerfile  -t local/mxl-fabrics-gateway:dev .
docker build -f docker/shim.Dockerfile     -t local/mxl-shim:dev .

kubectl apply -k examples/rdma-demo/
```

## Inspect

```sh
kubectl -n mxl-system get mxlnodecapabilities
kubectl -n mxl-system get mxlflowmirrors -o wide
kubectl -n mxl-system logs ds/mxl-fabrics-gateway | grep -i verbs
```

The `MxlNodeCapabilities` resource for each node should list
`verbs` under `status.providers`. The `MxlFlowMirror` for this
demo should reach `phase: Ready` with a `status.targetInfo`
populated by libmxl-fabrics' verbs endpoint encoding.

## EFA variant

To run the same demo on AWS with Elastic Fabric Adapter, swap
`--providers=verbs` for `--providers=efa` in the gateway DaemonSet
and `provider: verbs` for `provider: efa` in the receiver. The
`/dev/infiniband` mount and `IPC_LOCK` cap still apply; remove
the SR-IOV / Multus knob (EFA pods don't use Multus). The AWS EFA
driver presents its devices under the same character-device tree
as InfiniBand, so no further mounts are needed.
