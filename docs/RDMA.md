# RDMA prerequisites

The mxl-fabrics-gateway claims `verbs` or `efa` providers based on
what's passed to `--providers`. The code path through
libmxl-fabrics is the same; the host plumbing is what differs.
This document collects the prerequisites that have to be in place
on the worker nodes for either provider to actually work.

## verbs (RoCEv2 / InfiniBand)

### Host

- Kernel modules for the NIC vendor: `mlx5_ib` (Mellanox / NVIDIA
  ConnectX), `bnxt_re` (Broadcom Stingray), `irdma` (Intel E810),
  `qedr` (Marvell FastLinQ), `efa` is its own provider — see
  below.
- `rdma-core` userspace (provides `libibverbs`, `librdmacm`,
  `ibstat`, `rdma`, …). Most distros' default package set.
- `RLIMIT_MEMLOCK` set to `infinity` or at least multiple GiB.
  Common patterns:
  - `/etc/security/limits.d/rdma.conf`:
    ```
    *       hard    memlock         unlimited
    *       soft    memlock         unlimited
    ```
  - For containerd / cri-o, set `default_ulimits` or pass
    `LimitMEMLOCK=infinity` in the runtime's systemd unit.
  - The mxl-fabrics-gateway pod also asks for `SYS_RESOURCE` so
    it can raise its own limit if the host default is low.
- `/dev/infiniband/{rdma_cm,uverbs0,…}` present and readable by
  the container user. The gateway DaemonSet bind-mounts
  `/dev/infiniband` into the pod.
- For RoCEv2 specifically: a network fabric configured with PFC
  (priority-flow-control) on the lossless class, and DSCP markings
  the leaf/spine switches honour. PFC misconfiguration is the
  single most common cause of "the demo runs but throughput is
  awful" reports.

### Per-pod

- `securityContext.capabilities.add: ["IPC_LOCK", "SYS_RESOURCE"]`
  on the gateway container. `IPC_LOCK` lets libmxl call `mlock(2)`
  on the tmpfs grain pages; `SYS_RESOURCE` lets the process raise
  its own `RLIMIT_MEMLOCK` when the host default is low.
- Bind-mount `/dev/infiniband` from the host.
- Optional knobs the gateway forwards into libfabric via env:
  - `FI_VERBS_IFACE=<ifname>`: pin verbs to a specific interface
    (default: libfabric picks the first capable one).
  - `FI_VERBS_DEVICE_NAME=<dev>`: pin to a specific verbs device
    (e.g. `mlx5_0`); useful with multiple HCAs.
  - `FI_LOG_LEVEL=Info` (or `Debug`): noisy but invaluable when
    diagnosing why a Setup fails.

### Multus / SR-IOV

If pods can't share the host's RDMA NIC (for example because
multiple tenants need isolation, or the NIC supports SR-IOV and
you want one VF per pod), use Multus to attach a dedicated VF as
a secondary network. The gateway then runs *without* `hostNetwork`
and binds verbs to the secondary interface via `FI_VERBS_IFACE`.

A working pattern: a `NetworkAttachmentDefinition` per RDMA
fabric, an SR-IOV device plugin allocating VFs to pods, and the
gateway DaemonSet annotated with
`k8s.v1.cni.cncf.io/networks: <namespace>/<nad-name>`. Out of
scope for this document; see the SR-IOV CNI and rdma-shared-dev
device-plugin upstream docs.

## efa (AWS Elastic Fabric Adapter)

EFA is exposed via the same libfabric provider machinery, but its
host setup is AWS-specific.

### Host (worker AMI / userdata)

- Install the EFA installer from AWS
  (https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html);
  this lays down the `efa` kernel module and the userspace
  libfabric EFA provider plugin.
- `RLIMIT_MEMLOCK` as above.
- `/dev/infiniband/uverbs0` is present once the module is loaded;
  no PFC or DSCP knobs to fuss with (EFA is its own protocol on
  the Nitro fabric).
- Use one of the EFA-capable instance families. AWS publishes
  the list per-region; broadly anything in the `c5n`, `c6gn`,
  `c7gn`, `p4d`, `p5`, `m5dn`, `r5n` families and several others.

### Per-pod

- `securityContext.capabilities.add: ["IPC_LOCK", "SYS_RESOURCE"]`,
  same reason as verbs.
- Bind-mount `/dev/infiniband` from the host.
- `--providers=efa` on the gateway DaemonSet.
- `MxlFlowMirror.spec.provider: efa` (or
  `MxlReceiver.spec.provider: efa`).
- Multus is *not* the right tool for EFA pods — EFA is exposed
  via the host's network namespace. `hostNetwork: true` on the
  gateway DaemonSet (as in the rdma-demo example) keeps the
  configuration straightforward.

## How the gateway exposes capabilities

At startup the gateway publishes one `MxlNodeCapabilities` per
node listing the providers it was configured with. Today this is
trust-the-config; the gateway does *not* yet probe libfabric to
verify the underlying NIC and driver actually support the claimed
provider. A misconfiguration (e.g. `--providers=verbs` on a node
without InfiniBand) surfaces at the first `MxlFlowMirror` Setup
rather than at boot.

Real per-provider probing — a synthetic flow created at startup
to exercise each declared provider — is a deliberate follow-up;
see the project's deferred-work memory.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `MxlFlowMirror` stuck in `Materializing` | Gateway can't `fi_getinfo` the provider on the local NIC. Check `/dev/infiniband` is mounted and the host module is loaded. |
| `RDMA_CM_EVENT_REJECTED` in gateway logs | Both ends agree on the provider but the wire-side handshake fails. For RoCEv2 this is almost always PFC/DSCP misconfiguration on the switches. |
| Throughput far below NIC line rate | PFC pauses too aggressive or wrong traffic class. Use `mlnx_qos`, `ethtool -S` counters. |
| `cannot allocate memory` from libmxl-fabrics | `RLIMIT_MEMLOCK` too low. Bump the host default or rely on the gateway's `SYS_RESOURCE` cap. |
| Verbs fine within a node, fails across | RoCE traffic isn't getting through. Check `ip link`, `ip route`, and the underlying VLAN/MTU/PFC. |
| EFA endpoint setup fails | EFA security group rule missing. EFA traffic flows between instances *only* when an inbound rule allowing all traffic from the same SG is in place. |
