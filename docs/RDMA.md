# RDMA prerequisites

The mxl-fabrics-gateway advertises `verbs` or `efa` when
libmxl-fabrics finds a device for them on the node and
`--providers` admits them. The code path through libmxl-fabrics is
the same; the host plumbing is what differs. This document
collects the prerequisites that have to be in place on the worker
nodes for either provider to actually work, and how to tell the
gateway which of a node's NICs the fabric is.

## verbs (RoCEv2 / InfiniBand)

### Host

- Kernel modules for the NIC vendor: `mlx5_ib` (Mellanox / NVIDIA
  ConnectX), `bnxt_re` (Broadcom Stingray), `irdma` (Intel E810),
  `qedr` (Marvell FastLinQ), `efa` is its own provider -- see
  below.
- `rdma-core` userspace (provides `libibverbs`, `librdmacm`,
  `ibstat`, `rdma`, ...). Most distros' default package set.
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
- `/dev/infiniband/{rdma_cm,uverbs0,...}` present and readable by
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

### Choosing host vs. NAD-attached networking

The chart defaults to `gateway.hostNetwork: true` because the
fabric `TargetInfo` embeds a `host:port` a peer dials, and a CNI
overlay IP is not reachable cross-node. That default fits KIND
and any single-NIC topology where the same interface carries
both cluster traffic and RDMA traffic.

On clusters with a dedicated RDMA fabric -- typical for bare-metal
RoCEv2 deployments where the RoCE NIC sits on its own VLAN, often
bonded -- hostNetwork pins the gateway's bind address to `POD_IP`,
which the downward API resolves to the cluster-network interface,
not the RDMA fabric. `rdma_bind_addr` returns ENODEV. The cure is
to disable hostNetwork, attach a NAD that places the pod on the
RDMA fabric, and let libfabric pick the in-pod netdev. The
snippet below is a Helm values override for `charts/mxl-k8s`,
applied via `helm install -f <file>` (or merged into a
HelmRelease's `values:`):

```yaml
gateway:
  hostNetwork: false
  dnsPolicy: ClusterFirst
  podAnnotations:
    k8s.v1.cni.cncf.io/networks: network/rdma-roce
  flags:
    # Explicit empty -> --bind-address= (bare equals). Suppresses
    # the POD_IP fallback so libfabric+FI_VERBS_IFACE pick the
    # in-pod RDMA netdev.
    bindAddress: ""
  rdma:
    enabled: true
    # `k8s-rdma-shared-dev-plugin` or similar advertises the HCA
    # as an extended resource. The chart merges `<name>: 1` into
    # requests and limits when this is set.
    resourceName: rdma/hca_shared_devices
    extraEnv:
      - name: FI_VERBS_IFACE
        # In-pod netdev name (typically `net1` when the NAD is the
        # pod's only secondary network). NOT the host PF.
        value: net1
```

`FI_VERBS_IFACE` names the in-pod netdev -- usually `net1` for a
single secondary attachment, `net2` for the second, and so on.
With `hostNetwork: false` the gateway runs in the pod's netns and
cannot see the host PF, so a host-side name like `enp65s0f0` does
not resolve. (With `hostNetwork: true` the gateway shares the host
netns and the host PF name is what `FI_VERBS_IFACE` must use.)

A complete fixture demonstrating the bare-metal pattern lives at
`charts/mxl-k8s/tests/values/full-rdma-nad.yaml`.

The downward API exposes only the primary pod IP via
`fieldRef: status.podIP`; addresses e.g. Multus assigns on secondary
interfaces are not addressable through `fieldRef` today. In the
rare case libfabric cannot self-resolve a device even with
`FI_VERBS_IFACE` -- for example when whereabouts assigns an
address with a CIDR libfabric does not match against any of the
HCA's GIDs -- the workaround today is operator-side: pin
whereabouts to a per-node static allocation and set
`gateway.flags.bindAddress: <that-IP>` explicitly. Future
versions may add a built-in bind-address file flag; until then,
this is what the chart supports.

### Multus / SR-IOV

If pods can't share the host's RDMA NIC (for example because
multiple tenants need isolation, or the NIC supports SR-IOV and
you want one VF per pod), use Multus to attach a dedicated VF as
a secondary network. The gateway then runs *without* `hostNetwork`
and binds verbs to the secondary interface via `FI_VERBS_IFACE`,
following the NAD-attached pattern above.

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
- libmxl-fabrics moves grains over one-sided RDMA writes: the
  initiator registers its source region with `FI_WRITE` and the
  target registers its destination region with `FI_REMOTE_WRITE`.
  EFA support alone is therefore not sufficient -- the instance
  must also support RDMA write. Pick a row whose RDMA-write
  column reads `Yes` in AWS's [Supported instance types
  table](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html#efa-instance-types).
  Setting up a flow on an EFA-enabled instance without RDMA-write
  support fails at memory-region registration; upstream context
  is in [dmf-mxl/mxl#516](https://github.com/dmf-mxl/mxl/issues/516).

### Per-pod

- `securityContext.capabilities.add: ["IPC_LOCK", "SYS_RESOURCE"]`,
  same reason as verbs.
- Bind-mount `/dev/infiniband` from the host.
- Nothing on the gateway DaemonSet: the default `--providers=any`
  advertises efa on the nodes that have an adapter and leaves it
  off the ones that do not. Set `--providers` only to keep efa out
  of consideration where the hardware supports it.
- `MxlFlowMirror.spec.provider: efa` (or
  `MxlReceiver.spec.provider: efa`) to pin a mirror rather than
  letting `selection.Resolve` pick from what both nodes report.
- Multus is *not* the right tool for EFA pods -- EFA is exposed
  via the host's network namespace. `hostNetwork: true` on the
  gateway DaemonSet (as in the rdma-demo example) keeps the
  configuration straightforward.

## How the gateway exposes capabilities

The gateway publishes one `MxlNodeCapabilities` per node from a
libmxl-fabrics interface enumeration (`mxlFabricsGetInterfaces`),
refreshed on every `--resync-period` tick for as long as it runs,
so a link that goes down or a driver that loads late is picked up
without a restart.

Each provider entry carries the devices found for it:

```console
$ kubectl get mxlnodecapabilities node-a -o yaml
status:
  conditions:
    - type: Probed
      status: "True"
      reason: ProbeComplete
      message: libmxl-fabrics reports 4 provider(s) on this node
  providers:
    - name: efa
      deviceCount: 0
    - name: verbs
      deviceCount: 1
      interfaces:
        - address: 10.20.53.13
          device: mlx5_0
          linkState: up
          linkSpeedBitsPerSecond: 100000000000
          maxMessageSize: 1073741824
          pciAddress: "0000:41:00.1"
    - name: tcp
      deviceCount: 1
      interfaces:
        - address: 10.20.53.13
          device: eth0
```

`deviceCount: 0` means libmxl-fabrics knows the provider but found
no usable device here. A provider missing from the list entirely
was excluded by `--providers`. `selection.Resolve` reads the
difference: a provider with no device is dropped from the
intersection instead of being preferred, which is what lets one
DaemonSet serve a cluster where only some nodes carry an EFA
adapter or an HCA.

The `Probed` condition is what makes `deviceCount` readable. A
gateway that reports no probe published a configured list where
every count is zero whether or not the provider works, so
consumers keep treating those entries as available. During a
rolling upgrade both shapes are present in the cluster at once.

`version` is unset on every entry: libmxl-fabrics exposes no
per-provider version through its C API.

Which fields under `interfaces` are populated depends on the
provider and the hardware. The libmxl-fabrics header describes the
underlying attributes as best-effort, and an interface with no
physical NIC behind it (loopback, a veth pair, a container's
`eth0`) reports a device name but no link state, link speed, or
PCI address.

## Scoping the fabric on a multi-NIC node

A node in a broadcast plant carries NICs with different jobs: ST
2110 essence, the MXL fabric, cluster traffic, out-of-band
management. libfabric enumerates all of them, including loopback,
and nothing it reports says which is which. That is site policy,
not a property of the hardware, so the fabric has to be declared.

Three flags narrow it, and all three apply to both what a node
advertises and what a mirror binds, so a node never promises an
interface a setup would refuse:

| Flag | Chart value | Effect |
| --- | --- | --- |
| `--fabric-cidr` | `gateway.flags.fabricCidr` | Keeps only interfaces whose address is inside one of the given prefixes. |
| `--fabric-device` | `gateway.flags.fabricDevice` | Keeps only the named devices, as the provider names them (the kernel netdev name for tcp). |
| `--fabric-min-link-speed` | `gateway.flags.fabricMinLinkSpeed` | Rejects interfaces below a link speed, given in bits per second (`25G`). |

Loopback is always excluded: every mirror the gateway sets up
spans two nodes, and a peer handed `127.0.0.1` in a `TargetInfo`
dials itself. An interface whose link the provider reports as
`down` is always excluded too.

Two rules reject an interface whose detail is missing rather than
admitting it. `--fabric-device` needs a device name to match, and
`--fabric-min-link-speed` needs a speed to compare; an interface
that reports neither cannot be shown to satisfy either. Since most
interfaces with no physical NIC report no speed at all, a link
speed floor excludes them as a side effect.

`--bind-address` still narrows the enumeration to a single address
when set, and is the simplest scoping there is on a node whose
fabric address is known and stable. The flags above are what a
node needs when it is not: with `bindAddress: ""`, which the
NAD-attached pattern below requires, every interface the pod can
see is otherwise a candidate.

A node that ends up advertising nothing logs the count of excluded
interfaces and the first exclusion with its reason; the full list
appears with `gateway.flags.zapLogLevel: debug`.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `MxlFlowMirror` stuck in `Materializing` | Gateway can't `fi_getinfo` the provider on the local NIC. Check `/dev/infiniband` is mounted and the host module is loaded. |
| Mirror fails with `no usable fabric interface` | Every candidate was excluded. The error names the first exclusion and its reason; check the fabric flags against `kubectl get mxlnodecapabilities <node> -o yaml`. |
| A node advertises `deviceCount: 0` for a provider whose hardware is installed | libmxl-fabrics did not enumerate a usable device. Check the host module and `/dev/infiniband` first, then whether the fabric flags exclude the interface it would have used. |
| Every mirror sits on tcp after an upgrade | Nodes whose gateway has not rolled yet report no `Probed` condition. Check `kubectl get mxlnodecapabilities` for the ones still on the old shape. |
| `RDMA_CM_EVENT_REJECTED` in gateway logs | Both ends agree on the provider but the wire-side handshake fails. For RoCEv2 this is almost always PFC/DSCP misconfiguration on the switches. |
| Throughput far below NIC line rate | PFC pauses too aggressive or wrong traffic class. Use `mlnx_qos`, `ethtool -S` counters. |
| `cannot allocate memory` from libmxl-fabrics | `RLIMIT_MEMLOCK` too low. Bump the host default or rely on the gateway's `SYS_RESOURCE` cap. |
| Verbs fine within a node, fails across | RoCE traffic isn't getting through. Check `ip link`, `ip route`, and the underlying VLAN/MTU/PFC. |
| EFA endpoint setup fails | EFA security group rule missing. EFA traffic flows between instances *only* when an inbound rule allowing all traffic from the same SG is in place. |
