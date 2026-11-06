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
- Nothing for `RLIMIT_MEMLOCK`. A memory registration pins pages
  against that limit only for a process without `CAP_IPC_LOCK`, and
  the gateway runs with it, so the inherited 8 MiB default is not a
  ceiling for the gateway.
- `/dev/infiniband/{rdma_cm,uverbs0,...}` present and readable by
  the container user. The gateway DaemonSet bind-mounts
  `/dev/infiniband` into the pod.
- For RoCEv2 specifically: a network fabric configured with PFC
  (priority-flow-control) on the lossless class, and DSCP markings
  the leaf/spine switches honour. PFC misconfiguration is the
  single most common cause of "the demo runs but throughput is
  awful" reports.

### Per-pod

- `securityContext.capabilities.add: ["IPC_LOCK"]` on the gateway
  container, which exempts the provider's memory registrations from
  `RLIMIT_MEMLOCK`.
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
- Nothing for `RLIMIT_MEMLOCK`, as above.
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

- `securityContext.capabilities.add: ["IPC_LOCK"]`, same reason as
  verbs.
- Bind-mount `/dev/infiniband` from the host.
- Nothing on the gateway DaemonSet: the default `--providers=any`
  advertises efa on the nodes that have an adapter and leaves it
  off the ones that do not. Set `--providers` only to keep efa out
  of consideration where the hardware supports it.
- `MxlFlowMirror.spec.provider: efa` (or
  `MxlReceiver.spec.provider: efa`) to pin a mirror rather than
  letting `selection.Resolve` pick from what both nodes report.
- The gateway image sets `FI_EFA_ENABLE_SHM_TRANSFER=0`: every mirror
  crosses nodes, so the shared-memory path the provider would open
  beside each endpoint carries nothing.
- EFA delivers the RMA writes of one initiator in any order
  (`fi_info -p efa -v` reports no write-after-write ordering). The
  gateway sends each grain as one transfer, and libmxl-fabrics sends
  each audio transfer as a single write.
- Multus is *not* the right tool for EFA pods -- EFA is exposed
  via the host's network namespace. `hostNetwork: true` on the
  gateway DaemonSet (as in the rdma-demo example) keeps the
  configuration straightforward.

## How the gateway exposes capabilities

The gateway publishes one `MxlNodeCapabilities` per node from a
libmxl-fabrics interface enumeration (`mxlFabricsGetInterfaces`),
so a link that goes down or a driver that loads late is picked up
without a restart. The status refreshes every `--resync-period`; the
enumeration behind it runs at most every `--probe-period`, because
each sweep covers every provider libfabric was built with and warns
about those it finds no device for.

The resource is owned by its Node, so it is collected with the node.

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

The `RDMADevicesEnumerated` condition covers the one thing
`deviceCount` cannot say for itself. libfabric builds a provider's
device list once per process, on the first enumeration, and rebuilds
it only for a caller that asks to rescan; the libmxl-fabrics
enumeration entry point takes no flags, so nothing here can ask. A
gateway that first enumerated while the host's RDMA devices were
unusable -- the module not yet loaded, the port not yet up, the
device node not yet mounted -- therefore publishes `deviceCount: 0`
for the rest of its life, and that entry is indistinguishable from
the one a node with no RDMA hardware produces. Every mirror the node
takes part in resolves to tcp and still reaches `Ready`.

The gateway cross-checks the probe against the RDMA devices the host
kernel exposes, read from sysfs on each refresh rather than from the
cached provider list:

```console
status:
  conditions:
    - type: RDMADevicesEnumerated
      status: "False"
      reason: HostDevicesUnenumerated
      message: host exposes dev0 with an active port and no RDMA provider
        enumerated a device; mirrors on this node resolve to a non-RDMA
        provider until the gateway restarts
```

`False` is a discrepancy rather than a proven fault, and the gateway
reports it rather than acting on it. An active port is necessary for
the verbs provider to offer an endpoint but not sufficient -- it also
needs an address on the matching interface -- so a node can hold this
condition legitimately. Restarting the gateway pod is what runs the
enumeration again; nothing reachable from a running process rebuilds
it.

The condition is absent on a gateway whose `--providers` admits no
RDMA provider: such a node advertises none by instruction rather than
by measurement, so the host device list contradicts nothing.

`--rdma-enumeration-liveness` asks the kubelet to perform that restart
rather than waiting for somebody to read the condition. It fails the
liveness probe once the discrepancy has held for
`--rdma-enumeration-grace`, and only for `HostDevicesUnenumerated`:
`HostDevicesUnreadable` withdraws the cross-check rather than a
provider, and a host with no RDMA hardware reports
`HostDevicesRepresented`. Neither fails the probe, and a gateway that
has not completed a probe has observed nothing to fail on.

It is off by default because one condition covers two outcomes: a
gateway that enumerated too early, which a restart clears, and a node
whose devices the built providers never drive, which a restart
repeats. The grace separates a discrepancy that outlasts several
enumerations from one seen once, so it is set in multiples of
`--probe-period`. The check is registered on `/healthz` rather than
`/readyz` deliberately -- readiness withdraws the pod from its
Service, which stops the per-mirror counters being collected without
revisiting the enumeration that caused it.

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

`--bind-address` still narrows to a single address when set, and
is the simplest scoping there is on a node whose fabric address is
known and stable. It applies to the providers that name an
interface by IP, `tcp` and `verbs`. `efa` names one by its EFA GID
and `shm` by the host's name, so neither is held to it and a node
carrying an EFA adapter advertises it whatever the bind address
is. The flags above are what a node needs when the fabric address
is not stable: with `bindAddress: ""`, which the NAD-attached
pattern below requires, every interface the pod can see is
otherwise a candidate.

A node that ends up advertising nothing logs the count of excluded
interfaces and the first exclusion with its reason; the full list
appears with `gateway.flags.zapLogLevel: debug`.

## Placing mirrors on fabric interfaces

A node whose selection keeps more than one interface for a provider
carries every one of them as a port of its own: with `bindAddress:
""` and the fabric scoped by `fabricCidr` or `fabricDevice`, each
selected interface is advertised, and the operator chooses for every
mirror the interface its source leaves on and the one its target
arrives on. The choice is `spec.placement` on the MxlFlowMirror, and
what the two gateways bound is `status.sourceInterface` and
`status.targetInterface`. `kubectl get mxlflowmirrors` shows the
pair and the `Placed` reason; `-o wide` adds the bound pair.

libmxl-fabrics binds a target or initiator to the address of the
interface its setup is handed, so a mirror's endpoints bind the
placed addresses.

### Several ports in one subnet

The ports of a node can all sit in the fabric's one subnet, each with
an address of its own, so every port reaches every other without a
router. The host then has two connected routes for one prefix and
several interfaces that could answer ARP for each address, and needs,
per fabric port:

| Setting | Value | Effect |
| --- | --- | --- |
| `net.ipv4.conf.<port>.arp_ignore` | 1 | A port answers ARP only for its own address, so peers learn each address on the port that holds it. |
| `net.ipv4.conf.<port>.arp_announce` | 2 | ARP requests leaving a port carry that port's address. |
| `net.ipv4.conf.<port>.rp_filter` | 2, or 0 on the port and on `all` | The kernel applies the larger of `conf/all` and the port's value, so 0 on the port alone leaves a strict `all` in force. Strict (1) drops a packet whose source the routing table would reach through another port, which in one subnet is the normal case: both ports hold a connected route to it and only one is preferred. Loose (2) drops only a source with no route at all. |
| A policy rule per port address | `from <address> lookup <table>`, the table holding the subnet route on that port | A socket bound to a port's address leaves on that port rather than on the first matching connected route. |

`verbs` and `efa` connections set up through RDMA CM are pinned to
the bound port by the kernel: binding the source address records its
interface, and the route lookup is restricted to it. `tcp` sockets are
not, which is what the policy rules cover. The ARP settings apply to
every provider.

Ports in separate subnets need none of the policy rules, since each
subnet is a connected route on its own port, but only reach the peers'
ports in the same subnet. `--placement-pairing` keeps pairs within one
subnet when the nodes have them.

A node with one interface per provider, or one bound to
`--bind-address`, has one candidate, and its mirrors bind it exactly
as without placement.

### What is planned against

Load is accounted per port: the device the provider names for an
interface, or its address where it names none. Two addresses of one
device, an IPv4 and an IPv6 address or two addresses on one
link, share that port's capacity. The device is named per provider
(the netdev for `tcp`, the RDMA device for `verbs`), so mirrors of two
providers on one NIC are accounted on two ports. Every port is planned
at a capacity per direction, sending and receiving counted separately:

| Source | Chart value | Meaning |
| --- | --- | --- |
| Provider link speed | - | Default capacity. An interface reporting none is never counted full. |
| `--fabric-port-capacity` | `gateway.flags.fabricPortCapacity` | `<match>=<quantity>` overrides, match a device name, an address or a CIDR; first match wins. |
| `--fabric-shared-budget` | `gateway.flags.fabricSharedBudget` | `<scope>=<quantity>[:<mode>]` caps shared by several interfaces. Scope `nic` makes one per PCI device over its functions, `host` one over every interface. Mode `ingress` (default) counts mirrors received, `egress` mirrors sent, `combined` both against one cap. Off unless set. |
| `--fabric-max-utilization` | `gateway.flags.fabricMaxUtilization` | Percent of a capacity this node class may commit; 0 uses the operator's. |
| `--placement-max-utilization` | `operator.flags.placementMaxUtilization` | Default percent, 80. |

Grains leave as whole-grain bursts at line rate, so a receiver can
overflow while its average load is below the link speed; the
utilization ceiling is the headroom kept for that.

A mirror is charged its flow's `status.bitsPerSecond`, which the
gateway on the flow's origin node reads off libmxl (grain size times
grain rate, or sample width times channels times sample rate), plus
`--placement-protocol-overhead` percent (default 5). A flow that has
not published a rate within `--placement-demand-wait` (default 5s) of
the mirror's creation is charged `--placement-provisional-demand`
(default 0, placing by mirror count alone), and charged its real rate
once it publishes one. `MxlNodeCapabilities` shows each interface's
`capacityBitsPerSecond`, `subnet`, `budgets` and `numaNode`, and the
node's `budgets`.

A data flow's grain size is the maximum its definition declares, so its
rate is an upper bound on what it carries.

### How a pair is chosen

Candidates are every pair of a source interface and a target
interface for the mirror's provider whose links are not down. With
`--placement-pairing=prefer-same-subnet` (default) only pairs within
one subnet are kept when any exists; `same-subnet` keeps only those,
`any` keeps every pair. Link-local addresses are left out whenever
the node offers another: every node's link-local addresses share one
prefix and would otherwise read as a subnet of their own. Among them:

1. a pair with room for the demand on both interfaces and every
   budget they draw on beats one without;
2. for a member of an MxlFlowRedundancySet, a pair sharing no
   port with the set's other members arriving on the same node
   beats one sharing only a sending port, which beats one
   sharing a receiving port;
3. then the most headroom left on the tighter side, the least load,
   the fewest mirrors and the lowest addresses.

A mirror no pair has room for is placed and reported `Overcommitted`
with `--placement-overcommit=place` (default), on the pair the same
order picks among the pairs without room: disjointness from its
redundancy siblings first, then the most headroom, which is negative.
With `hold` it is left unplaced, not set up, and reported `Held`. A set
whose members share a port reports `PathsDisjoint=False` naming it.

### When a placement changes

A placement is not moved because load changed, and there is no
automatic rebalance. A mirror is placed again only when one of its
two interfaces is no longer advertised for its provider (link down,
deselected, capability change, as the gateway's next probe reports
it; see `gateway.flags.probePeriod`), when its provider or source node
changes to one the interfaces do not belong to, or when
`spec.placement` is cleared by hand. A placement written by hand is
kept while both addresses are advertised. Its gateways then rebuild
the side whose interface changed.

The operator places a mirror within its own reconcile, before the
target side opens. A target gateway on a node offering more than one
interface waits up to `gateway.flags.placementWait` (default 10s)
after the mirror's creation for a placement, and then binds the first
interface as an unplaced mirror does; that bound only matters when
the operator is down or slow. A mirror its gateways bound before
placement existed keeps the interfaces it is bound to.

### Metrics

Series per metric are bounded as the last column says: N is the nodes
reporting capabilities, P the ports a node reports, L the links under
them, B a node's shared budgets, M the MxlFlowMirrors, S the
MxlFlowRedundancySets.

Operator, placement as committed (the leader's replica only):

| Metric | Labels | Series |
| --- | --- | --- |
| `mxl_operator_port_committed_bits_per_second` | `node`, `port`, `direction` | N x P x 2 |
| `mxl_operator_port_mirrors` | `node`, `port`, `direction` | N x P x 2 |
| `mxl_operator_port_capacity_bits_per_second` | `node`, `port` | N x P; absent while capacity is unknown |
| `mxl_operator_port_plannable_bits_per_second` | `node`, `port` | N x P; capacity times the node's ceiling |
| `mxl_operator_node_max_utilization_ratio` | `node` | N; the node's own ceiling, or the operator's |
| `mxl_operator_budget_capacity_bits_per_second`, `_plannable_bits_per_second` | `node`, `budget`, `scope`, `mode` | N x B |
| `mxl_operator_budget_committed_bits_per_second` | `node`, `budget`, `scope`, `mode`, `direction` | N x B x 2 |
| `mxl_operator_placement_max_utilization_ratio`, `_protocol_overhead_ratio`, `_provisional_demand_bits_per_second`, `_demand_wait_seconds` | | 1 each; the operator's flags |
| `mxl_operator_placement_policy_info` | `overcommit`, `pairing` | 1 |
| `mxl_operator_mirrors` | `phase`, `placement` | phases x Placed reasons; `None` where a mirror has neither yet |
| `mxl_operator_mirror_demand_bits_per_second` | `namespace`, `mirror`, `domain`, `flow_id`, `source_node`, `target_node`, `provider` | M |
| `mxl_operator_mirror_placement_info` | the above, `source_port`, `target_port`, `placement`, `phase` | M |
| `mxl_operator_redundancyset_paths_disjoint` | `redundancyset`, `reason` | S |
| `mxl_operator_mirror_placement_seconds` | | histogram: time from a mirror's creation to its first placement |

Gateway, per node:

| Metric | Labels | Series |
| --- | --- | --- |
| `mxl_gateway_fabric_port_info` | `node`, `provider`, `port`, `address`, `link`, `netdev`, `rdma_device`, `rdma_port`, `pci_address`, `numa_node` | one per published interface |
| `mxl_gateway_fabric_max_utilization_ratio` | `node` | 1; absent when the gateway leaves it to the operator |
| `mxl_gateway_fabric_budget_bits_per_second` | `node`, `budget`, `scope`, `mode` | B |
| `mxl_gateway_link_received_bytes_total`, `_transmitted_bytes_total` | `node`, `link`, `source` | L x 3 |
| `mxl_gateway_link_up`, `mxl_gateway_link_speed_bits_per_second`, `mxl_gateway_link_carrier_changes_total`, `mxl_gateway_link_xmit_wait_total` | `node`, `link` | L |
| `mxl_gateway_link_errors_total` | `node`, `link`, `source`, `counter` | L x 15 |
| `mxl_gateway_link_pause_frames_total` | `node`, `link`, `direction` | L x 2 |
| `mxl_gateway_interface_transmitted_bytes_total`, `_received_bytes_total` | `node`, `interface` | addresses; bytes the live mirrors moved |
| `mxl_gateway_interface_mirrors` | `node`, `interface`, `direction` | addresses x 2; mirrors bound |

A port is what placement accounts load on: the device the provider
names, or the address. A link is the physical port under it: the
netdev, or the RDMA device where there is none. The tcp and verbs ports
of one NIC are two ports on one link. Only interfaces backed by a
device are links; a bridge, bond or veth has its info series with an
empty `link` and no counters. An RDMA device over a bond (RoCE link
aggregation) is the exception: its port names the bond as its netdev,
so the link is the bond, with the RDMA device's counters and the bond's
netdev statistics but no `mac` source, since a bond reports no MAC
statistics. The member ports are not exported.

A link's bytes are exported per `source`, because no one kernel
interface counts the same traffic on every driver:

| `source` | Kernel interface | Counts |
| --- | --- | --- |
| `mac` | ethtool netlink standard statistics, group `eth-mac` (`OctetsReceivedOK`, `OctetsTransmittedOK`) | Everything the MAC sent or received: the wire |
| `rdma` | `/sys/class/infiniband/<dev>/ports/<n>/counters/port_{rcv,xmit}_data` times 4, or the port's `hw_counters/{rx,tx}_bytes` where the driver has no port counters | RDMA traffic only |
| `netdev` | `/sys/class/net/<if>/statistics/{rx,tx}_bytes` | The kernel network stack's traffic; on some drivers RDMA traffic too, on others not |

A source the kernel or driver does not report is absent, never zero.
The dashboard reads `mac` where a link has it and otherwise the larger
of `rdma` and `netdev`, a lower bound on the wire. `errors` are the
netdev drop and error statistics, the RDMA port error counters
(`port_rcv_errors`, `port_xmit_discards`, `symbol_error`,
`link_downed`, ...) and the MAC's `FrameCheckSequenceErrors`. Pause
frames are 802.3x pause only; per-priority flow control is not in the
standard statistics. With a `gateway.variants` entry, the chart's
ServiceMonitor labels every gateway series with `variant`.

#### Replication: where each unit ends

The gateway records every mirror through
[libmxl-monitoring](https://github.com/marquies/libmxl-monitoring)'s
replication roles: one handle per mirror on the node that sources it
(`mxl_gateway_source_*`) and one on the node that targets it
(`mxl_gateway_target_*`). The `flow` label is the mirror,
`<namespace>/<name>`, the same on both nodes; it joins
`mxl_operator_mirror_placement_info` through
`label_join(..., "flow", "/", "namespace", "mirror")`. Units are grains
for a discrete flow and samples for a continuous one.

Every unit the source meets ends in exactly one of:

| Where | Series | Whose |
| --- | --- | --- |
| delivered within one grain or batch interval | `mxl_gateway_source_transfers_total{result="delivered"}` | - |
| delivered after it | `mxl_gateway_source_transfers_total{result="late"}` | fabric (latency) |
| passed over by a resync while complete | `mxl_gateway_source_transfers_total{result="dropped"}` | fabric or gateway |
| outstanding when the target was lost, or not sent until a rebuilt loop resumed | `mxl_gateway_source_transfers_total{result="failed"}` | fabric |
| reused before the loop read it | `mxl_gateway_source_skipped_total` | fabric or gateway |
| invalid, incomplete or another index's grain in the ring | `mxl_gateway_source_reads_total{result=~"invalid\|incomplete\|missing"}` | the writer |

libmxl's writer flags the grains it jumps over or leaves incomplete
invalid, so a grain the writer never completed is never counted as
delivered or as fabric loss. Fabric loss is `dropped + failed +
skipped` over every unit the source held complete. On the target,
`mxl_gateway_target_writes_total` counts commits,
`mxl_gateway_target_invalid_input_total` arrivals committed invalid
(`invalid` from the source, `missing` for a slot a later write
overtook), and `mxl_gateway_target_gaps_total` the units no arrival
filled below one that did, at most the ring per gap.

`mxl_gateway_source_reads_total{result="too_early"}` counts a grain the
gateway still waited for a grace period (one grain, or one batch of
samples, plus 50 ms) after its media timestamp: the writer's lateness.
`mxl_gateway_source_arrival_seconds` and
`mxl_gateway_target_commit_seconds` are the library's latency
histograms against media timestamps, so source and target compare
directly. The library's other families (`flow_state`,
`flow_delivering`, `open_failures_total`, `opens_total`,
`lost_total`, `age_seconds`, `stalls_total`, `flow_info`) are exported
as it renders them.

Series per mirror and role are fixed: 25 to 27 counters and gauges
and one histogram of 20 series (17 bounds, +Inf, sum, count). The
`mxl_gateway_mirror_*` families above remain;
`mxl_gateway_mirror_source_unsent_grains_total` and
`mxl_gateway_mirror_target_invalid_grains_total` count a subset of the
same events without the source's state.

Utilization is the committed or measured rate over
`capacityBitsPerSecond`, or over the plannable gauge.

## Clusters whose nodes differ

Everything above assumes every node can do the same thing. When they
cannot -- some nodes carry an HCA or an EFA adapter and the rest do
not -- one gateway DaemonSet cannot serve them all, and the reason
is not about MXL at all.

This is not a cloud problem. An on-prem cluster where only some nodes
have RDMA-capable NICs has exactly the same shape as an AWS cluster
where only some instances carry an EFA adapter; the provider and the
device plugin differ, nothing else does.

Access to a fabric device goes through an extended resource that a
device plugin advertises -- `devic.es/rdma` from
generic-device-plugin, `rdma/hca_shared_devices` from
k8s-rdma-shared-dev-plugin, `vpc.amazonaws.com/efa` from
aws-efa-k8s-device-plugin, or whatever a site's plugin is configured
to expose. The request is not bookkeeping. The plugin's `Allocate`
response is what injects the device nodes and the cgroup permissions
that let the container open them, which is why a bind mount of
`/dev/infiniband` on its own does not grant access under the cgroup-v2
device controller.

A pod requesting a resource no node advertises is unschedulable, and
a DaemonSet carries exactly one pod template. So the request cannot be
made conditional:

- Request it, and the gateway never schedules on the nodes without
  the hardware.
- Do not request it, and the gateway cannot open the device on the
  nodes that have it.

Kubernetes has no conditional resource request. Dynamic Resource
Allocation is where that is heading, but it needs driver support this
chart cannot assume. The available answer is one DaemonSet per node
class, which is what `gateway.variants` renders.

### Placing the variants

Label the nodes by which fabric they carry, and enable the variant
matching each value. Keying on the value rather than on the presence
of a label means one mechanism covers every fabric a cluster has, in
any combination:

```yaml
gateway:
  variants:
    verbs:
      enabled: true
      nodeSelector:
        mxl.qvest-digital.com/fabric: verbs
      rdma:
        resourceName: devic.es/rdma
    efa:
      enabled: true
      nodeSelector:
        mxl.qvest-digital.com/fabric: efa
      tolerations:
        - key: vpc.amazonaws.com/efa
          operator: Exists
          effect: NoSchedule
    tcp:
      enabled: true
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: mxl.qvest-digital.com/fabric
                    operator: DoesNotExist
```

That is the whole override. The provider list, the capabilities, and
the EFA resource name come from the chart's own defaults for each
variant; what a site supplies is what the chart cannot know -- which
label marks the node, which plugin advertises the device, and which
taint the node group carries.

A cluster with only one fabric enables two variants rather than
three, and a variant key beyond the three shipped is allowed. The
shape does not change.

Each entry may override any key under `gateway`; maps merge key by
key with the shared values, lists replace wholesale. What the node
class does not change -- image, probes, update strategy, service
account, and the single metrics Service, which keeps selecting every
variant -- stays shared.

**Exactly one gateway per node.** Two gateways on one node open the
same MXL domain, reconcile the same mirrors, and write the same
`MxlNodeCapabilities`, which is named after the node. Nothing detects
this at runtime today, so the selectors have to be complementary by
construction. Deriving every variant from distinct values of one
label key is what makes overlap impossible; a class defined by the
*absence* of that label needs `nodeAffinity` with `DoesNotExist`,
which a `nodeSelector` cannot express.

Helm cannot read node labels, so it rejects only the combinations
that guarantee overlap: an enabled variant with neither `nodeSelector`
nor `affinity` alongside another, and two enabled variants placed
identically. Everything else is on the operator.

It also rejects a variant given a placement that nobody enabled,
because that shape fails quietly: the chart falls back to the single
unplaced DaemonSet, which comes up healthy on every node while
silently omitting the device request the placed nodes needed. Staging
a variant alongside an enabled one is fine; leaving every one of them
off is what gets caught.

With no variant enabled -- the default -- exactly one DaemonSet
renders from the `gateway` values, unchanged, which is what a uniform
cluster wants. Enabling the first variant renames the workload, so the
old DaemonSet is replaced rather than patched: a DaemonSet's
`spec.selector` is immutable and every variant carries its own.

### Which provider each class advertises

`--providers` is an upper bound, and the probe decides the rest, so a
variant does not have to name its hardware. `providers: [any]` on the
RDMA class advertises whatever libmxl-fabrics finds on each node,
which keeps the values honest if the node group's adapters change.
Naming a provider is for keeping one *out* of consideration.

The tcp class is the exception worth being explicit about: it exists
because those nodes have no fabric device, so `providers: [tcp]`
states that and stops a probe from advertising something the class
was never given access to.

### Taints

Node groups provisioned for RDMA are often tainted, so that unrelated
workloads do not consume the hardware. AWS's EFA node groups
conventionally carry `vpc.amazonaws.com/efa`; an on-prem cluster may
taint its RDMA nodes with anything. A variant selecting such a class
needs the matching toleration next to its resource request, or it
will not schedule at all:

```yaml
    efa:
      enabled: true
      nodeSelector:
        mxl.qvest-digital.com/fabric: efa
      tolerations:
        - key: vpc.amazonaws.com/efa
          operator: Exists
          effect: NoSchedule
```

`charts/mxl-k8s/tests/values/mixed-fabric.yaml` is a rendered fixture
covering a verbs class, an EFA class, and a tcp catch-all together.

### The hostPath mount

`rdma.enabled` adds `IPC_LOCK`, which exempts the providers' memory
registrations from `RLIMIT_MEMLOCK`, and is independent of how the
device is reached.
`rdma.mountInfiniband` controls the `/dev/infiniband` bind mount
separately, because a node whose devices come from a device plugin
wants the capabilities without the mount.

Where the mount is used, `rdma.infinibandHostPathType` picks between
`Directory`, which holds the pod in `ContainerCreating` while the path
is absent, and `DirectoryOrCreate`, which creates it. On a DaemonSet
placed only on RDMA-capable nodes the first is a useful assertion; on
one that also lands elsewhere it is a hang, which is another reason
those nodes belong in their own variant.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `MxlFlowMirror` `Degraded` with `TargetProgress=False/OpenTargetFailed` | Gateway can't `fi_getinfo` the provider on the local NIC. Check `/dev/infiniband` is mounted and the host module is loaded. `status.targetAttemptCount` carries the length of the failure run. |
| Mirror fails with `no usable fabric interface` | Every candidate was excluded. The error names the first exclusion and its reason; check the fabric flags against `kubectl get mxlnodecapabilities <node> -o yaml`. |
| A node advertises `deviceCount: 0` for a provider whose hardware is installed | libmxl-fabrics did not enumerate a usable device. Check `RDMADevicesEnumerated` first: `False` means the host exposes an active device this process never enumerated, which only a gateway restart clears. Otherwise check the host module and `/dev/infiniband`, then whether the fabric flags exclude the interface it would have used. |
| Every mirror on one node sits on tcp while its peers use verbs | The gateway enumerated before the node's RDMA device was usable, and libfabric does not rebuild a provider's device list within a process. `RDMADevicesEnumerated=False/HostDevicesUnenumerated` reports it; restart the gateway pod on that node. |
| Every mirror sits on tcp after an upgrade | Nodes whose gateway has not rolled yet report no `Probed` condition. Check `kubectl get mxlnodecapabilities` for the ones still on the old shape. |
| Gateway pods Pending with `Insufficient <resource>` | The DaemonSet requests a device-plugin resource on nodes that do not advertise it. Split those nodes into their own `gateway.variants` entry. |
| Gateway pods stuck in `ContainerCreating` | The `/dev/infiniband` hostPath is absent and `rdma.infinibandHostPathType` is `Directory`. Either place the DaemonSet only on RDMA-capable nodes or set `rdma.mountInfiniband: false` and reach the device through a device plugin. |
| Gateway opens the device only when privileged | The container is reaching `/dev/infiniband` through the bind mount alone, which does not pass the cgroup-v2 device controller. Request the resource the site's device plugin advertises via `rdma.resourceName`. |
| Two gateway pods on one node | Two `gateway.variants` entries match the same node. Their selectors have to be complementary; both pods open the same domain and overwrite each other's `MxlNodeCapabilities`. |
| `RDMA_CM_EVENT_REJECTED` in gateway logs | Both ends agree on the provider but the wire-side handshake fails. For RoCEv2 this is almost always PFC/DSCP misconfiguration on the switches. |
| Throughput far below NIC line rate | PFC pauses too aggressive or wrong traffic class. Use `mlnx_qos`, `ethtool -S` counters. |
| `cannot allocate memory` from libmxl-fabrics | The gateway runs without `IPC_LOCK`, so its registrations count against `RLIMIT_MEMLOCK`. Set `rdma.enabled`. |
| Verbs fine within a node, fails across | RoCE traffic isn't getting through. Check `ip link`, `ip route`, and the underlying VLAN/MTU/PFC. |
| EFA endpoint setup fails | EFA security group rule missing. EFA traffic flows between instances *only* when an inbound rule allowing all traffic from the same SG is in place. |
