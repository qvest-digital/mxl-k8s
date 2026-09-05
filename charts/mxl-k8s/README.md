# mxl-k8s

Kubernetes control plane for MXL (Media eXchange Layer). Installs the operator, per-node agent and gateway, CRDs, and RBAC.

![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)

## Prerequisites

- Helm >= 3.14 (chart uses `values.schema.json`).
- Kubernetes >= 1.28.
- Kernel >= 5.17 on every node that runs the agent (`fanotify` with
  `FAN_REPORT_DFID_NAME`).
- Prometheus Operator CRDs (`monitoring.coreos.com`) optional, only
  when `*.metrics.serviceMonitor.enabled=true`.

## Install via Helm CLI

<!-- x-release-please-start-version -->
```sh
helm install mxl oci://ghcr.io/qvest-digital/mxl-k8s/charts/mxl-k8s \
  --version 1.1.0-rc.13 \
  --namespace mxl-system --create-namespace
```
<!-- x-release-please-end -->

The chart installs the CRDs from its `crds/` directory on first
install. Helm itself never updates or removes them.

## Install via FluxCD

<!-- x-release-please-start-version -->
```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: mxl-k8s
  namespace: flux-system
spec:
  type: oci
  interval: 1h
  url: oci://ghcr.io/qvest-digital/mxl-k8s/charts
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: mxl-k8s
  namespace: mxl-system
spec:
  interval: 10m
  chart:
    spec:
      chart: mxl-k8s
      version: "1.1.0-rc.13"
      sourceRef:
        kind: HelmRepository
        name: mxl-k8s
        namespace: flux-system
  install:
    createNamespace: true
    crds: Create
  upgrade:
    crds: CreateReplace
  values: {}
```
<!-- x-release-please-end -->

Track `:dev` instead of a semver range by setting `version: ">=0.0.0-0"`
on the HelmRelease. The chart is published as `0.0.0-dev+sha.<short>`
on every merge to `main`; Helm encodes the `+` as `_` in the OCI tag.

## Bundled image versions

Operator, agent, and gateway are released together with this chart
under one repository version, so a chart at `X` bundles the images
published as `vX`. `<component>.image.tag` ships empty and resolves to
`v{{ .Chart.AppVersion }}` at render time; there is no
per-component pin to read or keep current.

The bundled-versions table for each released chart appears in the
GitHub release notes
(`https://github.com/qvest-digital/mxl-k8s/releases/tag/v<X>`). The
`:dev` chart published on every merge to `main` sets every tag to
`dev` so it tracks main HEAD images.

Overriding stays available per component: `--set
<comp>.image.tag=<tag>` and `--set <comp>.image.digest=sha256:...`
both win over the appVersion default, and a digest wins over a tag.

## Common overrides

### Minimal tcp deployment

```yaml
gateway:
  flags:
    providers: [tcp]
  rdma:
    enabled: false
```

### Full RDMA deployment (verbs + shm)

```yaml
operator:
  replicaCount: 3
  flags:
    leaderElect: true
  podDisruptionBudget:
    enabled: true
    minAvailable: 2
gateway:
  flags:
    providers: [verbs, shm]
  rdma:
    enabled: true
    extraEnv:
      - name: FI_VERBS_IFACE
        value: net1
```

### Private registry with pinned digests

```yaml
global:
  image:
    registry: registry.internal.example.com/mxl-k8s
    pullPolicy: Always
    pullSecrets:
      - name: registry-pull-secret
operator:
  image:
    digest: sha256:<digest>
agent:
  image:
    digest: sha256:<digest>
gateway:
  image:
    digest: sha256:<digest>
```

### IRSA-style ServiceAccount bindings

```yaml
operator:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/mxl-operator
```

## Source Code

* <https://github.com/qvest-digital/mxl-k8s>

## Requirements

Kubernetes: `>=1.28-0`

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| agent | object | `{"affinity":{},"args":[],"containerSecurityContext":{"allowPrivilegeEscalation":false,"capabilities":{"add":["SYS_ADMIN"],"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":false},"enabled":true,"extraContainers":[],"extraEnv":[],"extraVolumeMounts":[],"extraVolumes":[],"flags":{"domainPath":"/run/mxl/domain","healthProbeBindAddress":":8081","intentSocket":"/run/mxl/agent.sock","kubeApiBurst":100,"kubeApiQps":50,"materializeTimeout":"5s","metricsBindAddress":":8080","provider":"","resyncPeriod":"30s","shimPath":"/run/mxl/libmxl-intent.so","zapLogLevel":"info"},"hostPID":true,"hostPath":{"run":"/run/mxl","type":"DirectoryOrCreate"},"image":{"digest":"","pullPolicy":"","repository":"agent","tag":""},"initContainers":[],"livenessProbe":{"httpGet":{"path":"/healthz","port":"probe"},"initialDelaySeconds":5},"metrics":{"service":{"enabled":true,"port":8080,"type":"ClusterIP"},"serviceMonitor":{"enabled":false,"interval":"30s","labels":{},"metricRelabelings":[],"relabelings":[],"scrapeTimeout":"10s"}},"nodeSelector":{},"podAnnotations":{},"podLabels":{},"podSecurityContext":{},"priorityClassName":"","readinessProbe":{"httpGet":{"path":"/readyz","port":"probe"},"initialDelaySeconds":2},"resources":{"limits":{},"requests":{"cpu":"25m","memory":"32Mi"}},"serviceAccount":{"annotations":{},"automountServiceAccountToken":true,"create":true,"labels":{},"name":""},"tolerations":[],"topologySpreadConstraints":[],"updateStrategy":{"rollingUpdate":{"maxUnavailable":1},"type":"RollingUpdate"}}` | Agent: per-node DaemonSet that watches the MXL domain via fanotify, publishes flow state, and serves the LD_PRELOAD intent socket. |
| agent.containerSecurityContext.runAsNonRoot | bool | `false` | fanotify on /run/mxl/domain reads host inodes, so the agent currently runs as root. PSA-restricted environments cannot host the agent without a policy exception. |
| agent.flags.kubeApiBurst | int | `100` | Client-side burst above kubeApiQps. 0 leaves the client-go default in place. |
| agent.flags.kubeApiQps | int | `50` | Client-side request rate to the API server. A node holding many flows spends one Get and one Update per flow per renewal pass; at the client-go default of 5 the loop blocks rather than fails, stretching past the Lease duration it exists to keep fresh. 0 leaves the client-go default in place. |
| agent.flags.provider | string | `""` | Explicit libmxl-fabrics provider stamped onto on-demand (intent-driven) mirrors, bypassing per-node resolution. Empty (or "auto") resolves a concrete provider from the source and target nodes' MxlNodeCapabilities; set a concrete value to force one cluster-wide. |
| agent.flags.shimPath | string | `"/run/mxl/libmxl-intent.so"` | Where the agent drops the copy of libmxl-intent.so its image carries, on every start. A consumer that already mounts hostPath.run for the intent socket preloads the shim from there instead of copying it out of the carrier image with an initContainer. Keep the path under hostPath.run, or consumer pods cannot see it. Empty disables the drop. |
| agent.hostPID | bool | `true` | Run the agent in the host PID namespace. Required for the intent socket's SO_PEERCRED-based pod identification: without it the connecting consumer pod's PID is not visible to the agent and the on-demand mirror materialization path silently fails. |
| agent.hostPath | object | `{"run":"/run/mxl","type":"DirectoryOrCreate"}` | hostPath layout. The agent mounts the whole /run/mxl so the intent socket lands on the host where consumer pods can see it. |
| agent.image.tag | string | `""` | Image tag. Empty defaults to the chart appVersion prefixed with "v": the agent image is released under the chart version. Set this or image.digest. |
| agent.podSecurityContext | object | `{}` | Pod-level security context, rendered into the pod spec as given. The agent mounts the same host run directory as the gateway and writes to it, so an SELinux-enabled node needs seLinuxOptions here for the reason documented on gateway.podSecurityContext. |
| cleanup | object | `{"preDeleteDomainWipe":{"enabled":true,"guardImage":"bitnami/kubectl:1.31","image":"busybox:1.38","nodeCount":1,"podSecurityContext":{},"timeoutSeconds":120}}` | Pre-delete lifecycle hooks. The domain wipe removes stale .mxl-flow directories under gateway.flags.domainPath at chart uninstall, because they live on a hostPath and survive namespace and CRD teardown. |
| cleanup.preDeleteDomainWipe.enabled | bool | `true` | Wipe stale .mxl-flow directories under gateway.flags.domainPath during helm uninstall, closing the cold-reinstall lifecycle bug where surviving hostPath state from a prior install causes the agent classifier to default the dirs to Origin on the next install (ghost source-side initiator).  Set to false if either applies:   - producers (libmxl writers) are still mmap-attached to files     under domainPath at uninstall time. rm -rf does not crash     producers (mmap regions survive unlink), but their writes     are orphaned from any new consumer. Drain producers first,     then uninstall.   - host state under domainPath is intentionally reused across     chart reinstalls (advanced; expect to clean up manually to     avoid the ghost-initiator bug). |
| cleanup.preDeleteDomainWipe.guardImage | string | `"bitnami/kubectl:1.31"` | Image used by the nodecount-guard init-container. Must carry a kubectl binary on PATH. |
| cleanup.preDeleteDomainWipe.image | string | `"busybox:1.38"` | Image used by the cleanup Job. Pinned tag (no :latest). |
| cleanup.preDeleteDomainWipe.nodeCount | int | `1` | Effective node count for the agent DaemonSet. An under-set value yields a partial wipe; the nodecount-guard init-container fails the chart with a clear error in that case. |
| cleanup.preDeleteDomainWipe.podSecurityContext | object | `{}` | Pod-level security context for the cleanup Job, rendered into the pod spec as given. The Job removes directories inside the domain hostPath, which is the same write an SELinux-enabled node denies the gateway, so it needs the seLinuxOptions documented on gateway.podSecurityContext. Carried separately because the Job is a helm hook and takes none of the gateway's pod settings. |
| cleanup.preDeleteDomainWipe.timeoutSeconds | int | `120` | Per-pod completion timeout. |
| crds | object | `{"skipInstall":false}` | CRD handling. The chart installs CRDs from its crds/ directory by default. Helm only installs them on first install and never deletes or upgrades them; Flux's HelmRelease.spec.{install,upgrade}.crds governs replace semantics. Set skipInstall=true when CRDs are managed by a separate Kustomization or operator framework. |
| dashboards.annotations | object | `{"grafana_folder":"MXL"}` | Annotations on the ConfigMaps. grafana_folder places the dashboard when the sidecar runs with folderAnnotation set. |
| dashboards.datasourceUid | string | `"prometheus"` | uid of the Prometheus datasource the panels query. The default is what kube-prometheus-stack names its own; a Grafana provisioned otherwise states its uid here. |
| dashboards.enabled | bool | `false` | Ship the bundled dashboards. |
| dashboards.labels | object | `{"grafana_dashboard":"1"}` | Labels the sidecar selects on. A deployment configured with a non-empty sidecar labelValue, or selecting on a release label, sets its own here. |
| dashboards.namespace | string | `""` | Namespace to place the ConfigMaps in. Empty uses the release namespace. The sidecar's searchNamespace defaults to the namespace Grafana runs in, so a Grafana installed elsewhere needs either that setting widened or this pointed at it. |
| exporter.affinity | object | `{}` |  |
| exporter.args | list | `[]` | Extra raw args appended after the rendered flags. |
| exporter.containerSecurityContext.allowPrivilegeEscalation | bool | `false` |  |
| exporter.containerSecurityContext.capabilities.drop[0] | string | `"ALL"` |  |
| exporter.containerSecurityContext.readOnlyRootFilesystem | bool | `true` |  |
| exporter.enabled | bool | `false` | Deploy the exporter DaemonSet. |
| exporter.extraEnv | list | `[]` |  |
| exporter.extraVolumeMounts | list | `[]` |  |
| exporter.extraVolumes | list | `[]` |  |
| exporter.flags.domainPath | string | `"/run/mxl/domain"` | Absolute path of the MXL domain directory to report on. Must resolve to the same domain the agent watches, below hostPath.run. |
| exporter.flags.flowLifetime | string | `"5m"` | How long a departed flow keeps being exported with mxl_flow_present at 0, so a flow that ends between two scrapes leaves a record rather than a gap. One sweep of gateway.flags.domainGcInterval, so the metric set describes the domain as it is rather than as it was. |
| exporter.flags.healthProbeBindAddress | string | `":8081"` |  |
| exporter.flags.metricsBindAddress | string | `":8080"` |  |
| exporter.flags.scanPeriod | string | `"5s"` | How often the domain directory is re-listed for flows that appeared or disappeared. Scrapes read the cache this maintains. |
| exporter.flags.zapLogLevel | string | `"info"` |  |
| exporter.hostPath | object | `{"run":"/run/mxl","type":"Directory"}` | hostPath layout. The exporter mounts the runtime root read-only; it opens libmxl flow readers and never writes into the domain. |
| exporter.image.digest | string | `""` | Image digest. Wins over tag when set. @schema pattern: ^$|^sha256:[0-9a-f]{64}$ @schema |
| exporter.image.pullPolicy | string | `""` |  |
| exporter.image.repository | string | `"exporter"` |  |
| exporter.image.tag | string | `""` | Image tag. Empty defaults to the chart appVersion prefixed with "v": the exporter image is released under the chart version. Set this or image.digest. |
| exporter.livenessProbe.httpGet.path | string | `"/healthz"` |  |
| exporter.livenessProbe.httpGet.port | string | `"probe"` |  |
| exporter.livenessProbe.initialDelaySeconds | int | `5` |  |
| exporter.livenessProbe.periodSeconds | int | `20` |  |
| exporter.metrics.service.enabled | bool | `true` |  |
| exporter.metrics.service.port | int | `8080` |  |
| exporter.metrics.service.type | string | `"ClusterIP"` |  |
| exporter.metrics.serviceMonitor.enabled | bool | `false` | Create a ServiceMonitor. Requires the monitoring.coreos.com CRDs to be installed cluster-wide. |
| exporter.metrics.serviceMonitor.interval | string | `"30s"` |  |
| exporter.metrics.serviceMonitor.labels | object | `{}` |  |
| exporter.metrics.serviceMonitor.metricRelabelings | list | `[]` |  |
| exporter.metrics.serviceMonitor.relabelings | list | `[]` |  |
| exporter.metrics.serviceMonitor.scrapeTimeout | string | `"10s"` |  |
| exporter.nodeSelector | object | `{}` |  |
| exporter.podAnnotations | object | `{}` |  |
| exporter.podLabels | object | `{}` |  |
| exporter.podSecurityContext | object | `{}` | Pod-level security context, rendered into the pod spec as given. The exporter mounts the host run directory read-only and never writes into the domain, so it does not need the seLinuxOptions that gateway.podSecurityContext documents. |
| exporter.priorityClassName | string | `""` |  |
| exporter.readinessProbe.httpGet.path | string | `"/readyz"` |  |
| exporter.readinessProbe.httpGet.port | string | `"probe"` |  |
| exporter.readinessProbe.initialDelaySeconds | int | `2` |  |
| exporter.readinessProbe.periodSeconds | int | `10` |  |
| exporter.resources.limits | object | `{}` |  |
| exporter.resources.requests.cpu | string | `"25m"` |  |
| exporter.resources.requests.memory | string | `"64Mi"` |  |
| exporter.serviceAccount.annotations | object | `{}` |  |
| exporter.serviceAccount.automountServiceAccountToken | bool | `true` |  |
| exporter.serviceAccount.create | bool | `true` |  |
| exporter.serviceAccount.labels | object | `{}` |  |
| exporter.serviceAccount.name | string | `""` | ServiceAccount name. Empty falls back to the generated name. |
| exporter.tolerations | list | `[]` |  |
| exporter.topology.enabled | bool | `true` | Publish mxl_flow_location_info from the MxlFlow CRs. That is what names the producing node (phase Origin), so dashboards do not need media functions to label themselves. Disabling it drops the metric and the exporter's only API access. |
| exporter.updateStrategy.rollingUpdate.maxUnavailable | int | `1` |  |
| exporter.updateStrategy.type | string | `"RollingUpdate"` |  |
| gateway | object | `{"affinity":{},"args":[],"containerSecurityContext":{"allowPrivilegeEscalation":false,"capabilities":{"add":[],"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":false},"dnsPolicy":"ClusterFirstWithHostNet","enabled":true,"extraContainers":[],"extraEnv":[],"extraVolumeMounts":[],"extraVolumes":[],"flags":{"bindAddress":"$(POD_IP)","degradedAfter":"10s","domainGcGrace":"2m","domainGcInterval":"5m","domainGcScaffoldGrace":"10m","domainPath":"/run/mxl/domain","fabricCidr":[],"fabricDevice":[],"fabricMinLinkSpeed":"","healthProbeBindAddress":":8081","kubeApiBurst":100,"kubeApiQps":50,"metricsBindAddress":":8080","pacingChunks":8,"pacingFraction":-1,"pprofBindAddress":"","probePeriod":"5m","providers":["any"],"rdmaEnumerationGrace":"10m","rdmaEnumerationLiveness":false,"readerStallAfter":"20s","resyncPeriod":"30s","zapLogLevel":"info"},"hostNetwork":true,"hostPath":{"domain":"/run/mxl/domain","type":"DirectoryOrCreate"},"image":{"digest":"","pullPolicy":"","repository":"gateway","tag":""},"initContainers":[],"livenessProbe":{"httpGet":{"path":"/healthz","port":"probe"},"initialDelaySeconds":5},"metrics":{"service":{"enabled":true,"port":8080,"type":"ClusterIP"},"serviceMonitor":{"enabled":false,"interval":"30s","labels":{},"metricRelabelings":[],"relabelings":[],"scrapeTimeout":"10s"}},"nodeSelector":{},"podAnnotations":{},"podLabels":{},"podSecurityContext":{},"priorityClassName":"","rdma":{"enabled":false,"extraEnv":[],"infinibandHostPath":"/dev/infiniband","infinibandHostPathType":"Directory","mountInfiniband":true,"resourceName":""},"readinessProbe":{"httpGet":{"path":"/readyz","port":"probe"},"initialDelaySeconds":2},"resources":{"limits":{},"requests":{"cpu":"100m","memory":"128Mi"}},"serviceAccount":{"annotations":{},"automountServiceAccountToken":true,"create":true,"labels":{},"name":""},"tolerations":[],"topologySpreadConstraints":[],"updateStrategy":{"rollingUpdate":{"maxUnavailable":1},"type":"RollingUpdate"},"variants":{"efa":{"enabled":false,"flags":{"providers":["tcp","efa"]},"rdma":{"enabled":true,"mountInfiniband":false,"resourceName":"vpc.amazonaws.com/efa"}},"tcp":{"enabled":false,"flags":{"providers":["tcp"]}},"verbs":{"enabled":false,"flags":{"providers":["tcp","verbs"]},"rdma":{"enabled":true,"mountInfiniband":false,"resourceName":""}}}}` | Gateway: per-node DaemonSet that owns libmxl-fabrics handles and moves grains across the wire. Runs with hostNetwork so the fabric endpoint binds the node interface. |
| gateway.containerSecurityContext.capabilities.add | list | `[]` | Additional caps beyond what rdma.enabled implies. |
| gateway.flags.bindAddress | string | `"$(POD_IP)"` | Bind address for libmxl-fabrics endpoints. Default uses the downward-API POD_IP so each gateway binds its node IP. An explicit empty string ("") emits `--bind-address=` (bare equals), which suppresses the POD_IP fallback so libfabric picks the interface; useful when the gateway runs on a Multus-attached NAD and the RDMA fabric is a secondary netdev. |
| gateway.flags.degradedAfter | string | `"10s"` | Grain-commit inactivity after which the target gateway demotes a mirror to Degraded. Same threshold gates the Reconcile fast-path so a stale Ready status falls through to re-establish. |
| gateway.flags.domainGcGrace | string | `"2m"` | How long after start the first reclaim sweep is held back. A restarted gateway has no writer attached to the mirror copies it was serving, and a consumer's reader does not protect them, so a sweep inside this window would delete flows consumers are still reading and the reconciler is about to re-establish. Must outlast the reconciler's re-establish path. |
| gateway.flags.domainGcInterval | string | `"5m"` | How often libmxl reclaims flow directories no writer holds. The criterion is libmxl's own: a writer keeps a shared lock for as long as it is attached, so a live producer or mirror writer is never a candidate however long since its last grain. Set to 0 to disable reclaiming. |
| gateway.flags.domainGcScaffoldGrace | string | `"10m"` | How old a half-built flow directory must be before it is reclaimed. libmxl builds a flow under a temporary name and renames it into place, removing it on every unwinding exit, so only a death that skips unwinding leaves one -- and libmxl's own collection walks published flows, so nothing sees it afterwards. A producer SIGKILLed part way through allocating its ring therefore leaves the whole allocation in the shared domain, at the node's expense rather than its own. Whether a writer still holds the scaffold is decided by the same lock libmxl uses; this only spans the moment between the data file being created and locked. Negative disables it. |
| gateway.flags.fabricCidr | list | `[]` | CIDRs the fabric interfaces must fall inside. Empty considers every address libmxl-fabrics reports, which on a node with several NICs includes the ones carrying ST 2110 essence, cluster traffic, or out-of-band management. Set this to the subnet of the MXL fabric to keep mirrors off them. Applies to both what a node advertises and what a mirror binds. |
| gateway.flags.fabricDevice | list | `[]` | Device names the fabric interfaces must match, as the provider names them (the kernel netdev name for tcp, the NIC name libfabric reports otherwise). Empty considers every device. Useful where the fabric shares a subnet with other traffic. |
| gateway.flags.fabricMinLinkSpeed | string | `""` | Minimum link speed a fabric interface must report, as a quantity in bits per second (for example 25G). Empty imposes no floor. An interface whose provider reports no link speed is rejected when this is set, which includes interfaces with no physical NIC behind them. |
| gateway.flags.kubeApiBurst | int | `100` | Client-side burst above kubeApiQps. 0 leaves the client-go default in place. |
| gateway.flags.kubeApiQps | int | `50` | Client-side request rate to the API server. A node holding many flows spends one Get and one Update per flow per renewal pass; at the client-go default of 5 the loop blocks rather than fails, stretching past the Lease duration it exists to keep fresh. 0 leaves the client-go default in place. |
| gateway.flags.pacingChunks | int | `8` | How many slice ranges a paced grain is split into. Read only while pacingFraction is positive. This sets how short the individual bursts get: the contiguous burst is one grain divided by this, and switch buffer occupancy follows that length. Each range costs one cgo call and, because a partial range leaves libmxl-fabrics's one-write-per-grain fast path, one RMA write per plane. Raise it if the switch still reports drops at a pacing fraction you are happy with; lower it if gateway CPU is the constraint. Capped by the grain's slice count, so a flow with fewer slices than this uses one range per slice. Under 2 disables pacing. |
| gateway.flags.pacingFraction | int | `-1` | How much of a grain's own interval the source gateway spreads that grain's transmission over, instead of handing the fabric a whole grain the NIC then drains at line rate. Negative disables pacing and restores whole-grain transfers, which is the default: the burst is only worth shaping where a receiver loses grains to it, and where nothing is lost the added latency and the per-chunk cost buy nothing. Turn it on when a receiver several flows converge on reports drops well below nominal link utilisation, which is the signature of overlapping bursts rather than of offered load. Peak rate becomes the flow's own rate divided by this, so 0.5 halves the burst and doubles its length, and is where to start. The cost is latency: a grain completes at the receiver up to this much of its interval later. Depends on the flow's edit rate alone, never on the link speed, so one value holds from 25GbE to EFA. Raise it toward 1 while drops remain; lower it when the added latency matters more than the burst. Must stay below 1, or a grain's transmission runs into the next one. |
| gateway.flags.pprofBindAddress | string | `""` | Bind address for the net/http/pprof endpoint. Empty disables; otherwise must be a loopback bind (127.0.0.1: or localhost:). Use kubectl port-forward to reach the endpoint; see docs/diagnostics/perf-shortfall.md for the capture recipe. |
| gateway.flags.probePeriod | string | `"5m"` | Shortest interval between two libmxl-fabrics interface enumerations. Status still refreshes every resyncPeriod from the last result. Each enumeration sweeps every provider libfabric was built with and warns about those it finds no device for. |
| gateway.flags.providers | list | `["any"]` | Upper bound on the libmxl-fabrics providers a gateway may advertise and use. Each node publishes this list intersected with what libmxl-fabrics finds on it, so `any` advertises the hardware that is present and nothing else. Narrow it to keep a provider out of consideration on every node. |
| gateway.flags.rdmaEnumerationGrace | string | `"10m"` | How long that discrepancy holds before the probe fails. Measured across consecutive enumerations, so it spans several probePeriod rather than letting one decide it. |
| gateway.flags.rdmaEnumerationLiveness | bool | `false` | Fail the liveness probe while the host exposes RDMA devices that no enumerated provider accounts for. libfabric fixes a provider's device list at the first enumeration, so a gateway that started before the hardware was usable never revisits it and only a restart does. Off by default: a node whose devices the built providers cannot drive reports the same discrepancy, and there a restart loops instead of recovering. The RDMADevicesEnumerated condition reports the state either way. |
| gateway.flags.readerStallAfter | string | `"20s"` | How long a source-side reader may report an unchanged head index, having never transferred a grain, before SourceProgress reports ReaderNotAdvancing. |
| gateway.hostNetwork | bool | `true` | hostNetwork is required because the libmxl-fabrics TargetInfo embeds the host:port a peer dials; a CNI overlay IP would not be reachable cross-node. |
| gateway.image.tag | string | `""` | Image tag. Empty defaults to the chart appVersion prefixed with "v": the gateway image is released under the chart version. Set this or image.digest. |
| gateway.podSecurityContext | object | `{}` | Pod-level security context, rendered into the pod spec as given.  An SELinux-enabled node needs seLinuxOptions here. The domain is a hostPath, and Kubernetes never relabels one: the hostPath volume plugin reports SELinuxRelabel false and declines SELinux context mounts, so the directory keeps the type the host gives it. Under /run that is usually run_t, while the container is confined to a pod type, and every write to the domain is denied.  seLinuxOptions labels the container process rather than the directory, which makes it the only lever available from a chart. Which type is accepted depends on the policy loaded on the node, so no value is correct everywhere and this stays empty.  In permissive mode the write still succeeds and each denial only emits an audit record set, so the cost is invisible until the node's audit backlog overruns and records are lost. |
| gateway.rdma.enabled | bool | `false` | Add the capabilities the verbs/efa providers need (IPC_LOCK, SYS_RESOURCE; libmxl pins shm pages). |
| gateway.rdma.infinibandHostPathType | string | `"Directory"` | hostPath type for the InfiniBand mount. `Directory` holds the pod in ContainerCreating on a node where the path is absent, which is a deliberate failure on a DaemonSet that is only meant to run on RDMA-capable nodes, and a hang on one that is not. |
| gateway.rdma.mountInfiniband | bool | `true` | Bind-mount the host's InfiniBand device directory into the gateway. Only consulted when rdma.enabled is set. A bind mount alone does not pass the cgroup-v2 device controller, so a node whose devices come from a device plugin wants this off and `resourceName` set instead. |
| gateway.rdma.resourceName | string | `""` | Extended-resource name advertised by a Kubernetes device plugin. `devic.es/rdma` from generic-device-plugin and `rdma/hca_shared_devices` from k8s-rdma-shared-dev-plugin cover on-prem verbs; `vpc.amazonaws.com/efa` from aws-efa-k8s-device-plugin covers EFA; a site may advertise something else entirely. When non-empty, the gateway container gets `<resourceName>: 1` on both requests and limits. Empty disables the request.  The request is what grants the container its cgroup device permissions, and it also makes the pod unschedulable on any node not advertising the resource -- which is why a cluster with mixed hardware needs `gateway.variants` rather than one DaemonSet. |
| gateway.variants | object | `{"efa":{"enabled":false,"flags":{"providers":["tcp","efa"]},"rdma":{"enabled":true,"mountInfiniband":false,"resourceName":"vpc.amazonaws.com/efa"}},"tcp":{"enabled":false,"flags":{"providers":["tcp"]}},"verbs":{"enabled":false,"flags":{"providers":["tcp","verbs"]},"rdma":{"enabled":true,"mountInfiniband":false,"resourceName":""}}}` | Render the gateway DaemonSet once per enabled entry instead of once, for a cluster whose nodes do not all carry the same fabric hardware. With nothing enabled -- the default -- exactly one DaemonSet renders from the values above, which is what a uniform cluster wants.  Keys name the variant and suffix the DaemonSet name, so they have to be DNS-1123 labels. Entries beyond the three below are allowed; each may override any key under `gateway`, with maps merging key by key against the values above and lists replacing wholesale.  Placement is deliberately unset on every entry. Which label marks a node's fabric is a site's choice, and a default here would not be replaced by a downstream override but merged with it, leaving a nodeSelector that demands both labels and matches no node.  Enabling two or more entries means declaring their placement, and the placements have to be complementary: two gateways on one node open the same MXL domain, reconcile the same mirrors, and write the same MxlNodeCapabilities, which is named after the node. Helm cannot read node labels, so it rejects only the combinations that guarantee overlap. Keying every variant on a distinct value of one label is what makes overlap impossible; a class defined by the absence of that label needs `affinity` with `DoesNotExist`, which a nodeSelector cannot express. See docs/RDMA.md. |
| global | object | `{"commonAnnotations":{},"commonLabels":{},"image":{"pullPolicy":"IfNotPresent","pullSecrets":[],"registry":"ghcr.io/qvest-digital/mxl-k8s"}}` | Global settings inherited by every component unless overridden. |
| global.commonAnnotations | object | `{}` | Annotations added to every object the chart renders. |
| global.commonLabels | object | `{}` | Labels added to every object the chart renders. |
| global.image.pullPolicy | string | `"IfNotPresent"` | Default imagePullPolicy. Per-component override wins. @schema enum: ["Always", "IfNotPresent", "Never"] @schema |
| global.image.pullSecrets | list | `[]` | Image pull secrets, applied to every Pod. |
| global.image.registry | string | `"ghcr.io/qvest-digital/mxl-k8s"` | Container registry prefix. Per-component image.repository is appended to this. |
| namespace | object | `{"create":false,"name":""}` | Namespace handling. Most Flux users set createNamespace on the HelmRelease and leave namespace.create=false here. |
| namespace.name | string | `""` | Namespace the chart's namespaced resources land in. Falls back to .Release.Namespace when empty. |
| operator | object | `{"affinity":{},"args":[],"containerSecurityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":true},"enabled":true,"extraContainers":[],"extraEnv":[],"extraVolumeMounts":[],"extraVolumes":[],"flags":{"healthProbeBindAddress":":8081","leaderElect":false,"metricsBindAddress":":8080","zapDevel":false,"zapLogLevel":"info"},"image":{"digest":"","pullPolicy":"","repository":"operator","tag":""},"initContainers":[],"livenessProbe":{"httpGet":{"path":"/healthz","port":"probe"},"initialDelaySeconds":5},"metrics":{"service":{"enabled":true,"port":8080,"type":"ClusterIP"},"serviceMonitor":{"enabled":false,"interval":"30s","labels":{},"metricRelabelings":[],"relabelings":[],"scrapeTimeout":"10s"}},"nodeSelector":{},"podAnnotations":{},"podDisruptionBudget":{"enabled":false,"minAvailable":1},"podLabels":{},"podSecurityContext":{},"priorityClassName":"","readinessProbe":{"httpGet":{"path":"/readyz","port":"probe"},"initialDelaySeconds":2},"replicaCount":1,"resources":{"limits":{},"requests":{"cpu":"50m","memory":"64Mi"}},"serviceAccount":{"annotations":{},"automountServiceAccountToken":true,"create":true,"labels":{},"name":""},"tolerations":[],"topologySpreadConstraints":[]}` | Operator: cluster-wide reconciler for MxlReceiver, MxlFlowMirror, and related CRDs. Single Deployment. |
| operator.args | list | `[]` | Extra raw args appended after the structured flags. |
| operator.flags.leaderElect | bool | `false` | Enable leader election. Required for replicaCount > 1. |
| operator.image.digest | string | `""` | Image digest. Wins over tag when set. @schema pattern: ^$|^sha256:[0-9a-f]{64}$ @schema |
| operator.image.pullPolicy | string | `""` | imagePullPolicy override. Empty falls back to global. @schema enum: ["", "Always", "IfNotPresent", "Never"] @schema |
| operator.image.tag | string | `""` | Image tag. Empty defaults to the chart appVersion prefixed with "v": the operator image is released under the chart version. Set this or image.digest. |
| operator.metrics.serviceMonitor.enabled | bool | `false` | Render a Prometheus Operator ServiceMonitor. Requires the monitoring.coreos.com CRDs to be installed cluster-wide. |
| operator.serviceAccount.name | string | `""` | ServiceAccount name. Empty falls back to a generated name. |
| rbac | object | `{"create":true}` | ClusterRoles and ClusterRoleBindings for every enabled component. Set to false when RBAC is centrally managed. |

## Uninstall

Helm leaves the CRDs installed when the chart is uninstalled (they
live in `crds/`, not `templates/`). Delete them by hand if you want
them gone:

```sh
kubectl delete crd \
  mxldomains.mxl.qvest-digital.com \
  mxlflows.mxl.qvest-digital.com \
  mxlflowmirrors.mxl.qvest-digital.com \
  mxlnodecapabilities.mxl.qvest-digital.com \
  mxlreceivers.mxl.qvest-digital.com
```

