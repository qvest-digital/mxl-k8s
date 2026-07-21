# mxl-k8s

Kubernetes control plane for MXL (Media eXchange Layer). Installs the operator, per-node agent and gateway, CRDs, and RBAC.

![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.0](https://img.shields.io/badge/AppVersion-0.0.0-informational?style=flat-square)

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
  --version 1.0.0-rc.8 \
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
      version: "1.0.0-rc.8"
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

## Per-component image versions

Operator, agent, and gateway release independently from the chart;
their versions can diverge. `<component>.image.tag` is pinned per
component in `values.yaml` and committed, so a chart release at
`1.0.0-rc.5` can ship operator at `v1.0.0-rc.4`, agent at
`v1.0.0-rc.5`, and gateway at `v1.0.0-rc.2` without the user touching
values. Renovate keeps each committed pin current; a bump opens a
`deps(chart)` PR that release-please turns into a chart release.

The bundled-versions table for each released chart appears in the
chart's GitHub release notes
(`https://github.com/qvest-digital/mxl-k8s/releases/tag/charts/mxl-k8s/v<X>`).
The `:dev` chart published on every merge to `main` rewrites every tag
to `dev` so it tracks main HEAD images.

A `helm install ./charts/mxl-k8s` from a clone uses the committed pins
as-is. Each component must resolve to an image: set `image.tag` or
`image.digest` (both ship pinned in `values.yaml`); leaving both empty
makes rendering fail. `--set <comp>.image.tag=<tag>` overrides the pin
as usual.

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
| agent | object | `{"affinity":{},"args":[],"containerSecurityContext":{"allowPrivilegeEscalation":false,"capabilities":{"add":["SYS_ADMIN"],"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":false},"enabled":true,"extraContainers":[],"extraEnv":[],"extraVolumeMounts":[],"extraVolumes":[],"flags":{"domainPath":"/run/mxl/domain","healthProbeBindAddress":":8081","intentSocket":"/run/mxl/agent.sock","materializeTimeout":"5s","metricsBindAddress":":8080","provider":"","resyncPeriod":"30s","zapLogLevel":"info"},"hostPID":true,"hostPath":{"run":"/run/mxl","type":"DirectoryOrCreate"},"image":{"digest":"","pullPolicy":"","repository":"agent","tag":"v1.0.0-rc.6"},"initContainers":[],"livenessProbe":{"httpGet":{"path":"/healthz","port":"probe"},"initialDelaySeconds":5},"metrics":{"service":{"enabled":true,"port":8080,"type":"ClusterIP"},"serviceMonitor":{"enabled":false,"interval":"30s","labels":{},"metricRelabelings":[],"relabelings":[],"scrapeTimeout":"10s"}},"nodeSelector":{},"podAnnotations":{},"podLabels":{},"podSecurityContext":{},"priorityClassName":"","readinessProbe":{"httpGet":{"path":"/readyz","port":"probe"},"initialDelaySeconds":2},"resources":{"limits":{},"requests":{"cpu":"25m","memory":"32Mi"}},"serviceAccount":{"annotations":{},"automountServiceAccountToken":true,"create":true,"labels":{},"name":""},"tolerations":[],"topologySpreadConstraints":[],"updateStrategy":{"rollingUpdate":{"maxUnavailable":1},"type":"RollingUpdate"}}` | Agent: per-node DaemonSet that watches the MXL domain via fanotify, publishes flow state, and serves the LD_PRELOAD intent socket. |
| agent.containerSecurityContext.runAsNonRoot | bool | `false` | fanotify on /run/mxl/domain reads host inodes, so the agent currently runs as root. PSA-restricted environments cannot host the agent without a policy exception. |
| agent.flags.provider | string | `""` | Explicit libmxl-fabrics provider stamped onto on-demand (intent-driven) mirrors, bypassing per-node resolution. Empty (or "auto") resolves a concrete provider from the source and target nodes' MxlNodeCapabilities; set a concrete value to force one cluster-wide. |
| agent.hostPID | bool | `true` | Run the agent in the host PID namespace. Required for the intent socket's SO_PEERCRED-based pod identification: without it the connecting consumer pod's PID is not visible to the agent and the on-demand mirror materialization path silently fails. |
| agent.hostPath | object | `{"run":"/run/mxl","type":"DirectoryOrCreate"}` | hostPath layout. The agent mounts the whole /run/mxl so the intent socket lands on the host where consumer pods can see it. |
| agent.image.tag | string | `"v1.0.0-rc.6"` | Image tag for the agent, pinned to the release this chart version bundles and kept current by Renovate. Set this or image.digest; with both empty, rendering fails. |
| cleanup | object | `{"preDeleteDomainWipe":{"enabled":true,"guardImage":"bitnami/kubectl:1.31","image":"busybox:1.38","nodeCount":1,"timeoutSeconds":120}}` | Pre-delete lifecycle hooks. The domain wipe removes stale .mxl-flow directories under gateway.flags.domainPath at chart uninstall, because they live on a hostPath and survive namespace and CRD teardown. |
| cleanup.preDeleteDomainWipe.enabled | bool | `true` | Wipe stale .mxl-flow directories under gateway.flags.domainPath during helm uninstall, closing the cold-reinstall lifecycle bug where surviving hostPath state from a prior install causes the agent classifier to default the dirs to Origin on the next install (ghost source-side initiator).  Set to false if either applies:   - producers (libmxl writers) are still mmap-attached to files     under domainPath at uninstall time. rm -rf does not crash     producers (mmap regions survive unlink), but their writes     are orphaned from any new consumer. Drain producers first,     then uninstall.   - host state under domainPath is intentionally reused across     chart reinstalls (advanced; expect to clean up manually to     avoid the ghost-initiator bug). |
| cleanup.preDeleteDomainWipe.guardImage | string | `"bitnami/kubectl:1.31"` | Image used by the nodecount-guard init-container. Must carry a kubectl binary on PATH. |
| cleanup.preDeleteDomainWipe.image | string | `"busybox:1.38"` | Image used by the cleanup Job. Pinned tag (no :latest). |
| cleanup.preDeleteDomainWipe.nodeCount | int | `1` | Effective node count for the agent DaemonSet. An under-set value yields a partial wipe; the nodecount-guard init-container fails the chart with a clear error in that case. |
| cleanup.preDeleteDomainWipe.timeoutSeconds | int | `120` | Per-pod completion timeout. |
| crds | object | `{"skipInstall":false}` | CRD handling. The chart installs CRDs from its crds/ directory by default. Helm only installs them on first install and never deletes or upgrades them; Flux's HelmRelease.spec.{install,upgrade}.crds governs replace semantics. Set skipInstall=true when CRDs are managed by a separate Kustomization or operator framework. |
| gateway | object | `{"affinity":{},"args":[],"containerSecurityContext":{"allowPrivilegeEscalation":false,"capabilities":{"add":[],"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":false},"dnsPolicy":"ClusterFirstWithHostNet","enabled":true,"extraContainers":[],"extraEnv":[],"extraVolumeMounts":[],"extraVolumes":[],"flags":{"bindAddress":"$(POD_IP)","degradedAfter":"10s","domainPath":"/run/mxl/domain","healthProbeBindAddress":":8081","metricsBindAddress":":8080","pprofBindAddress":"","providers":["tcp"],"resyncPeriod":"30s","zapLogLevel":"info"},"hostNetwork":true,"hostPath":{"domain":"/run/mxl/domain","type":"DirectoryOrCreate"},"image":{"digest":"","pullPolicy":"","repository":"gateway","tag":"v1.0.0-rc.6"},"initContainers":[],"livenessProbe":{"httpGet":{"path":"/healthz","port":"probe"},"initialDelaySeconds":5},"metrics":{"service":{"enabled":true,"port":8080,"type":"ClusterIP"},"serviceMonitor":{"enabled":false,"interval":"30s","labels":{},"metricRelabelings":[],"relabelings":[],"scrapeTimeout":"10s"}},"nodeSelector":{},"podAnnotations":{},"podLabels":{},"podSecurityContext":{},"priorityClassName":"","rdma":{"enabled":false,"extraEnv":[],"infinibandHostPath":"/dev/infiniband","resourceName":""},"readinessProbe":{"httpGet":{"path":"/readyz","port":"probe"},"initialDelaySeconds":2},"resources":{"limits":{},"requests":{"cpu":"100m","memory":"128Mi"}},"serviceAccount":{"annotations":{},"automountServiceAccountToken":true,"create":true,"labels":{},"name":""},"tolerations":[],"topologySpreadConstraints":[],"updateStrategy":{"rollingUpdate":{"maxUnavailable":1},"type":"RollingUpdate"}}` | Gateway: per-node DaemonSet that owns libmxl-fabrics handles and moves grains across the wire. Runs with hostNetwork so the fabric endpoint binds the node interface. |
| gateway.containerSecurityContext.capabilities.add | list | `[]` | Additional caps beyond what rdma.enabled implies. |
| gateway.flags.bindAddress | string | `"$(POD_IP)"` | Bind address for libmxl-fabrics endpoints. Default uses the downward-API POD_IP so each gateway binds its node IP. An explicit empty string ("") emits `--bind-address=` (bare equals), which suppresses the POD_IP fallback so libfabric picks the interface; useful when the gateway runs on a Multus-attached NAD and the RDMA fabric is a secondary netdev. |
| gateway.flags.degradedAfter | string | `"10s"` | Grain-commit inactivity after which the target gateway demotes a mirror to Degraded. Same threshold gates the Reconcile fast-path so a stale Ready status falls through to re-establish. |
| gateway.flags.pprofBindAddress | string | `""` | Bind address for the net/http/pprof endpoint. Empty disables; otherwise must be a loopback bind (127.0.0.1: or localhost:). Use kubectl port-forward to reach the endpoint; see docs/diagnostics/perf-shortfall.md for the capture recipe. |
| gateway.hostNetwork | bool | `true` | hostNetwork is required because the libmxl-fabrics TargetInfo embeds the host:port a peer dials; a CNI overlay IP would not be reachable cross-node. |
| gateway.image.tag | string | `"v1.0.0-rc.6"` | Image tag for the gateway, pinned to the release this chart version bundles and kept current by Renovate. Set this or image.digest; with both empty, rendering fails. |
| gateway.rdma.enabled | bool | `false` | Add the capabilities and mounts the verbs/efa providers need. |
| gateway.rdma.resourceName | string | `""` | Extended-resource name advertised by a Kubernetes device plugin (for example `rdma/hca_shared_devices` from k8s-rdma-shared-dev-plugin). When non-empty, the gateway container gets `<resourceName>: 1` on both requests and limits. Empty disables the request. |
| global | object | `{"commonAnnotations":{},"commonLabels":{},"image":{"pullPolicy":"IfNotPresent","pullSecrets":[],"registry":"ghcr.io/qvest-digital/mxl-k8s"}}` | Global settings inherited by every component unless overridden. |
| global.commonAnnotations | object | `{}` | Annotations added to every object the chart renders. |
| global.commonLabels | object | `{}` | Labels added to every object the chart renders. |
| global.image.pullPolicy | string | `"IfNotPresent"` | Default imagePullPolicy. Per-component override wins. @schema enum: ["Always", "IfNotPresent", "Never"] @schema |
| global.image.pullSecrets | list | `[]` | Image pull secrets, applied to every Pod. |
| global.image.registry | string | `"ghcr.io/qvest-digital/mxl-k8s"` | Container registry prefix. Per-component image.repository is appended to this. |
| namespace | object | `{"create":false,"name":""}` | Namespace handling. Most Flux users set createNamespace on the HelmRelease and leave namespace.create=false here. |
| namespace.name | string | `""` | Namespace the chart's namespaced resources land in. Falls back to .Release.Namespace when empty. |
| operator | object | `{"affinity":{},"args":[],"containerSecurityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true,"runAsNonRoot":true},"enabled":true,"extraContainers":[],"extraEnv":[],"extraVolumeMounts":[],"extraVolumes":[],"flags":{"healthProbeBindAddress":":8081","leaderElect":false,"metricsBindAddress":":8080","zapDevel":false,"zapLogLevel":"info"},"image":{"digest":"","pullPolicy":"","repository":"operator","tag":"v1.0.0-rc.4"},"initContainers":[],"livenessProbe":{"httpGet":{"path":"/healthz","port":"probe"},"initialDelaySeconds":5},"metrics":{"service":{"enabled":true,"port":8080,"type":"ClusterIP"},"serviceMonitor":{"enabled":false,"interval":"30s","labels":{},"metricRelabelings":[],"relabelings":[],"scrapeTimeout":"10s"}},"nodeSelector":{},"podAnnotations":{},"podDisruptionBudget":{"enabled":false,"minAvailable":1},"podLabels":{},"podSecurityContext":{},"priorityClassName":"","readinessProbe":{"httpGet":{"path":"/readyz","port":"probe"},"initialDelaySeconds":2},"replicaCount":1,"resources":{"limits":{},"requests":{"cpu":"50m","memory":"64Mi"}},"serviceAccount":{"annotations":{},"automountServiceAccountToken":true,"create":true,"labels":{},"name":""},"tolerations":[],"topologySpreadConstraints":[]}` | Operator: cluster-wide reconciler for MxlReceiver, MxlFlowMirror, and related CRDs. Single Deployment. |
| operator.args | list | `[]` | Extra raw args appended after the structured flags. |
| operator.flags.leaderElect | bool | `false` | Enable leader election. Required for replicaCount > 1. |
| operator.image.digest | string | `""` | Image digest. Wins over tag when set. @schema pattern: ^$|^sha256:[0-9a-f]{64}$ @schema |
| operator.image.pullPolicy | string | `""` | imagePullPolicy override. Empty falls back to global. @schema enum: ["", "Always", "IfNotPresent", "Never"] @schema |
| operator.image.tag | string | `"v1.0.0-rc.4"` | Image tag for the operator, pinned to the release this chart version bundles and kept current by Renovate. Set this or image.digest; with both empty, rendering fails. |
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

