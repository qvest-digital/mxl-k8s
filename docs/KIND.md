# Local KIND demo

`make kind-up` brings up a self-contained mxl-k8s deployment on
your laptop and runs an end-to-end TCP flow across two worker
nodes. This page is the longer version of that two-word
instruction: what each step does and what to look at if the
demo gets stuck.

## What runs

`make kind-up` installs the [`charts/mxl-k8s`](../charts/mxl-k8s/)
Helm chart (operator Deployment, agent and gateway DaemonSets,
the five CRDs, RBAC) into the cluster and applies a thin demo
overlay at [`examples/kind/demo/`](../examples/kind/demo/) on
top. The overlay references the writer/receiver/reader manifests
in [`examples/tcp-demo/`](../examples/tcp-demo/) directly, so the
demo workload definitions live in one place. `examples/tcp-demo/`
itself is also a self-contained vanilla-kustomize stack you can
apply without Helm if you prefer; `make kind-up` does not apply
it as a bundle.

The demo workload is:

- A writer pod (`mxl-tcp-demo-writer`) producing a 1080p29.97
  v210 video flow at the wall-clock grain rate, using go-mxl's
  `write-grain` example binary.
- A reader pod (`mxl-tcp-demo-reader`) consuming the same
  flow id, using go-mxl's `read-grain` example. The reader's
  `podAntiAffinity` forces it onto the other worker node so the
  cross-node mirror is actually exercised.
- An `MxlReceiver` pointing the reader at the writer's flow,
  with `provider: tcp`.

## What `make kind-up` does

The script in [`hack/kind-up.sh`](../hack/kind-up.sh) is
idempotent and re-runnable:

1. Builds the five component images locally
   (`operator`, `agent`, `gateway`, `shim`, `demo-tools`)
   under `ghcr.io/qvest-digital/mxl-k8s/<component>:dev`. The
   chart's image references resolve to the same tags, so no
   rename or manifest rewrite is needed. Docker's layer cache
   keeps subsequent runs fast.
2. Creates a three-node KIND cluster (control plane plus two
   workers) using
   [`hack/kind-config.yaml`](../hack/kind-config.yaml) if one is
   not already running.
3. Loads the images into every node so the `imagePullPolicy:
   IfNotPresent` references resolve without pulling from a
   registry.
4. Installs the chart with
   `helm upgrade --install mxl-k8s charts/mxl-k8s -n mxl-system
   --create-namespace -f examples/kind/values.yaml --set
   operator.image.tag=<TAG> --set agent.image.tag=<TAG> --set
   gateway.image.tag=<TAG>`. Helm 3 installs the chart's
   `crds/` directory on first install automatically; no separate
   `kubectl apply` pass.
5. Waits for the five CRDs to reach `Established` before
   applying any custom resources; without that gate the demo
   overlay can fail discovery on its own CRs.
6. Applies the demo overlay at `examples/kind/demo/`, which
   pulls the writer / receiver / reader manifests from
   `examples/tcp-demo/` via kustomize's load-restrictor escape
   hatch (the overlay lives in a sibling directory).
7. On re-runs against an existing cluster, forces a rollout
   restart on `deploy/mxl-k8s-operator`, `ds/mxl-k8s-agent`, and
   `ds/mxl-k8s-gateway` so any newly-rebuilt `:dev` image takes
   effect.
8. Waits for the resulting `MxlFlowMirror` to reach `Ready`.

When it returns, the writer is producing and the reader is
consuming.

## What to expect

```sh
kubectl --context kind-mxl-k8s-demo -n mxl-system get pods
```

Steady state looks like:

```
NAME                            READY   STATUS    RESTARTS
mxl-k8s-agent-...               1/1     Running   0
mxl-k8s-agent-...               1/1     Running   0
mxl-k8s-gateway-...             1/1     Running   0
mxl-k8s-gateway-...             1/1     Running   0
mxl-k8s-operator-...            1/1     Running   0
mxl-tcp-demo-reader             1/1     Running   0
mxl-tcp-demo-writer             1/1     Running   0
```

The LD_PRELOAD shim blocks the reader's first libmxl probe until
the agent has the mirror materialised, so the reader reaches
`Running` without a CrashLoop.

```sh
kubectl --context kind-mxl-k8s-demo -n mxl-system logs pod/mxl-tcp-demo-reader
```

prints one line per grain:

```
idx=53318102095 size=5529600 slices=1080/1080 flags=0x0 invalid=false
idx=53318102096 size=5529600 slices=1080/1080 flags=0x0 invalid=false
...
```

`size=5529600` is the 1920x1080 v210 payload, `slices=1080/1080`
means every slice arrived, `invalid=false` means the grain
header committed cleanly. At 30000/1001 fps that is roughly
1.33 Gbps of payload going across the docker bridge between the
two worker containers.

## Inspecting the data plane

```sh
make kind-status
```

dumps every CR's state plus the pods. The interesting bits are
`MxlFlow.status.locations` (which node hosts the origin, which
nodes host mirrors) and `MxlFlowMirror.status.phase` (`Ready`
when the gateway has the fabric handles open and is transferring).

## Cleanup

```sh
make kind-down
```

tears the cluster down. Image layers stay in your local
container-runtime cache; the next `make kind-up` reuses them.

## Cluster name

`make kind-up` looks at the `KIND_CLUSTER` env var (default
`mxl-k8s-demo`) so you can keep multiple parallel clusters if
needed. The status / down targets read the same variable.

## Container runtime

Both Docker and Podman are supported. The default is Docker;
select Podman with `CONTAINER_RUNTIME=podman`, which must be
passed to every `kind-*` target you invoke:

```sh
make kind-up     CONTAINER_RUNTIME=podman
make kind-status CONTAINER_RUNTIME=podman
make kind-down   CONTAINER_RUNTIME=podman
```

With Podman the machine must be rootful and have enough memory
for a three-node cluster; `kind-up.sh` checks this and prints
the fix command if either condition isn't met.

## Image source (`BUILD`)

`make kind-up` builds the five component images locally by default
and `kind load`s them into the cluster. This is `BUILD=local` (or
`BUILD` unset). Local builds use the same
`ghcr.io/qvest-digital/mxl-k8s/<component>:dev` reference shape as
CI publishes, so the chart's `image.tag: dev` resolves to the
kind-loaded image without any rewrite.

To skip the local build and use a CI-published image instead, pass
the image tag:

```sh
make kind-up BUILD=v1.0.0-rc.3
make kind-up BUILD=sha-abc1234
```

`kind-up.sh` resolves every component to
`ghcr.io/qvest-digital/mxl-k8s/<component>:<BUILD>`, pulls it,
loads it into KIND, and passes
`--set operator.image.tag=<BUILD> --set agent.image.tag=<BUILD>
--set gateway.image.tag=<BUILD>` to the `helm upgrade` invocation.
The `shim` and `demo-tools` images are aliased to `:dev` after
load because the demo workload manifests pin those tags.
Override the registry prefix with `IMAGE_REGISTRY=<prefix>` if
needed.

Empty or otherwise invalid `BUILD` values exit non-zero before
any side effects:

```
ERROR: BUILD must be 'local' or a non-empty image tag
```

## Integration suite

`make kind-test` runs the bash suite under
[`test/integration/kind/`](../test/integration/kind/) against an
already-running cluster. Cases assert the five CRDs reach
`Established`, the operator and the agent / gateway DaemonSets
finish rolling out, `/healthz` and `/readyz` on each probe port
answer `200`, every `MxlFlowMirror` reaches `phase=Ready` with a
non-empty `status.targetInfo`, and the reader pod's `idx=` log
lines actually advance over a sample window (catches the "Ready
but no grains flowing" failure mode).

```sh
make kind-up
make kind-test
make kind-down
```

The same suite drives the `kind-integration` GitHub Actions job in
`.github/workflows/images.yml`: on a same-repo PR that touches
anything `make kind-up` consumes, the job pulls the PR's per-tag
images from GHCR (`BUILD=pr-<num>-<sha>`), brings up a cluster on
the runner, and runs `make kind-test`. New assertions land as
`test/integration/kind/cases/<NN>-<name>.sh`; no runner changes
required. See the suite's [`README.md`](../test/integration/kind/README.md)
for the case-authoring conventions.

## NMOS verification

The agent's NMOS IS-04/IS-05 sender proxy is disabled by default.
To exercise it in the KIND cluster, enable it in the demo values
overlay at [`examples/kind/values.yaml`](../examples/kind/values.yaml):

```yaml
agent:
  flags:
    nmosBindAddress: ":1080"
```

Then `make kind-up` (or re-run against an existing cluster to
trigger a rollout restart). Once the agent pods are back to
`Running`, port-forward to one of them:

```sh
kubectl --context kind-mxl-k8s-demo -n mxl-system \
  port-forward ds/mxl-k8s-agent 1080:1080
```

### IS-04 Node API

```sh
# Version listing
curl http://localhost:1080/x-nmos/node/
# ["v1.3/"]

# Node self
curl http://localhost:1080/x-nmos/node/v1.3/self
```

The node resource advertises the Kubernetes node name as
`hostname`, and the `api.endpoints` array carries the host and
port from the bind address:

```json
{
  "id": "<uuid-v5>",
  "version": "2026-...",
  "label": "mxl-k8s-demo-worker",
  "hostname": "mxl-k8s-demo-worker",
  "api": {
    "versions": ["v1.3"],
    "endpoints": [{"host": "mxl-k8s-demo-worker", "port": 1080, "protocol": "http"}]
  },
  "href": "http://mxl-k8s-demo-worker:1080/x-nmos/node/v1.3/"
}
```

```sh
# Devices
curl http://localhost:1080/x-nmos/node/v1.3/devices
```

One device per MXL domain, type `urn:x-nmos:device:generic`, with
the sender IDs in `senders`:

```json
[
  {
    "id": "<uuid-v5>",
    "type": "urn:x-nmos:device:generic",
    "node_id": "<node-uuid>",
    "senders": ["<sender-uuid>"],
    "receivers": []
  }
]
```

```sh
# Sources, flows, senders
curl http://localhost:1080/x-nmos/node/v1.3/sources
curl http://localhost:1080/x-nmos/node/v1.3/flows
curl http://localhost:1080/x-nmos/node/v1.3/senders
```

Each MXL flow with origin on this node produces one source, one
flow, and one sender. The flow's `format` and `media_type` come
from the flow definition JSON (e.g. `urn:x-nmos:format:video` and
`video/v210` for the demo's v210 flow). The sender's `transport`
is `urn:x-nmos:transport:mxl`:

```json
[
  {
    "id": "<sender-uuid>",
    "flow_id": "5fbec3b1-1b0f-417d-9059-8b94a47197ed",
    "transport": "urn:x-nmos:transport:mxl",
    "device_id": "<device-uuid>",
    "subscription": {"active": true, "receiver_id": null}
  }
]
```

Receivers always return an empty array (the proxy is senders-only):

```sh
curl http://localhost:1080/x-nmos/node/v1.3/receivers
# []
```

### IS-05 Connection Management

```sh
# Version listing
curl http://localhost:1080/x-nmos/connection/
# ["v1.2/"]

# Get the sender ID from IS-04
SENDER_ID=$(curl -s http://localhost:1080/x-nmos/node/v1.3/senders | jq -r '.[0].id')
```

Active state -- MXL senders are always active with
`activate_immediate`:

```sh
curl http://localhost:1080/x-nmos/connection/v1.2/single/senders/$SENDER_ID/active
```

```json
{
  "receiver_id": null,
  "master_enable": true,
  "activation": {
    "mode": "activate_immediate",
    "requested_time": null,
    "activation_time": "2026-06-24T14:30:00.000000000Z"
  },
  "transport_params": [
    {
      "mxl_domain_id": "<domain-name>",
      "mxl_flow_id": "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
    }
  ]
}
```

Staged state is read-only. PATCH requests are accepted but return
the current active parameters unchanged:

```sh
curl http://localhost:1080/x-nmos/connection/v1.2/single/senders/$SENDER_ID/staged
```

Constraints restrict each parameter to the single concrete value
the sender exposes (no `auto`):

```sh
curl http://localhost:1080/x-nmos/connection/v1.2/single/senders/$SENDER_ID/constraints
```

```json
[
  {
    "mxl_domain_id": {"enum": ["<domain-name>"]},
    "mxl_flow_id": {"enum": ["5fbec3b1-1b0f-417d-9059-8b94a47197ed"]}
  }
]
```

Transport file returns the first transport_params leg as a
standalone object:

```sh
curl http://localhost:1080/x-nmos/connection/v1.2/single/senders/$SENDER_ID/transportfile
```

```json
{
  "mxl_domain_id": "<domain-name>",
  "mxl_flow_id": "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
}
```

### nmos-testing BCP-007-03 suite

The [nmos-testing](https://github.com/AMWA-TV/nmos-testing) tool
includes a BCP-007-03 test suite. To run it against the KIND
cluster:

1. Install nmos-testing per its README.
2. Point it at the agent's Node API URL
   (`http://<node-ip>:1080/x-nmos/node/`).
3. Run the IS-04 Node and IS-05 Connection test suites with the
   BCP-007-03 transport enabled.

The agent proxy passes the IS-04 discovery and IS-05 sender
read-only paths. The staged PATCH path is accepted but does not
mutate state, which is expected for MXL's always-active sender
model.
