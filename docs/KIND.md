# Local KIND demo

`make kind-up` brings up a self-contained mxl-k8s deployment on
your laptop and runs an end-to-end TCP flow across two worker
nodes. This page is the longer version of that two-word
instruction: what each step does and what to look at if the
demo gets stuck.

## What runs

[`examples/tcp-demo/`](../examples/tcp-demo/) is a kustomize
bundle with:

- A writer pod (`mxl-tcp-demo-writer`) producing a 1080p29.97
  v210 video flow at the wall-clock grain rate, using go-mxl's
  `write-grain` example binary.
- A reader pod (`mxl-tcp-demo-reader`) consuming the same
  flow id, using go-mxl's `read-grain` example. The reader's
  `podAntiAffinity` forces it onto the other worker node so the
  cross-node mirror is actually exercised.
- An `MxlReceiver` pointing the reader at the writer's flow,
  with `provider: tcp`.

The control plane comes from the standard mxl-k8s install:
operator Deployment, agent and gateway DaemonSets, the five
CRDs.

## What `make kind-up` does

The script in [`hack/kind-up.sh`](../hack/kind-up.sh) is
idempotent and re-runnable:

1. Builds the five component images locally
   (`operator`, `agent`, `gateway`, `shim`, `demo-tools`)
   under the `local/*:dev` tags. Docker's layer cache keeps
   subsequent runs fast.
2. Creates a three-node KIND cluster (control plane plus two
   workers) using
   [`hack/kind-config.yaml`](../hack/kind-config.yaml) if one is
   not already running.
3. Loads the images into every node so the `imagePullPolicy:
   IfNotPresent` references resolve without pulling from a
   registry.
4. Applies the CRDs in their own pass and waits for them to
   reach `Established`. Without this split, resources from the
   same apply can't see freshly-installed CRDs.
5. Applies the tcp-demo bundle.
6. Forces a rollout-restart on the agent, gateway, and operator
   workloads so any newly-rebuilt `:dev` image takes effect.
7. Waits for the resulting `MxlFlowMirror` to reach `Ready`.

When it returns, the writer is producing and the reader is
consuming.

## What to expect

```sh
kubectl --context kind-mxl-k8s-demo -n mxl-system get pods
```

Steady state looks like:

```
NAME                            READY   STATUS    RESTARTS
mxl-domain-agent-...            1/1     Running   0
mxl-domain-agent-...            1/1     Running   0
mxl-fabrics-gateway-...         1/1     Running   0
mxl-fabrics-gateway-...         1/1     Running   0
mxl-operator-...                1/1     Running   0
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

tears the cluster down. Image layers stay in your local Docker
cache; the next `make kind-up` reuses them.

## Cluster name

`make kind-up` looks at the `KIND_CLUSTER` env var (default
`mxl-k8s-demo`) so you can keep multiple parallel clusters if
needed. The status / down targets read the same variable.
