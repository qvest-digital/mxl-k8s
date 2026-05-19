# libmxl-intent.so

LD_PRELOAD shim that turns the `ENOENT` a libmxl consumer hits on
`mxlCreateFlowReader(flowID)` for a not-yet-materialized flow into a
synchronous wait until the agent has arranged for the flow to appear
locally.

## Build

```sh
make
```

Produces `libmxl-intent.so`. With the runtime image
(`docker/shim.Dockerfile`) you don't need to build this by hand; the
image ships the compiled `.so` at
`/opt/mxl-intent/libmxl-intent.so`.

## Use

In a consumer pod:

1. Add an `initContainer` that copies `libmxl-intent.so` out of the
   shim image into a shared `emptyDir` volume.
2. Mount that volume into the main container.
3. Set `LD_PRELOAD=/path/to/libmxl-intent.so`.
4. Mount the agent's UDS (`/run/mxl/agent.sock`) into the main
   container so the shim can reach it. `MXL_INTENT_SOCK` overrides
   the default path.

See `examples/tcp-demo/21-reader.yaml` for a working example.

## Protocol

One line of JSON each direction over `/run/mxl/agent.sock`. The
shim sends:

```json
{"path":"/run/mxl/domain/<uuid>.mxl-flow/flow_def.json"}
```

The agent replies with either:

```json
{"ok":true}
```

(meaning the open should now succeed — the shim retries it) or:

```json
{"ok":false,"error":"<reason>"}
```

(meaning the agent gave up; the shim returns the original `ENOENT`).

The agent identifies the calling pod via `SO_PEERCRED` on the UDS,
so the shim never asserts its own identity.

## What it hooks, and what it doesn't

The shim overrides `openat(2)`, `open(2)`, `access(2)`, `stat(2)`,
and `lstat(2)`. libmxl probes the flow_def.json with `access` and
`stat` before it ever reaches `open`, so hooking `openat` alone
left the very first `mxlCreateFlowReader` call returning
`FLOW_NOT_FOUND` without the shim being consulted.

Calls that don't return `ENOENT`, and calls whose target path
doesn't match `.../*.mxl-flow/flow_def.json`, fall straight through
to glibc. Direct syscalls (e.g. `syscall(SYS_openat, ...)` from Go)
bypass the shim; libmxl uses libc symbols, so this is fine.
