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

`make` also runs `make check`, which fails when the `.so` requires a
versioned glibc symbol newer than `GLIBC_FLOOR` (2.28, the EL8
version). The shim is preloaded into consumer images the build never
sees, so the build glibc is the floor of the supported consumer set:
a `.so` above it does not load, and the loader reports the failure
against the consumer rather than the shim. What keeps the requirement
low is reaching libc through direct syscalls instead of `dlsym`,
which lands at 2.34 where libdl merged into libc. Override the floor
with `make GLIBC_FLOOR=2.17` to target something older.

## Use

Every consumer pod mounts the agent's `/run/mxl` and sets
`LD_PRELOAD` to a copy of this `.so`. `MXL_INTENT_SOCK` overrides
the socket path, which defaults to `/run/mxl/agent.sock`.

Mount the containing directory (`/run/mxl`), not the socket file by
itself: a single-file hostPath mount pins the socket's inode at
container start, and the agent unlinks and recreates `agent.sock` on
every restart. A consumer pod already running when that happens keeps
the old, orphaned inode and never reaches the agent again until the
pod itself restarts. Mounting the directory re-resolves the path on
every connect, so it always reaches whichever agent is currently
listening.

What differs between the two delivery methods is where the `.so`
comes from.

### From the node

The agent carries this library and writes it to
`/run/mxl/libmxl-intent.so` on every start, so the mount the consumer
already needs for the socket carries the shim too:

```yaml
env:
  - name: LD_PRELOAD
    value: /run/mxl/libmxl-intent.so
```

Nothing else goes into the pod spec. `--shim-path` (the agent flag,
`agent.flags.shimPath` in the chart) moves the drop or, set empty,
turns it off.

See `examples/tcp-demo/23-reader-audio.yaml` for a working example.

### From the carrier image

`docker/shim.Dockerfile` builds an image whose only content is the
compiled `.so` at `/opt/mxl-intent/libmxl-intent.so`. A consumer runs
it as an `initContainer` that copies the file into an `emptyDir`
shared with the main container, and preloads it from there.

See `examples/tcp-demo/21-reader.yaml` for a working example.

### Which one

The node-delivered copy needs nothing in the pod spec beyond the
mount and the `LD_PRELOAD` value, and it always matches the agent
answering on the socket, because the two ship in one image.

The carrier image is what a consumer uses to pin a shim version of
its own, independent of the agent the cluster runs, or where the node
drop is turned off.

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

(meaning the open should now succeed -- the shim retries it) or:

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

The stat pair is exported twice. glibc from 2.33 on has plain `stat`
and `lstat` symbols; before that `<sys/stat.h>` rewrites the call to
`__xstat(_STAT_VER, ...)`, so a consumer built against an older
glibc reaches `__xstat` / `__lxstat` and never the plain names. Both
spellings are hooked, along with the `_64` variants a consumer
compiled with `_FILE_OFFSET_BITS=64` uses. A consumer linked
against a pre-2.33 glibc depends on this: without it its directory
probe returns `ENOENT` unseen and no intent is ever raised.

Calls that don't return `ENOENT`, and calls whose target path
doesn't match `.../*.mxl-flow/flow_def.json`, fall straight through
to glibc. Direct syscalls (e.g. `syscall(SYS_openat, ...)` from Go)
bypass the shim; libmxl uses libc symbols, so this is fine.
