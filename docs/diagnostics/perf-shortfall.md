# Gateway steady-state fps shortfall diagnostics

This runbook helps diagnose a steady-state fps shortfall in the mxl-k8s gateway when grain transfer appears healthy by gateway-side telemetry but consumer-side framerate is below target. The captures here distinguish between gateway-side latency (the current hypothesis) and consumer-side processing limits.

## Prerequisites

To run these diagnostics, the operator needs:

- `kubectl` with cluster access
- `go` 1.26+ on the diagnostician's workstation (for `go tool trace` and `go tool pprof`)
- `bpftrace` on the gateway nodes (host, not inside the pod)
- The gateway pod started with `--pprof-bind-address=127.0.0.1:6060` (or whatever loopback bind address was chosen). This requires the `gateway.flags.pprofBindAddress` chart value to be set. This flag is added in PR-D `feat(gateway): pprof bind address flag`.

## Capture A: Gateway in-process pprof trace

This capture answers whether the transfer loop is blocking in the hot path.

```bash
kubectl -n mxl-system exec <gateway-pod> -- \
  curl -s http://127.0.0.1:6060/debug/pprof/trace?seconds=20 > /tmp/gw.trace
go tool trace /tmp/gw.trace
```

### What to look for

In `go tool trace`, sort goroutines by latency and look for the `runTransferLoop` goroutine (spawned at `gateway/internal/mirror/source.go:262`). Inspect blocking syscall durations near the cgo crossing into libmxl-fabrics. The `MakeProgressNonBlocking` call at `gateway/internal/mirror/source.go:579` is the cgo call site. The loop's outer ticker is set at `progressInterval = 2 * time.Millisecond` (see `gateway/internal/mirror/source.go:235-237`).

If `MakeProgress` median latency consistently exceeds 1 ms, the gateway hot-path is a suspect.

## Capture B: Consumer-side grain cadence

This capture isolates whether the consumer is seeing grains in real time from the libmxl reader, before mediamtx transcoding.

```bash
kubectl -n mxl-demo exec deploy/reader-media-function -c mediamtx -- \
  strace -e trace=read,readv -p $(pgrep -f 'mxl-source') -tt -T 2>&1 | head -200
```

### What to look for

If the strace output shows a cadence close to on-time at 30 fps but mediamtx still outputs below target, the problem is downstream of the libmxl reader (in mediamtx-mxl source itself; see issue `qvest-digital/mediamtx#3` for the parallel mediamtx investigation).

## Capture C: RDMA HCA counters

Counters that increment during the slow period are evidence of fabric-layer trouble (link errors, retransmits, buffer overruns). Counters at zero rule out the fabric layer.

```bash
for c in port_xmit_data port_rcv_data port_xmit_packets port_rcv_packets \
         symbol_error port_xmit_discards port_rcv_errors VL15_dropped \
         local_link_integrity_errors port_rcv_constraint_errors \
         port_xmit_constraint_errors excessive_buffer_overrun_errors; do
  grep -H . /sys/class/infiniband/*/ports/*/counters/$c 2>/dev/null
done
```

## Capture D: libfabric provider state

This confirms the verbs provider is healthy, the HCA is visible to libfabric, and the RDMA link is up.

```bash
kubectl -n mxl-system exec <gateway-pod> -- fi_info -p verbs
kubectl -n mxl-system exec <gateway-pod> -- ibv_devinfo -v
kubectl -n mxl-system exec <gateway-pod> -- rdma statistic show link
```

## Capture E: Kernel RDMA path histogram

This capture requires `bpftrace` and kallsyms on the host, plus the in-tree mlx5/ib_core symbols. Before relying on this, confirm `bpftrace -l 'kprobe:ib_post_send'` returns the symbol.

```bash
bpftrace -e 'kprobe:ib_post_send { @[comm] = count(); } interval:s:5 { exit(); }'
```

## Capture F: Raw libmxl-fabrics status code

The `unrecognized status` error strings originate from `gateway/internal/mirror/source.go:579` (the `errors.Is(err, fabrics.ErrNotReady)` filter) but the wrapper does not expose the integer status code. This is tracked upstream in `qvest-digital/go-mxl#45`.

Until that issue lands, capture the raw status code via one of:

- Attach a core dump and inspect with `dlv` on the next occurrence
- Wait for the typed-error refactor in go-mxl to surface the integer code

When you have the status code, file it as a comment on the `qvest-digital/go-mxl#45` issue.

## Triage flowchart

- **Capture A shows blocking in MakeProgress > 1 ms median:** Gateway hot-path issue. Escalate to gateway maintainers with the pprof trace attached.

- **Capture A is clean AND Capture B shows on-time grain cadence:** Consumer-side issue (mediamtx, ffmpeg). See `qvest-digital/mediamtx#3` for the mediamtx-mxl re-attachment problem.

- **Capture C shows HCA counter increments:** Fabric or driver issue. Escalate to RDMA/networking team with counter names and delta values.

- **Capture D shows libfabric provider misconfigured** (e.g., wrong device, wrong network namespace): Chart values issue. Check `gateway.flags.providers` and the NAD attachment. Review the configuration guidance in `docs/RDMA.md`.

- **Capture F shows a specific libmxl status code:** File it as a comment on `qvest-digital/go-mxl#45`.

## References

### Source code

- `gateway/internal/mirror/source.go:165-329` — `Reconcile` and the source-entry lifecycle
- `gateway/internal/mirror/source.go:235-237` — `progressInterval` default
- `gateway/internal/mirror/source.go:262` — `runTransferLoop` go statement
- `gateway/internal/mirror/source.go:579` — `MakeProgress` error filter
- `gateway/internal/mirror/source.go:585-590` — `isReaderAgedOut`

### Related issues

- `qvest-digital/go-mxl#45` — Typed error for `fabrics.ErrNotReady` / `unrecognized status`
- `qvest-digital/mediamtx#3` — mediamtx-mxl source does not re-attach after writer epoch change

### Related chart values

- `gateway.flags.pprofBindAddress` (added in PR-D `feat(gateway): pprof bind address flag`)
- `gateway.flags.providers`
- `gateway.rdma.resourceName`
