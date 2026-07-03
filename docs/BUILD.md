# Local build

This repo is a Go workspace with four modules. One of them (`gateway`)
links libmxl and libmxl-fabrics via cgo, transitively through the
`go-mxl` binding; the rest are pure Go.

## Toolchain

- Go >= 1.26.
- For the `gateway` cgo build: a C compiler, `pkg-config`, and libmxl +
  libmxl-fabrics installed with headers and pkg-config files (see
  below).
- Linux kernel >= 5.17 on the host that will run the agent. The
  agent's `fanotify` watcher needs `FAN_REPORT_DFID_NAME`.

## Pure-Go modules

`api`, `operator`, and `agent` build without any system libraries.
`agent` watches the libmxl domain through `fanotify`
(`golang.org/x/sys/unix`) and does not link libmxl itself:

```sh
for m in api operator agent; do (cd "$m" && go build ./... && go vet ./...); done
```

## CGo module: gateway (libmxl + libmxl-fabrics)

`gateway` is the only cgo module. It imports the `go-mxl` binding
(`go-mxl/mxl` and `go-mxl/fabrics`), so the build links both libmxl and
libmxl-fabrics. Both libraries must be installed with headers and
pkg-config files (`libmxl.pc`, `libmxl-fabrics.pc`). `go-mxl` owns the
libmxl version.

### In the go-mxl builder image (CI path)

CI and [`docker/gateway.Dockerfile`](../docker/gateway.Dockerfile) build
`gateway` inside the published `go-mxl` builder image, which already has
the Go toolchain, libmxl, libmxl-fabrics, libfabric, and a working
`PKG_CONFIG_PATH`. The image tag tracks the `go-mxl` release the
`gateway` module requires (currently
`ghcr.io/qvest-digital/go-mxl-builder:1.0.0-rc.10`); bump it together
with the `go-mxl` require in `gateway/go.mod`.

### On the host

Install libmxl + libmxl-fabrics by following
[go-mxl's README](https://github.com/qvest-digital/go-mxl), then point
pkg-config at them and build:

```sh
export PKG_CONFIG_PATH=/opt/libmxl/lib/pkgconfig
export LD_LIBRARY_PATH=/opt/libmxl/lib
pkg-config --exists libmxl libmxl-fabrics && echo OK
(cd gateway && go build ./... && go vet ./...)
```

## Integration tests

Integration testing runs against a local KIND cluster: `make kind-up`
brings up the demo and `make kind-test` runs the suite (see
[`docs/KIND.md`](KIND.md)). Plain `go test ./...` per module needs no
cluster.

## Graphify

This repo carries a [Graphify](https://github.com/safishamsi/graphify)
knowledge graph under `graphify-out/`. The graph is committed so a
fresh clone already has it; `.graphifyignore` controls what gets
indexed.

Graphify is optional. To rebuild the graph locally, query it, or
have it auto-rebuild after each commit, install the `graphifyy`
PyPI package (CLI: `graphify`) and run `graphify hook install`.
Manage the hook with `graphify hook status` and
`graphify hook uninstall`.

See the upstream
[common commands](https://github.com/safishamsi/graphify#common-commands)
for usage.
