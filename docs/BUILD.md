# Local build

This repo is a Go workspace with five modules. Two of them (`agent`,
`gateway`) link against libmxl and libmxl-fabrics via cgo; the rest are
pure Go.

## Toolchain

- Go ≥ 1.26
- A C/C++ toolchain (clang-19 or gcc), CMake ≥ 3.20, Ninja, pkg-config.
- Linux kernel ≥ 5.9 on the host that will run the agent (for
  `fanotify` directory-event support).

## Pure-Go modules

`api`, `ipc`, and `operator` build without any system libraries:

```sh
for m in api ipc operator; do (cd "$m" && go build ./... && go vet ./...); done
```

## CGo modules (libmxl + libmxl-fabrics)

`agent` requires libmxl. `gateway` additionally requires libmxl-fabrics.
Both are built from the same upstream source pinned by
[`.github/libmxl.version`](../.github/libmxl.version) — the file holds
one `https://github.com/<owner>/<repo>/tree/<ref>` URL. Renovate
maintains it; humans should update it the same way.

### Install once

```sh
# Build deps.
sudo apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build clang-19 lld-19 \
    pkg-config bison flex curl zip unzip tar git ca-certificates \
    libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
    libfabric-dev libfabric-bin

# vcpkg (libmxl pulls its third-party deps through vcpkg).
git clone --filter=tree:0 https://github.com/microsoft/vcpkg.git ~/vcpkg
~/vcpkg/bootstrap-vcpkg.sh -disableMetrics

# Read the pin and split the URL.
url=$(tr -d '[:space:]' < .github/libmxl.version)
rest=${url#https://github.com/}
repo=${rest%%/tree/*}
ref=${rest#*/tree/}

# Clone and build libmxl with fabrics enabled.
git clone "https://github.com/$repo.git" /tmp/mxl
git -C /tmp/mxl checkout "$ref"
cmake --preset Linux-Clang-Release -S /tmp/mxl \
    -DBUILD_DOCS=OFF \
    -DMXL_ENABLE_FABRICS_OFI=ON \
    -DCMAKE_INSTALL_PREFIX=/opt/libmxl
cmake --build /tmp/mxl/build/Linux-Clang-Release
sudo cmake --install /tmp/mxl/build/Linux-Clang-Release

# libmxl.pc declares Requires.private: spdlog but ships spdlog as a
# static lib; strip the line so pkg-config doesn't trip over it.
sudo sed -i '/^Requires.private:/d' /opt/libmxl/lib/pkgconfig/libmxl.pc
```

### Per-shell

```sh
export PKG_CONFIG_PATH=/opt/libmxl/lib/pkgconfig
export LD_LIBRARY_PATH=/opt/libmxl/lib
```

Verify pkg-config sees both libraries:

```sh
pkg-config --exists libmxl libmxl-fabrics && echo OK
```

### Build the CGo modules

```sh
for m in agent gateway; do (cd "$m" && go build ./... && go vet ./...); done
```

> The `agent` module requires `github.com/qvest-digital/go-mxl` and the
> `gateway` module requires `github.com/qvest-digital/go-mxl/fabrics`.
> Both must be tagged on the proxy before these builds succeed without
> a `go.work`-level replace.

## Integration tests

Integration tests under the `mxl_integration` build tag exercise a real
libmxl install (and, for the gateway, real fabric endpoints):

```sh
(cd agent && go test -tags mxl_integration ./...)
```

The default unit/vet/build jobs don't run these.

## Future direction

The local recipe above is the baseline. A devcontainer image that bakes
in the toolchain, the pinned libmxl, and libfabric is on the roadmap;
when it lands, this document will move the manual recipe to an appendix
and recommend the devcontainer for everyday work.
