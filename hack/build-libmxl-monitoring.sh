#!/usr/bin/env bash
# Builds the static libraries of libmxl-monitoring that the gateway's Go
# binding (github.com/marquies/libmxl-monitoring/go) links.
#
# The sources are the module directory gateway/go.mod pins, as the Go
# module cache holds it, so the binding and the libraries are always the
# same version. Needs what the go-mxl-builder image carries: the Go
# toolchain, cmake, ninja, a C++20 compiler and libmxl under /opt/libmxl.
# picojson comes from gateway/third_party.
#
# Usage: hack/build-libmxl-monitoring.sh [OUTPUT_DIR]
#   OUTPUT_DIR defaults to bin/libmxl-monitoring. Add it to CGO_LDFLAGS:
#   CGO_LDFLAGS="$CGO_LDFLAGS -L$OUTPUT_DIR"
#
# Runs on bash 3.2.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
out=${1:-"$root/bin/libmxl-monitoring"}
module=github.com/marquies/libmxl-monitoring

cd "$root/gateway"
version=$(GOWORK=off go list -m -f '{{.Version}}' "$module")
src=$(GOWORK=off go list -m -f '{{.Dir}}' "$module")
if [ -z "$src" ]; then
  GOWORK=off go mod download "$module"
  src=$(GOWORK=off go list -m -f '{{.Dir}}' "$module")
fi

if [ -f "$out/VERSION" ] && [ "$(cat "$out/VERSION")" = "$version" ] &&
  [ -f "$out/libmxl-monitoring.a" ] && [ -f "$out/libmxl-monitoring-prometheus.a" ] && [ -f "$out/LICENSE.picojson" ]; then
  echo "libmxl-monitoring $version already built in $out"
  exit 0
fi

# picojson is header-only and the library's one build dependency besides
# libmxl; it is vendored, so the build fetches nothing.
picojson="$root/gateway/third_party/picojson"

build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT
cmake -S "$src" -B "$build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="${MXL_PREFIX:-/opt/libmxl}" \
  -DMXL_MONITORING_BUILD_TESTS=OFF \
  -DMXL_MONITORING_BUILD_EXAMPLES=OFF \
  -DMXL_MONITORING_PICOJSON_DIR="$picojson" >/dev/null
cmake --build "$build" --target mxl-monitoring mxl-monitoring-prometheus >/dev/null

mkdir -p "$out"
rm -f "$out/libmxl-monitoring.a" "$out/libmxl-monitoring-prometheus.a" "$out/LICENSE"
cp "$build/libmxl-monitoring.a" "$build/libmxl-monitoring-prometheus.a" "$src/LICENSE" "$out/"
# The module cache is read-only, and so is the LICENSE copied from it.
chmod u+w "$out/LICENSE"
cp "$picojson/LICENSE" "$out/LICENSE.picojson"
echo "$version" >"$out/VERSION"
echo "libmxl-monitoring $version built in $out"
