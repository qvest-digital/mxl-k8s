#!/usr/bin/env bash
# Builds the static libraries of libmxl-monitoring that the gateway's Go
# binding (github.com/marquies/libmxl-monitoring/go) links.
#
# The sources are the module directory gateway/go.mod pins, as the Go
# module cache holds it, so the binding and the libraries are always the
# same version. Needs what the go-mxl-builder image carries: the Go
# toolchain, cmake, ninja, a C++20 compiler, libmxl under /opt/libmxl and
# vcpkg for picojson.
#
# Usage: hack/build-libmxl-monitoring.sh [OUTPUT_DIR]
#   OUTPUT_DIR defaults to bin/libmxl-monitoring. Point CGO_LDFLAGS at it:
#   CGO_LDFLAGS="-L$OUTPUT_DIR"
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
  [ -f "$out/libmxl-monitoring.a" ] && [ -f "$out/libmxl-monitoring-prometheus.a" ]; then
  echo "libmxl-monitoring $version already built in $out"
  exit 0
fi

# picojson is header-only and the library's one build dependency besides
# libmxl. vcpkg runs in classic mode from a directory without a manifest.
picojson=""
find_picojson() {
  for d in "${VCPKG_ROOT:-/root/vcpkg}"/installed/*/include; do
    if [ -f "$d/picojson/picojson.h" ]; then
      picojson=$d
      return 0
    fi
  done
  return 1
}
if ! find_picojson; then
  (cd / && "${VCPKG_ROOT:-/root/vcpkg}/vcpkg" install picojson >/dev/null)
  find_picojson || {
    echo "build-libmxl-monitoring: picojson not found after vcpkg install" >&2
    exit 1
  }
fi

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
cp "$build/libmxl-monitoring.a" "$build/libmxl-monitoring-prometheus.a" "$src/LICENSE" "$out/"
echo "$version" >"$out/VERSION"
echo "libmxl-monitoring $version built in $out"
