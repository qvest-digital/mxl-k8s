#!/usr/bin/env bash
# check-go-mxl-pins.sh -- fail when the go-mxl pins disagree.
#
# The gateway and exporter compile against the go-mxl Go module named in
# their go.mod, but the images ship the libmxl of the go-mxl builder and
# runtime images named by GO_MXL_TAG in docker/. CI compiles in the image
# named by docker/gateway.Dockerfile, so a go.mod that moves without the
# Dockerfiles tests one libmxl and releases another.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -eu

REF_FILE="docker/gateway.Dockerfile"
DOCKERFILES="${DOCKERFILES:-docker/gateway.Dockerfile docker/exporter.Dockerfile docker/demo-tools.Dockerfile}"
MODULES="${MODULES:-gateway exporter}"
PROBE="hack/flow-probe.sh"
GO_MXL_MODULE="github.com/qvest-digital/go-mxl"

arg_of() {
  sed -nE 's/^ARG GO_MXL_TAG=([^[:space:]]+).*/\1/p' "$1" | head -1
}

tag="$(arg_of "$REF_FILE")"
[ -n "$tag" ] || { echo "no ARG GO_MXL_TAG in ${REF_FILE}" >&2; exit 2; }

rc=0
check() {
  if [ "$2" = "$tag" ]; then
    echo "ok   $1: ${2}"
  else
    echo "FAIL $1: ${2:-<none>}, want ${tag} as in ${REF_FILE}" >&2
    rc=1
  fi
}

for f in $DOCKERFILES; do
  check "$f" "$(arg_of "$f")"
done

for m in $MODULES; do
  v="$(sed -nE "s#^[[:space:]]*${GO_MXL_MODULE} v([^[:space:]]+).*#\1#p" "${m}/go.mod" | head -1)"
  check "${m}/go.mod" "$v"
done

check "$PROBE" "$(sed -nE 's/^GO_MXL_TAG=\$\{GO_MXL_TAG:-([^}]+)\}.*/\1/p' "$PROBE" | head -1)"

exit "$rc"
