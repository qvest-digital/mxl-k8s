# mxl-fabrics-gateway: cgo binary linking libmxl + libmxl-fabrics
# (via github.com/qvest-digital/go-mxl/fabrics).
#
# amd64 runtime swapped to jonasohland/mxl-base build that bundles
# libfabric 2.4 (Amazon EFA-installer rdma-core/libfabric .debs) +
# Jonas's read-only-mmap patched libmxl-fabrics — the combination he
# flagged in dmf-mxl/mxl#516 as the working EFA userspace stack.
#
# arm64 keeps the upstream go-mxl-runtime: Jonas's image is amd64-only
# and arm64 EFA validation is not in scope of this PR. The merge step
# stitches both into a multi-arch manifest as usual.

ARG GO_MXL_TAG=1.0.0-rc.5

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
WORKDIR /workspace
COPY api/ api/
COPY gateway/ gateway/
WORKDIR /workspace/gateway
ENV GOWORK=off
RUN git config --global --add safe.directory '*' && \
    go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-fabrics-gateway ./cmd/mxl-fabrics-gateway

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG} AS libmxl-source

FROM jonasohland/mxl:3518992-fabrics-efa AS runtime-amd64

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG} AS runtime-arm64

ARG TARGETARCH
FROM runtime-${TARGETARCH}

USER root

COPY --from=libmxl-source /opt/libmxl/ /opt/libmxl/

# Keep /opt/libmxl/lib in front so the cgo gateway binary resolves
# libmxl SONAMEs from the build environment's prefix on both bases.
ENV LD_LIBRARY_PATH=/opt/libmxl/lib:/usr/lib:/usr/local/lib

COPY --from=builder /out/mxl-fabrics-gateway /usr/local/bin/mxl-fabrics-gateway
ENTRYPOINT ["/usr/local/bin/mxl-fabrics-gateway"]
