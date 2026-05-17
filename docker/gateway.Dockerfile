# mxl-fabrics-gateway: cgo binary linking libmxl + libmxl-fabrics
# (via github.com/qvest-digital/go-mxl/fabrics). Builds in go-mxl's
# published builder image and ships in its runtime image so libmxl,
# libmxl-fabrics, and libfabric are already in place.
# Build context: repo root.

ARG GO_MXL_TAG=1.0.0-rc.3

# Patched libmxl from the qvest fork: drops the noisy MXL_INFO that
# fires on every ReadGrainNonBlocking idle return. Pinned to the
# fork commit and overlaid on top of go-mxl-runtime's libmxl.
ARG LIBMXL_FORK_REPO=https://github.com/qvest-digital/mxl-dmf-demo.git
ARG LIBMXL_FORK_REF=fix/silence-not-ready-info

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS libmxl-builder
ARG LIBMXL_FORK_REPO
ARG LIBMXL_FORK_REF
WORKDIR /src
RUN git clone --depth=1 --branch=${LIBMXL_FORK_REF} ${LIBMXL_FORK_REPO} . && \
    cmake --preset Linux-GCC-Release -DMXL_ENABLE_FABRICS_OFI=ON -DBUILD_DOC=OFF && \
    cmake --build build/Linux-GCC-Release --target mxl mxl-fabrics

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
WORKDIR /workspace
COPY api/ api/
COPY gateway/ gateway/
WORKDIR /workspace/gateway
ENV GOWORK=off
RUN git config --global --add safe.directory '*' && \
    go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-fabrics-gateway ./cmd/mxl-fabrics-gateway

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}
COPY --from=libmxl-builder /src/build/Linux-GCC-Release/lib/libmxl.so.1.1 /opt/libmxl/lib/libmxl.so.1.1
COPY --from=libmxl-builder /src/build/Linux-GCC-Release/lib/fabrics/ofi/libmxl-fabrics.so.1.1 /opt/libmxl/lib/libmxl-fabrics.so.1.1
COPY --from=builder /out/mxl-fabrics-gateway /usr/local/bin/mxl-fabrics-gateway
ENTRYPOINT ["/usr/local/bin/mxl-fabrics-gateway"]
