# mxl-fabrics-gateway: cgo binary linking libmxl + libmxl-fabrics
# (via github.com/qvest-digital/go-mxl/fabrics). Builds in go-mxl's
# published builder image. Final runtime image swapped to
# jonasohland/mxl-base build that bundles libfabric 2.4 (Amazon
# EFA-installer rdma-core/libfabric .debs) + Jonas's read-only-mmap
# patched libmxl-fabrics — the combination he flagged in
# dmf-mxl/mxl#516 as the working EFA userspace stack.
# Build context: repo root.

ARG GO_MXL_TAG=1.0.0-rc.5
ARG MXL_BASE_REF=jonasohland/mxl:3518992-fabrics-efa

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
WORKDIR /workspace
COPY api/ api/
COPY gateway/ gateway/
WORKDIR /workspace/gateway
ENV GOWORK=off
RUN git config --global --add safe.directory '*' && \
    go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-fabrics-gateway ./cmd/mxl-fabrics-gateway

# Pull libmxl install from go-mxl-runtime so the cgo gateway binary
# resolves its libmxl SONAMEs at runtime — Jonas's base ships
# libmxl-fabrics but installs to a different prefix; we keep
# /opt/libmxl/lib as the canonical install path the gateway expects.
FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG} AS libmxl-source

FROM ${MXL_BASE_REF}

# Most jonasohland/mxl images run as the unprivileged `mxl` user;
# layer copies + entrypoint need root permissions in the container.
USER root

COPY --from=libmxl-source /opt/libmxl/ /opt/libmxl/

# Jonas's base installs libfabric (with EFA provider) into /usr/lib.
# Keep /opt/libmxl/lib in front so libmxl + libmxl-fabrics linkage
# stays consistent with the gateway binary's build environment.
ENV LD_LIBRARY_PATH=/opt/libmxl/lib:/usr/lib:/usr/local/lib

COPY --from=builder /out/mxl-fabrics-gateway /usr/local/bin/mxl-fabrics-gateway
ENTRYPOINT ["/usr/local/bin/mxl-fabrics-gateway"]
