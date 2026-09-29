# mxl-fabrics-gateway: cgo binary linking libmxl + libmxl-fabrics
# (via github.com/qvest-digital/go-mxl/fabrics). Builds in go-mxl's
# published builder image and ships in its runtime image so libmxl,
# libmxl-fabrics, and libfabric are already in place.
# Build context: repo root.

# renovate: datasource=docker depName=ghcr.io/qvest-digital/go-mxl-builder
ARG GO_MXL_TAG=1.1.0-rc.5

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
COPY --from=builder /out/mxl-fabrics-gateway /usr/local/bin/mxl-fabrics-gateway
# The runtime preempts a long-running goroutine by sending its thread
# SIGURG. The gateway's long-running goroutines sit in blocking libfabric
# reads, where the signal cuts epoll_wait short with EINTR and libfabric
# logs "poll failed" for each: thousands of warnings a minute on a busy
# gateway, for preemption that buys nothing inside a cgo call.
#
# Every mirror crosses nodes, so the shared-memory endpoint the EFA
# provider opens beside each of its own carries nothing; in a container's
# 64 MiB /dev/shm it failed to open and warned at every target setup.
ENV GODEBUG=asyncpreemptoff=1 \
    FI_EFA_ENABLE_SHM_TRANSFER=0
ENTRYPOINT ["/usr/local/bin/mxl-fabrics-gateway"]
