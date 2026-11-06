# mxl-fabrics-gateway: cgo binary linking libmxl + libmxl-fabrics
# (via github.com/qvest-digital/go-mxl/fabrics). Builds in go-mxl's
# published builder image and ships in its runtime image so libmxl,
# libmxl-fabrics, and libfabric are already in place.
# Build context: repo root.

# renovate: datasource=docker depName=ghcr.io/qvest-digital/go-mxl-builder
ARG GO_MXL_TAG=1.1.0-rc.13

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
WORKDIR /workspace
COPY api/ api/
COPY gateway/ gateway/
COPY hack/build-libmxl-monitoring.sh hack/
WORKDIR /workspace/gateway
ENV GOWORK=off
# libmxl-monitoring's static libraries are built from the module
# directory gateway/go.mod pins, so they match its Go binding.
RUN git config --global --add safe.directory '*' && \
    go mod download && \
    ../hack/build-libmxl-monitoring.sh /opt/libmxl-monitoring && \
    CGO_LDFLAGS=-L/opt/libmxl-monitoring go build -trimpath -ldflags="-s -w" -o /out/mxl-fabrics-gateway ./cmd/mxl-fabrics-gateway

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}
COPY --from=builder /out/mxl-fabrics-gateway /usr/local/bin/mxl-fabrics-gateway
# libmxl-monitoring is linked statically (Apache-2.0).
COPY --from=builder /opt/libmxl-monitoring/LICENSE /usr/share/licenses/libmxl-monitoring/LICENSE
# The runtime preempts a long-running goroutine by sending its thread
# SIGURG. The gateway's long-running goroutines sit in blocking libfabric
# reads, where the signal cuts epoll_wait short with EINTR and libfabric
# logs "poll failed" for each: thousands of warnings a minute on a busy
# gateway, for preemption that buys nothing inside a cgo call.
#
# Every mirror crosses nodes, so the shared-memory endpoint the EFA
# provider opens beside each of its own carries nothing; in a container's
# 64 MiB /dev/shm it failed to open and warned at every target setup.
#
# A GODEBUG set through the chart's extraEnv replaces this value, so it
# has to repeat asyncpreemptoff=1.
ENV GODEBUG=asyncpreemptoff=1 \
    FI_EFA_ENABLE_SHM_TRANSFER=0
ENTRYPOINT ["/usr/local/bin/mxl-fabrics-gateway"]
