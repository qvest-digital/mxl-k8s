# mxl-fabrics-gateway: cgo binary linking libmxl + libmxl-fabrics
# (via github.com/qvest-digital/go-mxl/fabrics). Builds in go-mxl's
# published builder image and ships in its runtime image so libmxl,
# libmxl-fabrics, and libfabric are already in place.
# Build context: repo root.

# renovate: datasource=docker depName=ghcr.io/qvest-digital/go-mxl-builder
ARG GO_MXL_TAG=1.0.0-rc.9

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
WORKDIR /workspace
COPY api/ api/
COPY gateway/ gateway/
WORKDIR /workspace/gateway
ENV GOWORK=off GOFLAGS=-mod=mod
RUN git config --global --add safe.directory '*' && \
    go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-fabrics-gateway ./cmd/mxl-fabrics-gateway

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}
COPY --from=builder /out/mxl-fabrics-gateway /usr/local/bin/mxl-fabrics-gateway
ENTRYPOINT ["/usr/local/bin/mxl-fabrics-gateway"]
