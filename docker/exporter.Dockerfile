# mxl-flow-exporter: cgo binary linking libmxl (via
# github.com/qvest-digital/go-mxl/mxl). Builds in go-mxl's published
# builder image and ships in its runtime image so libmxl is already in
# place. Unlike the gateway it does not link libmxl-fabrics: it only
# reads flow headers from the local domain.
# Build context: repo root.

# renovate: datasource=docker depName=ghcr.io/qvest-digital/go-mxl-builder
ARG GO_MXL_TAG=1.0.0-rc.15

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
WORKDIR /workspace
COPY api/ api/
COPY exporter/ exporter/
WORKDIR /workspace/exporter
ENV GOWORK=off
RUN git config --global --add safe.directory '*' && \
    go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-flow-exporter ./cmd/mxl-flow-exporter

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}
COPY --from=builder /out/mxl-flow-exporter /usr/local/bin/mxl-flow-exporter
ENTRYPOINT ["/usr/local/bin/mxl-flow-exporter"]
