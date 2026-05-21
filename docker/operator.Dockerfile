# mxl-operator: pure-Go controller-runtime manager. No libmxl.
# Build context: repo root.

ARG GO_VERSION=1.26

FROM docker.io/library/golang:${GO_VERSION}-bookworm AS builder

WORKDIR /workspace
COPY api/ api/
COPY operator/ operator/

WORKDIR /workspace/operator
ENV GOWORK=off
ENV CGO_ENABLED=0
RUN go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-operator ./cmd/mxl-operator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/mxl-operator /usr/local/bin/mxl-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/mxl-operator"]
