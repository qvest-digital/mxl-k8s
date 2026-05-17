# mxl-domain-agent: pure-Go DaemonSet. Uses fanotify via
# golang.org/x/sys/unix; no cgo, no libmxl. Linux-only.
# Build context: repo root.

ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-bookworm AS builder

WORKDIR /workspace
COPY api/ api/
COPY agent/ agent/

WORKDIR /workspace/agent
ENV GOWORK=off
ENV CGO_ENABLED=0
ENV GOOS=linux
RUN go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-domain-agent ./cmd/mxl-domain-agent

# Runtime stage. Stays on distroless even though the agent needs
# CAP_SYS_ADMIN at runtime for fanotify_init — the capability is
# granted in the DaemonSet's securityContext; the binary itself
# does not need to be root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/mxl-domain-agent /usr/local/bin/mxl-domain-agent
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/mxl-domain-agent"]
