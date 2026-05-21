# mxl-domain-agent: pure-Go DaemonSet. Uses fanotify via
# golang.org/x/sys/unix; no cgo, no libmxl. Linux-only.
# Build context: repo root.

ARG GO_VERSION=1.26

FROM docker.io/library/golang:${GO_VERSION}-bookworm AS builder

WORKDIR /workspace
COPY api/ api/
COPY agent/ agent/

WORKDIR /workspace/agent
ENV GOWORK=off
ENV CGO_ENABLED=0
ENV GOOS=linux
RUN go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-domain-agent ./cmd/mxl-domain-agent

# Runtime stage. Runs as root because:
#  - fanotify_init in FAN_CLASS_NOTIF mode needs CAP_SYS_ADMIN;
#  - the intent socket at /run/mxl/agent.sock has to be created in
#    a host-owned tmpfs path that's root-only by default on most
#    distros.
# Both can be relaxed (rootless + chowned bind-mounts) in a later
# hardening pass; for now the DaemonSet manifest grants SYS_ADMIN
# alongside running as root.
FROM gcr.io/distroless/static-debian12:latest
COPY --from=builder /out/mxl-domain-agent /usr/local/bin/mxl-domain-agent
ENTRYPOINT ["/usr/local/bin/mxl-domain-agent"]
