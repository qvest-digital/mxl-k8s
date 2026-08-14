# mxl-domain-agent: pure-Go DaemonSet. Uses fanotify via
# golang.org/x/sys/unix; no cgo, no libmxl. Linux-only.
# Build context: repo root.

ARG GO_VERSION=1.26

# The agent drops libmxl-intent.so on the node at startup, so it
# carries its own build of the shim rather than the carrier image's:
# the .so a node serves then always matches the agent serving the
# intent socket it talks to. Same source, same glibc floor check as
# docker/shim.Dockerfile.
FROM docker.io/library/debian:trixie-slim AS shim-builder
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc6-dev make binutils && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY shim/libmxl-intent.c shim/Makefile ./
RUN make

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
COPY --from=shim-builder /src/libmxl-intent.so /opt/mxl-intent/libmxl-intent.so
ENTRYPOINT ["/usr/local/bin/mxl-domain-agent"]
