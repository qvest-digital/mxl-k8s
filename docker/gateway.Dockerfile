# mxl-fabrics-gateway: cgo binary linking libmxl + libmxl-fabrics
# (via github.com/qvest-digital/go-mxl/fabrics). Builds in go-mxl's
# published builder image and ships in its runtime image so libmxl,
# libmxl-fabrics, and libfabric are already in place.
# Build context: repo root.

ARG GO_MXL_TAG=1.0.0-rc.5

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
WORKDIR /workspace
COPY api/ api/
COPY gateway/ gateway/
WORKDIR /workspace/gateway
ENV GOWORK=off
RUN git config --global --add safe.directory '*' && \
    go mod download && \
    go build -trimpath -ldflags="-s -w" -o /out/mxl-fabrics-gateway ./cmd/mxl-fabrics-gateway

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG} AS efa-libfabric

# Debian trixie's libfabric1 is built WITHOUT --enable-efa (see
# https://sources.debian.org/data/main/libf/libfabric/2.1.0-1.1/debian/rules)
# and libibverbs has no EFA userspace provider. As a result the
# gateway running on AWS EKS EFA nodes reports
#   fi_getinfo: provider efa output empty list
# and falls through to TCP. Layer the AWS EFA installer's
# in-container subset (libfabric + libibverbs + EFA userspace
# provider) on top of go-mxl-runtime so the upstream image gains a
# real EFA path while the rest of the libmxl install stays untouched.
ARG EFA_INSTALLER_VERSION=latest
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl tar xz-utils \
 && curl -fsSL "https://efa-installer.amazonaws.com/aws-efa-installer-${EFA_INSTALLER_VERSION}.tar.gz" \
      -o /tmp/efa.tar.gz \
 && tar -xzf /tmp/efa.tar.gz -C /tmp \
 && cd /tmp/aws-efa-installer \
 # --skip-kmod / --skip-limit-conf / -k: don't touch host kernel
 # modules or rlimits — container talks to host-loaded EFA driver
 # through the device files the EFA k8s device plugin injects.
 # -n: no kernel-module rebuild.
 && ./efa_installer.sh -y -n --skip-kmod --skip-limit-conf \
 && cd / \
 && rm -rf /tmp/efa.tar.gz /tmp/aws-efa-installer \
 && rm -rf /var/lib/apt/lists/*

FROM efa-libfabric
COPY --from=builder /out/mxl-fabrics-gateway /usr/local/bin/mxl-fabrics-gateway
ENTRYPOINT ["/usr/local/bin/mxl-fabrics-gateway"]
