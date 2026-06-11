# libmxl-intent.so builder + carrier image.
#
# The runtime stage exists only to hold the compiled .so so a
# consumer pod can mount it as an initContainer and copy it onto an
# emptyDir shared with the main container.
# Build context: repo root.
#
# Builder is pinned to Rocky Linux 8 (glibc 2.28) so the resulting
# .so links against versioned glibc symbols no newer than 2.28 and
# stays loadable on EL8-based consumer images. A trixie builder
# produced an .so that failed to load on RHEL/Rocky/Alma 8 hosts
# with "version GLIBC_2.34 not found".

FROM docker.io/library/rockylinux:8 AS builder
RUN dnf install -y gcc glibc-devel make && dnf clean all
WORKDIR /src
COPY shim/libmxl-intent.c shim/Makefile ./
RUN make

FROM docker.io/library/debian:trixie-slim
COPY --from=builder /src/libmxl-intent.so /opt/mxl-intent/libmxl-intent.so
# Default: copy the .so to /shared and exit. Consumer pods override
# the command if they need a different drop path.
CMD ["sh", "-c", "cp /opt/mxl-intent/libmxl-intent.so /shared/libmxl-intent.so"]
