# libmxl-intent.so builder + carrier image.
#
# The runtime stage exists only to hold the compiled .so so a
# consumer pod can mount it as an initContainer and copy it onto an
# emptyDir shared with the main container.
# Build context: repo root.

# binutils carries the objdump the Makefile's glibc floor check runs. It
# arrives with gcc today, named here because the check fails the build without
# it rather than skipping quietly.
FROM docker.io/library/debian:trixie-slim AS builder
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc6-dev make binutils && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY shim/libmxl-intent.c shim/Makefile ./
# make runs the floor check, so a build whose glibc symbols outrun the oldest
# supported consumer fails here rather than at that consumer's pod start.
RUN make

FROM docker.io/library/debian:trixie-slim
COPY --from=builder /src/libmxl-intent.so /opt/mxl-intent/libmxl-intent.so
# Default: copy the .so to /shared and exit. Consumer pods override
# the command if they need a different drop path.
CMD ["sh", "-c", "cp /opt/mxl-intent/libmxl-intent.so /shared/libmxl-intent.so"]
