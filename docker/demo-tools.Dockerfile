# demo-tools: minimal image carrying go-mxl's write-grain and
# read-grain example binaries, on top of go-mxl-runtime.
#
# These pure-Go (cgo-to-libmxl) tools sidestep the GStreamer-plugin
# rabbit hole `mxl-gst-testsrc` runs into on Debian trixie (where
# the pango plugin shipping textoverlay/clockoverlay isn't packaged)
# while still producing a real, schema-correct flow on the local
# MXL domain.
#
# Build:
#   docker build -f docker/demo-tools.Dockerfile -t local/mxl-demo-tools:dev .

# renovate: datasource=docker depName=ghcr.io/qvest-digital/go-mxl-builder
ARG GO_MXL_TAG=1.0.0-rc.7

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS builder
ARG GO_MXL_TAG
ENV GOBIN=/out
RUN go install \
        github.com/qvest-digital/go-mxl/examples/write-grain@v${GO_MXL_TAG} \
        github.com/qvest-digital/go-mxl/examples/read-grain@v${GO_MXL_TAG}

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}
COPY --from=builder /out/write-grain /usr/local/bin/write-grain
COPY --from=builder /out/read-grain  /usr/local/bin/read-grain
