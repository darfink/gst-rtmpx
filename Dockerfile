# Build and run the rtmpx elements without installing GStreamer locally.
#
#   docker build -t gst-rtmpx .
#   docker run --rm -p 1935:1935 gst-rtmpx
#
# Then publish to rtmp://127.0.0.1:1935/live/test from the host.
#
# Ubuntu 26.04 is the base because it packages GStreamer 1.28, which this
# element's `v1_28` feature requires.
FROM ubuntu:26.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG RUST_VERSION=1.97.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      curl \
      git \
      libgstreamer1.0-dev \
      libgstreamer-plugins-base1.0-dev \
      pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain "${RUST_VERSION}" --profile minimal
ENV PATH=/root/.cargo/bin:${PATH}

WORKDIR /work

# Dependencies first, so editing the elements does not rebuild the world.
COPY Cargo.toml Cargo.lock ./
COPY gst-rtmpx/Cargo.toml gst-rtmpx/build.rs ./gst-rtmpx/
RUN mkdir -p gst-rtmpx/src && echo '' > gst-rtmpx/src/lib.rs && cargo build --release || true

COPY gst-rtmpx/src ./gst-rtmpx/src
RUN touch gst-rtmpx/src/lib.rs && cargo build --release --locked

FROM ubuntu:26.04 AS runtime

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-tools \
    && rm -rf /var/lib/apt/lists/*

# Not /usr/lib/gstreamer-1.0: Debian and Ubuntu put the system plugin directory
# under a multiarch path, so a plain /usr/lib drop is never scanned.
COPY --from=builder /work/target/release/libgstrtmpx.so /usr/local/lib/gstreamer-1.0/
ENV GST_PLUGIN_PATH=/usr/local/lib/gstreamer-1.0

# Fail the build rather than ship an image whose plugins do not load.
RUN gst-inspect-1.0 rtmpxsrc > /dev/null && gst-inspect-1.0 rtmpxsink > /dev/null

EXPOSE 1935

# Accept one publisher and demux it. Override the command to build your own
# pipeline around the elements.
CMD ["gst-launch-1.0", "-e", \
     "rtmpxsrc", "uri=rtmp://0.0.0.0:1935/live", "!", "flvdemux", "name=demux", \
     "demux.video", "!", "queue", "!", "h264parse", "!", "fakesink", "sync=false", \
     "demux.audio", "!", "queue", "!", "aacparse", "!", "fakesink", "sync=false"]
