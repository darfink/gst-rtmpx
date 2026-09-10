<div align="center">

# `gst-rtmpx`

## GStreamer RTMP source & sink with built-in listen modes

[![CI][ci-shield]][ci]
[![Version][version-shield]][releases]
[![GStreamer][gstreamer-shield]][gstreamer]
[![License][license-shield]][license]

</div>

GStreamer source and sink elements for RTMP, built on the
[rtmpx](https://github.com/darfink/rtmpx) protocol crate.

Both elements can act as a listener: `rtmpxsrc` accepts incoming publisher
connections directly, and `rtmpxsink` waits for and serves incoming player
connections — no separate server element needed.

| Element | Direction | Purpose |
| --- | --- | --- |
| `rtmpxsrc` | source | Receive an RTMP stream: listen for a publisher, or play from a server |
| `rtmpxsink` | sink | Send an RTMP stream: publish to a server, or listen for a player |

Both elements are configured with a single `uri` property; `tc-url` remains
as an optional connect override. A missing app or stream key in listen mode
means "accept any". See [gst-rtmpx/README.md](gst-rtmpx/README.md) for the
full property and behaviour reference.

## Examples

Listen for a publisher and demux — the source is the server, so no
separate RTMP server is needed. Publish to it with `ffmpeg` or any RTMP
encoder at `rtmp://127.0.0.1:1935/live/test`:

```sh
gst-launch-1.0 -e rtmpxsrc uri=rtmp://0.0.0.0:1935/live ! flvdemux name=demux \
  demux.video ! queue ! h264parse ! fakesink sync=false \
  demux.audio ! queue ! aacparse ! fakesink sync=false
```

Serve players directly from the sink — it waits for an incoming player
connection and applies backpressure until one arrives. Play
`rtmp://127.0.0.1:1935/live/test` with `ffplay` or any RTMP player:

```sh
gst-launch-1.0 -e videotestsrc ! x264enc tune=zerolatency ! flvmux ! \
  rtmpxsink mode=listen uri=rtmp://0.0.0.0:1935/live/test
```

Or publish from the sink to a server, then play it back through the source:

```sh
gst-launch-1.0 -e videotestsrc ! x264enc ! flvmux ! rtmpxsink uri=rtmp://127.0.0.1:1935/live/test
```

## Build

Requires Rust 1.97+ and GStreamer 1.28 development headers.

```sh
cargo build --workspace --release
```

Expose the built plugin and confirm both elements register:

```sh
export GST_PLUGIN_PATH="$PWD/target/release"
gst-inspect-1.0 rtmpxsrc rtmpxsink
```

On macOS with the GStreamer framework build, point pkg-config at it first:

```sh
export PKG_CONFIG_PATH=/Library/Frameworks/GStreamer.framework/Versions/Current/lib/pkgconfig:$PKG_CONFIG_PATH
export DYLD_FALLBACK_LIBRARY_PATH=/Library/Frameworks/GStreamer.framework/Versions/Current/lib:$DYLD_FALLBACK_LIBRARY_PATH
export PATH=/Library/Frameworks/GStreamer.framework/Versions/Current/bin:$PATH
```

The `rtmpx` protocol dependency is fetched from GitHub as a git dependency
(pinned via `Cargo.lock`). If your git setup rewrites `https://github.com/`
to SSH, cargo already uses the git CLI for fetching (see
`.cargo/config.toml`), so your normal GitHub SSH credentials apply.

## Testing

```sh
cargo test --workspace
./gst-rtmpx/tests/integration.sh
```

The `ffmpeg_interop` suite needs `ffmpeg`/`ffprobe` on `PATH`.

## Install

With [`cargo-c`](https://github.com/lu-zero/cargo-c):

```sh
cargo install cargo-c
GSTREAMER_LIBDIR="$(pkg-config --variable=libdir gstreamer-1.0)"
cargo cinstall --release --library-type cdylib -p gst-rtmpx \
  --prefix="$(pkg-config --variable=prefix gstreamer-1.0)" \
  --libdir="$GSTREAMER_LIBDIR"
```

Or run the Docker image, which listens on port 1935:

```sh
docker build -t gst-rtmpx .
docker run --rm -p 1935:1935 gst-rtmpx
```

## License

MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.

[ci-shield]: https://img.shields.io/github/actions/workflow/status/darfink/gst-rtmpx/ci.yml?branch=main&label=CI&logo=github&style=for-the-badge
[ci]: https://github.com/darfink/gst-rtmpx/actions/workflows/ci.yml?query=branch%3Amain
[version-shield]: https://img.shields.io/github/v/tag/darfink/gst-rtmpx?style=for-the-badge&label=version
[releases]: https://github.com/darfink/gst-rtmpx/releases
[gstreamer-shield]: https://img.shields.io/badge/GStreamer-1.28+-orange?style=for-the-badge
[gstreamer]: https://gstreamer.freedesktop.org/
[license-shield]: https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green?style=for-the-badge
[license]: https://github.com/darfink/gst-rtmpx
