<div align="center">

# `gst-rtmpx`

## GStreamer RTMP and RTMPS source & sink with built-in listen modes

[![CI][ci-shield]][ci]
[![crates.io][crate-shield]][crate]
[![Version][version-shield]][releases]
[![GStreamer][gstreamer-shield]][gstreamer]
[![License][license-shield]][license]

</div>

GStreamer source and sink elements for RTMP and RTMPS, built on [`rtmpx`](https://github.com/darfink/rtmpx).

Both elements default to client behavior (like `rtmp2src` and `rtmp2sink`), and either can switch to server mode with `mode=listen`—no separate RTMP server required:

- **`rtmpxsrc`** receives an RTMP stream by playing from a server (`mode=play`) or listening for an incoming publisher (`mode=listen`).
- **`rtmpxsink`** sends an RTMP stream by publishing to a server (`mode=publish`) or listening for an incoming player (`mode=listen`).

Both elements support plain `rtmp://` and encrypted `rtmps://` (including mutual TLS), advertise Enhanced RTMP (so modern encoders and servers negotiate HEVC, AV1, Opus, and multitrack alongside classic H.264/AAC), and take a single `uri` property (`tc-url` is an optional connect override). In listen mode, omitting the app or stream key from `uri` accepts any value.

## Examples

1. **Play from an RTMP server:**

```sh
gst-launch-1.0 -e rtmpxsrc uri=rtmp://127.0.0.1:1935/live/test ! flvdemux ! fakesink
```

2. **Publish to an RTMP server:**

```sh
gst-launch-1.0 -e videotestsrc ! x264enc tune=zerolatency ! flvmux \
  ! rtmpxsink uri=rtmp://127.0.0.1:1935/live/test
```

3. **Listen for an incoming publisher (`rtmpxsrc` as server):**

```sh
gst-launch-1.0 -e rtmpxsrc mode=listen uri=rtmp://0.0.0.0:1935/live ! flvdemux ! fakesink
```

4. **Listen for an incoming player (`rtmpxsink` as server):**

```sh
gst-launch-1.0 -e videotestsrc ! x264enc tune=zerolatency ! flvmux \
  ! rtmpxsink mode=listen uri=rtmp://0.0.0.0:1935/live/test
```

## Encrypted RTMP (RTMPS)

Use an `rtmps://` URI to negotiate TLS before the RTMP handshake:

- **Clients** (`rtmpxsrc mode=play`, `rtmpxsink mode=publish`) verify the server against the platform trust store plus optional `tls-ca-cert` (for private or self-signed CAs). Set `tls-cert` and `tls-key` if the server requires a client certificate (mTLS).
- **Listeners** (`mode=listen`) require `tls-cert` and `tls-key`. Setting `tls-ca-cert` on a listener requires connecting clients to present an mTLS certificate verified against that bundle plus the platform store.

```sh
# Listener with TLS
gst-launch-1.0 -e rtmpxsrc mode=listen uri=rtmps://0.0.0.0:1935/live \
  tls-cert=/etc/rtmpx/cert.pem tls-key=/etc/rtmpx/key.pem ! flvdemux ! fakesink

# Client trusting an extra CA bundle
gst-launch-1.0 -e rtmpxsrc uri=rtmps://example.com:443/live/test \
  tls-ca-cert=/etc/rtmpx/ca.pem ! flvdemux ! fakesink

# Listener requiring mTLS client certificates
gst-launch-1.0 -e rtmpxsrc mode=listen uri=rtmps://0.0.0.0:1935/live \
  tls-cert=/etc/rtmpx/cert.pem tls-key=/etc/rtmpx/key.pem \
  tls-ca-cert=/etc/rtmpx/client-ca.pem ! flvdemux ! fakesink
```

## Build

Requires Rust 1.97+ and GStreamer 1.28+ development headers.

If you installed GStreamer from the official macOS framework installer rather than Homebrew, expose its `pkg-config` files first:

```sh
export PKG_CONFIG_PATH="/Library/Frameworks/GStreamer.framework/Versions/Current/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
```

Clone and build:

```sh
git clone https://github.com/darfink/gst-rtmpx.git
cd gst-rtmpx
cargo build --release --locked
```

Prefer prebuilt binaries? Each [GitHub release](https://github.com/darfink/gst-rtmpx/releases) ships libraries for Linux (`.so`) and macOS (`.dylib`).

### Loading and installing the plugin

Try it straight from the build directory:

```sh
export GST_PLUGIN_PATH="$PWD/target/release"
gst-inspect-1.0 rtmpxsrc rtmpxsink
```

Install for your user (`libgstrtmpx.dylib` on macOS):

```sh
mkdir -p ~/.local/share/gstreamer-1.0/plugins
install -m 0755 target/release/libgstrtmpx.so ~/.local/share/gstreamer-1.0/plugins/
```

Or install system-wide:

```sh
sudo install -m 0755 target/release/libgstrtmpx.so "$(pkg-config --variable=pluginsdir gstreamer-1.0)/"
```

Or install with [`cargo-c`](https://github.com/lu-zero/cargo-c):

```sh
cargo install cargo-c
GSTREAMER_LIBDIR="$(pkg-config --variable=libdir gstreamer-1.0)"
cargo cinstall --release --library-type cdylib \
  --prefix="$(pkg-config --variable=prefix gstreamer-1.0)" \
  --libdir="$GSTREAMER_LIBDIR"
```

You can also run the Docker image, which listens on port 1935 and demuxes incoming streams:

```sh
docker build -t gst-rtmpx .
docker run --rm -p 1935:1935 gst-rtmpx
```

### Embedding in a Rust application

To register `rtmpxsrc` and `rtmpxsink` statically inside a Rust GStreamer application without installing a shared library, add `gst-rtmpx` to `Cargo.toml`:

```toml
[dependencies]
gst-rtmpx = "0.3"
```

Note that the Rust library crate name is `gstrtmpx`:

```rust
gst::init()?;
gstrtmpx::plugin_register_static()?;
```

## Element reference

### `rtmpxsrc`

Outputs a `video/x-flv` byte stream (pair with `flvdemux`). Defaults to `mode=play`, connecting to `rtmp(s)://host:port/app/key`; `mode=listen` binds `rtmp(s)://bind-host:port[/app[/key]]` and accepts a publisher.

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `mode` | string | `play` | `play` connects to a server; `listen` waits for a publisher |
| `uri` | string | (required) | Play: `rtmp(s)://host:port/app/key`. Listen: `rtmp(s)://bind-host:port[/app[/key]]` |
| `tc-url` | string | (none) | Optional `tcUrl` override on connect (defaults to `rtmp(s)://host:port/app`) |
| `tcp-nodelay` | boolean | `true` | Disable Nagle's algorithm on the TCP socket |
| `connect-timeout` | uint64 (ns) | `10000000000` (10 s) | Play: TCP connect timeout (`0` disables) |
| `accept-timeout` | uint64 (ns) | `0` (indefinite) | Listen: how long to wait for a publisher (`0` waits forever) |
| `handshake-timeout` | uint64 (ns) | `10000000000` (10 s) | Timeout per RTMP handshake read (`0` disables) |
| `read-timeout` | uint64 (ns) | `0` (disabled) | Timeout without incoming session data (`0` disables) |
| `write-timeout` | uint64 (ns) | `10000000000` (10 s) | Timeout per socket write (`0` disables) |
| `graceful-shutdown-timeout` | uint64 (ns) | `0` (immediate) | Listen: wait for the publisher to close during shutdown |
| `keep-listening` | boolean | `false` | Listen: wait for the next publisher after one ends instead of emitting EOS |
| `reconnect` | boolean | `false` | Play: reconnect and resume if the server disconnects |
| `tls-cert` | string | (none) | PEM certificate for `rtmps://` (required in listen mode; optional mTLS client cert in play mode) |
| `tls-key` | string | (none) | PEM private key matching `tls-cert` |
| `tls-ca-cert` | string | (none) | Extra PEM CA bundle (play: additional trusted servers; listen: enables mTLS client verification) |

**Events and behavior:**
- No GObject signals. Emits downstream custom events `rtmpx-publish-start` (`connection-id`) and `rtmpx-publish-end` (`connection-id`, `reason`) on the src pad, plus an element bus message `connection-removed` (`connection-id`, `reason`) when each publisher disconnects.
- Listen mode accepts one publisher at a time, starting a fresh stream generation (`stream-start` / `caps` / `segment`) for each publisher. Without `keep-listening=true`, the first publisher ending triggers EOS.
- In listen mode, the bind host must be an IP literal; port `0` allocates an ephemeral port and writes the bound address back to `uri`.
- Preserves received script data, including playback startup messages.

### `rtmpxsink`

Consumes a `video/x-flv` byte stream (pair with `flvmux`). Defaults to `mode=publish`, connecting to `rtmp(s)://host:port/app/key`; `mode=listen` binds `rtmp(s)://bind-host:port[/app[/key]]` and serves incoming players.

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `mode` | string | `publish` | `publish` connects to a server; `listen` serves incoming players |
| `uri` | string | (required) | Publish: `rtmp(s)://host:port/app/key`. Listen: `rtmp(s)://bind-host:port[/app[/key]]` |
| `tc-url` | string | (none) | Optional `tcUrl` override on connect (defaults to `rtmp(s)://host:port/app`) |
| `tcp-nodelay` | boolean | `true` | Disable Nagle's algorithm on the TCP socket |
| `connect-timeout` | uint64 (ns) | `10000000000` (10 s) | Publish: TCP connect timeout (`0` disables) |
| `accept-timeout` | uint64 (ns) | `0` (indefinite) | Listen: how long to wait for a player (`0` waits forever) |
| `wait-for-connection` | boolean | `true` | Listen: backpressure upstream until a player connects (`false` drops frames while unconnected) |
| `handshake-timeout` | uint64 (ns) | `10000000000` (10 s) | Timeout per RTMP handshake read (`0` disables) |
| `read-timeout` | uint64 (ns) | `0` (disabled) | Timeout without incoming session data (`0` disables) |
| `write-timeout` | uint64 (ns) | `10000000000` (10 s) | Timeout per socket write (`0` disables) |
| `tls-cert` | string | (none) | PEM certificate for `rtmps://` (required in listen mode; optional mTLS client cert in publish mode) |
| `tls-key` | string | (none) | PEM private key matching `tls-cert` |
| `tls-ca-cert` | string | (none) | Extra PEM CA bundle (publish: additional trusted servers; listen: enables mTLS client verification) |

**Events and behavior:**
- No GObject signals, custom downstream events, or bus messages.
- Forwards audio, video, and script-data tags in both modes.
- Listen mode serves one player at a time, replaying cached sequence headers to late joiners and continuing to accept new players across disconnects until EOS or shutdown. A failed TLS handshake closes only that client, leaving the listener running.
- Uses the same listen bind rules as `rtmpxsrc` (IP literal, port `0` updates `uri`, omitted app/key accepts any).

## Architecture and layout

Built on `rtmpx` 3 from crates.io, each connection drains protocol outputs one at a time, uses vectored writes with partial-write progress, and feeds segmented receive buffers straight into FLV framing without an intermediate contiguous RTMP copy (GStreamer's FLV buffers and TLS still perform their own buffering).

- `src/rtmpxsrc/`: Dual-mode source (`play` client and `listen` server workers)
- `src/rtmpxsink/`: Dual-mode sink (`publish` client and `listen` server workers)
- `src/common.rs`: FLV framing, URI parsing, Enhanced RTMP capability maps, worker channels, and TLS helpers
- `tests/`: Loopback (`listen_loopback`, `sink_listen`), TLS (`rtmps`), FFmpeg H.264/AAC and Enhanced HEVC/Opus interop (`ffmpeg_interop`), and the smoke script (`integration.sh`)

## Testing

```sh
cargo test --locked
./tests/integration.sh
```

Loopback tests use an independent `rtmpx` 2 peer to verify wire compatibility; `ffmpeg_interop` requires `ffmpeg` and `ffprobe` on `PATH`.

## License

Dual-licensed under MIT or Apache-2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

[ci-shield]: https://img.shields.io/github/actions/workflow/status/darfink/gst-rtmpx/ci.yml?branch=main&label=CI&logo=github&style=for-the-badge
[ci]: https://github.com/darfink/gst-rtmpx/actions/workflows/ci.yml?query=branch%3Amain
[crate-shield]: https://img.shields.io/crates/v/gst-rtmpx.svg?style=for-the-badge
[crate]: https://crates.io/crates/gst-rtmpx
[version-shield]: https://img.shields.io/github/v/tag/darfink/gst-rtmpx?style=for-the-badge&label=version
[releases]: https://github.com/darfink/gst-rtmpx/releases
[gstreamer-shield]: https://img.shields.io/badge/GStreamer-1.28+-orange?style=for-the-badge
[gstreamer]: https://gstreamer.freedesktop.org/
[license-shield]: https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green?style=for-the-badge
[license]: LICENSE-MIT
