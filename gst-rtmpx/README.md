## gst-rtmpx — RTMP source / sink on rtmpx (URI-only)

Both elements default to client behaviour (like `rtmp2src` / `rtmp2sink`) and
can opt into listening with `mode=listen`: `rtmpxsrc` accepts incoming publisher
connections, and `rtmpxsink` waits for and serves incoming player connections
— no separate server element needed.

Both take a single `uri`; `tc-url` remains as an optional connect override.
A missing app or stream key in listen mode means "accept any".

Both speak plain `rtmp://` and encrypted `rtmps://` (RTMP over TLS). The scheme
decides the transport: `rtmps://` clients verify the server against the
platform trust store plus `tls-ca-cert`, and `rtmps://` listeners present the
`tls-cert` / `tls-key` pair (setting `tls-ca-cert` there requires an mTLS client certificate).

### rtmpxsrc

Play is the default. `mode=play` connects and plays `rtmp://host:port/app/key`
from a server; `mode=listen` binds `rtmp://bind-host:port[/app[/key]]` and waits
for a publisher.

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `mode` | string | `play` | `play` connects to a server, `listen` waits for a publisher. |
| `uri` | string | (none, required) | Play: `rtmp(s)://host:port/app/key`. Listen: `rtmp(s)://bind-host:port[/app[/key]]` (`rtmps` listen needs `tls-cert`/`tls-key`). |
| `tc-url` | string | (none) | Override tcUrl in the connect; defaults to `rtmp(s)://host:port/app`. |
| `tcp-nodelay` | boolean | `true` | Disable Nagle algorithm on the connection. |
| `connect-timeout` | uint64 (ns) | `10000000000` (10 s) | Play: time allowed for TCP connect; `0` disables. |
| `accept-timeout` | uint64 (ns) | `0` (wait indefinitely) | Listen: time to wait for a publisher. |
| `handshake-timeout` | uint64 (ns) | `10000000000` (10 s) | Time allowed per handshake read; `0` disables. |
| `read-timeout` | uint64 (ns) | `0` (disabled) | Time allowed without session input; `0` disables. |
| `write-timeout` | uint64 (ns) | `10000000000` (10 s) | Time allowed per socket write; `0` disables. |
| `graceful-shutdown-timeout` | uint64 (ns) | `0` (close immediately) | Listen: wait for the publisher to close during shutdown. |
| `keep-listening` | boolean | `false` | Listen: wait for the next publisher after one ends instead of EOS. |
| `reconnect` | boolean | `false` | Play: reconnect and resume after the server disconnects. |
| `tls-cert` | string | (none) | Certificate presented (`rtmps://`): server certificate in listen mode (required); mTLS client certificate in play mode (optional, needs `tls-key`). |
| `tls-key` | string | (none) | PEM private key matching `tls-cert`. |
| `tls-ca-cert` | string | (none) | Extra PEM CA bundle alongside the platform store. Play (`rtmps://`): which servers to trust. Listen (`rtmps://`): when set, require an mTLS client certificate. |

Signals and events: no GObject signals. Emits downstream custom events
`rtmpx-publish-start` (`connection-id`) and `rtmpx-publish-end` (`connection-id`,
`reason`) on the src pad, plus an element bus message `connection-removed`
(`connection-id`, `reason`) per publisher.

Quirks: outputs a `video/x-flv` byte stream (FLV header + tags), pair with
`flvdemux`. Listen serves one publisher at a time and each publisher starts a new
stream generation (stream-start / caps / segment). Without `keep-listening` the
first publisher ending ends the stream (EOS). The bind host must be an IP literal;
port `0` allocates a free port and publishes it back on `uri`. Enhanced RTMP is
advertised, so modern encoders keep negotiating HEVC/AV1/multitrack.

### rtmpxsink

Publish is the default. `mode=publish` connects to `rtmp://host:port/app/key` and
publishes; `mode=listen` binds `rtmp://bind-host:port[/app[/key]]` and serves players.

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `mode` | string | `publish` | `publish` connects to a server, `listen` serves players. |
| `uri` | string | (none, required) | Publish: `rtmp(s)://host:port/app/key`. Listen: `rtmp(s)://bind-host:port[/app[/key]]` (`rtmps` listen needs `tls-cert`/`tls-key`). |
| `tc-url` | string | (none) | Override tcUrl in the connect; defaults to `rtmp(s)://host:port/app`. |
| `tcp-nodelay` | boolean | `true` | Disable Nagle algorithm on the connection. |
| `connect-timeout` | uint64 (ns) | `10000000000` (10 s) | Time allowed for TCP connect; `0` disables. |
| `accept-timeout` | uint64 (ns) | `0` (wait indefinitely) | Listen: time to wait for a player. |
| `wait-for-connection` | boolean | `true` | Listen: block (backpressure) until a player connects instead of dropping. |
| `handshake-timeout` | uint64 (ns) | `10000000000` (10 s) | Time allowed per handshake read; `0` disables. |
| `read-timeout` | uint64 (ns) | `0` (disabled) | Time allowed without session input; `0` disables. |
| `write-timeout` | uint64 (ns) | `10000000000` (10 s) | Time allowed per socket write; `0` disables. |
| `tls-cert` | string | (none) | Certificate presented (`rtmps://`): server certificate in listen mode (required); mTLS client certificate in publish mode (optional, needs `tls-key`). |
| `tls-key` | string | (none) | PEM private key matching `tls-cert`. |
| `tls-ca-cert` | string | (none) | Extra PEM CA bundle alongside the platform store. Publish (`rtmps://`): which servers to trust. Listen (`rtmps://`): when set, require an mTLS client certificate. |

Signals and events: none. No GObject signals, downstream events, or bus messages.

Quirks: expects a `video/x-flv` byte stream, pair with `flvmux`; publish mode demuxes
it and publishes. Listen serves one player at a time SRT-sink style: `render()` blocks
until a player connects, unless `wait-for-connection=false` (drop while unconnected).
Sequence headers replay to late joiners and the listener keeps accepting across player
disconnects until EOS/shutdown. Same bind rules as the source: IP literal, port `0`
allocates and updates `uri`, missing app/key accepts any. A failed TLS handshake
ends that player, not the listener.

### Protocol integration

The plugin uses RTMPX 3 from crates.io. Each connection drains protocol outputs
one at a time and uses stream handles for media and stream teardown.
Outbound packets use vectored writes and retain their cursor across partial writes.
Inbound reads use owned buffers; received segments go directly into FLV framing
without an intermediate contiguous RTMP payload.

The GStreamer FLV boundary still allocates and copies data. TLS also has its own
buffering. These changes reduce protocol overhead; the whole plugin is not allocation-free.
Both sink modes forward audio, video, and script-data tags.
The source preserves received script data, including playback startup messages.

### Layout

- `src/rtmpxsrc/`: dual-mode source (listen worker + play worker).
- `src/rtmpxsink/`: dual-mode sink (publish client + listen server).
- `src/common.rs`: shared FLV framing, `parse_rtmp_uri` (+ unit tests),
  worker-channel helpers, capabilities map, TLS stream wrappers.

### Tests

The raw loopback peers use released RTMPX 2 to check wire compatibility independently
of the production RTMPX 3 dependency.

- `tests/listen_loopback.rs`: source listen round-trip over real TCP.
- `tests/sink_listen.rs`: sink listen mode against a raw protocol player.
- `tests/rtmps.rs`: source-listen and sink-listen round-trips over TLS with a throwaway self-signed certificate.
- `tests/ffmpeg_interop.rs`: publish baseline H264/AAC and enhanced
  HEVC/Opus with ffmpeg (needs `ffmpeg`/`ffprobe` on `PATH`).
- `tests/integration.sh`: build, unit tests, and `gst-inspect-1.0` smoke.

### Build / inspect

From the repository root:

```sh
cargo build --release
export GST_PLUGIN_PATH="$PWD/target/release"
gst-inspect-1.0 rtmpxsrc rtmpxsink
```
