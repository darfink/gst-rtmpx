## gst-rtmpx — RTMP source / sink on rtmpx (URI-only)

Both elements double as a listener: `rtmpxsrc` accepts incoming publisher
connections, and `rtmpxsink` waits for and serves incoming player
connections — no separate server element needed.

`rtmpxsrc` and `rtmpxsink` take a single `uri`; `tc-url` remains as an
optional connect override.

### rtmpxsrc modes

- `mode=listen` (default, `uri` defaults to `rtmp://0.0.0.0:1935/`): bind
  the uri host:port, accept one publisher at a time, emit FLV
  (`video/x-flv`, `streamheader` + `tagged-stream`). A missing app/key
  accepts any (`rtmp://[::]:1234/` listens everywhere on 1234);
  `rtmp://0.0.0.0:1935/live` requires app `live`; port `0` allocates a
  free port and updates `uri`. Sans-I/O `ServerSession` drive with lifecycle
  bus messages `rtmpx-publish-start` / `rtmpx-publish-end` and
  `keep-listening`.
- `mode=play`: connect and play a full uri (`rtmp://host:port/app/key`).
  `tc-url` overrides the connect tcUrl (defaults to
  `rtmp://host:port/app`). `reconnect=true` retries after
  disconnect-class failures.

Common timeouts are nanoseconds (`0` = disabled): `connect-timeout`,
`accept-timeout` (listen), `handshake-timeout`, `read-timeout`,
`write-timeout`. `tcp-nodelay` defaults on.

### rtmpxsink modes

- `mode=publish` (default): connect to `uri` (`rtmp://host:port/app/key`)
  plus optional `tc-url`, demux incoming FLV and publish it.
- `mode=listen`: bind the uri host:port instead and serve one player at a
  time, SRT-sink style. `render()` applies backpressure until a player
  connects; with `wait-for-connection=false` buffers are dropped while no
  player is connected. Sequence headers are replayed to mid-stream joiners
  and the listener keeps accepting across player disconnects.

### Layout

- `src/rtmpxsrc/`: dual-mode source (listen worker + play worker).
- `src/rtmpxsink/`: dual-mode sink (publish client + listen server).
- `src/common.rs`: shared FLV framing, `parse_rtmp_uri` (+ unit tests),
  worker-channel helpers, capabilities map.

### Tests

- `tests/listen_loopback.rs`: source listen round-trip over real TCP.
- `tests/sink_listen.rs`: sink listen mode against a raw protocol player.
- `tests/ffmpeg_interop.rs`: publish baseline H264/AAC and enhanced
  HEVC/Opus with ffmpeg (needs `ffmpeg`/`ffprobe` on `PATH`).
- `tests/integration.sh`: build, unit tests, and `gst-inspect-1.0` smoke.

### Build / inspect

From the repository root:

```sh
cargo build -p gst-rtmpx
export GST_PLUGIN_PATH=$PWD/target/debug
gst-inspect-1.0 rtmpxsrc rtmpxsink
```
