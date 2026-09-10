// Shared helpers for the rtmpx plugin (rtmpxsrc / rtmpxsink).
// The listen worker in rtmpxsrc is a direct descendant of
// scufflertmplistensrc: same sans-I/O rtmpx ServerSession drive, same FLV
// framing, same publisher lifecycle events (renamed to rtmpx-publish-start
// and rtmpx-publish-end). This module holds the pieces both elements need so
// the play-client path and the future publish path in rtmpxsink share one
// implementation.

use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::rml_amf0::{Amf0Object, Amf0Value};
use rtmpx::sessions::{
  ClientSession, ClientSessionConfig, ClientSessionResult, ServerSessionResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

pub const FLV_TAG_AUDIO: u8 = 8;
pub const FLV_TAG_VIDEO: u8 = 9;
pub const FLV_TAG_SCRIPT_DATA: u8 = 18;

// FLV file header: signature FLV, version 1, flags 0x05 (audio+video),
// header length 9, previous-tag-size 0. Written as numbers to keep the
// source free of string escapes.
pub const FLV_HEADER: &[u8] = &[
  0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00,
];

pub const OUTPUT_QUEUE_CAPACITY: usize = 1;
pub const CREATE_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const WORKER_START_TIMEOUT: Duration = Duration::from_secs(2);

pub static CAT_SRC: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
  gst::DebugCategory::new(
    "rtmpxsrc",
    gst::DebugColorFlags::empty(),
    Some("RTMPX source (listen or play)"),
  )
});

pub static CAT_SINK: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
  gst::DebugCategory::new(
    "rtmpxsink",
    gst::DebugColorFlags::empty(),
    Some("RTMPX sink (publish)"),
  )
});

pub enum WorkerOutput {
  Data(Vec<u8>),
  PublishStarted { connection_id: u64 },
  PublishEnded { connection_id: u64, reason: String },
  Warning(String),
  Error(String),
  Eos,
  Wake,
}

pub fn nanoseconds_timeout(nanoseconds: u64) -> Option<Duration> {
  (nanoseconds != 0).then(|| Duration::from_nanos(nanoseconds))
}

// True when the socket ended because the peer vanished rather than because
// this element misbehaved. A killed peer surfaces as reset/abort/EOF on read
// or EPIPE on the next write; read/idle timeouts also count as vanished.
pub fn is_client_io_error(error: &io::Error) -> bool {
  matches!(
    error.kind(),
    io::ErrorKind::ConnectionAborted
      | io::ErrorKind::ConnectionReset
      | io::ErrorKind::UnexpectedEof
      | io::ErrorKind::BrokenPipe
  )
}

pub fn push_u24_be(output: &mut Vec<u8>, value: u32) {
  output.extend_from_slice(&[
    ((value >> 16) & 0xff) as u8,
    ((value >> 8) & 0xff) as u8,
    (value & 0xff) as u8,
  ]);
}

pub fn frame_flv_tag(tag_type: u8, timestamp: u32, payload: &[u8]) -> io::Result<Vec<u8>> {
  if payload.len() > 0x00ff_ffff {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "RTMP media message exceeds the FLV 24-bit tag-size limit",
    ));
  }
  let payload_len = payload.len() as u32;

  let mut tag = Vec::with_capacity(11 + payload.len() + 4);
  tag.push(tag_type);
  push_u24_be(&mut tag, payload_len);
  push_u24_be(&mut tag, timestamp & 0x00ff_ffff);
  tag.push((timestamp >> 24) as u8);
  push_u24_be(&mut tag, 0);
  tag.extend_from_slice(payload);
  tag.extend_from_slice(&(11 + payload_len).to_be_bytes());
  Ok(tag)
}

pub fn publish_event(name: &str, connection_id: u64, reason: Option<&str>) -> gst::Event {
  let structure = gst::Structure::builder(name).field("connection-id", connection_id);
  let structure = if let Some(reason) = reason {
    structure.field("reason", reason).build()
  } else {
    structure.build()
  };
  gst::event::CustomDownstream::builder(structure).build()
}

// Enhanced RTMP capabilities advertised when accepting a connect
// (server side) and when issuing one (client play path), mirroring the
// ingest proxy so modern servers keep negotiating HEVC/AV1/multitrack.
pub fn enhanced_rtmp_capabilities() -> Amf0Object {
  Amf0Object::from([
    (
      "fourCcList".to_owned(),
      Amf0Value::StrictArray(vec![Amf0Value::Utf8String("*".to_owned())]),
    ),
    (
      "videoFourCcInfoMap".to_owned(),
      Amf0Value::Object(Amf0Object::from([("*".to_owned(), Amf0Value::Number(4.0))])),
    ),
    (
      "audioFourCcInfoMap".to_owned(),
      Amf0Value::Object(Amf0Object::from([("*".to_owned(), Amf0Value::Number(4.0))])),
    ),
    ("capsEx".to_owned(), Amf0Value::Number(14.0)),
  ])
}

pub async fn send_output(
  sender: &flume::Sender<WorkerOutput>,
  cancellation: &CancellationToken,
  output: WorkerOutput,
) -> bool {
  tokio::select! {
    _ = cancellation.cancelled() => false,
    result = sender.send_async(output) => result.is_ok(),
  }
}

pub async fn send_publish_end(
  output: &flume::Sender<WorkerOutput>,
  cancellation: &CancellationToken,
  connection_id: u64,
  reason: &str,
) -> bool {
  send_output(
    output,
    cancellation,
    WorkerOutput::PublishEnded {
      connection_id,
      reason: reason.into(),
    },
  )
  .await
}

/// Failure from serving one RTMP connection: a message for the pipeline/log
/// plus whether the peer vanished (so keep-listening / reconnect keeps going
/// instead of failing). A killed peer surfaces as reset/abort/EOF on read or
/// EPIPE on the next write; read/idle timeouts also count as vanished.
pub struct SessionFailure {
  pub message: String,
  pub client_disconnect: bool,
}

impl SessionFailure {
  pub fn disconnect(message: impl Into<String>) -> Self {
    Self {
      message: message.into(),
      client_disconnect: true,
    }
  }

  pub fn error(message: impl Into<String>) -> Self {
    Self {
      message: message.into(),
      client_disconnect: false,
    }
  }
}

/// Connection-id plumbing shared with each publish/play session.
///
/// `next` hands out a fresh id on every publish so ids stay unique across
/// keep-listening reconnects; `current` records the id of the live publish so
/// the worker can report the end event after the session closes.
#[derive(Clone)]
pub struct PublishIds {
  pub next: Arc<AtomicU64>,
  pub current: Arc<AtomicU64>,
}

/// Run the client-side RTMP handshake, returning trailing non-handshake bytes.
pub async fn client_handshake(
  stream: &mut tokio::net::TcpStream,
  cancellation: &CancellationToken,
  handshake_timeout: &Option<Duration>,
) -> Result<Vec<u8>, SessionFailure> {
  let mut handshake = Handshake::new(PeerType::Client);
  let outbound = handshake
    .generate_outbound_p0_and_p1()
    .map_err(|error| SessionFailure::error(format!("failed to start RTMP handshake: {error:?}")))?;
  stream.write_all(&outbound).await.map_err(|error| {
    if is_client_io_error(&error) {
      SessionFailure::disconnect(format!(
        "RTMP server closed the connection during handshake: {error}"
      ))
    } else {
      SessionFailure::error(format!("failed to write RTMP handshake: {error}"))
    }
  })?;
  stream
    .flush()
    .await
    .map_err(|error| SessionFailure::error(format!("failed to write RTMP handshake: {error}")))?;
  handshake_loop(
    stream,
    cancellation,
    handshake_timeout,
    handshake,
    "RTMP server",
  )
  .await
}

/// Run the server-side RTMP handshake with a per-read timeout, returning any
/// trailing non-handshake bytes for the session.
pub async fn server_handshake(
  stream: &mut tokio::net::TcpStream,
  cancellation: &CancellationToken,
  handshake_timeout: &Option<Duration>,
) -> Result<Vec<u8>, SessionFailure> {
  let handshake = Handshake::new(PeerType::Server);
  handshake_loop(
    stream,
    cancellation,
    handshake_timeout,
    handshake,
    "publisher",
  )
  .await
}

async fn handshake_loop(
  stream: &mut tokio::net::TcpStream,
  cancellation: &CancellationToken,
  handshake_timeout: &Option<Duration>,
  mut handshake: Handshake,
  peer_label: &str,
) -> Result<Vec<u8>, SessionFailure> {
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    let read_now = async {
      if let Some(limit) = handshake_timeout {
        tokio::time::timeout(*limit, stream.read(&mut read_buf))
          .await
          .map_err(|_| {
            SessionFailure::disconnect(format!(
              "timed out after {limit:?} waiting for RTMP handshake input"
            ))
          })
      } else {
        Ok(stream.read(&mut read_buf).await)
      }
    };
    let n = tokio::select! {
      _ = cancellation.cancelled() => return Err(SessionFailure::error("listener is shutting down")),
      outcome = read_now => match outcome {
        Ok(Ok(n)) => n,
        Ok(Err(error)) => {
          if is_client_io_error(&error) {
            return Err(SessionFailure::disconnect(format!("{peer_label} closed the connection during handshake: {error}")));
          }
          return Err(SessionFailure::error(format!("failed to read RTMP handshake: {error}")));
        }
        Err(failure) => return Err(failure),
      },
    };
    if n == 0 {
      return Err(SessionFailure::disconnect(format!(
        "{peer_label} closed the connection during handshake"
      )));
    }
    let handshake_error = if peer_label == "publisher" {
      "publisher handshake failed".to_string()
    } else {
      "RTMP handshake failed".to_string()
    };
    match handshake
      .process_bytes(&read_buf[..n])
      .map_err(|error| SessionFailure::error(format!("{handshake_error}: {error:?}")))?
    {
      HandshakeProcessResult::InProgress { response_bytes } => {
        if !response_bytes.is_empty() {
          write_handshake_response(stream, &response_bytes, peer_label).await?;
        }
      }
      HandshakeProcessResult::Completed {
        response_bytes,
        remaining_bytes,
      } => {
        if !response_bytes.is_empty() {
          write_handshake_response(stream, &response_bytes, peer_label).await?;
        }
        return Ok(remaining_bytes);
      }
    }
  }
}

async fn write_handshake_response(
  stream: &mut tokio::net::TcpStream,
  response_bytes: &[u8],
  peer_label: &str,
) -> Result<(), SessionFailure> {
  stream.write_all(response_bytes).await.map_err(|error| {
    if is_client_io_error(&error) {
      SessionFailure::disconnect(format!(
        "{peer_label} closed the connection during handshake: {error}"
      ))
    } else {
      SessionFailure::error(format!("failed to write RTMP handshake: {error}"))
    }
  })?;
  stream
    .flush()
    .await
    .map_err(|error| SessionFailure::error(format!("failed to write RTMP handshake: {error}")))?;
  Ok(())
}

/// Write outbound client session packets with a per-write timeout.
pub async fn write_client_results(
  stream: &mut tokio::net::TcpStream,
  results: Vec<ClientSessionResult>,
  _output: &flume::Sender<WorkerOutput>,
  _cancellation: &CancellationToken,
  write_timeout: &Option<Duration>,
) -> Result<(), SessionFailure> {
  let mut wrote = false;
  for result in results {
    if let ClientSessionResult::OutboundResponse(packet) = result {
      if let Some(limit) = write_timeout {
        tokio::time::timeout(*limit, stream.write_all(&packet.bytes))
          .await
          .map_err(|_| {
            SessionFailure::disconnect(format!("timed out after {limit:?} writing RTMP request"))
          })?
          .map_err(|error| {
            if is_client_io_error(&error) {
              SessionFailure::disconnect(format!("RTMP connection lost while writing: {error}"))
            } else {
              SessionFailure::error(format!("failed to write RTMP request: {error}"))
            }
          })?;
      } else {
        stream.write_all(&packet.bytes).await.map_err(|error| {
          if is_client_io_error(&error) {
            SessionFailure::disconnect(format!("RTMP connection lost while writing: {error}"))
          } else {
            SessionFailure::error(format!("failed to write RTMP request: {error}"))
          }
        })?;
      }
      wrote = true;
    }
  }
  if wrote {
    stream.flush().await.map_err(|error| {
      if is_client_io_error(&error) {
        SessionFailure::disconnect(format!("RTMP connection lost while writing: {error}"))
      } else {
        SessionFailure::error(format!("failed to write RTMP request: {error}"))
      }
    })?;
  }
  Ok(())
}

/// Write outbound server session packets with a per-write timeout.
pub async fn write_session_results(
  stream: &mut tokio::net::TcpStream,
  results: Vec<ServerSessionResult>,
  _output: &flume::Sender<WorkerOutput>,
  _cancellation: &CancellationToken,
  write_timeout: &Option<Duration>,
) -> Result<(), SessionFailure> {
  let mut wrote = false;
  for result in results {
    if let ServerSessionResult::OutboundResponse(packet) = result {
      if let Some(limit) = write_timeout {
        tokio::time::timeout(*limit, stream.write_all(&packet.bytes))
          .await
          .map_err(|_| {
            SessionFailure::disconnect(format!("timed out after {limit:?} writing RTMP response"))
          })?
          .map_err(|error| {
            if is_client_io_error(&error) {
              SessionFailure::disconnect(format!("RTMP connection lost while writing: {error}"))
            } else {
              SessionFailure::error(format!("failed to write RTMP response: {error}"))
            }
          })?;
      } else {
        stream.write_all(&packet.bytes).await.map_err(|error| {
          if is_client_io_error(&error) {
            SessionFailure::disconnect(format!("RTMP connection lost while writing: {error}"))
          } else {
            SessionFailure::error(format!("failed to write RTMP response: {error}"))
          }
        })?;
      }
      wrote = true;
    }
  }
  if wrote {
    stream.flush().await.map_err(|error| {
      if is_client_io_error(&error) {
        SessionFailure::disconnect(format!("RTMP connection lost while writing: {error}"))
      } else {
        SessionFailure::error(format!("failed to write RTMP response: {error}"))
      }
    })?;
  }
  Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtmpEndpoint {
  pub host: String,
  pub port: u16,
  pub app: Option<String>,
  pub stream_key: Option<String>,
}

// Parse rtmp://host[:port][/app[/stream-key...]]. The scheme must be rtmp.
// The host may be a DNS name, an IPv4 literal, or a bracketed IPv6 literal
// such as [::]. The port defaults to 1935 (0 is allowed so a listener can
// ask the OS for a free port). The app is the first path segment; the
// stream key is everything after it and may itself contain slashes.
// Missing app/key means "no constraint" for a listener; play/publish
// callers must reject that via require_app_key. Query strings are ignored.
pub fn parse_rtmp_uri(uri: &str) -> Result<RtmpEndpoint, String> {
  let rest = uri
    .strip_prefix("rtmp://")
    .ok_or_else(|| "RTMP URI must start with rtmp://".to_string())?;
  let (authority, path) = match rest.find('/') {
    Some(index) => (&rest[..index], &rest[index + 1..]),
    None => (rest, ""),
  };
  if authority.is_empty() {
    return Err("RTMP URI must contain a host".to_string());
  }
  let (host, port) = parse_authority(authority)?;
  let path = match path.find('?') {
    Some(index) => &path[..index],
    None => path,
  };
  let path = path.strip_suffix('/').unwrap_or(path);
  let (app, stream_key) = match path.find('/') {
    Some(index) => {
      let (app, key) = (&path[..index], &path[index + 1..]);
      (
        (!app.is_empty()).then(|| app.to_owned()),
        (!key.is_empty()).then(|| key.to_owned()),
      )
    }
    None => ((!path.is_empty()).then(|| path.to_owned()), None),
  };
  Ok(RtmpEndpoint {
    host,
    port,
    app,
    stream_key,
  })
}

fn parse_authority(authority: &str) -> Result<(String, u16), String> {
  if let Some(rest) = authority.strip_prefix('[') {
    let (host, rest) = rest
      .split_once(']')
      .ok_or_else(|| "RTMP URI has an unterminated IPv6 literal".to_string())?;
    if host.is_empty() {
      return Err("RTMP URI must contain a host".to_string());
    }
    if rest.is_empty() {
      return Ok((host.to_owned(), 1935));
    }
    let port = rest
      .strip_prefix(':')
      .filter(|port| !port.is_empty())
      .ok_or_else(|| "RTMP URI has an invalid port".to_string())?;
    let port = port
      .parse::<u16>()
      .map_err(|_| "RTMP URI has an invalid port".to_string())?;
    return Ok((host.to_owned(), port));
  }
  if authority.chars().filter(|c| *c == ':').count() > 1 {
    return Err("RTMP URI IPv6 literals must use brackets, e.g. rtmp://[::]:1935/".to_string());
  }
  match authority.split_once(':') {
    Some((host, port)) => {
      if host.is_empty() {
        return Err("RTMP URI must contain a host".to_string());
      }
      let port = port
        .parse::<u16>()
        .map_err(|_| "RTMP URI has an invalid port".to_string())?;
      Ok((host.to_owned(), port))
    }
    None => Ok((authority.to_owned(), 1935)),
  }
}

/// Default tcUrl for a connect: scheme + host + port + app, never the key.
pub fn default_tc_url(host: &str, port: u16, app: &str) -> String {
  if host.contains(':') {
    format!("rtmp://[{host}]:{port}/{app}")
  } else {
    format!("rtmp://{host}:{port}/{app}")
  }
}

/// Require app + stream key for play/publish callers. Listen callers accept
/// None as "any".
pub fn require_app_key(endpoint: &RtmpEndpoint, what: &str) -> Result<(String, String), String> {
  match (endpoint.app.clone(), endpoint.stream_key.clone()) {
    (Some(app), Some(key)) => Ok((app, key)),
    _ => Err(format!(
      "{what} needs a full RTMP URI with application and stream key"
    )),
  }
}

/// URI-only endpoint for the RTMP client paths (rtmpxsrc play, rtmpxsink
/// publish). Both need exactly host + port + app + key + tcUrl, so they
/// share one struct and one resolver instead of two copies.
#[derive(Clone, Debug)]
pub struct ClientEndpoint {
  pub host: String,
  pub port: u16,
  pub app: String,
  pub stream_key: String,
  pub tc_url: String,
}

pub fn require_uri(uri: &Option<String>, what: &str) -> Result<String, String> {
  uri
    .clone()
    .filter(|uri| !uri.is_empty())
    .ok_or_else(|| format!("{what} needs uri, e.g. rtmp://host:1935/app/key"))
}

pub fn bracketed_host(host: &str) -> String {
  if host.contains(':') {
    format!("[{host}]")
  } else {
    host.to_owned()
  }
}

pub fn resolve_client_endpoint(
  uri: Option<String>,
  tc_url: Option<String>,
  what: &str,
) -> Result<ClientEndpoint, String> {
  let uri = require_uri(&uri, what)?;
  let parsed = parse_rtmp_uri(&uri)?;
  let (app, stream_key) = require_app_key(&parsed, what)?;
  let tc_url = tc_url
    .filter(|url| !url.is_empty())
    .unwrap_or_else(|| default_tc_url(&parsed.host, parsed.port, &app));
  Ok(ClientEndpoint {
    host: parsed.host,
    port: parsed.port,
    app,
    stream_key,
    tc_url,
  })
}

/// TCP connect with an optional nanosecond timeout plus TCP_NODELAY setup.
/// Shared by the play path (rtmpxsrc) and the publish path (rtmpxsink).
pub async fn tcp_connect(
  host: &str,
  port: u16,
  connect_timeout_ns: u64,
  tcp_nodelay: bool,
) -> Result<tokio::net::TcpStream, SessionFailure> {
  let stream = if connect_timeout_ns == 0 {
    tokio::net::TcpStream::connect((host.to_owned(), port))
      .await
      .map_err(|error| {
        SessionFailure::disconnect(format!("failed to connect to RTMP server: {error}"))
      })?
  } else {
    let limit = Duration::from_nanos(connect_timeout_ns);
    tokio::time::timeout(
      limit,
      tokio::net::TcpStream::connect((host.to_owned(), port)),
    )
    .await
    .map_err(|_| {
      SessionFailure::disconnect(format!(
        "timed out after {limit:?} connecting to RTMP server"
      ))
    })?
    .map_err(|error| {
      SessionFailure::disconnect(format!("failed to connect to RTMP server: {error}"))
    })?
  };
  if let Err(error) = stream.set_nodelay(tcp_nodelay) {
    return Err(SessionFailure::error(format!(
      "Failed to configure TCP_NODELAY: {error}"
    )));
  }
  Ok(stream)
}

/// Fresh client session with the window/chunk sizes both elements use, plus
/// the tcUrl for this connection.
pub fn new_client_session(tc_url: String) -> Result<ClientSession, SessionFailure> {
  let mut config = ClientSessionConfig::new();
  config.window_ack_size = 2_500_000;
  config.chunk_size = 4096;
  config.tc_url = Some(tc_url);
  let (session, initial) = ClientSession::new(config)
    .map_err(|error| SessionFailure::error(format!("failed to create RTMP session: {error:?}")))?;
  debug_assert!(initial.is_empty(), "client must not write before connect");
  Ok(session)
}

/// Cancellable socket read with an optional idle timeout. Used by every
/// client and server session read loop so EOF/timeout/peer-gone mapping
/// stays identical everywhere.
pub async fn read_session_chunk(
  stream: &mut tokio::net::TcpStream,
  buf: &mut [u8],
  cancellation: &CancellationToken,
  read_timeout: &Option<Duration>,
) -> Result<usize, SessionFailure> {
  let read_now = async {
    if let Some(limit) = read_timeout {
      tokio::time::timeout(*limit, stream.read(buf))
        .await
        .map_err(|_| {
          SessionFailure::disconnect(format!("timed out after {limit:?} waiting for RTMP input"))
        })
    } else {
      Ok(stream.read(buf).await)
    }
  };
  tokio::select! {
    _ = cancellation.cancelled() => Err(SessionFailure::error("worker is shutting down")),
    outcome = read_now => match outcome {
      Ok(Ok(n)) => Ok(n),
      Ok(Err(error)) => {
        if is_client_io_error(&error) {
          Err(SessionFailure::disconnect(format!("RTMP connection lost: {error}")))
        } else {
          Err(SessionFailure::error(format!("failed to read RTMP input: {error}")))
        }
      }
      Err(failure) => Err(failure),
    },
  }
}

/// FLV framing sink shared by the listen and play paths in rtmpxsrc.
///
/// Both paths emit the 13-byte FLV header once, then frame every media
/// message with the 11-byte tag header plus trailing previous-tag-size.
/// The writer owns that "have we sent the header yet" bit so the two
/// session structs stop duplicating it.
pub struct FlvTagWriter {
  output: flume::Sender<WorkerOutput>,
  cancellation: CancellationToken,
  header_sent: bool,
}

impl FlvTagWriter {
  pub fn new(output: flume::Sender<WorkerOutput>, cancellation: CancellationToken) -> Self {
    Self {
      output,
      cancellation,
      header_sent: false,
    }
  }

  pub async fn ensure_header(&mut self) -> bool {
    if self.header_sent {
      return true;
    }
    self.header_sent = true;
    send_output(
      &self.output,
      &self.cancellation,
      WorkerOutput::Data(FLV_HEADER.to_vec()),
    )
    .await
  }

  pub async fn push_media(
    &mut self,
    tag_type: u8,
    timestamp: u32,
    payload: &[u8],
  ) -> Result<(), SessionFailure> {
    if !self.ensure_header().await {
      return Err(SessionFailure::error("listener is shutting down"));
    }
    match frame_flv_tag(tag_type, timestamp, payload) {
      Ok(tag) => {
        if !send_output(&self.output, &self.cancellation, WorkerOutput::Data(tag)).await {
          return Err(SessionFailure::error("listener is shutting down"));
        }
        Ok(())
      }
      Err(error) => {
        send_output(
          &self.output,
          &self.cancellation,
          WorkerOutput::Error(error.to_string()),
        )
        .await;
        self.cancellation.cancel();
        Err(SessionFailure::error(error.to_string()))
      }
    }
  }
}

/// One parsed FLV tag: type (8 audio, 9 video, 18 script), timestamp, and
/// the raw payload without tag header or previous-tag-size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlvTag {
  pub tag_type: u8,
  pub timestamp: u32,
  pub payload: Vec<u8>,
}

/// Incremental FLV demuxer for the sink: feed arbitrary video/x-flv byte
/// chunks (header + tags, split anywhere across buffers) and pull out
/// complete tags. Partial data stays buffered until the rest arrives.
/// Unknown tag types are skipped; audio/video/script tags are returned.
#[derive(Default)]
pub struct FlvDemux {
  pending: Vec<u8>,
  header_skipped: bool,
}

impl FlvDemux {
  pub fn push(&mut self, chunk: &[u8]) -> Vec<FlvTag> {
    self.pending.extend_from_slice(chunk);
    let mut tags = Vec::new();
    if !self.header_skipped {
      if self.pending.len() < 13 {
        return tags;
      }
      // The upstream normally starts with the 13-byte FLV header, but in
      // listen mode with wait-for-connection=false the header may have been
      // dropped in render() while no player was attached. Only consume the
      // 13 bytes when they really look like an FLV header; otherwise treat
      // the buffered bytes as tags starting immediately. Tag types (8/9/18)
      // never collide with b'F' (0x46), so the magic check is unambiguous.
      if self.pending[0] == b'F' && self.pending[1] == b'L' && self.pending[2] == b'V' {
        self.pending.drain(..13);
      }
      self.header_skipped = true;
    }
    loop {
      if self.pending.len() < 11 {
        break;
      }
      let tag_type = self.pending[0];
      let data_size = ((self.pending[1] as usize) << 16)
        | ((self.pending[2] as usize) << 8)
        | (self.pending[3] as usize);
      let timestamp_low = ((self.pending[4] as u32) << 16)
        | ((self.pending[5] as u32) << 8)
        | (self.pending[6] as u32);
      let timestamp = timestamp_low | ((self.pending[7] as u32) << 24);
      let total = 11 + data_size + 4;
      if self.pending.len() < total {
        break;
      }
      let payload = self.pending[11..11 + data_size].to_vec();
      self.pending.drain(..total);
      match tag_type {
        FLV_TAG_AUDIO | FLV_TAG_VIDEO | FLV_TAG_SCRIPT_DATA => tags.push(FlvTag {
          tag_type,
          timestamp,
          payload,
        }),
        _ => {}
      }
    }
    tags
  }

  #[allow(dead_code)]
  pub fn is_header_skipped(&self) -> bool {
    self.header_skipped
  }

  #[allow(dead_code)]
  pub fn buffered_len(&self) -> usize {
    self.pending.len()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_full_uri_with_port_and_nested_key() {
    let endpoint = parse_rtmp_uri("rtmp://example.com:1945/live/nested/key").unwrap();
    assert_eq!(endpoint.host, "example.com");
    assert_eq!(endpoint.port, 1945);
    assert_eq!(endpoint.app.as_deref(), Some("live"));
    assert_eq!(endpoint.stream_key.as_deref(), Some("nested/key"));
  }

  #[test]
  fn parses_uri_with_default_port() {
    let endpoint = parse_rtmp_uri("rtmp://127.0.0.1/live/test").unwrap();
    assert_eq!(endpoint.port, 1935);
  }

  #[test]
  fn parses_listen_uri_without_app_or_key() {
    let endpoint = parse_rtmp_uri("rtmp://[::]:1234/").unwrap();
    assert_eq!(endpoint.host, "::");
    assert_eq!(endpoint.port, 1234);
    assert_eq!(endpoint.app, None);
    assert_eq!(endpoint.stream_key, None);
    let endpoint = parse_rtmp_uri("rtmp://0.0.0.0:1935/live").unwrap();
    assert_eq!(endpoint.app.as_deref(), Some("live"));
    assert_eq!(endpoint.stream_key, None);
  }

  #[test]
  fn rejects_non_rtmp_scheme_and_bad_authority() {
    assert!(parse_rtmp_uri("rtmps://host/live/key").is_err());
    assert!(parse_rtmp_uri("rtmp://:1935/live/key").is_err());
    assert!(parse_rtmp_uri("rtmp://::1/live/key").is_err());
    let endpoint = parse_rtmp_uri("rtmp://host/app-only").unwrap();
    assert!(require_app_key(&endpoint, "play").is_err());
  }

  #[test]
  fn script_data_tag_preserves_payload_verbatim() {
    let payload: &[u8] = &[0x02, 0x00, 0x09, 0x6f, 0x6e, 0x43];
    let tag = frame_flv_tag(FLV_TAG_SCRIPT_DATA, 42, payload).unwrap();
    assert_eq!(tag[0], 18);
    assert_eq!(&tag[11..11 + payload.len()], payload);
  }

  #[test]
  fn client_endpoint_resolves_full_uri_with_tc_url_default() {
    let endpoint = resolve_client_endpoint(
      Some("rtmp://example.com:1945/live/nested/key".into()),
      None,
      "rtmpxsink",
    )
    .unwrap();
    assert_eq!(endpoint.host, "example.com");
    assert_eq!(endpoint.port, 1945);
    assert_eq!(endpoint.app, "live");
    assert_eq!(endpoint.stream_key, "nested/key");
    assert_eq!(endpoint.tc_url, "rtmp://example.com:1945/live");
  }

  #[test]
  fn client_endpoint_rejects_uri_without_app_or_key() {
    assert!(resolve_client_endpoint(Some("rtmp://host/".into()), None, "play").is_err());
    assert!(resolve_client_endpoint(Some("rtmp://host/app-only".into()), None, "play").is_err());
    assert!(resolve_client_endpoint(None, None, "play").is_err());
  }

  #[test]
  fn flv_demux_buffers_partial_header_and_tag() {
    let mut demux = FlvDemux::default();
    assert!(demux.push(&FLV_HEADER[..5]).is_empty());
    assert!(!demux.is_header_skipped());
    assert!(demux.push(&FLV_HEADER[5..]).is_empty());
    assert!(demux.is_header_skipped());
    let payload = vec![0x17, 0x01, 0x02, 0x03];
    let tag = frame_flv_tag(FLV_TAG_VIDEO, 100, &payload).unwrap();
    // Split the tag mid-payload: nothing complete yet.
    let cut = 11 + 2;
    assert!(demux.push(&tag[..cut]).is_empty());
    let tags = demux.push(&tag[cut..]);
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].tag_type, FLV_TAG_VIDEO);
    assert_eq!(tags[0].timestamp, 100);
    assert_eq!(tags[0].payload, payload);
  }

  #[test]
  fn flv_demux_skips_unknown_tag_types() {
    let mut demux = FlvDemux::default();
    let _ = demux.push(FLV_HEADER);
    let audio = frame_flv_tag(FLV_TAG_AUDIO, 0, &[0x01]).unwrap();
    let mut unknown = frame_flv_tag(FLV_TAG_VIDEO, 0, &[0x02]).unwrap();
    unknown[0] = 0x0f;
    let mut combined = Vec::new();
    combined.extend_from_slice(&unknown);
    combined.extend_from_slice(&audio);
    let tags = demux.push(&combined);
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].tag_type, FLV_TAG_AUDIO);
  }

  #[test]
  fn flv_demux_parses_tags_without_header() {
    let mut demux = FlvDemux::default();
    let video = frame_flv_tag(FLV_TAG_VIDEO, 9000, &[0x17, 0x00, 0x99]).unwrap();
    let audio = frame_flv_tag(FLV_TAG_AUDIO, 9000, &[0xAF, 0x00, 0x88]).unwrap();
    let mut combined = Vec::new();
    combined.extend_from_slice(&video);
    combined.extend_from_slice(&audio);
    let tags = demux.push(&combined);
    assert!(demux.is_header_skipped());
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].tag_type, FLV_TAG_VIDEO);
    assert_eq!(tags[0].timestamp, 9000);
    assert_eq!(tags[0].payload, vec![0x17, 0x00, 0x99]);
    assert_eq!(tags[1].tag_type, FLV_TAG_AUDIO);
  }
}
