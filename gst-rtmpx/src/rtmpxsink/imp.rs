// rtmpxsink: RTMP publish sink built on rtmpx (URI-only).
//
// Publish mode takes a single publish uri plus an optional tc-url override
// and connects with the shared rtmpx sans-I/O client drive (same
// handshake, Enhanced RTMP connect properties and write helpers as
// rtmpxsrc), demuxing the incoming video/x-flv byte stream into audio/video
// and script-data publish messages.
//
// Listen mode binds instead and serves one player at a time, like an SRT
// server sink: render() applies backpressure until a player connects, the
// worker replays cached sequence headers so mid-stream joiners can decode,
// and the listener keeps accepting across player disconnects.

use std::net::TcpListener;
use std::net::{IpAddr, SocketAddr};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use rtmpx::sessions::{
  ClientSession, ClientSessionEvent, ClientSessionResult, PublishRequestType, ServerSession,
  ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use rtmpx::time::RtmpTimestamp;

use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;
use gst_base::prelude::BaseSinkExt;
use gst_base::subclass::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::common::{
  CAT_SINK, ClientEndpoint, FLV_TAG_AUDIO, FLV_TAG_SCRIPT_DATA, FLV_TAG_VIDEO, FlvDemux, FlvTag,
  RtmpStream, SessionFailure, WORKER_START_TIMEOUT, accept_tls_server, bracketed_host,
  client_handshake, enhanced_rtmp_capabilities, is_client_io_error, nanoseconds_timeout,
  new_client_session, parse_rtmp_uri, read_session_chunk, require_uri, resolve_client_endpoint,
  resolve_tls_acceptor, server_handshake, tcp_connect, wrap_tls_client, write_client_results,
  write_session_results,
};

const DEFAULT_MODE: &str = "publish";
const DEFAULT_ACCEPT_TIMEOUT: u64 = 0;
const DEFAULT_WAIT_FOR_CONNECTION: bool = true;
const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_CONNECT_TIMEOUT: u64 = 10_000_000_000;
const DEFAULT_HANDSHAKE_TIMEOUT: u64 = 10_000_000_000;
const DEFAULT_READ_TIMEOUT: u64 = 0;
const DEFAULT_WRITE_TIMEOUT: u64 = 10_000_000_000;
const DATA_QUEUE_CAPACITY: usize = 128;
const SEND_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct Settings {
  mode: String,
  uri: Option<String>,
  tc_url: Option<String>,
  tcp_nodelay: bool,
  accept_timeout: u64,
  wait_for_connection: bool,
  connect_timeout: u64,
  handshake_timeout: u64,
  read_timeout: u64,
  write_timeout: u64,
  tls_cert: Option<String>,
  tls_key: Option<String>,
  tls_ca_cert: Option<String>,
}

impl Default for Settings {
  fn default() -> Self {
    Self {
      mode: DEFAULT_MODE.into(),
      uri: None,
      tc_url: None,
      tcp_nodelay: DEFAULT_TCP_NODELAY,
      accept_timeout: DEFAULT_ACCEPT_TIMEOUT,
      wait_for_connection: DEFAULT_WAIT_FOR_CONNECTION,
      connect_timeout: DEFAULT_CONNECT_TIMEOUT,
      handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
      read_timeout: DEFAULT_READ_TIMEOUT,
      write_timeout: DEFAULT_WRITE_TIMEOUT,
      tls_cert: None,
      tls_key: None,
      tls_ca_cert: None,
    }
  }
}

fn resolve_endpoint(settings: &Settings) -> Result<ClientEndpoint, String> {
  resolve_client_endpoint(settings.uri.clone(), settings.tc_url.clone(), "rtmpxsink")
}

#[derive(Clone, Debug)]
struct ListenEndpoint {
  bind_host: String,
  port: u16,
  app_filter: Option<String>,
  key_filter: Option<String>,
  tls: bool,
}

// URI-only: the uri carries the bind host, port, and optional app/key
// filters. A missing app/key means "accept any player" in listen mode.
fn resolve_listen_endpoint(settings: &Settings) -> Result<(ListenEndpoint, String), String> {
  let uri = require_uri(&settings.uri, "rtmpxsink listen mode")?;
  let parsed = parse_rtmp_uri(&uri)?;
  Ok((
    ListenEndpoint {
      bind_host: parsed.host,
      port: parsed.port,
      app_filter: parsed.app,
      key_filter: parsed.stream_key,
      tls: parsed.tls,
    },
    uri,
  ))
}

/// Live state shared between render() and the listen worker.
///
/// `wait_for_connection` is snapshotted at start so a READY-state property
/// change cannot flip render() between blocking and dropping mid-stream.
struct ListenLive {
  wait_for_connection: bool,
  player_connected: AtomicBool,
}

#[derive(Default)]
struct State {
  sender: Option<flume::Sender<Vec<u8>>>,
  failure: Option<Arc<Mutex<Option<String>>>>,
  cancellation: Option<CancellationToken>,
  worker: Option<JoinHandle<()>>,
  live: Option<Arc<ListenLive>>,
}

#[derive(Default)]
pub struct RtmpxSink {
  settings: Mutex<Settings>,
  state: Mutex<State>,
  flushing: AtomicBool,
}

#[glib::object_subclass]
impl ObjectSubclass for RtmpxSink {
  const NAME: &'static str = "GstRtmpxSink";
  type Type = super::RtmpxSink;
  type ParentType = gst_base::BaseSink;
}

impl ObjectImpl for RtmpxSink {
  fn properties() -> &'static [glib::ParamSpec] {
    static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
      vec![
        glib::ParamSpecString::builder("mode")
          .nick("Mode")
          .blurb("Sink mode: publish connects to a server, listen binds and serves players")
          .default_value(Some(DEFAULT_MODE))
          .mutable_ready()
          .build(),
        glib::ParamSpecString::builder("uri")
          .nick("URI")
          .blurb("RTMP URI (rtmp:// or rtmps:// for TLS). Publish: rtmp(s)://host:port/app/key. Listen: rtmp(s)://bind-host:port[/app[/key]] (missing app/key accepts any player; port 0 allocates one; rtmps listen needs tls-cert/tls-key)")
          .mutable_ready()
          .build(),
        glib::ParamSpecString::builder("tc-url")
          .nick("TC URL")
          .blurb("Override tcUrl in the RTMP connect (defaults to rtmp://host:port/app)")
          .mutable_ready()
          .build(),
        glib::ParamSpecBoolean::builder("tcp-nodelay")
          .nick("TCP no-delay")
          .blurb("Disable Nagle algorithm on the RTMP connection")
          .default_value(DEFAULT_TCP_NODELAY)
          .mutable_ready()
          .build(),
        glib::ParamSpecUInt64::builder("connect-timeout")
          .nick("Connect timeout")
          .blurb("Nanoseconds allowed for TCP connect (0 disables the timeout)")
          .default_value(DEFAULT_CONNECT_TIMEOUT)
          .mutable_ready()
          .build(),
        glib::ParamSpecUInt64::builder("accept-timeout")
          .nick("Player accept timeout")
          .blurb("Listen mode: nanoseconds to wait for a player connection (0 waits indefinitely)")
          .default_value(DEFAULT_ACCEPT_TIMEOUT)
          .mutable_ready()
          .build(),
        glib::ParamSpecBoolean::builder("wait-for-connection")
          .nick("Wait for connection")
          .blurb("Listen mode: block (apply backpressure) until a player connects instead of dropping data")
          .default_value(DEFAULT_WAIT_FOR_CONNECTION)
          .mutable_ready()
          .build(),
        glib::ParamSpecUInt64::builder("handshake-timeout")
          .nick("Handshake read timeout")
          .blurb("Nanoseconds allowed for each RTMP handshake read (0 disables the timeout)")
          .default_value(DEFAULT_HANDSHAKE_TIMEOUT)
          .mutable_ready()
          .build(),
        glib::ParamSpecUInt64::builder("read-timeout")
          .nick("Session read timeout")
          .blurb("Nanoseconds allowed without RTMP session input (0 disables the timeout)")
          .default_value(DEFAULT_READ_TIMEOUT)
          .mutable_ready()
          .build(),
        glib::ParamSpecUInt64::builder("write-timeout")
          .nick("Session write timeout")
          .blurb("Nanoseconds allowed for each RTMP socket write (0 disables the timeout)")
          .default_value(DEFAULT_WRITE_TIMEOUT)
          .mutable_ready()
          .build(),
        glib::ParamSpecString::builder("tls-cert")
          .nick("TLS certificate")
        .blurb("Certificate this element presents (rtmps://): the listener's server certificate in listen mode (required); the mTLS client certificate in play/publish mode (optional, needs tls-key too)")
          .mutable_ready()
          .build(),
        glib::ParamSpecString::builder("tls-key")
          .nick("TLS private key")
        .blurb("PEM private key matching tls-cert")
          .mutable_ready()
          .build(),
        glib::ParamSpecString::builder("tls-ca-cert")
          .nick("TLS CA certificate")
          .blurb("Publish mode with an rtmps:// uri: path to an extra PEM CA bundle trusted in addition to the platform store (e.g. a self-signed server certificate)")
          .mutable_ready()
          .build(),
      ]
    });

    PROPERTIES.as_ref()
  }

  fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
    let mut settings = self.settings.lock().expect("settings mutex poisoned");

    match pspec.name() {
      "mode" => {
        settings.mode = value.get::<String>().expect("mode type checked upstream");
      }
      "uri" => {
        settings.uri = value.get().expect("uri type checked upstream");
      }
      "tc-url" => {
        settings.tc_url = value.get().expect("tc-url type checked upstream");
      }
      "tcp-nodelay" => {
        settings.tcp_nodelay = value.get().expect("tcp-nodelay type checked upstream");
      }
      "accept-timeout" => {
        settings.accept_timeout = value.get().expect("accept-timeout type checked upstream");
      }
      "wait-for-connection" => {
        settings.wait_for_connection = value
          .get()
          .expect("wait-for-connection type checked upstream");
      }
      "connect-timeout" => {
        settings.connect_timeout = value.get().expect("connect-timeout type checked upstream");
      }
      "handshake-timeout" => {
        settings.handshake_timeout = value
          .get()
          .expect("handshake-timeout type checked upstream");
      }
      "read-timeout" => {
        settings.read_timeout = value.get().expect("read-timeout type checked upstream");
      }
      "write-timeout" => {
        settings.write_timeout = value.get().expect("write-timeout type checked upstream");
      }
      "tls-cert" => {
        settings.tls_cert = value.get().expect("tls-cert type checked upstream");
      }
      "tls-key" => {
        settings.tls_key = value.get().expect("tls-key type checked upstream");
      }
      "tls-ca-cert" => {
        settings.tls_ca_cert = value.get().expect("tls-ca-cert type checked upstream");
      }
      _ => unimplemented!(),
    }
  }

  fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
    let settings = self.settings.lock().expect("settings mutex poisoned");

    match pspec.name() {
      "mode" => settings.mode.to_value(),
      "uri" => settings.uri.to_value(),
      "tc-url" => settings.tc_url.to_value(),
      "tcp-nodelay" => settings.tcp_nodelay.to_value(),
      "accept-timeout" => settings.accept_timeout.to_value(),
      "wait-for-connection" => settings.wait_for_connection.to_value(),
      "connect-timeout" => settings.connect_timeout.to_value(),
      "handshake-timeout" => settings.handshake_timeout.to_value(),
      "read-timeout" => settings.read_timeout.to_value(),
      "write-timeout" => settings.write_timeout.to_value(),
      "tls-cert" => settings.tls_cert.to_value(),
      "tls-key" => settings.tls_key.to_value(),
      "tls-ca-cert" => settings.tls_ca_cert.to_value(),
      _ => unimplemented!(),
    }
  }

  fn constructed(&self) {
    self.parent_constructed();

    let sink = self.obj();
    sink.set_sync(false);
  }
}

impl GstObjectImpl for RtmpxSink {}

impl ElementImpl for RtmpxSink {
  fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
    static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
      gst::subclass::ElementMetadata::new(
        "RTMPX sink",
        "Sink/Network",
        "Publishes an FLV byte stream to an RTMP server, or serves players as an RTMP listener",
        "Elliott Linder <elliott@linder.dev>",
      )
    });

    Some(&*METADATA)
  }

  fn pad_templates() -> &'static [gst::PadTemplate] {
    static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
      let caps = gst::Caps::builder("video/x-flv").build();
      let sink_template = gst::PadTemplate::new(
        "sink",
        gst::PadDirection::Sink,
        gst::PadPresence::Always,
        &caps,
      )
      .expect("valid sink pad template");

      vec![sink_template]
    });

    PAD_TEMPLATES.as_ref()
  }
}

impl BaseSinkImpl for RtmpxSink {
  fn start(&self) -> Result<(), gst::ErrorMessage> {
    let settings = self
      .settings
      .lock()
      .expect("settings mutex poisoned")
      .clone();
    if settings.mode.eq_ignore_ascii_case("listen") {
      return Self::start_listen(self, settings);
    }
    if !settings.mode.eq_ignore_ascii_case("publish") {
      return Err(gst::error_msg!(
        gst::ResourceError::Settings,
        [
          "Invalid rtmpxsink mode '{}': expected publish or listen",
          settings.mode
        ]
      ));
    }
    let endpoint = resolve_endpoint(&settings)
      .map_err(|message| gst::error_msg!(gst::ResourceError::Settings, ["{message}"]))?;
    gst::info!(
      CAT_SINK,
      imp = self,
      "rtmpxsink connecting to {}:{}/{}",
      endpoint.host,
      endpoint.port,
      endpoint.app,
    );
    let (data_sender, data_receiver) = flume::bounded::<Vec<u8>>(DATA_QUEUE_CAPACITY);
    let (startup_sender, startup_receiver) = std::sync::mpsc::sync_channel(1);
    let cancellation = CancellationToken::new();
    let failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let worker_cancellation = cancellation.clone();
    let worker_failure = failure.clone();
    let worker = std::thread::Builder::new()
      .name("rtmpxsink-publish".into())
      .spawn(move || {
        run_publish_worker(
          endpoint,
          settings,
          data_receiver,
          worker_cancellation,
          worker_failure,
          startup_sender,
        )
      })
      .map_err(|error| {
        gst::error_msg!(
          gst::ResourceError::Failed,
          ["failed to spawn RTMP publish worker: {error}"]
        )
      })?;
    {
      let mut state = self.state.lock().expect("state mutex poisoned");
      state.sender = Some(data_sender);
      state.failure = Some(failure);
      state.cancellation = Some(cancellation);
      state.worker = Some(worker);
    }
    match startup_receiver.recv_timeout(WORKER_START_TIMEOUT) {
      Ok(Ok(())) => {
        gst::info!(CAT_SINK, imp = self, "RTMP publish ready");
        Ok(())
      }
      Ok(Err(message)) => {
        let _ = self.stop();
        Err(gst::error_msg!(
          gst::ResourceError::OpenWrite,
          ["{message}"]
        ))
      }
      Err(_) => {
        let _ = self.stop();
        Err(gst::error_msg!(
          gst::ResourceError::OpenWrite,
          ["timed out waiting for RTMP publish to be accepted"]
        ))
      }
    }
  }

  fn stop(&self) -> Result<(), gst::ErrorMessage> {
    gst::info!(CAT_SINK, imp = self, "Stopping rtmpxsink");
    let (cancellation, worker) = {
      let mut state = self.state.lock().expect("state mutex poisoned");
      let cancellation = state.cancellation.take();
      let worker = state.worker.take();
      state.sender = None;
      state.failure = None;
      state.live = None;
      (cancellation, worker)
    };
    if let Some(cancellation) = cancellation {
      cancellation.cancel();
    }
    if let Some(worker) = worker {
      let _ = worker.join();
    }
    self.flushing.store(false, Ordering::Release);
    Ok(())
  }

  fn unlock(&self) -> Result<(), gst::ErrorMessage> {
    self.flushing.store(true, Ordering::Release);
    Ok(())
  }

  fn unlock_stop(&self) -> Result<(), gst::ErrorMessage> {
    self.flushing.store(false, Ordering::Release);
    Ok(())
  }

  fn render(&self, buffer: &gst::Buffer) -> Result<gst::FlowSuccess, gst::FlowError> {
    if self.flushing.load(Ordering::Acquire) {
      return Err(gst::FlowError::Flushing);
    }
    let (sender, failure, live) = {
      let state = self.state.lock().expect("state mutex poisoned");
      (
        state.sender.clone(),
        state.failure.clone(),
        state.live.clone(),
      )
    };
    let sender = sender.ok_or_else(|| {
      gst::element_imp_error!(
        self,
        gst::ResourceError::Failed,
        ["rtmpxsink is not started"]
      );
      gst::FlowError::Error
    })?;
    if let Some(ref failure) = failure
      && let Some(message) = failure.lock().expect("failure mutex poisoned").clone()
    {
      gst::element_imp_error!(
        self,
        gst::ResourceError::Write,
        ["RTMP publish failed: {message}"]
      );
      return Err(gst::FlowError::Error);
    }
    let map = buffer.map_readable().map_err(|_| {
      gst::element_imp_error!(
        self,
        gst::ResourceError::Failed,
        ["Failed to map FLV buffer"]
      );
      gst::FlowError::Error
    })?;
    if map.is_empty() {
      return Ok(gst::FlowSuccess::Ok);
    }
    let mut chunk = map.to_vec();
    drop(map);
    // Listen mode without wait-for-connection drops data while no player
    // is attached (SRT wait-for-connection=false); otherwise render()
    // blocks on the bounded queue below, which is the backpressure that
    // holds the pipeline until a player connects.
    if let Some(live) = live
      && !live.wait_for_connection
      && !live.player_connected.load(Ordering::Acquire)
    {
      return Ok(gst::FlowSuccess::Ok);
    }
    loop {
      if self.flushing.load(Ordering::Acquire) {
        return Err(gst::FlowError::Flushing);
      }
      if let Some(failure) = failure.clone()
        && let Some(message) = failure.lock().expect("failure mutex poisoned").clone()
      {
        gst::element_imp_error!(
          self,
          gst::ResourceError::Write,
          ["RTMP publish failed: {message}"]
        );
        return Err(gst::FlowError::Error);
      }
      match sender.send_timeout(chunk, SEND_POLL_INTERVAL) {
        Ok(()) => return Ok(gst::FlowSuccess::Ok),
        Err(flume::SendTimeoutError::Timeout(returned)) => {
          chunk = returned;
        }
        Err(flume::SendTimeoutError::Disconnected(_)) => {
          gst::element_imp_error!(
            self,
            gst::ResourceError::Write,
            ["RTMP publish worker is gone"]
          );
          return Err(gst::FlowError::Error);
        }
      }
    }
  }
}

impl RtmpxSink {
  fn start_listen(this: &RtmpxSink, settings: Settings) -> Result<(), gst::ErrorMessage> {
    let (endpoint, uri) = resolve_listen_endpoint(&settings)
      .map_err(|message| gst::error_msg!(gst::ResourceError::Settings, ["{message}"]))?;
    let tls_acceptor = resolve_tls_acceptor(
      endpoint.tls,
      settings.tls_cert.as_deref(),
      settings.tls_key.as_deref(),
      "rtmpxsink listen mode",
    )
    .map_err(|message| gst::error_msg!(gst::ResourceError::Settings, ["{message}"]))?;
    let ip_address = endpoint.bind_host.parse::<IpAddr>().map_err(|error| {
      gst::error_msg!(
        gst::ResourceError::Settings,
        [
          "Invalid listen host '{}': {error} (use an IP literal)",
          endpoint.bind_host
        ]
      )
    })?;
    let socket_address = SocketAddr::new(ip_address, endpoint.port);
    let listener = TcpListener::bind(socket_address).map_err(|error| {
      gst::error_msg!(
        gst::ResourceError::OpenWrite,
        ["Failed to bind RTMP listener on {socket_address}: {error}"]
      )
    })?;
    listener.set_nonblocking(true).map_err(|error| {
      gst::error_msg!(
        gst::ResourceError::Settings,
        ["Failed to configure RTMP listener as nonblocking: {error}"]
      )
    })?;
    let local_addr = listener.local_addr().map_err(|error| {
      gst::error_msg!(
        gst::ResourceError::OpenWrite,
        ["Failed to query RTMP listener address: {error}"]
      )
    })?;
    let local_port = local_addr.port();
    let bind_host = endpoint.bind_host.clone();
    let bind_port = endpoint.port;
    let app_filter = endpoint.app_filter.clone();
    let key_filter = endpoint.key_filter.clone();

    let (data_sender, data_receiver) = flume::bounded::<Vec<u8>>(DATA_QUEUE_CAPACITY);
    let (startup_sender, startup_receiver) = std::sync::mpsc::sync_channel(1);
    let cancellation = CancellationToken::new();
    let failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let live = Arc::new(ListenLive {
      wait_for_connection: settings.wait_for_connection,
      player_connected: AtomicBool::new(false),
    });
    let worker_cancellation = cancellation.clone();
    let worker_failure = failure.clone();
    let worker_live = live.clone();
    let worker = std::thread::Builder::new()
      .name("rtmpxsink-listen".into())
      .spawn(move || {
        run_listen_worker(
          listener,
          tls_acceptor,
          app_filter,
          key_filter,
          settings,
          data_receiver,
          worker_cancellation,
          worker_failure,
          startup_sender,
          worker_live,
        )
      })
      .map_err(|error| {
        gst::error_msg!(
          gst::ResourceError::OpenWrite,
          ["Failed to spawn RTMP listener worker: {error}"]
        )
      })?;
    {
      let mut state = this.state.lock().expect("state mutex poisoned");
      state.sender = Some(data_sender);
      state.failure = Some(failure);
      state.cancellation = Some(cancellation);
      state.worker = Some(worker);
      state.live = Some(live);
    }
    match startup_receiver.recv_timeout(WORKER_START_TIMEOUT) {
      Ok(Ok(())) => {}
      Ok(Err(message)) => {
        let _ = this.stop();
        return Err(gst::error_msg!(
          gst::ResourceError::OpenWrite,
          ["{message}"]
        ));
      }
      Err(error) => {
        let _ = this.stop();
        return Err(gst::error_msg!(
          gst::ResourceError::OpenWrite,
          ["RTMP listener worker failed to start: {error}"]
        ));
      }
    }

    // Like rtmpxsrc, a :0 port asks the OS for a free one; publish the
    // bound port back on the uri so players can discover it.
    if bind_port == 0 {
      let mut path = String::new();
      if let Some(app) = endpoint.app_filter.as_deref() {
        path.push('/');
        path.push_str(app);
        if let Some(key) = endpoint.key_filter.as_deref() {
          path.push('/');
          path.push_str(key);
        }
      }
      let scheme = if endpoint.tls { "rtmps" } else { "rtmp" };
      let resolved = format!(
        "{scheme}://{}:{local_port}{path}",
        bracketed_host(&bind_host)
      );
      this.settings.lock().expect("settings mutex poisoned").uri = Some(resolved);
      this.obj().notify("uri");
    }

    gst::info!(
      CAT_SINK,
      imp = this,
      "Listening for RTMP players on {uri} (bound {local_addr})",
    );
    Ok(())
  }
}

fn run_publish_worker(
  endpoint: ClientEndpoint,
  settings: Settings,
  data_receiver: flume::Receiver<Vec<u8>>,
  cancellation: CancellationToken,
  failure: Arc<Mutex<Option<String>>>,
  startup: std::sync::mpsc::SyncSender<Result<(), String>>,
) {
  let runtime = match tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
  {
    Ok(runtime) => runtime,
    Err(error) => {
      let message = format!("Failed to create Tokio runtime: {error}");
      *failure.lock().expect("failure mutex poisoned") = Some(message.clone());
      let _ = startup.send(Err(message));
      return;
    }
  };
  runtime.block_on(async move {
    if let Err(error) = publish_until_done(
      &endpoint,
      &settings,
      &data_receiver,
      &cancellation,
      &startup,
    )
    .await
    {
      let message = error.message.clone();
      *failure.lock().expect("failure mutex poisoned") = Some(message.clone());
      // If startup has not been answered yet, report there; otherwise the
      // next render() observes the sticky failure and fails the pipeline.
      let _ = startup.send(Err(message));
      if !error.client_disconnect {
        gst::warning!(CAT_SINK, "RTMP publish failed: {}", error.message);
      } else {
        gst::info!(CAT_SINK, "RTMP publish ended: {}", error.message);
      }
    }
  });
}

async fn publish_until_done(
  endpoint: &ClientEndpoint,
  settings: &Settings,
  data_receiver: &flume::Receiver<Vec<u8>>,
  cancellation: &CancellationToken,
  startup: &std::sync::mpsc::SyncSender<Result<(), String>>,
) -> Result<(), SessionFailure> {
  let write_timeout = nanoseconds_timeout(settings.write_timeout);
  let read_timeout = nanoseconds_timeout(settings.read_timeout);
  let handshake_timeout = nanoseconds_timeout(settings.handshake_timeout);
  let stream = tcp_connect(
    &endpoint.host,
    endpoint.port,
    settings.connect_timeout,
    settings.tcp_nodelay,
  )
  .await?;
  let mut stream = if endpoint.tls {
    wrap_tls_client(
      stream,
      &endpoint.host,
      settings.tls_ca_cert.as_deref(),
      settings.tls_cert.as_deref(),
      settings.tls_key.as_deref(),
    )
    .await?
  } else {
    RtmpStream::Plain(stream)
  };
  gst::info!(
    CAT_SINK,
    "Connected to RTMP server at {}:{}",
    endpoint.host,
    endpoint.port
  );
  let carry = client_handshake(&mut stream, cancellation, &handshake_timeout).await?;
  let mut session = new_client_session(endpoint.tc_url.clone())?;
  // write_client_results needs a WorkerOutput channel it never uses; hand it
  // a dummy one so the sink shares the exact helper with rtmpxsrc.
  let (dummy_output, _dummy_rx) = flume::bounded(1);
  let connect = session
    .request_connection_with_properties(endpoint.app.clone(), enhanced_rtmp_capabilities())
    .map_err(|error| {
      SessionFailure::error(format!("failed to request RTMP connection: {error:?}"))
    })?;
  write_client_results(
    &mut stream,
    vec![connect],
    &dummy_output,
    cancellation,
    &write_timeout,
  )
  .await?;
  // The handshake carry can already contain the server's connect reply
  // (WindowAck/SetChunkSize/_result). If it does, the connection is already
  // accepted and we must not wait for a second accept that never arrives.
  let mut connected = false;
  if !carry.is_empty() {
    let results = session
      .handle_input(&carry)
      .map_err(|error| SessionFailure::error(format!("server sent unreadable RTMP: {error:?}")))?;
    connected = drain_connect_results(
      &mut session,
      &mut stream,
      results,
      &dummy_output,
      cancellation,
      &write_timeout,
    )
    .await?;
  }
  if !connected {
    wait_for_connection(
      &mut session,
      &mut stream,
      &dummy_output,
      cancellation,
      &read_timeout,
      &write_timeout,
    )
    .await?;
  }
  let publish = session
    .request_publishing(endpoint.stream_key.clone(), PublishRequestType::Live)
    .map_err(|error| SessionFailure::error(format!("failed to request RTMP publish: {error:?}")))?;
  write_client_results(
    &mut stream,
    vec![publish],
    &dummy_output,
    cancellation,
    &write_timeout,
  )
  .await?;
  wait_for_publish_accept(
    &mut session,
    &mut stream,
    &dummy_output,
    cancellation,
    &read_timeout,
    &write_timeout,
  )
  .await?;
  gst::info!(
    CAT_SINK,
    "RTMP server accepted publish for {}",
    endpoint.stream_key
  );
  if startup.send(Ok(())).is_err() {
    return Err(SessionFailure::error("sink is shutting down"));
  }
  run_publish_loop(
    &mut session,
    &mut stream,
    data_receiver,
    &dummy_output,
    cancellation,
    &read_timeout,
    &write_timeout,
  )
  .await
}

async fn wait_for_connection(
  session: &mut ClientSession,
  stream: &mut RtmpStream,
  dummy_output: &flume::Sender<crate::common::WorkerOutput>,
  cancellation: &CancellationToken,
  read_timeout: &Option<Duration>,
  write_timeout: &Option<Duration>,
) -> Result<(), SessionFailure> {
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    let n = read_session_chunk(stream, &mut read_buf, cancellation, read_timeout).await?;
    if n == 0 {
      return Err(SessionFailure::disconnect(
        "RTMP server closed the connection while connecting",
      ));
    }
    let results = session
      .handle_input(&read_buf[..n])
      .map_err(|error| SessionFailure::error(format!("server sent unreadable RTMP: {error:?}")))?;
    if drain_connect_results(
      session,
      stream,
      results,
      dummy_output,
      cancellation,
      write_timeout,
    )
    .await?
    {
      return Ok(());
    }
  }
}

async fn drain_connect_results(
  _session: &mut ClientSession,
  stream: &mut RtmpStream,
  results: Vec<ClientSessionResult>,
  dummy_output: &flume::Sender<crate::common::WorkerOutput>,
  cancellation: &CancellationToken,
  write_timeout: &Option<Duration>,
) -> Result<bool, SessionFailure> {
  // rtmpx emits [WindowAck, ConnectionAccepted event, SetChunkSize] for a
  // connect accept. The SetChunkSize packet MUST hit the wire: it tells the
  // server we now send 4096-byte chunks. Returning early on the event drops
  // it, leaving our serializer at 4096 while the server still reads 128.
  // The server then parses video payload bytes as chunk headers, which is
  // exactly the NoPreviousChunkOnStream { csid: 33 } failure seen in the
  // sink->src smoke test. Drain the whole batch before reporting accepted.
  let mut accepted = false;
  for result in results {
    match result {
      ClientSessionResult::OutboundResponse(packet) => {
        write_client_results(
          stream,
          vec![ClientSessionResult::OutboundResponse(packet)],
          dummy_output,
          cancellation,
          write_timeout,
        )
        .await?;
      }
      ClientSessionResult::RaisedEvent(event) => match event {
        ClientSessionEvent::ConnectionRequestAccepted { .. } => {
          gst::info!(CAT_SINK, "RTMP server accepted connection");
          accepted = true;
        }
        ClientSessionEvent::ConnectionRequestRejected { description } => {
          return Err(SessionFailure::error(format!(
            "RTMP server rejected connection: {description}"
          )));
        }
        _ => {}
      },
      ClientSessionResult::UnhandleableMessageReceived(_) => {}
    }
  }
  Ok(accepted)
}

async fn wait_for_publish_accept(
  session: &mut ClientSession,
  stream: &mut RtmpStream,
  dummy_output: &flume::Sender<crate::common::WorkerOutput>,
  cancellation: &CancellationToken,
  read_timeout: &Option<Duration>,
  write_timeout: &Option<Duration>,
) -> Result<(), SessionFailure> {
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    let n = read_session_chunk(stream, &mut read_buf, cancellation, read_timeout).await?;
    if n == 0 {
      return Err(SessionFailure::disconnect(
        "RTMP server closed the connection while requesting publish",
      ));
    }
    let results = session
      .handle_input(&read_buf[..n])
      .map_err(|error| SessionFailure::error(format!("server sent unreadable RTMP: {error:?}")))?;
    // Same drain-fully rule as the connect path: finish writing every
    // OutboundResponse in this batch before reporting publish-accepted, so
    // a trailing packet can never be dropped by an early return.
    let mut accepted = false;
    for result in results {
      match result {
        ClientSessionResult::OutboundResponse(packet) => {
          write_client_results(
            stream,
            vec![ClientSessionResult::OutboundResponse(packet)],
            dummy_output,
            cancellation,
            write_timeout,
          )
          .await?;
        }
        ClientSessionResult::RaisedEvent(event) => match event {
          ClientSessionEvent::PublishRequestAccepted => accepted = true,
          ClientSessionEvent::ConnectionRequestRejected { description } => {
            return Err(SessionFailure::error(format!(
              "RTMP server rejected publish: {description}"
            )));
          }
          _ => {}
        },
        ClientSessionResult::UnhandleableMessageReceived(_) => {}
      }
    }
    if accepted {
      return Ok(());
    }
  }
}

/// Turn demuxed FLV tags into rtmpx publish results. The demuxer itself
/// (header handling, partial-tag buffering) lives in common so both the
/// unit tests and any future muxer share it; this only maps tag types to
/// the matching publish call.
fn publish_flv_tags(
  session: &mut ClientSession,
  tags: Vec<crate::common::FlvTag>,
) -> Result<Vec<ClientSessionResult>, SessionFailure> {
  let mut results = Vec::with_capacity(tags.len());
  for tag in tags {
    let timestamp = RtmpTimestamp::new(tag.timestamp);
    let result = match tag.tag_type {
      FLV_TAG_AUDIO => session
        .publish_audio_data(Bytes::from(tag.payload), timestamp, false)
        .map_err(|error| {
          SessionFailure::error(format!("failed to publish audio data: {error:?}"))
        })?,
      FLV_TAG_VIDEO => session
        .publish_video_data(Bytes::from(tag.payload), timestamp, false)
        .map_err(|error| {
          SessionFailure::error(format!("failed to publish video data: {error:?}"))
        })?,
      FLV_TAG_SCRIPT_DATA => session
        .publish_raw_data_payload(Bytes::from(tag.payload), timestamp)
        .map_err(|error| {
          SessionFailure::error(format!("failed to publish script data: {error:?}"))
        })?,
      other => {
        gst::debug!(CAT_SINK, "Ignoring FLV tag type {other}");
        continue;
      }
    };
    results.push(result);
  }
  Ok(results)
}

async fn run_publish_loop(
  session: &mut ClientSession,
  stream: &mut RtmpStream,
  data_receiver: &flume::Receiver<Vec<u8>>,
  dummy_output: &flume::Sender<crate::common::WorkerOutput>,
  cancellation: &CancellationToken,
  read_timeout: &Option<Duration>,
  write_timeout: &Option<Duration>,
) -> Result<(), SessionFailure> {
  let mut demux = FlvDemux::default();
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    tokio::select! {
      _ = cancellation.cancelled() => {
        let stopping = session.stop_publishing().unwrap_or_default();
        if !stopping.is_empty() {
          let _ = write_client_results(stream, stopping, dummy_output, cancellation, write_timeout).await;
        }
        let _ = stream.shutdown().await;
        return Err(SessionFailure::error("sink is shutting down"));
      }
      incoming = data_receiver.recv_async() => match incoming {
        Ok(chunk) => {
          let tags = demux.push(&chunk);
          let results = publish_flv_tags(session, tags)?;
          if !results.is_empty() {
            write_client_results(stream, results, dummy_output, cancellation, write_timeout).await?;
          }
        }
        Err(flume::RecvError::Disconnected) => {
          let stopping = session.stop_publishing().unwrap_or_default();
          if !stopping.is_empty() {
            let _ = write_client_results(stream, stopping, dummy_output, cancellation, write_timeout).await;
          }
          let _ = stream.shutdown().await;
          return Ok(());
        }
      },
      read_outcome = read_session_chunk(stream, &mut read_buf, cancellation, read_timeout) => {
        let n = read_outcome?;
        if n == 0 {
          return Err(SessionFailure::disconnect("RTMP server closed the connection"));
        }
        let results = session.handle_input(&read_buf[..n]).map_err(|error| {
          SessionFailure::error(format!("server sent unreadable RTMP: {error:?}"))
        })?;
        for result in results {
          match result {
            ClientSessionResult::OutboundResponse(packet) => {
              write_client_results(
                stream,
                vec![ClientSessionResult::OutboundResponse(packet)],
                dummy_output,
                cancellation,
                write_timeout,
              )
              .await?;
            }
            ClientSessionResult::RaisedEvent(event) => {
              gst::debug!(CAT_SINK, "Ignoring RTMP client event while publishing: {event:?}");
            }
            ClientSessionResult::UnhandleableMessageReceived(_) => {}
          }
        }
      }
    }
  }
}

/// Heuristic FLV sequence-header detection for the listen replay cache.
///
/// Classic AVC is CodecID 7 with AVCPacketType 0; classic AAC is
/// SoundFormat 10 with AACPacketType 0. Enhanced RTMP video and audio carry
/// packet type 0 (sequence start) in the low nibble of the first payload
/// byte. Legacy CodecIDs never use 0 for video (1-7) and legacy audio never
/// uses SoundFormat 9, so the low-nibble checks cannot misfire on classic
/// tags. Script tags are never cached: the server path has no raw AMF send
/// and players join fine without onMetaData.
fn is_video_sequence_header(payload: &[u8]) -> bool {
  if payload.len() < 2 {
    return false;
  }
  match payload[0] & 0x0F {
    7 => payload[1] == 0,
    0 => payload.len() >= 5,
    _ => false,
  }
}

fn is_audio_sequence_header(payload: &[u8]) -> bool {
  if payload.len() < 2 {
    return false;
  }
  if payload[0] >> 4 == 10 {
    return payload[1] == 0;
  }
  payload[0] & 0xF0 == 0x90 && payload[0] & 0x0F == 0
}

/// Last sequence headers seen on the listener, replayed to mid-stream
/// joiners so they can decode from the live edge.
#[derive(Default)]
struct HeaderCache {
  video: Option<FlvTag>,
  audio: Option<FlvTag>,
}

impl HeaderCache {
  fn observe(&mut self, tag: &FlvTag) {
    match tag.tag_type {
      FLV_TAG_VIDEO if is_video_sequence_header(&tag.payload) => self.video = Some(tag.clone()),
      FLV_TAG_AUDIO if is_audio_sequence_header(&tag.payload) => self.audio = Some(tag.clone()),
      _ => {}
    }
  }

  fn ordered(&self) -> Vec<FlvTag> {
    let mut out = Vec::with_capacity(2);
    if let Some(video) = self.video.clone() {
      out.push(video);
    }
    if let Some(audio) = self.audio.clone() {
      out.push(audio);
    }
    out
  }
}

/// How one player connection ended. Player problems only ever end that
/// player; the listener keeps accepting afterwards.
enum PlayerOutcome {
  PlayerDone,
  Eos,
  Shutdown,
}

/// What the pre-play pump found in one batch of session results.
enum PlayWait {
  Accepted(u32),
  Rejected,
  NeedMore,
  Gone,
}

#[allow(clippy::too_many_arguments)]
fn run_listen_worker(
  listener: TcpListener,
  tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
  app_filter: Option<String>,
  key_filter: Option<String>,
  settings: Settings,
  data_receiver: flume::Receiver<Vec<u8>>,
  cancellation: CancellationToken,
  failure: Arc<Mutex<Option<String>>>,
  startup: std::sync::mpsc::SyncSender<Result<(), String>>,
  live: Arc<ListenLive>,
) {
  let runtime = match tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
  {
    Ok(runtime) => runtime,
    Err(error) => {
      let message = format!("Failed to create Tokio runtime: {error}");
      *failure.lock().expect("failure mutex poisoned") = Some(message.clone());
      let _ = startup.send(Err(message));
      return;
    }
  };
  runtime.block_on(async move {
    let listener = match tokio::net::TcpListener::from_std(listener) {
      Ok(listener) => listener,
      Err(error) => {
        let message = format!("Failed to initialize Tokio RTMP listener: {error}");
        *failure.lock().expect("failure mutex poisoned") = Some(message.clone());
        let _ = startup.send(Err(message));
        return;
      }
    };
    if startup.send(Ok(())).is_err() {
      return;
    }
    // start() reports ready once bound; the wait for the first player
    // happens here, as backpressure in render(), like an SRT server sink.
    if let Err(error) = listen_until_done(
      &listener,
      &tls_acceptor,
      &app_filter,
      &key_filter,
      &settings,
      &data_receiver,
      &cancellation,
      &live,
    )
    .await
    {
      let message = error.message.clone();
      *failure.lock().expect("failure mutex poisoned") = Some(message.clone());
      gst::warning!(CAT_SINK, "RTMP listen failed: {}", error.message);
    }
  });
}

/// Accept players forever, serving one at a time. Player failures only end
/// that player; the loop keeps listening. Returns Ok on EOS/shutdown and
/// Err only for fatal listener problems (accept timeout), which render()
/// surfaces as a sticky failure.
#[allow(clippy::too_many_arguments)]
async fn listen_until_done(
  listener: &tokio::net::TcpListener,
  tls_acceptor: &Option<tokio_rustls::TlsAcceptor>,
  app_filter: &Option<String>,
  key_filter: &Option<String>,
  settings: &Settings,
  data_receiver: &flume::Receiver<Vec<u8>>,
  cancellation: &CancellationToken,
  live: &ListenLive,
) -> Result<(), SessionFailure> {
  let handshake_timeout = nanoseconds_timeout(settings.handshake_timeout);
  let read_timeout = nanoseconds_timeout(settings.read_timeout);
  let write_timeout = nanoseconds_timeout(settings.write_timeout);
  // FlvDemux cannot resync, so every byte must flow through it exactly
  // once, in order, for the whole life of the listener. Player gaps never
  // drop raw chunks: bytes queued while nobody listens are drained through
  // the demux on each new player, updating the header cache without being
  // sent, and the worker never drains the queue while no player is
  // attached (that is what makes render() block as backpressure).
  let mut demux = FlvDemux::default();
  let mut headers = HeaderCache::default();
  loop {
    live.player_connected.store(false, Ordering::Release);
    if data_receiver.is_disconnected() {
      return Ok(());
    }
    let accept = async {
      if settings.accept_timeout == 0 {
        listener.accept().await.map_err(|error| error.to_string())
      } else {
        let limit = Duration::from_nanos(settings.accept_timeout);
        tokio::time::timeout(limit, listener.accept())
          .await
          .map_err(|_| format!("timed out after {limit:?} waiting for an RTMP player"))?
          .map_err(|error| error.to_string())
      }
    };
    let accepted = tokio::select! {
      _ = cancellation.cancelled() => return Err(SessionFailure::error("sink is shutting down")),
      accepted = accept => accepted,
    };
    let (stream, peer_address) = match accepted {
      Ok(accepted) => accepted,
      Err(error) => {
        return Err(SessionFailure::error(format!(
          "failed to accept RTMP player: {error}"
        )));
      }
    };
    if let Err(error) = stream.set_nodelay(settings.tcp_nodelay) {
      return Err(SessionFailure::error(format!(
        "failed to configure TCP_NODELAY: {error}"
      )));
    }
    // A failed TLS handshake ends that player, not the listener: the next
    // accept still gets its turn.
    let stream = if let Some(acceptor) = tls_acceptor.as_ref() {
      match accept_tls_server(acceptor, stream, &handshake_timeout).await {
        Ok(tls) => tls,
        Err(error) => {
          gst::warning!(CAT_SINK, "RTMPS player {peer_address}: {error}");
          continue;
        }
      }
    } else {
      RtmpStream::Plain(stream)
    };
    gst::info!(
      CAT_SINK,
      "Accepted RTMP player connection from {peer_address}"
    );
    match serve_one_player(
      stream,
      app_filter,
      key_filter,
      &mut demux,
      &mut headers,
      data_receiver,
      cancellation,
      live,
      &handshake_timeout,
      &read_timeout,
      &write_timeout,
    )
    .await
    {
      PlayerOutcome::PlayerDone => continue,
      PlayerOutcome::Eos => return Ok(()),
      PlayerOutcome::Shutdown => {
        return Err(SessionFailure::error("sink is shutting down"));
      }
    }
  }
}

/// Drive one player connection through handshake, connect and play, then
/// stream to it. Anything the player does wrong ends that player with
/// PlayerDone; the listener keeps accepting afterwards.
#[allow(clippy::too_many_arguments)]
async fn serve_one_player(
  stream: RtmpStream,
  app_filter: &Option<String>,
  key_filter: &Option<String>,
  demux: &mut FlvDemux,
  headers: &mut HeaderCache,
  data_receiver: &flume::Receiver<Vec<u8>>,
  cancellation: &CancellationToken,
  live: &ListenLive,
  handshake_timeout: &Option<Duration>,
  read_timeout: &Option<Duration>,
  write_timeout: &Option<Duration>,
) -> PlayerOutcome {
  let mut stream = stream;
  let carry = match server_handshake(&mut stream, cancellation, handshake_timeout).await {
    Ok(carry) => carry,
    Err(failure) => {
      if cancellation.is_cancelled() {
        return PlayerOutcome::Shutdown;
      }
      gst::warning!(
        CAT_SINK,
        "RTMP player handshake failed: {}",
        failure.message
      );
      return PlayerOutcome::PlayerDone;
    }
  };
  let mut config = ServerSessionConfig::new();
  config.window_ack_size = 2_500_000;
  config.chunk_size = 4096;
  let (mut session, initial) = match ServerSession::new(config) {
    Ok(session) => session,
    Err(error) => {
      gst::warning!(CAT_SINK, "Failed to create RTMP session: {error:?}");
      return PlayerOutcome::PlayerDone;
    }
  };
  debug_assert!(initial.is_empty(), "server must not write before connect");
  // write_session_results needs a WorkerOutput channel it never uses;
  // hand it a dummy one like the publish path does.
  let (dummy_output, _dummy_rx) = flume::bounded(1);
  if !carry.is_empty() {
    let results = match session.handle_input(&carry) {
      Ok(results) => results,
      Err(error) => {
        gst::warning!(CAT_SINK, "Player sent unreadable RTMP: {error:?}");
        return PlayerOutcome::PlayerDone;
      }
    };
    match pump_until_play(
      &mut session,
      &mut stream,
      results,
      app_filter,
      key_filter,
      &dummy_output,
      cancellation,
      write_timeout,
    )
    .await
    {
      PlayWait::Accepted(stream_id) => {
        return stream_to_player(
          &mut session,
          &mut stream,
          stream_id,
          demux,
          headers,
          data_receiver,
          cancellation,
          live,
          read_timeout,
          write_timeout,
        )
        .await;
      }
      PlayWait::Rejected | PlayWait::Gone => return PlayerOutcome::PlayerDone,
      PlayWait::NeedMore => {}
    }
  }
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    let n = match read_session_chunk(&mut stream, &mut read_buf, cancellation, read_timeout).await {
      Ok(n) => n,
      Err(failure) => {
        if cancellation.is_cancelled() {
          return PlayerOutcome::Shutdown;
        }
        if failure.client_disconnect {
          gst::info!(
            CAT_SINK,
            "RTMP player went away before play: {}",
            failure.message
          );
        } else {
          gst::warning!(CAT_SINK, "RTMP player read failed: {}", failure.message);
        }
        return PlayerOutcome::PlayerDone;
      }
    };
    if n == 0 {
      gst::info!(CAT_SINK, "RTMP player closed the connection before play");
      return PlayerOutcome::PlayerDone;
    }
    let results = match session.handle_input(&read_buf[..n]) {
      Ok(results) => results,
      Err(error) => {
        gst::warning!(CAT_SINK, "Player sent unreadable RTMP: {error:?}");
        return PlayerOutcome::PlayerDone;
      }
    };
    match pump_until_play(
      &mut session,
      &mut stream,
      results,
      app_filter,
      key_filter,
      &dummy_output,
      cancellation,
      write_timeout,
    )
    .await
    {
      PlayWait::Accepted(stream_id) => {
        return stream_to_player(
          &mut session,
          &mut stream,
          stream_id,
          demux,
          headers,
          data_receiver,
          cancellation,
          live,
          read_timeout,
          write_timeout,
        )
        .await;
      }
      PlayWait::Rejected | PlayWait::Gone => return PlayerOutcome::PlayerDone,
      PlayWait::NeedMore => {}
    }
  }
}

/// Handle one batch of session results while waiting for the play request:
/// write outbound packets and accept or reject the play. Non-matching app
/// or stream key is rejected and the connection is dropped so the listener
/// keeps serving the configured stream.
#[allow(clippy::too_many_arguments)]
async fn pump_until_play(
  session: &mut ServerSession,
  stream: &mut RtmpStream,
  results: Vec<ServerSessionResult>,
  app_filter: &Option<String>,
  key_filter: &Option<String>,
  dummy_output: &flume::Sender<crate::common::WorkerOutput>,
  cancellation: &CancellationToken,
  write_timeout: &Option<Duration>,
) -> PlayWait {
  for result in results {
    match result {
      ServerSessionResult::OutboundResponse(packet) => {
        if write_session_results(
          stream,
          vec![ServerSessionResult::OutboundResponse(packet)],
          dummy_output,
          cancellation,
          write_timeout,
        )
        .await
        .is_err()
        {
          return PlayWait::Gone;
        }
      }
      ServerSessionResult::RaisedEvent(event) => match event {
        ServerSessionEvent::ConnectionRequested {
          request_id,
          app_name,
          ..
        } => {
          gst::info!(CAT_SINK, "Accepted RTMP connection for app '{app_name}'");
          let follow_up = match session
            .accept_request_with_properties(request_id, enhanced_rtmp_capabilities())
          {
            Ok(follow_up) => follow_up,
            Err(error) => {
              gst::warning!(CAT_SINK, "Failed to accept RTMP connection: {error:?}");
              return PlayWait::Gone;
            }
          };
          if write_session_results(stream, follow_up, dummy_output, cancellation, write_timeout)
            .await
            .is_err()
          {
            return PlayWait::Gone;
          }
        }
        ServerSessionEvent::PlayStreamRequested {
          request_id,
          app_name,
          stream_key,
          stream_id,
          ..
        } => {
          let allowed = app_filter
            .as_deref()
            .is_none_or(|app| app == app_name.as_ref())
            && key_filter
              .as_deref()
              .is_none_or(|key| key == stream_key.as_ref());
          if !allowed {
            gst::warning!(
              CAT_SINK,
              "Rejecting RTMP play for '{app_name}/{stream_key}': not the configured stream"
            );
            let packet = match session.reject_request(
              request_id,
              "NetStream.Play.Failed",
              "this endpoint only serves its configured stream",
            ) {
              Ok(packet) => packet,
              Err(error) => {
                gst::warning!(CAT_SINK, "Failed to reject RTMP play: {error:?}");
                return PlayWait::Gone;
              }
            };
            if write_session_results(stream, packet, dummy_output, cancellation, write_timeout)
              .await
              .is_err()
            {
              return PlayWait::Gone;
            }
            return PlayWait::Rejected;
          }
          let follow_up = match session
            .accept_request_with_properties(request_id, enhanced_rtmp_capabilities())
          {
            Ok(follow_up) => follow_up,
            Err(error) => {
              gst::warning!(CAT_SINK, "Failed to accept RTMP play: {error:?}");
              return PlayWait::Gone;
            }
          };
          if write_session_results(stream, follow_up, dummy_output, cancellation, write_timeout)
            .await
            .is_err()
          {
            return PlayWait::Gone;
          }
          gst::info!(CAT_SINK, "RTMP player is playing '{app_name}/{stream_key}'");
          return PlayWait::Accepted(stream_id);
        }
        // A publisher on a sink listener is as welcome as a player on a
        // src listener: reject it the same way so the port keeps serving.
        ServerSessionEvent::PublishStreamRequested {
          request_id,
          app_name,
          ..
        } => {
          gst::warning!(
            CAT_SINK,
            "Rejecting RTMP publish request for app '{app_name}': listener only serves players"
          );
          let packet = match session.reject_request(
            request_id,
            "NetStream.Publish.BadName",
            "this endpoint only serves players",
          ) {
            Ok(packet) => packet,
            Err(error) => {
              gst::warning!(CAT_SINK, "Failed to reject RTMP publish: {error:?}");
              return PlayWait::Gone;
            }
          };
          if write_session_results(stream, packet, dummy_output, cancellation, write_timeout)
            .await
            .is_err()
          {
            return PlayWait::Gone;
          }
          return PlayWait::Rejected;
        }
        _ => {}
      },
      ServerSessionResult::UnhandleableMessageReceived(_) => {}
    }
  }
  PlayWait::NeedMore
}

/// Write one server-side media packet with a per-write timeout, mapping
/// peer-gone errors to disconnects like write_session_results does.
async fn write_packet_bytes(
  stream: &mut RtmpStream,
  bytes: &[u8],
  write_timeout: &Option<Duration>,
) -> Result<(), SessionFailure> {
  if let Some(limit) = write_timeout {
    tokio::time::timeout(*limit, stream.write_all(bytes))
      .await
      .map_err(|_| {
        SessionFailure::disconnect(format!("timed out after {limit:?} writing RTMP media"))
      })?
      .map_err(|error| {
        if is_client_io_error(&error) {
          SessionFailure::disconnect(format!("RTMP player went away while writing: {error}"))
        } else {
          SessionFailure::error(format!("failed to write RTMP media: {error}"))
        }
      })?;
  } else {
    stream.write_all(bytes).await.map_err(|error| {
      if is_client_io_error(&error) {
        SessionFailure::disconnect(format!("RTMP player went away while writing: {error}"))
      } else {
        SessionFailure::error(format!("failed to write RTMP media: {error}"))
      }
    })?;
  }
  stream.flush().await.map_err(|error| {
    if is_client_io_error(&error) {
      SessionFailure::disconnect(format!("RTMP player went away while writing: {error}"))
    } else {
      SessionFailure::error(format!("failed to write RTMP media: {error}"))
    }
  })
}

/// Send one cached sequence header to a fresh player.
async fn send_cached_header(
  session: &mut ServerSession,
  stream: &mut RtmpStream,
  stream_id: u32,
  tag: &FlvTag,
  write_timeout: &Option<Duration>,
) -> Result<(), SessionFailure> {
  let timestamp = RtmpTimestamp::new(tag.timestamp);
  let packet = match tag.tag_type {
    FLV_TAG_VIDEO => session
      .send_video_data(
        stream_id,
        Bytes::from(tag.payload.clone()),
        timestamp,
        false,
      )
      .map_err(|error| SessionFailure::error(format!("failed to frame video header: {error:?}")))?,
    FLV_TAG_AUDIO => session
      .send_audio_data(
        stream_id,
        Bytes::from(tag.payload.clone()),
        timestamp,
        false,
      )
      .map_err(|error| SessionFailure::error(format!("failed to frame audio header: {error:?}")))?,
    _ => return Ok(()),
  };
  write_packet_bytes(stream, &packet.bytes, write_timeout).await
}

/// Stream live tags to the accepted player until it leaves, EOS arrives,
/// or the sink shuts down. Every tag flows through the shared demux first
/// so the header cache stays current for the next joiner.
#[allow(clippy::too_many_arguments)]
async fn stream_to_player(
  session: &mut ServerSession,
  stream: &mut RtmpStream,
  stream_id: u32,
  demux: &mut FlvDemux,
  headers: &mut HeaderCache,
  data_receiver: &flume::Receiver<Vec<u8>>,
  cancellation: &CancellationToken,
  live: &ListenLive,
  read_timeout: &Option<Duration>,
  write_timeout: &Option<Duration>,
) -> PlayerOutcome {
  // Drain anything queued while nobody was listening through the demux so
  // the cache is current, then replay the cached sequence headers so the
  // joiner can decode from the live edge. The drained bytes are stale
  // media, never resent.
  loop {
    match data_receiver.try_recv() {
      Ok(chunk) => {
        for tag in demux.push(&chunk) {
          headers.observe(&tag);
        }
      }
      Err(flume::TryRecvError::Empty) => break,
      Err(flume::TryRecvError::Disconnected) => return PlayerOutcome::Eos,
    }
  }
  for cached in headers.ordered() {
    if let Err(failure) =
      send_cached_header(session, stream, stream_id, &cached, write_timeout).await
    {
      gst::info!(
        CAT_SINK,
        "RTMP player left during header replay: {}",
        failure.message
      );
      return PlayerOutcome::PlayerDone;
    }
  }
  live.player_connected.store(true, Ordering::Release);
  // Dummy channel kept alive for the serve loop in case a future rtmpx
  // version routes replies through it; today the serve path writes packets
  // directly.
  let (dummy_output, _dummy_rx) = flume::bounded::<crate::common::WorkerOutput>(1);
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    tokio::select! {
      _ = cancellation.cancelled() => {
        live.player_connected.store(false, Ordering::Release);
        let _ = stream.shutdown().await;
        return PlayerOutcome::Shutdown;
      }
      incoming = data_receiver.recv_async() => match incoming {
        Ok(chunk) => {
          let mut failed: Option<SessionFailure> = None;
          for tag in demux.push(&chunk) {
            headers.observe(&tag);
            // Script tags are skipped on the serve path: the server side
            // only has send_metadata, no raw AMF send.
            if tag.tag_type == FLV_TAG_SCRIPT_DATA {
              continue;
            }
            let timestamp = RtmpTimestamp::new(tag.timestamp);
            let packet = match tag.tag_type {
              FLV_TAG_VIDEO => session
                .send_video_data(stream_id, Bytes::from(tag.payload), timestamp, false)
                .map_err(|error| {
                  SessionFailure::error(format!("failed to frame video data: {error:?}"))
                }),
              FLV_TAG_AUDIO => session
                .send_audio_data(stream_id, Bytes::from(tag.payload), timestamp, false)
                .map_err(|error| {
                  SessionFailure::error(format!("failed to frame audio data: {error:?}"))
                }),
              _ => continue,
            };
            let packet = match packet {
              Ok(packet) => packet,
              Err(failure) => {
                failed = Some(failure);
                break;
              }
            };
            if let Err(failure) =
              write_packet_bytes(stream, &packet.bytes, write_timeout).await
            {
              failed = Some(failure);
              break;
            }
          }
          if let Some(failure) = failed {
            live.player_connected.store(false, Ordering::Release);
            if failure.client_disconnect {
              gst::info!(CAT_SINK, "RTMP player went away: {}", failure.message);
            } else {
              gst::warning!(CAT_SINK, "RTMP serve failed: {}", failure.message);
            }
            return PlayerOutcome::PlayerDone;
          }
        }
        Err(flume::RecvError::Disconnected) => {
          live.player_connected.store(false, Ordering::Release);
          if let Ok(packet) = session.finish_playing(stream_id) {
            let _ = write_packet_bytes(stream, &packet.bytes, write_timeout).await;
          }
          let _ = stream.shutdown().await;
          return PlayerOutcome::Eos;
        }
      },
      read_outcome = read_session_chunk(stream, &mut read_buf, cancellation, read_timeout) => {
        let n = match read_outcome {
          Ok(n) => n,
          Err(failure) => {
            live.player_connected.store(false, Ordering::Release);
            if cancellation.is_cancelled() {
              return PlayerOutcome::Shutdown;
            }
            if failure.client_disconnect {
              gst::info!(CAT_SINK, "RTMP player went away: {}", failure.message);
            } else {
              gst::warning!(CAT_SINK, "RTMP player read failed: {}", failure.message);
            }
            return PlayerOutcome::PlayerDone;
          }
        };
        if n == 0 {
          live.player_connected.store(false, Ordering::Release);
          gst::info!(CAT_SINK, "RTMP player closed the connection");
          return PlayerOutcome::PlayerDone;
        }
        let results = match session.handle_input(&read_buf[..n]) {
          Ok(results) => results,
          Err(error) => {
            live.player_connected.store(false, Ordering::Release);
            gst::warning!(CAT_SINK, "Player sent unreadable RTMP: {error:?}");
            return PlayerOutcome::PlayerDone;
          }
        };
        let mut player_over = false;
        for result in results {
          match result {
            ServerSessionResult::OutboundResponse(packet) => {
              if write_packet_bytes(stream, &packet.bytes, write_timeout).await.is_err() {
                live.player_connected.store(false, Ordering::Release);
                gst::info!(CAT_SINK, "RTMP player went away");
                return PlayerOutcome::PlayerDone;
              }
            }
            ServerSessionResult::RaisedEvent(event) => if let ServerSessionEvent::PlayStreamFinished { stream_key, .. } = event {
              gst::info!(CAT_SINK, "RTMP player stopped playing '{stream_key}'");
              player_over = true;
            },
            ServerSessionResult::UnhandleableMessageReceived(_) => {}
          }
        }
        if player_over {
          live.player_connected.store(false, Ordering::Release);
          let _ = stream.shutdown().await;
          return PlayerOutcome::PlayerDone;
        }
        // Keep the dummy channel referenced so it clearly outlives the
        // serve loop if a future rtmpx version routes replies through
        // it; today the serve path writes packets directly.
        let _ = &dummy_output;
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn detects_classic_and_enhanced_sequence_headers() {
    // Classic AVC sequence header and coded frame.
    assert!(is_video_sequence_header(&[0x17, 0x00, 0x01]));
    assert!(!is_video_sequence_header(&[0x17, 0x01, 0x01]));
    // Enhanced video: packet type 0 is sequence start (needs the FourCC).
    assert!(is_video_sequence_header(&[0x90, b'h', b'v', b'c', b'1']));
    assert!(!is_video_sequence_header(&[0x91, b'h', b'v', b'c', b'1']));
    assert!(!is_video_sequence_header(&[0x94, b'h', b'v', b'c', b'1']));
    // Classic AAC sequence header and raw frame.
    assert!(is_audio_sequence_header(&[0xAF, 0x00, 0x12]));
    assert!(!is_audio_sequence_header(&[0xAF, 0x01, 0x21]));
    // Enhanced audio: 0x90 is sequence start, 0x91/0x94 are frames.
    assert!(is_audio_sequence_header(&[0x90, b'o', b'p', b'u', b's']));
    assert!(!is_audio_sequence_header(&[0x91, 0x02]));
    assert!(!is_audio_sequence_header(&[0x94, 0x02]));
    // Truncated payloads are never headers.
    assert!(!is_video_sequence_header(&[0x17]));
    assert!(!is_audio_sequence_header(&[0xAF]));
    assert!(!is_video_sequence_header(&[]));
  }

  #[test]
  fn header_cache_keeps_latest_sequence_headers_only() {
    let mut cache = HeaderCache::default();
    cache.observe(&FlvTag {
      tag_type: FLV_TAG_VIDEO,
      timestamp: 0,
      payload: vec![0x17, 0x00],
    });
    // Coded frames must not evict the cached header.
    cache.observe(&FlvTag {
      tag_type: FLV_TAG_VIDEO,
      timestamp: 40,
      payload: vec![0x17, 0x01],
    });
    cache.observe(&FlvTag {
      tag_type: FLV_TAG_AUDIO,
      timestamp: 20,
      payload: vec![0xAF, 0x00],
    });
    // Script tags are never cached.
    cache.observe(&FlvTag {
      tag_type: FLV_TAG_SCRIPT_DATA,
      timestamp: 0,
      payload: vec![0x02, 0x00],
    });
    let ordered = cache.ordered();
    assert_eq!(ordered.len(), 2);
    assert_eq!(ordered[0].tag_type, FLV_TAG_VIDEO);
    assert_eq!(ordered[0].timestamp, 0);
    assert_eq!(ordered[1].tag_type, FLV_TAG_AUDIO);
    // A newer sequence header replaces the old one.
    cache.observe(&FlvTag {
      tag_type: FLV_TAG_VIDEO,
      timestamp: 8000,
      payload: vec![0x17, 0x00],
    });
    assert_eq!(cache.ordered()[0].timestamp, 8000);
  }
}
