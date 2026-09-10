use std::net::{IpAddr, SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::common::{
  CAT_SRC as CAT, CREATE_POLL_INTERVAL, ClientEndpoint, FLV_TAG_AUDIO, FLV_TAG_SCRIPT_DATA,
  FLV_TAG_VIDEO, FlvTagWriter, OUTPUT_QUEUE_CAPACITY, PublishIds, RtmpStream, SessionFailure,
  WORKER_START_TIMEOUT, WorkerOutput, accept_tls_server, bracketed_host, client_handshake,
  enhanced_rtmp_capabilities, nanoseconds_timeout, new_client_session, parse_rtmp_uri,
  publish_event, read_session_chunk, require_uri, resolve_client_endpoint, resolve_tls_acceptor,
  send_output, send_publish_end, server_handshake, tcp_connect, wrap_tls_client,
  write_client_results, write_session_results,
};
use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;
use gst_base::prelude::*;
use gst_base::subclass::base_src::CreateSuccess;
use gst_base::subclass::prelude::*;
use rtmpx::sessions::{
  ClientSession, ClientSessionEvent, ClientSessionResult, ServerSession, ServerSessionConfig,
  ServerSessionEvent, ServerSessionResult,
};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

const DEFAULT_MODE: &str = "play";
const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_ACCEPT_TIMEOUT: u64 = 0;
const DEFAULT_HANDSHAKE_TIMEOUT: u64 = 10_000_000_000;
const DEFAULT_READ_TIMEOUT: u64 = 0;
const DEFAULT_WRITE_TIMEOUT: u64 = 10_000_000_000;
const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: u64 = 0;
const DEFAULT_KEEP_LISTENING: bool = false;
const DEFAULT_CONNECT_TIMEOUT: u64 = 10_000_000_000;
const DEFAULT_RECONNECT: bool = false;
const RECONNECT_DELAY: Duration = Duration::from_millis(500);
const PUBLISH_START_EVENT: &str = "rtmpx-publish-start";
const PUBLISH_END_EVENT: &str = "rtmpx-publish-end";

#[derive(Clone)]
struct Settings {
  mode: String,
  uri: Option<String>,
  tcp_nodelay: bool,
  connect_timeout: u64,
  accept_timeout: u64,
  handshake_timeout: u64,
  read_timeout: u64,
  write_timeout: u64,
  graceful_shutdown_timeout: u64,
  keep_listening: bool,
  reconnect: bool,
  tc_url: Option<String>,
  tls_cert: Option<String>,
  tls_key: Option<String>,
  tls_ca_cert: Option<String>,
}

impl Default for Settings {
  fn default() -> Self {
    Self {
      mode: DEFAULT_MODE.into(),
      uri: None,
      tcp_nodelay: DEFAULT_TCP_NODELAY,
      connect_timeout: DEFAULT_CONNECT_TIMEOUT,
      accept_timeout: DEFAULT_ACCEPT_TIMEOUT,
      handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
      read_timeout: DEFAULT_READ_TIMEOUT,
      write_timeout: DEFAULT_WRITE_TIMEOUT,
      graceful_shutdown_timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
      keep_listening: DEFAULT_KEEP_LISTENING,
      reconnect: DEFAULT_RECONNECT,
      tc_url: None,
      tls_cert: None,
      tls_key: None,
      tls_ca_cert: None,
    }
  }
}

#[derive(Clone, Debug)]
struct ListenEndpoint {
  bind_host: String,
  port: u16,
  app_filter: Option<String>,
  key_filter: Option<String>,
  tls: bool,
}

// URI-only: the uri carries host, port, and optional app/key filters. A
// missing app/key means "accept any" in listen mode.
fn resolve_listen_endpoint(settings: &Settings) -> Result<(ListenEndpoint, String), String> {
  let uri = require_uri(&settings.uri, "rtmpxsrc listen mode")?;
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

// URI-only: play needs a full uri with app and stream key.
fn resolve_play_endpoint(settings: &Settings) -> Result<ClientEndpoint, String> {
  resolve_client_endpoint(
    settings.uri.clone(),
    settings.tc_url.clone(),
    "rtmpxsrc play mode",
  )
}

#[derive(Default)]
struct State {
  receiver: Option<flume::Receiver<WorkerOutput>>,
  sender: Option<flume::Sender<WorkerOutput>>,
  cancellation: Option<CancellationToken>,
  graceful_shutdown: Option<Arc<AtomicBool>>,
  worker: Option<JoinHandle<()>>,
}

#[derive(Default)]
pub struct RtmpxSrc {
  settings: Mutex<Settings>,
  state: Mutex<State>,
  flushing: AtomicBool,
  new_stream_pending: AtomicBool,
}

#[glib::object_subclass]
impl ObjectSubclass for RtmpxSrc {
  const NAME: &'static str = "GstRtmpxSrc";
  type Type = super::RtmpxSrc;
  type ParentType = gst_base::PushSrc;
}

impl ObjectImpl for RtmpxSrc {
  fn properties() -> &'static [glib::ParamSpec] {
    static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
      vec![
        glib::ParamSpecString::builder("mode")
          .nick("Mode")
          .blurb("Source mode: play connects and plays from a server, listen waits for a publisher")
          .default_value(Some(DEFAULT_MODE))
          .mutable_ready()
          .build(),
        glib::ParamSpecString::builder("uri")
          .nick("URI")
          .blurb("RTMP URI (rtmp:// or rtmps:// for TLS). Play (default): rtmp(s)://host:port/app/key. Listen: rtmp(s)://bind-host:port[/app[/key]] (missing app/key accepts any; port 0 allocates one; rtmps listen needs tls-cert/tls-key)")
          .mutable_ready()
          .build(),
        glib::ParamSpecBoolean::builder("tcp-nodelay")
          .nick("TCP no-delay")
          .blurb("Disable Nagle's algorithm on the accepted RTMP connection")
          .default_value(DEFAULT_TCP_NODELAY)
          .mutable_ready()
          .build(),
        glib::ParamSpecUInt64::builder("connect-timeout")
          .nick("Connect timeout")
          .blurb("Play mode: nanoseconds allowed for TCP connect (0 disables the timeout)")
          .default_value(DEFAULT_CONNECT_TIMEOUT)
          .mutable_ready()
          .build(),
        glib::ParamSpecUInt64::builder("accept-timeout")
          .nick("Publisher accept timeout")
          .blurb("Listen mode: nanoseconds to wait for a publisher connection (0 waits indefinitely)")
          .default_value(DEFAULT_ACCEPT_TIMEOUT)
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
        glib::ParamSpecUInt64::builder("graceful-shutdown-timeout")
          .nick("Graceful shutdown timeout")
          .blurb("Nanoseconds to wait for an RTMP publisher to close during listener shutdown (0 closes immediately)")
          .default_value(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT)
          .mutable_ready()
          .build(),
        glib::ParamSpecBoolean::builder("keep-listening")
          .nick("Keep listening")
          .blurb("Listen mode: keep waiting for another RTMP publisher after disconnect")
          .default_value(DEFAULT_KEEP_LISTENING)
          .mutable_ready()
          .build(),
        glib::ParamSpecBoolean::builder("reconnect")
          .nick("Reconnect")
          .blurb("Play mode: reconnect and resume playback after the server disconnects")
          .default_value(DEFAULT_RECONNECT)
          .mutable_ready()
          .build(),
        glib::ParamSpecString::builder("tc-url")
          .nick("TC URL")
          .blurb("Override tcUrl in the RTMP connect (defaults to rtmp://host:port/app)")
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
          .blurb("Play mode with an rtmps:// uri: path to an extra PEM CA bundle trusted in addition to the platform store (e.g. a self-signed server certificate)")
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
        settings.mode = value
          .get::<Option<String>>()
          .expect("mode type checked upstream")
          .unwrap_or_else(|| DEFAULT_MODE.into());
      }
      "uri" => {
        settings.uri = value.get().expect("uri type checked upstream");
      }
      "tcp-nodelay" => {
        settings.tcp_nodelay = value.get().expect("tcp-nodelay type checked upstream");
      }
      "connect-timeout" => {
        settings.connect_timeout = value.get().expect("connect-timeout type checked upstream");
      }
      "accept-timeout" => {
        settings.accept_timeout = value.get().expect("accept-timeout type checked upstream");
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
      "graceful-shutdown-timeout" => {
        settings.graceful_shutdown_timeout = value
          .get()
          .expect("graceful-shutdown-timeout type checked upstream");
      }
      "keep-listening" => {
        settings.keep_listening = value.get().expect("keep-listening type checked upstream");
      }
      "reconnect" => {
        settings.reconnect = value.get().expect("reconnect type checked upstream");
      }
      "tc-url" => {
        settings.tc_url = value.get().expect("tc-url type checked upstream");
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
      "tcp-nodelay" => settings.tcp_nodelay.to_value(),
      "connect-timeout" => settings.connect_timeout.to_value(),
      "accept-timeout" => settings.accept_timeout.to_value(),
      "handshake-timeout" => settings.handshake_timeout.to_value(),
      "read-timeout" => settings.read_timeout.to_value(),
      "write-timeout" => settings.write_timeout.to_value(),
      "graceful-shutdown-timeout" => settings.graceful_shutdown_timeout.to_value(),
      "keep-listening" => settings.keep_listening.to_value(),
      "reconnect" => settings.reconnect.to_value(),
      "tc-url" => settings.tc_url.to_value(),
      "tls-cert" => settings.tls_cert.to_value(),
      "tls-key" => settings.tls_key.to_value(),
      "tls-ca-cert" => settings.tls_ca_cert.to_value(),
      _ => unimplemented!(),
    }
  }

  fn constructed(&self) {
    self.parent_constructed();

    let source = self.obj();
    source.set_live(true);
    source.set_format(gst::Format::Bytes);
    source.set_do_timestamp(false);
  }
}

impl GstObjectImpl for RtmpxSrc {}

impl ElementImpl for RtmpxSrc {
  fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
    static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
      gst::subclass::ElementMetadata::new(
        "RTMPX source",
        "Source/Network",
        "Accepts an RTMP publisher or plays from an RTMP server and outputs an FLV byte stream",
        "Elliott Linder <elliott@linder.dev>",
      )
    });

    Some(&*METADATA)
  }

  fn pad_templates() -> &'static [gst::PadTemplate] {
    static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
      let caps = gst::Caps::builder("video/x-flv").build();
      let source_template = gst::PadTemplate::new(
        "src",
        gst::PadDirection::Src,
        gst::PadPresence::Always,
        &caps,
      )
      .expect("valid source pad template");

      vec![source_template]
    });

    PAD_TEMPLATES.as_ref()
  }
}

impl RtmpxSrc {
  fn store_worker(
    &self,
    receiver: flume::Receiver<WorkerOutput>,
    sender: flume::Sender<WorkerOutput>,
    cancellation: CancellationToken,
    graceful_shutdown: Arc<AtomicBool>,
    worker: JoinHandle<()>,
  ) -> Result<(), gst::ErrorMessage> {
    {
      let mut state = self.state.lock().expect("state mutex poisoned");
      if state.worker.is_some() {
        cancellation.cancel();
        drop(state);
        let _ = worker.join();
        return Err(gst::error_msg!(
          gst::CoreError::StateChange,
          ["rtmpxsrc worker is already running"]
        ));
      }

      state.receiver = Some(receiver);
      state.sender = Some(sender);
      state.cancellation = Some(cancellation);
      state.graceful_shutdown = Some(graceful_shutdown);
      state.worker = Some(worker);
    }

    self.flushing.store(false, Ordering::Release);
    self.new_stream_pending.store(false, Ordering::Release);
    Ok(())
  }

  fn start_listen(&self, settings: Settings) -> Result<(), gst::ErrorMessage> {
    let (endpoint, uri) = resolve_listen_endpoint(&settings)
      .map_err(|message| gst::error_msg!(gst::ResourceError::Settings, ["{message}"]))?;
    let tls_acceptor = resolve_tls_acceptor(
      endpoint.tls,
      settings.tls_cert.as_deref(),
      settings.tls_key.as_deref(),
      "rtmpxsrc listen mode",
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
        gst::ResourceError::OpenRead,
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
        gst::ResourceError::OpenRead,
        ["Failed to query RTMP listener address: {error}"]
      )
    })?;
    let local_port = local_addr.port();

    let (sender, receiver) = flume::bounded(OUTPUT_QUEUE_CAPACITY);
    let cancellation = CancellationToken::new();
    let graceful_shutdown = Arc::new(AtomicBool::new(false));
    let (startup_sender, startup_receiver) = std::sync::mpsc::sync_channel(1);
    let worker_sender = sender.clone();
    let worker_cancellation = cancellation.clone();
    let worker_graceful_shutdown = graceful_shutdown.clone();
    let worker_settings = settings.clone();
    let worker_app = endpoint.app_filter.clone();
    let worker_key = endpoint.key_filter.clone();
    let worker = std::thread::Builder::new()
      .name("rtmpxsrc-worker".into())
      .spawn(move || {
        run_worker(
          listener,
          tls_acceptor,
          worker_sender,
          worker_cancellation,
          worker_graceful_shutdown,
          worker_settings,
          worker_app,
          worker_key,
          startup_sender,
        );
      })
      .map_err(|error| {
        gst::error_msg!(
          gst::ResourceError::OpenRead,
          ["Failed to spawn RTMP listener worker: {error}"]
        )
      })?;

    match startup_receiver.recv_timeout(WORKER_START_TIMEOUT) {
      Ok(Ok(())) => {}
      Ok(Err(error)) => {
        cancellation.cancel();
        let _ = worker.join();
        return Err(gst::error_msg!(gst::ResourceError::OpenRead, ["{error}"]));
      }
      Err(error) => {
        cancellation.cancel();
        let _ = worker.join();
        return Err(gst::error_msg!(
          gst::ResourceError::OpenRead,
          ["RTMP listener worker failed to start: {error}"]
        ));
      }
    }

    self.store_worker(receiver, sender, cancellation, graceful_shutdown, worker)?;

    if endpoint.port == 0 {
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
        bracketed_host(&endpoint.bind_host)
      );
      self.settings.lock().expect("settings mutex poisoned").uri = Some(resolved);
      self.obj().notify("uri");
    }

    gst::info!(
      CAT,
      imp = self,
      "Listening for one RTMP publisher on {uri} (bound {local_addr})",
    );

    Ok(())
  }

  fn start_play(&self, settings: Settings) -> Result<(), gst::ErrorMessage> {
    let endpoint = resolve_play_endpoint(&settings)
      .map_err(|message| gst::error_msg!(gst::ResourceError::Settings, ["{message}"]))?;
    let log_endpoint = endpoint.clone();

    let (sender, receiver) = flume::bounded(OUTPUT_QUEUE_CAPACITY);
    let cancellation = CancellationToken::new();
    let graceful_shutdown = Arc::new(AtomicBool::new(false));
    let (startup_sender, startup_receiver) = std::sync::mpsc::sync_channel(1);
    let worker_sender = sender.clone();
    let worker_cancellation = cancellation.clone();
    let worker_graceful_shutdown = graceful_shutdown.clone();
    let worker_settings = settings.clone();
    let worker = std::thread::Builder::new()
      .name("rtmpxsrc-play".into())
      .spawn(move || {
        run_play_worker(
          worker_sender,
          worker_cancellation,
          worker_graceful_shutdown,
          worker_settings,
          endpoint,
          startup_sender,
        );
      })
      .map_err(|error| {
        gst::error_msg!(
          gst::ResourceError::OpenRead,
          ["Failed to spawn RTMP play worker: {error}"]
        )
      })?;

    match startup_receiver.recv_timeout(WORKER_START_TIMEOUT) {
      Ok(Ok(())) => {}
      Ok(Err(error)) => {
        cancellation.cancel();
        let _ = worker.join();
        return Err(gst::error_msg!(gst::ResourceError::OpenRead, ["{error}"]));
      }
      Err(error) => {
        cancellation.cancel();
        let _ = worker.join();
        return Err(gst::error_msg!(
          gst::ResourceError::OpenRead,
          ["RTMP play worker failed to start: {error}"]
        ));
      }
    }

    self.store_worker(receiver, sender, cancellation, graceful_shutdown, worker)?;

    gst::info!(
      CAT,
      imp = self,
      "Playing RTMP stream {} from {}:{}/{}",
      log_endpoint.tc_url,
      log_endpoint.host,
      log_endpoint.port,
      log_endpoint.stream_key,
    );

    Ok(())
  }
}

impl BaseSrcImpl for RtmpxSrc {
  fn start(&self) -> Result<(), gst::ErrorMessage> {
    let settings = self
      .settings
      .lock()
      .expect("settings mutex poisoned")
      .clone();
    if settings.mode.eq_ignore_ascii_case("play") {
      return self.start_play(settings);
    }
    if !settings.mode.eq_ignore_ascii_case("listen") {
      return Err(gst::error_msg!(
        gst::ResourceError::Settings,
        [
          "Invalid rtmpxsrc mode '{}': expected listen or play",
          settings.mode
        ]
      ));
    }
    self.start_listen(settings)
  }

  fn stop(&self) -> Result<(), gst::ErrorMessage> {
    gst::info!(CAT, imp = self, "Stopping rtmpxsrc");
    self.flushing.store(true, Ordering::Release);
    self.new_stream_pending.store(false, Ordering::Release);
    let graceful_shutdown_timeout = self
      .settings
      .lock()
      .expect("settings mutex poisoned")
      .graceful_shutdown_timeout;

    let (sender, cancellation, graceful_shutdown, worker) = {
      let mut state = self.state.lock().expect("state mutex poisoned");
      let sender = state.sender.take();
      let cancellation = state.cancellation.take();
      let graceful_shutdown = state.graceful_shutdown.take();
      let worker = state.worker.take();
      state.receiver.take();
      (sender, cancellation, graceful_shutdown, worker)
    };

    if let Some(graceful_shutdown) = graceful_shutdown {
      graceful_shutdown.store(graceful_shutdown_timeout > 0, Ordering::Release);
    }
    if let Some(sender) = sender {
      let _ = sender.try_send(WorkerOutput::Wake);
    }
    if let Some(cancellation) = cancellation {
      cancellation.cancel();
    }
    if let Some(worker) = worker {
      worker.join().map_err(|_| {
        gst::error_msg!(
          gst::ResourceError::Close,
          ["RTMP listener worker panicked during shutdown"]
        )
      })?;
    }

    Ok(())
  }

  fn is_seekable(&self) -> bool {
    false
  }

  fn unlock(&self) -> Result<(), gst::ErrorMessage> {
    self.flushing.store(true, Ordering::Release);
    if let Some(sender) = self
      .state
      .lock()
      .expect("state mutex poisoned")
      .sender
      .as_ref()
    {
      let _ = sender.try_send(WorkerOutput::Wake);
    }
    Ok(())
  }

  fn unlock_stop(&self) -> Result<(), gst::ErrorMessage> {
    self.flushing.store(false, Ordering::Release);
    Ok(())
  }
}

impl PushSrcImpl for RtmpxSrc {
  fn create(&self, _buffer: Option<&mut gst::BufferRef>) -> Result<CreateSuccess, gst::FlowError> {
    let receiver = self
      .state
      .lock()
      .expect("state mutex poisoned")
      .receiver
      .clone()
      .ok_or(gst::FlowError::Flushing)?;
    let mut pending_publish_start = None;

    loop {
      if self.flushing.load(Ordering::Acquire) {
        return Err(gst::FlowError::Flushing);
      }

      match receiver.recv_timeout(CREATE_POLL_INTERVAL) {
        Ok(WorkerOutput::PublishStarted { connection_id }) => {
          pending_publish_start = Some(connection_id);
        }
        Ok(WorkerOutput::Data(data)) => {
          if self.new_stream_pending.swap(false, Ordering::AcqRel) {
            // Each reconnect (keep-listening) starts a new GStreamer stream
            // generation. The publisher lifecycle event follows the normal
            // STREAM_START/CAPS/SEGMENT sequence and precedes the first FLV
            // buffer of that generation.
            let src = self.obj();
            if let Some(pad) = src.static_pad("src") {
              let stream_id = pad.create_stream_id(&*src, Some("rtmp"));
              let group_id = gst::GroupId::next();
              let caps = gst::Caps::builder("video/x-flv").build();
              let _ = src.set_caps(&caps);
              let stream_start = gst::event::StreamStart::builder(&stream_id)
                .group_id(group_id)
                .build();
              let _ = pad.push_event(stream_start);
              let _ = pad.push_event(gst::event::Caps::new(&caps));
              let segment = gst::FormattedSegment::<gst::format::Bytes>::new();
              let _ = pad.push_event(gst::event::Segment::new(&segment));
            }
          }
          if let Some(connection_id) = pending_publish_start.take() {
            let src = self.obj();
            if let Some(pad) = src.static_pad("src") {
              let _ = pad.push_event(publish_event(PUBLISH_START_EVENT, connection_id, None));
            }
          }
          let buffer = gst::Buffer::from_mut_slice(data);
          return Ok(CreateSuccess::NewBuffer(buffer));
        }
        Ok(WorkerOutput::Warning(warning)) => {
          gst::element_imp_warning!(self, gst::ResourceError::Read, ["{warning}"]);
        }
        Ok(WorkerOutput::Eos) => return Err(gst::FlowError::Eos),
        Ok(WorkerOutput::PublishEnded {
          connection_id,
          reason,
        }) => {
          self.new_stream_pending.store(true, Ordering::Release);
          if let Some(pad) = self.obj().static_pad("src") {
            let _ = pad.push_event(publish_event(
              PUBLISH_END_EVENT,
              connection_id,
              Some(&reason),
            ));
          }
          let message = gst::message::Element::new(
            gst::Structure::builder("connection-removed")
              .field("connection-id", connection_id)
              .field("reason", reason)
              .build(),
          );
          let _ = self.obj().post_message(message);
        }
        Ok(WorkerOutput::Error(error)) => {
          gst::element_imp_error!(self, gst::ResourceError::Read, ["{error}"]);
          return Err(gst::FlowError::Error);
        }
        Ok(WorkerOutput::Wake) | Err(flume::RecvTimeoutError::Timeout) => {}
        Err(flume::RecvTimeoutError::Disconnected) => {
          return if self.flushing.load(Ordering::Acquire) {
            Err(gst::FlowError::Flushing)
          } else {
            Err(gst::FlowError::Eos)
          };
        }
      }
    }
  }
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
  listener: TcpListener,
  tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
  output: flume::Sender<WorkerOutput>,
  cancellation: CancellationToken,
  graceful_shutdown: Arc<AtomicBool>,
  settings: Settings,
  app_filter: Option<String>,
  key_filter: Option<String>,
  startup: std::sync::mpsc::SyncSender<Result<(), String>>,
) {
  let runtime = match tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
  {
    Ok(runtime) => runtime,
    Err(error) => {
      let _ = startup.send(Err(format!("Failed to create Tokio runtime: {error}")));
      return;
    }
  };

  runtime.block_on(async move {
    let listener = match tokio::net::TcpListener::from_std(listener) {
      Ok(listener) => listener,
      Err(error) => {
        let _ = startup.send(Err(format!(
          "Failed to initialize Tokio RTMP listener: {error}"
        )));
        return;
      }
    };

    if startup.send(Ok(())).is_err() {
      return;
    }

    let next_connection_id = Arc::new(AtomicU64::new(1));

    loop {
      let accept = async {
        if settings.accept_timeout == 0 {
          listener.accept().await.map_err(|error| error.to_string())
        } else {
          let timeout = Duration::from_nanos(settings.accept_timeout);
          tokio::time::timeout(timeout, listener.accept())
            .await
            .map_err(|_| format!("Timed out waiting {timeout:?} for an RTMP publisher"))?
            .map_err(|error| error.to_string())
        }
      };
      let accepted = tokio::select! {
        _ = cancellation.cancelled() => return,
        accepted = accept => accepted,
      };
      let (stream, peer_address) = match accepted {
        Ok(accepted) => accepted,
        Err(error) => {
          send_output(
            &output,
            &cancellation,
            WorkerOutput::Error(format!("Failed to accept RTMP publisher: {error}")),
          )
          .await;
          return;
        }
      };

      if let Err(error) = stream.set_nodelay(settings.tcp_nodelay) {
        send_output(
          &output,
          &cancellation,
          WorkerOutput::Error(format!("Failed to configure TCP_NODELAY: {error}")),
        )
        .await;
        return;
      }

      let stream = if let Some(acceptor) = tls_acceptor.as_ref() {
        let handshake_timeout = nanoseconds_timeout(settings.handshake_timeout);
        match accept_tls_server(acceptor, stream, &handshake_timeout).await {
          Ok(tls) => tls,
          Err(error) => {
            gst::warning!(
              CAT,
              "RTMPS publisher {peer_address}: {error}; continuing to listen"
            );
            send_output(
              &output,
              &cancellation,
              WorkerOutput::Warning(format!("RTMPS publisher {peer_address}: {error}")),
            )
            .await;
            continue;
          }
        }
      } else {
        RtmpStream::Plain(stream)
      };

      gst::info!(
        CAT,
        "Accepted RTMP publisher connection from {peer_address}"
      );

      let rejected = Arc::new(AtomicBool::new(false));
      let publish_ids = PublishIds {
        next: next_connection_id.clone(),
        current: Arc::new(AtomicU64::new(0)),
      };
      let connection_id = publish_ids.current.clone();
      let result = serve_publisher(
        stream,
        &output,
        &cancellation,
        &graceful_shutdown,
        &settings,
        app_filter.clone(),
        key_filter.clone(),
        rejected.clone(),
        publish_ids,
      )
      .await;

      if settings.keep_listening && rejected.load(Ordering::Acquire) {
        gst::info!(CAT, "Rejected RTMP publisher; continuing to listen");
        continue;
      }

      let connection_id = connection_id.load(Ordering::Acquire);
      match result {
        Ok(true) => {
          gst::info!(CAT, "RTMP publisher completed cleanly");
          if connection_id != 0
            && !send_publish_end(&output, &cancellation, connection_id, "unpublished").await
          {
            return;
          }
          if settings.keep_listening {
            continue;
          }
          send_output(&output, &cancellation, WorkerOutput::Eos).await;
          return;
        }
        Ok(false) => {
          if connection_id != 0
            && !send_publish_end(&output, &cancellation, connection_id, "disconnect").await
          {
            return;
          }
          if !send_output(
            &output,
            &cancellation,
            WorkerOutput::Warning("RTMP publisher disconnected without unpublishing".into()),
          )
          .await
          {
            return;
          }
          if settings.keep_listening {
            continue;
          }
          send_output(&output, &cancellation, WorkerOutput::Eos).await;
          return;
        }
        Err(error) => {
          let client_disconnect = error.client_disconnect;
          if connection_id != 0
            && !send_publish_end(
              &output,
              &cancellation,
              connection_id,
              if client_disconnect {
                "disconnect"
              } else {
                "error"
              },
            )
            .await
          {
            return;
          }
          if settings.keep_listening && client_disconnect {
            gst::warning!(
              CAT,
              "RTMP publisher disconnected abruptly: {}",
              error.message
            );
            continue;
          }
          gst::warning!(CAT, "RTMP session failed: {}", error.message);
          send_output(
            &output,
            &cancellation,
            WorkerOutput::Error(format!("RTMP session failed: {}", error.message)),
          )
          .await;
          return;
        }
      }
    }
  });
}

/// Play-client state for the sans-I/O rtmpx ClientSession.
///
/// Mirrors PublishSession on the listen path: the worker owns the socket,
/// runs the client handshake, feeds input bytes, writes outbound responses in
/// order, and turns raised events into FLV tags.
struct PlaySession {
  output: flume::Sender<WorkerOutput>,
  cancellation: CancellationToken,
  writer: FlvTagWriter,
  stream_key: String,
  next_connection_id: Arc<AtomicU64>,
  connection_id: Arc<AtomicU64>,
}

impl PlaySession {
  fn new(
    output: flume::Sender<WorkerOutput>,
    cancellation: CancellationToken,
    stream_key: String,
    publish_ids: PublishIds,
  ) -> Self {
    Self {
      writer: FlvTagWriter::new(output.clone(), cancellation.clone()),
      output,
      cancellation,
      stream_key,
      next_connection_id: publish_ids.next,
      connection_id: publish_ids.current,
    }
  }

  async fn ensure_header(&mut self) -> bool {
    self.writer.ensure_header().await
  }

  async fn on_media(
    &mut self,
    tag_type: u8,
    timestamp: u32,
    payload: &[u8],
  ) -> Result<(), SessionFailure> {
    self.writer.push_media(tag_type, timestamp, payload).await
  }
}

/// Connect to an RTMP server and play one stream to the end.
///
/// Returns Ok(true) when the server cleanly ended playback (Play.Stop and
/// friends), Ok(false) when the socket closed without an ending, and Err on
/// protocol/socket failure.
async fn connect_and_play(
  endpoint: &ClientEndpoint,
  output: &flume::Sender<WorkerOutput>,
  cancellation: &CancellationToken,
  graceful_shutdown: &Arc<AtomicBool>,
  settings: &Settings,
  publish_ids: PublishIds,
) -> Result<bool, SessionFailure> {
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
    CAT,
    "Connected to RTMP server at {}:{}",
    endpoint.host,
    endpoint.port
  );

  let carry = client_handshake(&mut stream, cancellation, &handshake_timeout).await?;
  let mut session = new_client_session(endpoint.tc_url.clone())?;
  let mut play = PlaySession::new(
    output.clone(),
    cancellation.clone(),
    endpoint.stream_key.clone(),
    publish_ids,
  );
  let connect = session
    .request_connection_with_properties(endpoint.app.clone(), enhanced_rtmp_capabilities())
    .map_err(|error| {
      SessionFailure::error(format!("failed to request RTMP connection: {error:?}"))
    })?;
  write_client_results(
    &mut stream,
    vec![connect],
    output,
    cancellation,
    &write_timeout,
  )
  .await?;
  if !carry.is_empty() {
    let results = session
      .handle_input(&carry)
      .map_err(|error| SessionFailure::error(format!("server sent unreadable RTMP: {error:?}")))?;
    if !process_client_results(
      &mut session,
      &mut stream,
      results,
      &mut play,
      output,
      cancellation,
      &write_timeout,
    )
    .await?
    {
      return Ok(true);
    }
  }
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    let n = match read_session_chunk(&mut stream, &mut read_buf, cancellation, &read_timeout).await
    {
      Ok(n) => n,
      Err(failure) => {
        if cancellation.is_cancelled() && graceful_shutdown.load(Ordering::Acquire) {
          let _ = stream.shutdown().await;
        }
        return Err(failure);
      }
    };
    if n == 0 {
      return Ok(false);
    }
    let results = session
      .handle_input(&read_buf[..n])
      .map_err(|error| SessionFailure::error(format!("server sent unreadable RTMP: {error:?}")))?;
    if !process_client_results(
      &mut session,
      &mut stream,
      results,
      &mut play,
      output,
      cancellation,
      &write_timeout,
    )
    .await?
    {
      return Ok(true);
    }
  }
}

/// Handle one batch of client session results: write outbound packets and
/// react to raised events. Returns Ok(true) to keep reading, Ok(false) when
/// the server cleanly ended playback.
async fn process_client_results(
  session: &mut ClientSession,
  stream: &mut RtmpStream,
  results: Vec<ClientSessionResult>,
  play: &mut PlaySession,
  output: &flume::Sender<WorkerOutput>,
  cancellation: &CancellationToken,
  write_timeout: &Option<Duration>,
) -> Result<bool, SessionFailure> {
  // rtmpx emits [WindowAck, ConnectionAccepted event, SetChunkSize] for a
  // connect accept. Our createStream/play request must go after the whole
  // batch (especially SetChunkSize) to keep the session's outbound order.
  // Sending play early reorders SetChunkSize behind createStream.
  let mut play_requested = false;
  for result in results {
    match result {
      ClientSessionResult::OutboundResponse(packet) => {
        write_client_results(
          stream,
          vec![ClientSessionResult::OutboundResponse(packet)],
          output,
          cancellation,
          write_timeout,
        )
        .await?;
      }
      ClientSessionResult::RaisedEvent(event) => match event {
        ClientSessionEvent::ConnectionRequestAccepted { .. } => {
          gst::info!(CAT, "RTMP server accepted connection");
          play_requested = true;
        }
        ClientSessionEvent::ConnectionRequestRejected { description } => {
          return Err(SessionFailure::error(format!(
            "RTMP server rejected connection: {description}"
          )));
        }
        ClientSessionEvent::PlaybackRequestAccepted => {
          let connection_id = play.next_connection_id.fetch_add(1, Ordering::Relaxed);
          play.connection_id.store(connection_id, Ordering::Release);
          gst::info!(CAT, "RTMP server accepted playback");
          if !send_output(
            &play.output,
            &play.cancellation,
            WorkerOutput::PublishStarted { connection_id },
          )
          .await
          {
            return Err(SessionFailure::error("listener is shutting down"));
          }
          if !play.ensure_header().await {
            return Err(SessionFailure::error("listener is shutting down"));
          }
        }
        ClientSessionEvent::VideoDataReceived { data, timestamp } => {
          play.on_media(FLV_TAG_VIDEO, timestamp.value, &data).await?;
        }
        ClientSessionEvent::AudioDataReceived { data, timestamp } => {
          play.on_media(FLV_TAG_AUDIO, timestamp.value, &data).await?;
        }
        ClientSessionEvent::StreamMetadataReceived {
          raw_payload,
          timestamp,
          ..
        } => {
          play
            .on_media(FLV_TAG_SCRIPT_DATA, timestamp.value, &raw_payload)
            .await?;
        }
        ClientSessionEvent::UnhandleableOnStatusCode { code } => {
          if code == "NetStream.Play.Stop"
            || code == "NetStream.Play.UnpublishNotify"
            || code == "NetStream.Play.Complete"
          {
            gst::info!(CAT, "RTMP server ended playback ({code})");
            return Ok(false);
          }
          gst::debug!(CAT, "Ignoring RTMP onStatus {code}");
        }
        _ => {}
      },
      ClientSessionResult::UnhandleableMessageReceived(_) => {}
    }
  }
  if play_requested {
    let request = session
      .request_playback(play.stream_key.clone())
      .map_err(|error| {
        SessionFailure::error(format!("failed to request RTMP playback: {error:?}"))
      })?;
    write_client_results(stream, vec![request], output, cancellation, write_timeout).await?;
  }
  Ok(true)
}

fn run_play_worker(
  output: flume::Sender<WorkerOutput>,
  cancellation: CancellationToken,
  graceful_shutdown: Arc<AtomicBool>,
  settings: Settings,
  endpoint: ClientEndpoint,
  startup: std::sync::mpsc::SyncSender<Result<(), String>>,
) {
  let runtime = match tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
  {
    Ok(runtime) => runtime,
    Err(error) => {
      let _ = startup.send(Err(format!("Failed to create Tokio runtime: {error}")));
      return;
    }
  };

  // Signal readiness immediately: the TCP connect and play handshake happen
  // asynchronously and surface through the usual output channel.
  if startup.send(Ok(())).is_err() {
    return;
  }

  let next_connection_id = Arc::new(AtomicU64::new(1));
  runtime.block_on(async move {
    loop {
      let publish_ids = PublishIds {
        next: next_connection_id.clone(),
        current: Arc::new(AtomicU64::new(0)),
      };
      let connection_id = publish_ids.current.clone();
      let result = connect_and_play(
        &endpoint,
        &output,
        &cancellation,
        &graceful_shutdown,
        &settings,
        publish_ids,
      )
      .await;

      let connection_id = connection_id.load(Ordering::Acquire);
      match result {
        Ok(true) => {
          gst::info!(CAT, "RTMP playback ended cleanly");
          if connection_id != 0
            && !send_publish_end(&output, &cancellation, connection_id, "unpublished").await
          {
            return;
          }
          if settings.reconnect {
            if !wait_reconnect(&cancellation).await {
              return;
            }
            continue;
          }
          send_output(&output, &cancellation, WorkerOutput::Eos).await;
          return;
        }
        Ok(false) => {
          if connection_id != 0
            && !send_publish_end(&output, &cancellation, connection_id, "disconnect").await
          {
            return;
          }
          if !send_output(
            &output,
            &cancellation,
            WorkerOutput::Warning("RTMP server closed the connection".into()),
          )
          .await
          {
            return;
          }
          if settings.reconnect {
            if !wait_reconnect(&cancellation).await {
              return;
            }
            continue;
          }
          send_output(&output, &cancellation, WorkerOutput::Eos).await;
          return;
        }
        Err(error) => {
          let client_disconnect = error.client_disconnect;
          if connection_id != 0
            && !send_publish_end(
              &output,
              &cancellation,
              connection_id,
              if client_disconnect {
                "disconnect"
              } else {
                "error"
              },
            )
            .await
          {
            return;
          }
          if settings.reconnect && client_disconnect {
            gst::warning!(CAT, "RTMP playback interrupted: {}", error.message);
            if !wait_reconnect(&cancellation).await {
              return;
            }
            continue;
          }
          gst::warning!(CAT, "RTMP play session failed: {}", error.message);
          send_output(
            &output,
            &cancellation,
            WorkerOutput::Error(format!("RTMP play session failed: {}", error.message)),
          )
          .await;
          return;
        }
      }
    }
  });
}

async fn wait_reconnect(cancellation: &CancellationToken) -> bool {
  tokio::select! {
    _ = cancellation.cancelled() => false,
    _ = tokio::time::sleep(RECONNECT_DELAY) => true,
  }
}

/// Per-connection publish state for the sans-I/O `rtmpx` session.
///
/// `rtmpx` owns no sockets: the worker drives handshake bytes, feeds input
/// chunks, writes outbound responses, and reacts to raised events here.
struct PublishSession {
  output: flume::Sender<WorkerOutput>,
  cancellation: CancellationToken,
  writer: FlvTagWriter,
  application: Option<String>,
  stream_key: Option<String>,
  keep_listening: bool,
  write_timeout: Option<Duration>,
  rejected: Arc<AtomicBool>,
  next_connection_id: Arc<AtomicU64>,
  connection_id: Arc<AtomicU64>,
}

impl PublishSession {
  #[allow(clippy::too_many_arguments)]
  fn new(
    output: flume::Sender<WorkerOutput>,
    cancellation: CancellationToken,
    application: Option<String>,
    stream_key: Option<String>,
    keep_listening: bool,
    write_timeout: Option<Duration>,
    rejected: Arc<AtomicBool>,
    publish_ids: PublishIds,
  ) -> Self {
    Self {
      writer: FlvTagWriter::new(output.clone(), cancellation.clone()),
      output,
      cancellation,
      application,
      stream_key,
      keep_listening,
      write_timeout,
      rejected,
      next_connection_id: publish_ids.next,
      connection_id: publish_ids.current,
    }
  }

  async fn ensure_header(&mut self) -> bool {
    self.writer.ensure_header().await
  }

  /// Check a publish request against the configured application/stream-key
  /// filter. Returns true when the publisher may proceed.
  async fn check_publish(
    &mut self,
    stream_id: u32,
    app_name: &str,
    stream_name: &str,
    request_id: u32,
    session: &mut ServerSession,
    stream: &mut RtmpStream,
  ) -> Result<bool, SessionFailure> {
    let app_matches = self
      .application
      .as_deref()
      .is_none_or(|expected| expected == app_name);
    let key_matches = self
      .stream_key
      .as_deref()
      .is_none_or(|expected| expected == stream_name);
    if app_matches && key_matches {
      let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
      self.connection_id.store(connection_id, Ordering::Release);
      gst::info!(
        CAT,
        "Accepted RTMP publish for app '{app_name}', stream id {stream_id}"
      );
      if !send_output(
        &self.output,
        &self.cancellation,
        WorkerOutput::PublishStarted { connection_id },
      )
      .await
      {
        return Err(SessionFailure::error("listener is shutting down"));
      }
      if !self.ensure_header().await {
        return Err(SessionFailure::error("listener is shutting down"));
      }
      let follow_up = session.accept_request(request_id).map_err(|error| {
        SessionFailure::error(format!("failed to accept RTMP publish: {error:?}"))
      })?;
      write_session_results(
        stream,
        follow_up,
        &self.output,
        &self.cancellation,
        &self.write_timeout,
      )
      .await
      .map_err(|failure| SessionFailure::error(failure.message))?;
      return Ok(true);
    }
    gst::warning!(CAT, "Rejected RTMP publish for stream id {stream_id}");
    if self.keep_listening {
      self.rejected.store(true, Ordering::Release);
      let packet = session
        .reject_request(
          request_id,
          "NetStream.Publish.Denied",
          "publish denied: application or stream key mismatch",
        )
        .map_err(|error| {
          SessionFailure::error(format!("failed to reject RTMP publish: {error:?}"))
        })?;
      let _ = write_session_results(
        stream,
        packet,
        &self.output,
        &self.cancellation,
        &self.write_timeout,
      )
      .await;
      return Ok(false);
    }
    let packet = session
      .reject_request(
        request_id,
        "NetStream.Publish.Denied",
        "publish denied: application or stream key mismatch",
      )
      .map_err(|error| {
        SessionFailure::error(format!("failed to reject RTMP publish: {error:?}"))
      })?;
    let _ = write_session_results(
      stream,
      packet,
      &self.output,
      &self.cancellation,
      &self.write_timeout,
    )
    .await;
    Err(SessionFailure::error(
      "RTMP publisher did not match the configured application and stream key",
    ))
  }

  async fn on_media(
    &mut self,
    tag_type: u8,
    timestamp: u32,
    payload: &[u8],
  ) -> Result<(), SessionFailure> {
    self.writer.push_media(tag_type, timestamp, payload).await
  }
}

/// Drive one accepted publisher with the sans-I/O `rtmpx` session.
///
/// Returns `Ok(true)` on clean unpublish, `Ok(false)` when the socket closes
/// without unpublishing, and `Err` on protocol/socket failure. A publish
/// rejected by the application/stream-key filter sets `rejected` so the worker
/// keeps listening instead of failing (the result is then ignored).
#[allow(clippy::too_many_arguments)]
async fn serve_publisher(
  stream: RtmpStream,
  output: &flume::Sender<WorkerOutput>,
  cancellation: &CancellationToken,
  graceful_shutdown: &Arc<AtomicBool>,
  settings: &Settings,
  app_filter: Option<String>,
  key_filter: Option<String>,
  rejected: Arc<AtomicBool>,
  publish_ids: PublishIds,
) -> Result<bool, SessionFailure> {
  let mut stream = stream;
  let handshake_timeout = nanoseconds_timeout(settings.handshake_timeout);
  let read_timeout = nanoseconds_timeout(settings.read_timeout);
  let write_timeout = nanoseconds_timeout(settings.write_timeout);
  let carry = server_handshake(&mut stream, cancellation, &handshake_timeout).await?;
  let mut config = ServerSessionConfig::new();
  config.window_ack_size = 2_500_000;
  config.chunk_size = 4096;
  let (mut session, initial) = ServerSession::new(config)
    .map_err(|error| SessionFailure::error(format!("failed to create RTMP session: {error:?}")))?;
  debug_assert!(initial.is_empty(), "server must not write before connect");
  let mut publish = PublishSession::new(
    output.clone(),
    cancellation.clone(),
    app_filter,
    key_filter,
    settings.keep_listening,
    write_timeout,
    rejected,
    publish_ids,
  );
  if !carry.is_empty() {
    let results = session.handle_input(&carry).map_err(|error| {
      SessionFailure::error(format!("publisher sent unreadable RTMP: {error:?}"))
    })?;
    if !process_session_results(
      &mut session,
      &mut stream,
      results,
      &mut publish,
      output,
      cancellation,
      &write_timeout,
    )
    .await?
    {
      return Ok(true);
    }
  }
  let mut read_buf = vec![0u8; 16 * 1024];
  loop {
    let n = match read_session_chunk(&mut stream, &mut read_buf, cancellation, &read_timeout).await
    {
      Ok(n) => n,
      Err(failure) => {
        if cancellation.is_cancelled() && graceful_shutdown.load(Ordering::Acquire) {
          let _ = stream.shutdown().await;
        }
        return Err(failure);
      }
    };
    if n == 0 {
      return Ok(false);
    }
    let results = session.handle_input(&read_buf[..n]).map_err(|error| {
      SessionFailure::error(format!("publisher sent unreadable RTMP: {error:?}"))
    })?;
    if !process_session_results(
      &mut session,
      &mut stream,
      results,
      &mut publish,
      output,
      cancellation,
      &write_timeout,
    )
    .await?
    {
      return Ok(true);
    }
  }
}

/// Handle one batch of sans-I/O session results: write outbound packets and
/// react to raised events. Returns `Ok(true)` to keep reading, `Ok(false)`
/// when the publisher cleanly finished (unpublished).
async fn process_session_results(
  session: &mut ServerSession,
  stream: &mut RtmpStream,
  results: Vec<ServerSessionResult>,
  publish: &mut PublishSession,
  output: &flume::Sender<WorkerOutput>,
  cancellation: &CancellationToken,
  write_timeout: &Option<Duration>,
) -> Result<bool, SessionFailure> {
  for result in results {
    match result {
      ServerSessionResult::OutboundResponse(packet) => {
        write_session_results(
          stream,
          vec![ServerSessionResult::OutboundResponse(packet)],
          output,
          cancellation,
          write_timeout,
        )
        .await?;
      }
      ServerSessionResult::RaisedEvent(event) => match event {
        ServerSessionEvent::ConnectionRequested {
          request_id,
          app_name,
          ..
        } => {
          gst::info!(CAT, "Accepted RTMP connection for app '{app_name}'");
          let follow_up = session
            .accept_request_with_properties(request_id, enhanced_rtmp_capabilities())
            .map_err(|error| {
              SessionFailure::error(format!("failed to accept RTMP connection: {error:?}"))
            })?;
          write_session_results(stream, follow_up, output, cancellation, write_timeout).await?;
        }
        ServerSessionEvent::PublishStreamRequested {
          request_id,
          app_name,
          stream_key,
          stream_id,
          ..
        } => {
          match publish
            .check_publish(
              stream_id,
              &app_name,
              &stream_key,
              request_id,
              session,
              stream,
            )
            .await
          {
            Ok(true) => {}
            Ok(false) => return Err(SessionFailure::error("publisher rejected")),
            Err(failure) => return Err(failure),
          }
        }
        ServerSessionEvent::AudioDataReceived {
          data, timestamp, ..
        } => {
          publish
            .on_media(FLV_TAG_AUDIO, timestamp.value, &data)
            .await?;
        }
        ServerSessionEvent::VideoDataReceived {
          data, timestamp, ..
        } => {
          publish
            .on_media(FLV_TAG_VIDEO, timestamp.value, &data)
            .await?;
        }
        ServerSessionEvent::StreamMetadataChanged {
          raw_payload,
          timestamp,
          ..
        } => {
          publish
            .on_media(FLV_TAG_SCRIPT_DATA, timestamp.value, &raw_payload)
            .await?;
        }
        ServerSessionEvent::StreamDataReceived {
          raw_payload,
          timestamp,
          ..
        } => {
          publish
            .on_media(FLV_TAG_SCRIPT_DATA, timestamp.value, &raw_payload)
            .await?;
        }
        ServerSessionEvent::PublishStreamFinished { stream_key, .. } => {
          gst::info!(CAT, "RTMP stream '{stream_key}' unpublished");
          return Ok(false);
        }
        ServerSessionEvent::PlayStreamRequested {
          request_id,
          app_name,
          ..
        } => {
          gst::warning!(
            CAT,
            "Rejecting RTMP play request for app '{app_name}': listener only accepts publishers"
          );
          let packet = session
            .reject_request(
              request_id,
              "NetStream.Play.Failed",
              "this endpoint only accepts publishers",
            )
            .map_err(|error| {
              SessionFailure::error(format!("failed to reject RTMP play: {error:?}"))
            })?;
          write_session_results(stream, packet, output, cancellation, write_timeout).await?;
          if publish.keep_listening {
            publish.rejected.store(true, Ordering::Release);
            return Err(SessionFailure::disconnect("RTMP play request rejected"));
          }
          return Err(SessionFailure::error(
            "RTMP play requests are not supported",
          ));
        }
        _ => {}
      },
      ServerSessionResult::UnhandleableMessageReceived(_) => {}
    }
  }
  Ok(true)
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Instant;

  // Regression test for the old stuck-handshake defect: a client that
  // connects and vanishes before sending anything must fail the handshake
  // as a disconnect right away, never spin past the timeout.
  #[tokio::test]
  async fn handshake_returns_disconnect_when_client_goes_away_first() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
      .await
      .expect("bind must work");
    let address = listener
      .local_addr()
      .expect("listener must have an address");
    let client = tokio::net::TcpStream::connect(address)
      .await
      .expect("connect must work");
    let (server, _) = listener.accept().await.expect("accept must work");
    let mut server = crate::common::RtmpStream::Plain(server);
    drop(client);

    let cancellation = CancellationToken::new();
    let timeout = Some(Duration::from_secs(10));
    let start = Instant::now();
    let failure = server_handshake(&mut server, &cancellation, &timeout)
      .await
      .expect_err("vanished client must fail the handshake");
    assert!(
      failure.client_disconnect,
      "vanished client must count as a disconnect: {}",
      failure.message
    );
    assert!(
      start.elapsed() < Duration::from_secs(5),
      "EOF during handshake must return well before the timeout"
    );
  }
}
