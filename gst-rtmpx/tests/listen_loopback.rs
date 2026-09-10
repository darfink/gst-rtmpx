// Layer-1 hermetic tests: rtmpxsink publishes into rtmpxsrc (listen mode)
// over real TCP on 127.0.0.1 with an ephemeral port.
//
// No ffmpeg, no MediaMTX, no external server. The FLV bytes are canned and
// framed by a small test-side helper (independent of the plugin's own
// framing code), pushed in awkward chunks to exercise reassembly, and the
// output is parsed back into tags for comparison.
use gst::prelude::*;
use std::sync::Once;
use std::time::{Duration, Instant};

use rtmpx::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx::rml_amf0::{Amf0Object, Amf0Value};
use rtmpx::sessions::{
  ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult, PublishRequestType,
};
use std::fmt::Write as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn init() {
  static INIT: Once = Once::new();
  INIT.call_once(|| {
    gst::init().unwrap();
    gstrtmpx::plugin_register_static().expect("rtmpx static plugin registration");
  });
}

const FLV_MAGIC: &[u8] = b"FLV";

fn flv_header() -> Vec<u8> {
  vec![
    0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00,
  ]
}

fn frame_tag(tag_type: u8, timestamp: u32, payload: &[u8]) -> Vec<u8> {
  let mut tag = Vec::with_capacity(11 + payload.len() + 4);
  tag.push(tag_type);
  let len = payload.len() as u32;
  tag.extend_from_slice(&len.to_be_bytes()[1..]);
  tag.extend_from_slice(&(timestamp & 0x00FF_FFFF).to_be_bytes()[1..]);
  tag.push((timestamp >> 24) as u8);
  tag.extend_from_slice(&[0, 0, 0]);
  tag.extend_from_slice(payload);
  let total = (11 + payload.len()) as u32;
  tag.extend_from_slice(&total.to_be_bytes());
  tag
}

fn parse_tags(bytes: &[u8]) -> Vec<(u8, u32, Vec<u8>)> {
  assert!(
    bytes.starts_with(FLV_MAGIC),
    "src output must start with the FLV header"
  );
  let mut pos = 13;
  let mut tags = Vec::new();
  while pos + 11 <= bytes.len() {
    let tag_type = bytes[pos];
    let size = ((bytes[pos + 1] as usize) << 16)
      | ((bytes[pos + 2] as usize) << 8)
      | bytes[pos + 3] as usize;
    let timestamp = ((bytes[pos + 4] as u32) << 16)
      | ((bytes[pos + 5] as u32) << 8)
      | bytes[pos + 6] as u32
      | ((bytes[pos + 7] as u32) << 24);
    let end = pos + 11 + size + 4;
    assert!(end <= bytes.len(), "truncated tag in src output");
    tags.push((
      tag_type,
      timestamp,
      bytes[pos + 11..pos + 11 + size].to_vec(),
    ));
    pos = end;
  }
  assert_eq!(pos, bytes.len(), "trailing garbage after last FLV tag");
  tags
}

/// Build a small deterministic FLV body: one script tag plus interleaved
/// video/audio tags with distinct timestamps and payloads.
#[allow(clippy::type_complexity)]
fn canned_stream() -> (Vec<u8>, Vec<(u8, u32, Vec<u8>)>) {
  let mut body = flv_header();
  let mut expected = Vec::new();
  let script_payload: Vec<u8> = (0..16).collect();
  body.extend_from_slice(&frame_tag(18, 0, &script_payload));
  expected.push((18u8, 0u32, script_payload));
  for i in 0..10u8 {
    let timestamp = (i as u32 + 1) * 40;
    let video_payload = vec![0x10 + i; 32 + i as usize];
    body.extend_from_slice(&frame_tag(9, timestamp, &video_payload));
    expected.push((9u8, timestamp, video_payload));
    let audio_payload = vec![0xA0 + i; 16];
    body.extend_from_slice(&frame_tag(8, timestamp + 20, &audio_payload));
    expected.push((8u8, timestamp + 20, audio_payload));
  }
  (body, expected)
}

/// Split bytes into awkward chunks so tags straddle buffer boundaries and
/// the sink-side demuxer's reassembly path is exercised.
fn awkward_chunks(body: &[u8]) -> Vec<Vec<u8>> {
  let mut chunks = Vec::new();
  let mut pos = 0;
  for size in [5usize, 7, 100, 1000] {
    if pos >= body.len() {
      break;
    }
    let end = (pos + size).min(body.len());
    chunks.push(body[pos..end].to_vec());
    pos = end;
  }
  if pos < body.len() {
    chunks.push(body[pos..].to_vec());
  }
  chunks
}

/// Start a listen-mode src on an ephemeral port and return the harness plus
/// the port the listener actually bound (the element rewrites its uri once
/// bound, so poll for that).
fn start_listener(
  app: &str,
  key: &str,
  accept_timeout_ns: u64,
  keep_listening: bool,
) -> (gst_check::Harness, u16) {
  let mut harness = gst_check::Harness::new_empty();
  harness.add_parse(&format!(
    "rtmpxsrc name=src mode=listen uri=rtmp://127.0.0.1:0/{app}/{key} accept-timeout={accept_timeout_ns} keep-listening={keep_listening}"
  ));
  harness.play();
  let bin = harness.element().expect("harness must hold a pipeline");
  let element = bin
    .downcast_ref::<gst::Bin>()
    .and_then(|bin| bin.by_name("src"))
    .expect("harness pipeline must contain the src element");
  let deadline = Instant::now() + Duration::from_secs(5);
  loop {
    let uri: String = element.property("uri");
    if !uri.contains(":0/") {
      let after = uri
        .strip_prefix("rtmp://127.0.0.1:")
        .expect("bound listen uri keeps its host");
      let port: u16 = after
        .split('/')
        .next()
        .expect("bound listen uri keeps a port")
        .parse()
        .expect("bound listen port parses");
      return (harness, port);
    }
    assert!(Instant::now() < deadline, "listener never bound");
    std::thread::sleep(Duration::from_millis(20));
  }
}

fn publish_flv(port: u16, app: &str, key: &str, chunks: Vec<Vec<u8>>) {
  let mut harness = gst_check::Harness::new_empty();
  harness.add_parse(&format!(
    "rtmpxsink sync=false uri=rtmp://127.0.0.1:{port}/{app}/{key}"
  ));
  harness.set_src_caps_str("video/x-flv");
  harness.play();
  for (i, chunk) in chunks.into_iter().enumerate() {
    let mut buffer = gst::Buffer::from_slice(chunk);
    buffer
      .make_mut()
      .set_pts(gst::ClockTime::from_mseconds(i as u64 * 40));
    harness.push(buffer).expect("sink must accept FLV bytes");
    eprintln!("publisher: pushed chunk {i}");
  }
  // Let the media flush through TCP before tearing down: a sub-millisecond
  // connect/publish/disconnect cycle is not representative of a live feed.
  std::thread::sleep(Duration::from_millis(500));
  harness.push_event(gst::event::Eos::new());
  eprintln!("publisher: eos sent");
  harness
    .element()
    .expect("harness must hold the sink element")
    .set_state(gst::State::Null)
    .expect("sink must shut down");
  eprintln!("publisher: null done");
}

fn drain_saw_eos(harness: &mut gst_check::Harness) -> bool {
  while let Some(event) = harness.try_pull_event() {
    if matches!(event.view(), gst::EventView::Eos(..)) {
      return true;
    }
  }
  false
}

#[test]
fn listen_roundtrip_preserves_flv_tags_and_eos() {
  init();
  let (body, expected) = canned_stream();
  let chunks = awkward_chunks(&body);

  let (mut src, port) = start_listener("live", "key", 20_000_000_000, false);
  let publisher = std::thread::spawn(move || publish_flv(port, "live", "key", chunks));

  // Note: plain pull() never unblocks once the src ends the stream, so drain
  // with pull_until_eos(), which yields None exactly at EOS.
  let mut out = Vec::new();
  loop {
    match src.pull_until_eos().expect("pull until EOS must not fail") {
      Some(buffer) => {
        let map = buffer.map_readable().expect("src buffer must map");
        out.extend_from_slice(&map);
        eprintln!("listener: pulled {} bytes total", out.len());
      }
      None => {
        eprintln!("listener: EOS with {} bytes total", out.len());
        break;
      }
    }
  }
  publisher.join().expect("publisher thread must finish");

  assert_eq!(parse_tags(&out), expected);
  assert!(
    drain_saw_eos(&mut src),
    "src must emit EOS after the publisher disconnects"
  );
  src
    .element()
    .expect("harness must hold the src element")
    .set_state(gst::State::Null)
    .expect("src must shut down");
}

/// Enhanced-RTMP connect properties for the raw denial probe, mirroring the
/// plugin so the probe negotiates the same capabilities a modern peer would.
fn probe_capabilities() -> Amf0Object {
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

async fn probe_write(stream: &mut tokio::net::TcpStream, result: ClientSessionResult) {
  if let ClientSessionResult::OutboundResponse(packet) = result {
    stream
      .write_all(&packet.bytes)
      .await
      .expect("probe must write its request");
  }
}

/// Publish to a key with a raw rtmpx client and return a transcript of the
/// server session responses. Panics if the server accepts the publish; a
/// denial ends with the server hanging up (EOF).
///
/// This deliberately avoids rtmpxsink: a denied publish fails the sink's
/// startup handshake, and a failed start inside a harness aborts the whole
/// test process via a C assertion.
fn probe_publish(port: u16, key: &str) -> String {
  tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .expect("probe runtime must build")
    .block_on(async move {
      let mut stream = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(("127.0.0.1", port)),
      )
      .await
      .expect("probe connect must not hang")
      .expect("probe must connect");
      stream.set_nodelay(true).expect("probe must set nodelay");

      let mut handshake = Handshake::new(PeerType::Client);
      let outbound = handshake
        .generate_outbound_p0_and_p1()
        .expect("probe must start the handshake");
      stream
        .write_all(&outbound)
        .await
        .expect("probe must write the handshake");
      stream.flush().await.expect("probe must flush");
      let mut buf = vec![0u8; 16 * 1024];
      let carry = loop {
        let reply = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
          .await
          .expect("handshake must not hang")
          .expect("server must answer the handshake");
        assert!(reply > 0, "server closed the connection during handshake");
        match handshake
          .process_bytes(&buf[..reply])
          .expect("handshake bytes must parse")
        {
          HandshakeProcessResult::InProgress { response_bytes } => {
            if !response_bytes.is_empty() {
              stream.write_all(&response_bytes).await.unwrap();
              stream.flush().await.unwrap();
            }
          }
          HandshakeProcessResult::Completed {
            response_bytes,
            remaining_bytes,
          } => {
            if !response_bytes.is_empty() {
              stream.write_all(&response_bytes).await.unwrap();
              stream.flush().await.unwrap();
            }
            break remaining_bytes;
          }
        }
      };

      let mut config = ClientSessionConfig::new();
      config.window_ack_size = 2_500_000;
      config.chunk_size = 4096;
      config.tc_url = Some(format!("rtmp://127.0.0.1:{}/live", port));
      let (mut session, _initial) =
        ClientSession::new(config).expect("probe must create a session");
      let mut transcript = String::new();
      if !carry.is_empty() {
        let results = session
          .handle_input(&carry)
          .expect("early server bytes must parse");
        for result in results {
          writeln!(transcript, "{:?}", result).unwrap();
          probe_write(&mut stream, result).await;
        }
      }

      let connect = session
        .request_connection_with_properties("live".to_owned(), probe_capabilities())
        .expect("probe must request a connection");
      probe_write(&mut stream, connect).await;
      stream.flush().await.expect("probe must flush");
      let mut connected = transcript.contains("ConnectionRequestAccepted");
      let deadline = Instant::now() + Duration::from_secs(5);
      while !connected {
        assert!(
          Instant::now() < deadline,
          "server never accepted the connection"
        );
        let reply = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
          .await
          .expect("connect reply must not hang")
          .expect("server must answer the connect");
        assert!(reply > 0, "server closed before accepting connect");
        let results = session
          .handle_input(&buf[..reply])
          .expect("connect reply must parse");
        for result in results {
          if matches!(
            result,
            ClientSessionResult::RaisedEvent(ClientSessionEvent::ConnectionRequestAccepted { .. })
          ) {
            connected = true;
          }
          writeln!(transcript, "{:?}", result).unwrap();
          probe_write(&mut stream, result).await;
        }
      }
      assert!(connected, "probe never saw the connect accept");

      let publish = session
        .request_publishing(key.to_owned(), PublishRequestType::Live)
        .expect("probe must request publish");
      probe_write(&mut stream, publish).await;
      stream.flush().await.expect("probe must flush");
      let deadline = Instant::now() + Duration::from_secs(8);
      loop {
        assert!(
          Instant::now() < deadline,
          "server neither accepted nor denied the publish"
        );
        let reply = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
          .await
          .expect("publish reply must not hang")
          .expect("server must answer the publish");
        if reply == 0 {
          break;
        }
        let results = session
          .handle_input(&buf[..reply])
          .expect("publish reply must parse");
        for result in results {
          if matches!(
            result,
            ClientSessionResult::RaisedEvent(ClientSessionEvent::PublishRequestAccepted)
          ) {
            panic!("server accepted publish for an unexpected key");
          }
          writeln!(transcript, "{:?}", result).unwrap();
          probe_write(&mut stream, result).await;
        }
      }
      transcript
    })
}

#[test]
fn listen_ignores_unknown_stream_key() {
  init();
  let (body, expected) = canned_stream();
  let chunks = awkward_chunks(&body);

  // Keep listening so the denied attempt does not tear the listener down.
  let (mut src, port) = start_listener("live", "key", 20_000_000_000, true);
  let started = Instant::now();

  let transcript = probe_publish(port, "wrong");
  eprintln!("denial transcript: {}", transcript);
  assert!(
    transcript.contains("enied"),
    "server must explicitly deny the wrong stream key"
  );

  // No media may arrive for the denied key.
  let quiet_until = Instant::now() + Duration::from_secs(1);
  let mut denied_out = Vec::new();
  while Instant::now() < quiet_until {
    while let Some(buffer) = src.try_pull() {
      let map = buffer.map_readable().expect("src buffer must map");
      denied_out.extend_from_slice(&map);
    }
    std::thread::sleep(Duration::from_millis(20));
  }
  assert!(
    denied_out.is_empty(),
    "no media must arrive for a rejected stream key"
  );
  assert!(
    started.elapsed() < Duration::from_secs(15),
    "denial must surface promptly instead of hanging"
  );

  // The listener must still serve the configured key afterwards. Drain by
  // byte count: with keep-listening there is no EOS after the publisher
  // leaves, so pull_until_eos would block past the last tag.
  let publisher = std::thread::spawn(move || publish_flv(port, "live", "key", chunks));
  let mut out = Vec::new();
  while out.len() < body.len() {
    match src.pull_until_eos().expect("pull until EOS must not fail") {
      Some(buffer) => {
        let map = buffer.map_readable().expect("src buffer must map");
        out.extend_from_slice(&map);
      }
      None => break,
    }
  }
  publisher.join().expect("publisher thread must finish");
  assert_eq!(parse_tags(&out), expected);
  let _ = src
    .element()
    .expect("harness must hold the src element")
    .set_state(gst::State::Null);
}
