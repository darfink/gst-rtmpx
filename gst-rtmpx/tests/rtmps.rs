// Layer-1 hermetic RTMPS tests: the same element-level loopback as
// listen_loopback / sink_listen, but over TLS (rtmps://) with a throwaway
// self-signed certificate. No ffmpeg, no external server.
//
// (A) rtmpxsrc in listen mode presents the certificate while rtmpxsink
// publishes to it; (B) rtmpxsink in listen mode presents the certificate
// while rtmpxsrc plays from it. Both directions assert the FLV tags
// survive the TLS roundtrip and the stream ends with EOS.
use gst::prelude::*;
use std::sync::Once;
use std::time::{Duration, Instant};

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

/// Split bytes into awkward chunks so tags straddle buffer boundaries.
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

/// A throwaway self-signed certificate for 127.0.0.1/localhost, written to
/// per-test temp files so parallel tests never share paths. Returns
/// (cert_path, key_path) as strings; the caller deletes them when done.
fn make_cert(tag: &str) -> (String, String) {
  let certified =
    rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string(), "localhost".to_string()])
      .expect("test certificate must generate");
  let dir = std::env::temp_dir();
  let file_tag = format!("rtmpx-test-{tag}-{}", std::process::id());
  let cert_path = dir.join(format!("{file_tag}-cert.pem"));
  let key_path = dir.join(format!("{file_tag}-key.pem"));
  std::fs::write(&cert_path, certified.cert.pem()).expect("cert must write");
  std::fs::write(&key_path, certified.signing_key.serialize_pem()).expect("key must write");
  (
    cert_path.to_str().unwrap().to_owned(),
    key_path.to_str().unwrap().to_owned(),
  )
}

fn drop_cert(cert: &str, key: &str) {
  std::fs::remove_file(cert).ok();
  std::fs::remove_file(key).ok();
}

/// Wait until a listen-mode element rewrites its uri with the bound port.
fn bound_port(element: &gst::Element, scheme: &str) -> u16 {
  let deadline = Instant::now() + Duration::from_secs(5);
  loop {
    let uri: String = element.property("uri");
    if !uri.contains(":0/") {
      let prefix = format!("{scheme}://127.0.0.1:");
      let after = uri
        .strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("bound listen uri keeps its host: {uri}"));
      return after
        .split('/')
        .next()
        .expect("bound listen uri keeps a port")
        .parse()
        .expect("bound listen port parses");
    }
    assert!(Instant::now() < deadline, "listener never bound");
    std::thread::sleep(Duration::from_millis(20));
  }
}

fn lookup(harness: &gst_check::Harness, name: &str) -> gst::Element {
  harness
    .element()
    .expect("harness must hold a pipeline")
    .downcast_ref::<gst::Bin>()
    .and_then(|bin| bin.by_name(name))
    .unwrap_or_else(|| panic!("harness pipeline must contain {name}"))
}

/// (A) rtmpxsrc listens on rtmps:// while rtmpxsink publishes to it.
#[test]
fn rtmps_src_listen_serves_tls_publisher() {
  init();
  let (cert, key) = make_cert("src-listen");
  let (body, expected) = canned_stream();
  let chunks = awkward_chunks(&body);

  let mut src = gst_check::Harness::new_empty();
  src.add_parse(&format!(
    "rtmpxsrc name=src mode=listen uri=rtmps://127.0.0.1:0/live/key accept-timeout=20000000000 keep-listening=false tls-cert={cert} tls-key={key}"
  ));
  src.play();
  let port = bound_port(&lookup(&src, "src"), "rtmps");

  let ca = cert.clone();
  let publisher = std::thread::spawn(move || {
    let mut sink = gst_check::Harness::new_empty();
    sink.add_parse(&format!(
      "rtmpxsink sync=false uri=rtmps://127.0.0.1:{port}/live/key tls-ca-cert={ca}"
    ));
    sink.set_src_caps_str("video/x-flv");
    sink.play();
    for (i, chunk) in chunks.into_iter().enumerate() {
      let mut buffer = gst::Buffer::from_slice(chunk);
      buffer
        .make_mut()
        .set_pts(gst::ClockTime::from_mseconds(i as u64 * 40));
      sink.push(buffer).expect("sink must accept FLV bytes");
    }
    std::thread::sleep(Duration::from_millis(500));
    sink.push_event(gst::event::Eos::new());
    sink
      .element()
      .expect("harness must hold the sink element")
      .set_state(gst::State::Null)
      .expect("sink must shut down");
  });

  let mut out = Vec::new();
  while let Some(buffer) = src.pull_until_eos().expect("pull until EOS must not fail") {
    let map = buffer.map_readable().expect("src buffer must map");
    out.extend_from_slice(&map);
  }
  publisher.join().expect("publisher thread must finish");

  assert_eq!(parse_tags(&out), expected);
  let mut saw_eos = false;
  while let Some(event) = src.try_pull_event() {
    if matches!(event.view(), gst::EventView::Eos(..)) {
      saw_eos = true;
    }
  }
  assert!(saw_eos, "src must emit EOS after the publisher disconnects");
  src
    .element()
    .expect("harness must hold the src element")
    .set_state(gst::State::Null)
    .expect("src must shut down");
  drop_cert(&cert, &key);
}

/// Walk the complete FLV tags in bytes, stopping at a truncated tail so a
/// partially received stream can be compared incrementally.
fn complete_tags(bytes: &[u8]) -> Vec<(u8, u32, Vec<u8>)> {
  let mut tags = Vec::new();
  if bytes.len() < 13 || !bytes.starts_with(FLV_MAGIC) {
    return tags;
  }
  let mut pos = 13;
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
    if end > bytes.len() {
      break;
    }
    tags.push((
      tag_type,
      timestamp,
      bytes[pos + 11..pos + 11 + size].to_vec(),
    ));
    pos = end;
  }
  tags
}

/// (B) rtmpxsink listens on rtmps:// while rtmpxsrc plays from it.
///
/// Mirrors sink_listen: live media is pushed only after the TLS player has
/// attached (bytes queued while nobody listens are stale by design and are
/// never resent), and the stream ends by shutting the sink down, which
/// drops the player connection so src-play emits EOS. Pushing EOS into a
/// listen-mode sink is a no-op for the player, so the test does not do that.
#[test]
fn rtmps_sink_listen_serves_tls_player() {
  init();
  let (cert, key) = make_cert("sink-listen");
  let (body, expected) = canned_stream();
  let chunks = awkward_chunks(&body);

  // The serve path skips script tags (the server side has no raw AMF send),
  // so the player must receive exactly the audio/video tags.
  let expected_media: Vec<(u8, u32, Vec<u8>)> = expected
    .into_iter()
    .filter(|(tag_type, _, _)| *tag_type != 18)
    .collect();

  let mut sink = gst_check::Harness::new_empty();
  sink.add_parse(&format!(
    "rtmpxsink name=sink mode=listen uri=rtmps://127.0.0.1:0/live/key sync=false tls-cert={cert} tls-key={key}"
  ));
  sink.set_src_caps_str("video/x-flv");
  sink.play();
  let port = bound_port(&lookup(&sink, "sink"), "rtmps");

  let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
  let want = expected_media.clone();
  let ca = cert.clone();
  let player = std::thread::spawn(move || {
    let mut src = gst_check::Harness::new_empty();
    src.add_parse(&format!(
      "rtmpxsrc name=src mode=play uri=rtmps://127.0.0.1:{port}/live/key tls-ca-cert={ca}"
    ));
    src.play();
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while complete_tags(&out) != want {
      assert!(
        Instant::now() < deadline,
        "timed out waiting for TLS media: got {} of {} tags ({} bytes)",
        complete_tags(&out).len(),
        want.len(),
        out.len(),
      );
      while let Some(buffer) = src.try_pull() {
        let map = buffer.map_readable().expect("src buffer must map");
        out.extend_from_slice(&map);
      }
      std::thread::sleep(Duration::from_millis(20));
    }
    done_tx.send(()).expect("test harness must be listening");
    // The main thread now shuts the sink down; the dropped player
    // connection must surface as EOS here.
    while let Some(buffer) = src.pull_until_eos().expect("pull until EOS must not fail") {
      let map = buffer.map_readable().expect("src buffer must map");
      out.extend_from_slice(&map);
    }
    let mut saw_eos = false;
    while let Some(event) = src.try_pull_event() {
      if matches!(event.view(), gst::EventView::Eos(..)) {
        saw_eos = true;
      }
    }
    src
      .element()
      .expect("harness must hold the src element")
      .set_state(gst::State::Null)
      .expect("src must shut down");
    (out, saw_eos)
  });

  // The TLS + RTMP handshake takes a moment; anything pushed before the
  // player attaches is stale by design, so wait before going live.
  std::thread::sleep(Duration::from_secs(3));
  for chunk in chunks {
    sink
      .push(gst::Buffer::from_slice(chunk))
      .expect("sink must accept FLV bytes");
  }
  done_rx
    .recv_timeout(Duration::from_secs(30))
    .expect("TLS player must receive all media tags");
  sink
    .element()
    .expect("harness must hold the sink element")
    .set_state(gst::State::Null)
    .expect("sink must shut down");

  let (out, saw_eos) = player.join().expect("player thread must finish");
  assert_eq!(parse_tags(&out), expected_media);
  assert!(saw_eos, "src must emit EOS after the sink shuts down");
  drop_cert(&cert, &key);
}
