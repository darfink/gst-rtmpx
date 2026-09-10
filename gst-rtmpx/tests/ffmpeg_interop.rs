// Layer-2 ffmpeg interop tests: a real ffmpeg publishes RTMP into
// rtmpxsrc (listen mode) over TCP loopback.
//
// Every test gates on ffmpeg (plus the encoders it needs) and returns early
// with a SKIP message when it is absent, so the hermetic suite stays green
// on machines without ffmpeg. ffmpeg always publishes a finite -t stream, so
// it exits on its own and the src must end the stream with EOS; the child is
// still reaped with a bounded wait and killed on timeout, and any nonzero
// exit fails the test with ffmpeg's stderr attached.
use gst::prelude::*;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::Once;
use std::time::{Duration, Instant};

fn init() {
  static INIT: Once = Once::new();
  INIT.call_once(|| {
    gst::init().unwrap();
    gstrtmpx::plugin_register_static().expect("rtmpx static plugin registration");
  });
}

fn ffmpeg_available() -> bool {
  Command::new("ffmpeg")
    .arg("-hide_banner")
    .arg("-version")
    .output()
    .map(|output| output.status.success())
    .unwrap_or(false)
}

fn ffmpeg_has_encoder(needle: &str) -> bool {
  Command::new("ffmpeg")
    .arg("-hide_banner")
    .arg("-encoders")
    .output()
    .map(|output| {
      output.status.success() && String::from_utf8_lossy(&output.stdout).contains(needle)
    })
    .unwrap_or(false)
}

// True when the test may proceed; prints a SKIP note and returns false when
// ffmpeg or any of the named encoders is missing (a passing skip).
fn require_ffmpeg(encoders: &[&str]) -> bool {
  if !ffmpeg_available() {
    eprintln!("SKIP: ffmpeg not found in PATH, skipping ffmpeg interop test");
    return false;
  }
  for encoder in encoders {
    if !ffmpeg_has_encoder(encoder) {
      eprintln!("SKIP: ffmpeg lacks encoder {encoder}, skipping ffmpeg interop test");
      return false;
    }
  }
  true
}

fn parse_tags(bytes: &[u8]) -> Vec<(u8, u32, Vec<u8>)> {
  assert!(
    bytes.starts_with(b"FLV"),
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

// Start a listen-mode src on an ephemeral port and return the harness plus
// the port the listener actually bound (the element rewrites its uri once
// bound, so poll for that). Self-contained duplicate of the layer-1 helper.
fn start_listener(app: &str, key: &str) -> (gst_check::Harness, u16) {
  let mut harness = gst_check::Harness::new_empty();
  harness.add_parse(&format!(
    "rtmpxsrc name=src mode=listen uri=rtmp://127.0.0.1:0/{app}/{key} accept-timeout=20000000000"
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

fn drain_saw_eos(harness: &mut gst_check::Harness) -> bool {
  while let Some(event) = harness.try_pull_event() {
    if matches!(event.view(), gst::EventView::Eos(..)) {
      return true;
    }
  }
  false
}

// Spawn ffmpeg publishing a finite synthetic stream (testsrc video plus sine
// audio) to the listener. stderr is piped and drained on a helper thread so
// a chatty ffmpeg can never block on a full pipe.
fn spawn_ffmpeg(
  port: u16,
  key: &str,
  stream_args: &[&str],
  duration_secs: u32,
) -> (Child, std::thread::JoinHandle<String>) {
  let mut args = vec![
    "-hide_banner".to_owned(),
    "-loglevel".to_owned(),
    "warning".to_owned(),
    "-f".to_owned(),
    "lavfi".to_owned(),
    "-i".to_owned(),
    "testsrc=size=320x240:rate=15".to_owned(),
    "-f".to_owned(),
    "lavfi".to_owned(),
    "-i".to_owned(),
    "sine=frequency=440:sample_rate=48000".to_owned(),
    "-t".to_owned(),
    duration_secs.to_string(),
  ];
  for arg in stream_args {
    args.push(arg.to_string());
  }
  args.extend([
    "-f".to_owned(),
    "flv".to_owned(),
    format!("rtmp://127.0.0.1:{port}/live/{key}"),
  ]);
  eprintln!("ffmpeg: {}", args.join(" "));
  let mut child = Command::new("ffmpeg")
    .args(&args)
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .spawn()
    .expect("ffmpeg must spawn (it passed the availability gate)");
  let mut stderr = child.stderr.take().expect("ffmpeg stderr must be piped");
  let drain = std::thread::spawn(move || {
    let mut text = String::new();
    let _ = stderr.read_to_string(&mut text);
    text
  });
  (child, drain)
}

// Reap ffmpeg with a bounded wait; kill it if it outlives the deadline.
// Returns (exit success, detail text with stderr attached).
fn wait_ffmpeg(child: &mut Child, drain: std::thread::JoinHandle<String>) -> (bool, String) {
  const WAIT: Duration = Duration::from_secs(90);
  let deadline = Instant::now() + WAIT;
  loop {
    match child.try_wait().expect("ffmpeg wait must not fail") {
      Some(status) => {
        let stderr = drain.join().unwrap_or_default();
        return (
          status.success(),
          format!("{status} detail follows: \n{stderr}"),
        );
      }
      None => {
        if Instant::now() >= deadline {
          eprintln!("ffmpeg did not exit within 90s; killing it");
          let _ = child.kill();
          let _ = child.wait();
          let stderr = drain.join().unwrap_or_default();
          return (false, format!("killed after 90s: \n{stderr}"));
        }
        std::thread::sleep(Duration::from_millis(100));
      }
    }
  }
}

// Pull the whole stream until the src ends it, then reap ffmpeg and check it
// exited cleanly. Returns the parsed FLV tags.
fn pull_stream_until_ffmpeg_eos(
  src: &mut gst_check::Harness,
  child: &mut Child,
  drain: std::thread::JoinHandle<String>,
  what: &str,
) -> Vec<(u8, u32, Vec<u8>)> {
  // Note: plain pull() never unblocks once the src ends the stream, so drain
  // with pull_until_eos(), which yields None exactly at EOS. This terminates
  // because ffmpeg publishes a finite -t stream and then disconnects, and the
  // listener has a finite accept timeout for the no-publisher path.
  let mut out = Vec::new();
  loop {
    match src.pull_until_eos() {
      Ok(Some(buffer)) => {
        let map = buffer.map_readable().expect("src buffer must map");
        out.extend_from_slice(&map);
        eprintln!("{what}: pulled {} bytes total", out.len());
      }
      Ok(None) => {
        eprintln!("{what}: EOS with {} bytes total", out.len());
        break;
      }
      Err(error) => {
        let (_, detail) = wait_ffmpeg(child, drain);
        panic!("{what}: pull failed ({error:?}); ffmpeg detail follows: \n{detail}");
      }
    }
  }
  let (success, detail) = wait_ffmpeg(child, drain);
  assert!(
    success,
    "{what}: ffmpeg must exit cleanly; detail follows: \n{detail}"
  );
  assert!(
    drain_saw_eos(src),
    "{what}: src must emit EOS after ffmpeg disconnects"
  );
  let tags = parse_tags(&out);
  src
    .element()
    .expect("harness must hold the src element")
    .set_state(gst::State::Null)
    .expect("src must shut down");
  tags
}

fn tag_kinds(tags: &[(u8, u32, Vec<u8>)], kind: u8) -> Vec<&Vec<u8>> {
  tags
    .iter()
    .filter(|tag| tag.0 == kind)
    .map(|tag| &tag.2)
    .collect()
}

#[test]
fn ffmpeg_publish_baseline_h264_aac() {
  init();
  if !require_ffmpeg(&["libx264", "aac"]) {
    return;
  }
  let (mut src, port) = start_listener("live", "ffbase");
  let (mut child, drain) = spawn_ffmpeg(
    port,
    "ffbase",
    &[
      "-c:v",
      "libx264",
      "-preset",
      "ultrafast",
      "-tune",
      "zerolatency",
      "-pix_fmt",
      "yuv420p",
      "-c:a",
      "aac",
      "-ar",
      "44100",
      "-ac",
      "2",
    ],
    3,
  );
  let tags = pull_stream_until_ffmpeg_eos(&mut src, &mut child, drain, "baseline");

  let videos = tag_kinds(&tags, 9);
  let audios = tag_kinds(&tags, 8);
  let scripts = tag_kinds(&tags, 18);
  eprintln!(
    "baseline: {} video, {} audio, {} script tags",
    videos.len(),
    audios.len(),
    scripts.len()
  );
  assert!(
    !videos.is_empty(),
    "ffmpeg H.264 publish must yield video tags"
  );
  assert!(
    !audios.is_empty(),
    "ffmpeg AAC publish must yield audio tags"
  );
  assert!(
    !scripts.is_empty(),
    "ffmpeg onMetaData script tag must pass through"
  );
  assert!(
    videos
      .iter()
      .any(|payload| payload.len() >= 2 && payload[0] == 0x17 && payload[1] == 0x00),
    "an AVC sequence header (0x17 0x00) must pass through"
  );
  assert!(
    audios
      .iter()
      .any(|payload| payload.len() >= 2 && payload[0] == 0xaf && payload[1] == 0x00),
    "an AAC sequence header (0xaf 0x00) must pass through"
  );
}

#[test]
fn ffmpeg_publish_enhanced_hevc_opus() {
  init();
  if !require_ffmpeg(&["libx265", "libopus"]) {
    return;
  }
  let (mut src, port) = start_listener("live", "ffenh");
  let (mut child, drain) = spawn_ffmpeg(
    port,
    "ffenh",
    &[
      "-c:v",
      "libx265",
      "-x265-params",
      "log-level=error",
      "-c:a",
      "libopus",
      "-ar",
      "48000",
      "-ac",
      "2",
    ],
    3,
  );
  let tags = pull_stream_until_ffmpeg_eos(&mut src, &mut child, drain, "enhanced");

  let videos = tag_kinds(&tags, 9);
  let audios = tag_kinds(&tags, 8);
  eprintln!(
    "enhanced: {} video, {} audio tags",
    videos.len(),
    audios.len()
  );
  assert!(
    !videos.is_empty(),
    "ffmpeg HEVC publish must yield video tags"
  );
  assert!(
    !audios.is_empty(),
    "ffmpeg Opus publish must yield audio tags"
  );
  assert!(
    videos
      .iter()
      .any(|payload| payload.len() >= 5 && payload[0] == 0x90 && payload[1..5] == *b"hvc1"),
    "an HEVC enhanced-RTMP sequence start (0x90 hvc1) must pass through"
  );
  assert!(
    audios
      .iter()
      .any(|payload| payload.len() >= 5 && payload[1..5] == *b"Opus"),
    "an Opus enhanced-RTMP sequence start (Opus fourcc) must pass through"
  );
}
