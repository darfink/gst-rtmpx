// Layer-1 hermetic tests for rtmpxsink listen mode.
// The sink binds and serves raw RTMP players, applying backpressure
// until one connects. No ffmpeg or external server is used.
use gst::prelude::*;
use rtmpx_legacy::amf0::{Amf0Object, Amf0Value};
use rtmpx_legacy::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rtmpx_legacy::sessions::{
  ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
};
use std::fmt::Write as _;
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn init() {
  static INIT: Once = Once::new();
  INIT.call_once(|| {
    gst::init().unwrap();
    gstrtmpx::plugin_register_static().expect("rtmpx static plugin registration");
  });
}
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
  tag.extend_from_slice(&(timestamp & 0x00FFFFFF).to_be_bytes()[1..]);
  tag.push((timestamp >> 24) as u8);
  tag.extend_from_slice(&[0, 0, 0]);
  tag.extend_from_slice(payload);
  let total = (11 + payload.len()) as u32;
  tag.extend_from_slice(&total.to_be_bytes());
  tag
}
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
fn start_sink_listen(app: &str, key: &str, extra: &str) -> (gst_check::Harness, u16) {
  let mut harness = gst_check::Harness::new_empty();
  let base = if key.is_empty() {
    format!("rtmp://127.0.0.1:0/{app}")
  } else {
    format!("rtmp://127.0.0.1:0/{app}/{key}")
  };
  harness.add_parse(&format!(
    "rtmpxsink name=sink mode=listen uri={base} sync=false {extra}"
  ));
  harness.set_src_caps_str("video/x-flv");
  harness.play();
  let bin = harness.element().expect("harness must hold a pipeline");
  let element = bin
    .downcast_ref::<gst::Bin>()
    .and_then(|bin| bin.by_name("sink"))
    .expect("pipeline must contain sink");
  let deadline = Instant::now() + Duration::from_secs(5);
  loop {
    let uri: String = element.property("uri");
    if !uri.contains(":0/") {
      let after = uri
        .strip_prefix("rtmp://127.0.0.1:")
        .expect("bound uri keeps host");
      let port: u16 = after
        .split('/')
        .next()
        .expect("bound uri keeps port")
        .parse()
        .expect("port parses");
      return (harness, port);
    }
    assert!(Instant::now() < deadline, "sink listener never bound");
    std::thread::sleep(Duration::from_millis(20));
  }
}
fn push_bytes(harness: &mut gst_check::Harness, data: &[u8]) {
  let buffer = gst::Buffer::from_slice(data.to_vec());
  harness.push(buffer).expect("sink must accept FLV bytes");
}
struct Collected {
  accepted: bool,
  videos: Vec<(u32, Vec<u8>)>,
  audios: Vec<(u32, Vec<u8>)>,
  scripts: Vec<(u32, Vec<u8>)>,
  transcript: String,
}
async fn player_write(stream: &mut tokio::net::TcpStream, result: ClientSessionResult) {
  if let ClientSessionResult::OutboundResponse(packet) = result {
    stream
      .write_all(&packet.bytes)
      .await
      .expect("player must write request");
  }
}
fn spawn_player(
  port: u16,
  app: &str,
  key: &str,
  want_video: usize,
  want_audio: usize,
) -> (
  std::sync::mpsc::Receiver<()>,
  std::thread::JoinHandle<Collected>,
) {
  let (tx, rx) = std::sync::mpsc::channel();
  let app_owned = app.to_owned();
  let key_owned = key.to_owned();
  let handle = std::thread::spawn(move || {
    run_player_blocking(
      port,
      &app_owned,
      &key_owned,
      want_video,
      want_audio,
      Some(tx),
    )
  });
  (rx, handle)
}
fn wait_ready(rx: &std::sync::mpsc::Receiver<()>, what: &str) {
  rx.recv_timeout(Duration::from_secs(8))
    .unwrap_or_else(|_| panic!("player must be accepted for {what}"));
}
fn shutdown_sink(harness: &gst_check::Harness) {
  harness
    .element()
    .expect("harness must hold sink")
    .set_state(gst::State::Null)
    .expect("sink must shut down");
}
#[test]
fn sink_listen_serves_player_live_classic() {
  init();
  let (mut sink, port) = start_sink_listen("live", "key1", "");
  let vseq = vec![0x17, 0x00, 0x11, 0x22];
  let aseq = vec![0xAF, 0x00, 0x11];
  let v1 = vec![0x17, 0x01, 0x33, 0x44];
  let a1 = vec![0xAF, 0x01, 0x44, 0x55];
  let v2 = vec![0x17, 0x01, 0x55, 0x66];
  let a2 = vec![0xAF, 0x01, 0x66, 0x77];
  let (ready, player) = spawn_player(port, "live", "key1", 3, 3);
  wait_ready(&ready, "live classic");
  push_bytes(&mut sink, &flv_header());
  push_bytes(&mut sink, &frame_tag(9, 0, &vseq));
  push_bytes(&mut sink, &frame_tag(8, 0, &aseq));
  let script = rtmpx_legacy::amf0::serialize(&[
    Amf0Value::Utf8String("onCaption".into()),
    Amf0Value::Utf8String("hello".into()),
  ])
  .unwrap();
  push_bytes(&mut sink, &frame_tag(18, 25, &script));
  push_bytes(&mut sink, &frame_tag(9, 40, &v1));
  push_bytes(&mut sink, &frame_tag(8, 60, &a1));
  push_bytes(&mut sink, &frame_tag(9, 80, &v2));
  push_bytes(&mut sink, &frame_tag(8, 100, &a2));
  let collected = player.join().expect("player thread must finish");
  eprintln!("live classic transcript len {}", collected.transcript.len());
  assert!(collected.accepted, "player must be accepted");
  assert_eq!(collected.videos.len(), 3, "must receive 3 video tags");
  assert_eq!(collected.audios.len(), 3, "must receive 3 audio tags");
  assert_eq!(collected.videos[0], (0, vseq.clone()));
  assert_eq!(collected.videos[1], (40, v1.clone()));
  assert_eq!(collected.videos[2], (80, v2.clone()));
  assert_eq!(collected.audios[0], (0, aseq.clone()));
  assert_eq!(collected.audios[1], (60, a1.clone()));
  assert_eq!(collected.audios[2], (100, a2.clone()));
  // RTMP playback emits sample-access and data-start before application data.
  assert_eq!(collected.scripts.len(), 3);
  for ((_, data), name) in collected.scripts[..2]
    .iter()
    .zip(["|RtmpSampleAccess", "onStatus"])
  {
    let values = rtmpx_legacy::amf0::deserialize(&mut data.as_slice()).unwrap();
    assert_eq!(
      values[0],
      rtmpx_legacy::amf0::Amf0Value::Utf8String(name.into())
    );
  }
  assert_eq!(collected.scripts[2], (25, script));
  sink.push_event(gst::event::Eos::new());
  shutdown_sink(&sink);
}
#[test]
fn sink_listen_replays_headers_to_mid_stream_joiner() {
  init();
  let (mut sink, port) = start_sink_listen("live", "mid", "");
  let vseq = vec![0x17, 0x00, 0xAA];
  let aseq = vec![0xAF, 0x00, 0xBB];
  let stale_v = vec![0x17, 0x01, 0xCC];
  let stale_a = vec![0xAF, 0x01, 0xDD];
  let live_v = vec![0x17, 0x01, 0xEE];
  let live_a = vec![0xAF, 0x01, 0xFF];
  push_bytes(&mut sink, &flv_header());
  push_bytes(&mut sink, &frame_tag(9, 0, &vseq));
  push_bytes(&mut sink, &frame_tag(8, 0, &aseq));
  push_bytes(&mut sink, &frame_tag(9, 40, &stale_v));
  push_bytes(&mut sink, &frame_tag(8, 60, &stale_a));
  let (ready, player) = spawn_player(port, "live", "mid", 2, 2);
  wait_ready(&ready, "mid-stream joiner");
  std::thread::sleep(Duration::from_millis(500));
  push_bytes(&mut sink, &frame_tag(9, 8000, &live_v));
  push_bytes(&mut sink, &frame_tag(8, 8020, &live_a));
  let collected = player.join().expect("player thread must finish");
  assert!(collected.accepted, "joiner must be accepted");
  assert_eq!(
    collected.videos.len(),
    2,
    "joiner must get header replay plus live video"
  );
  assert_eq!(
    collected.audios.len(),
    2,
    "joiner must get header replay plus live audio"
  );
  assert_eq!(
    collected.videos[0],
    (0, vseq.clone()),
    "first video must be cached sequence header"
  );
  assert_eq!(
    collected.videos[1],
    (8000, live_v.clone()),
    "second video must be live, stale dropped"
  );
  assert_eq!(
    collected.audios[0],
    (0, aseq.clone()),
    "first audio must be cached sequence header"
  );
  assert_eq!(
    collected.audios[1],
    (8020, live_a.clone()),
    "second audio must be live, stale dropped"
  );
  assert!(
    !collected.videos.iter().any(|(_, p)| p == &stale_v),
    "stale video must never be resent"
  );
  assert!(
    !collected.audios.iter().any(|(_, p)| p == &stale_a),
    "stale audio must never be resent"
  );
  sink.push_event(gst::event::Eos::new());
  shutdown_sink(&sink);
}
#[test]
fn sink_listen_enhanced_rtmp_passes_through() {
  init();
  let (mut sink, port) = start_sink_listen("live", "enh", "");
  let vseq = vec![0x90, 0x68, 0x76, 0x63, 0x31, 0x00, 0x01];
  let vframe = vec![0x91, 0x68, 0x76, 0x63, 0x31, 0x02, 0x03];
  let aseq = vec![0x90, 0x4F, 0x70, 0x75, 0x73, 0x00];
  let aframe = vec![0x91, 0x4F, 0x70, 0x75, 0x73, 0x04];
  let (ready, player) = spawn_player(port, "live", "enh", 2, 2);
  wait_ready(&ready, "enhanced");
  push_bytes(&mut sink, &flv_header());
  push_bytes(&mut sink, &frame_tag(9, 0, &vseq));
  push_bytes(&mut sink, &frame_tag(8, 0, &aseq));
  push_bytes(&mut sink, &frame_tag(9, 40, &vframe));
  push_bytes(&mut sink, &frame_tag(8, 40, &aframe));
  let collected = player.join().expect("player thread must finish");
  assert!(collected.accepted, "enhanced player must be accepted");
  assert_eq!(
    collected.videos,
    vec![(0, vseq.clone()), (40, vframe.clone())]
  );
  assert_eq!(
    collected.audios,
    vec![(0, aseq.clone()), (40, aframe.clone())]
  );
  sink.push_event(gst::event::Eos::new());
  shutdown_sink(&sink);
}
#[test]
fn sink_listen_rejects_wrong_stream_key_and_keeps_serving() {
  init();
  let (mut sink, port) = start_sink_listen("live", "good", "");
  let denied = run_player_blocking(port, "live", "wrong", 0, 0, None);
  eprintln!("denial transcript {}", denied.transcript);
  assert!(!denied.accepted, "wrong key must never be accepted");
  assert!(
    denied.videos.is_empty() && denied.audios.is_empty(),
    "no media for rejected key"
  );
  assert!(
    denied.transcript.contains("Failed")
      || denied.transcript.contains("closed")
      || denied.transcript.contains("Rejected"),
    "server must reject or hang up"
  );
  let vseq = vec![0x17, 0x00, 0x11];
  let aseq = vec![0xAF, 0x00, 0x22];
  let (ready, player) = spawn_player(port, "live", "good", 1, 1);
  wait_ready(&ready, "good key after rejection");
  push_bytes(&mut sink, &flv_header());
  push_bytes(&mut sink, &frame_tag(9, 0, &vseq));
  push_bytes(&mut sink, &frame_tag(8, 0, &aseq));
  let collected = player.join().expect("player thread must finish");
  assert!(
    collected.accepted,
    "correct key must still be served after rejection"
  );
  assert_eq!(collected.videos, vec![(0, vseq.clone())]);
  assert_eq!(collected.audios, vec![(0, aseq.clone())]);
  sink.push_event(gst::event::Eos::new());
  shutdown_sink(&sink);
}
#[test]
fn sink_listen_wait_for_connection_false_drops_without_player() {
  init();
  let (mut sink, port) = start_sink_listen("live", "drop", "wait-for-connection=false");
  let started = Instant::now();
  push_bytes(&mut sink, &flv_header());
  for i in 0..200u32 {
    let payload = vec![0x17, 0x01, (i & 0xFF) as u8];
    push_bytes(&mut sink, &frame_tag(9, i * 40, &payload));
  }
  assert!(
    started.elapsed() < Duration::from_secs(5),
    "drops must not block without a player"
  );
  let vseq = vec![0x17, 0x00, 0x99];
  let aseq = vec![0xAF, 0x00, 0x88];
  let live_v = vec![0x17, 0x01, 0x77];
  let live_a = vec![0xAF, 0x01, 0x66];
  let (ready, player) = spawn_player(port, "live", "drop", 2, 2);
  wait_ready(&ready, "drop then live");
  std::thread::sleep(Duration::from_millis(500));
  push_bytes(&mut sink, &frame_tag(9, 9000, &vseq));
  push_bytes(&mut sink, &frame_tag(8, 9000, &aseq));
  push_bytes(&mut sink, &frame_tag(9, 9040, &live_v));
  push_bytes(&mut sink, &frame_tag(8, 9060, &live_a));
  let collected = player.join().expect("player thread must finish");
  assert!(collected.accepted, "player must be accepted after drops");
  assert!(
    collected.videos.contains(&(9000, vseq.clone())),
    "live video header must arrive"
  );
  assert!(
    collected.videos.contains(&(9040, live_v.clone())),
    "live video frame must arrive"
  );
  assert!(
    collected.audios.contains(&(9000, aseq.clone())),
    "live audio header must arrive"
  );
  sink.push_event(gst::event::Eos::new());
  shutdown_sink(&sink);
}
#[test]
fn sink_listen_eos_without_player_shuts_down_cleanly() {
  init();
  let (mut sink, _port) = start_sink_listen("live", "eos", "");
  push_bytes(&mut sink, &flv_header());
  push_bytes(&mut sink, &frame_tag(9, 0, &[0x17, 0x00, 0x11]));
  let started = Instant::now();
  sink.push_event(gst::event::Eos::new());
  shutdown_sink(&sink);
  assert!(
    started.elapsed() < Duration::from_secs(5),
    "EOS with no player must shut down promptly"
  );
}
#[test]
fn sink_listen_applies_backpressure_until_player_connects() {
  init();
  let (port_tx, port_rx) = std::sync::mpsc::channel();
  let (done_tx, done_rx) = std::sync::mpsc::channel();
  let pusher = std::thread::spawn(move || {
    init();
    let (mut sink, port) = start_sink_listen("live", "back", "");
    port_tx.send(port).expect("main must receive listen port");
    push_bytes(&mut sink, &flv_header());
    push_bytes(&mut sink, &frame_tag(9, 0, &[0x17, 0x00, 0x11]));
    push_bytes(&mut sink, &frame_tag(8, 0, &[0xAF, 0x00, 0x22]));
    let started = Instant::now();
    for i in 0..200u32 {
      let payload = vec![0x17, 0x01, (i & 0xFF) as u8, 0x33];
      push_bytes(&mut sink, &frame_tag(9, 40 + i * 40, &payload));
    }
    let elapsed = started.elapsed();
    done_tx
      .send(elapsed)
      .expect("main must receive pusher done");
    sink.push_event(gst::event::Eos::new());
    shutdown_sink(&sink);
  });
  let port: u16 = port_rx
    .recv_timeout(Duration::from_secs(8))
    .expect("pusher must bind");
  std::thread::sleep(Duration::from_secs(1));
  assert!(
    done_rx.try_recv().is_err(),
    "pusher must still be blocked with no player"
  );
  let (ready, player) = spawn_player(port, "live", "back", 2, 1);
  wait_ready(&ready, "backpressure release");
  let elapsed = done_rx
    .recv_timeout(Duration::from_secs(12))
    .expect("pusher must unblock once player connects");
  eprintln!("backpressure pusher blocked for {:?}", elapsed);
  assert!(
    elapsed >= Duration::from_millis(800),
    "pusher must have blocked on the bounded queue"
  );
  let collected = player.join().expect("player thread must finish");
  assert!(collected.accepted, "player must be accepted");
  assert!(
    !collected.videos.is_empty(),
    "player must receive replay plus live after unblock"
  );
  pusher.join().expect("pusher thread must finish");
}
fn run_player_blocking(
  port: u16,
  app: &str,
  key: &str,
  want_video: usize,
  want_audio: usize,
  ready: Option<std::sync::mpsc::Sender<()>>,
) -> Collected {
  tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .expect("player runtime must build")
    .block_on(async move {
      let mut transcript = String::new();
      let mut videos: Vec<(u32, Vec<u8>)> = Vec::new();
      let mut audios: Vec<(u32, Vec<u8>)> = Vec::new();
      let mut scripts = Vec::new();
      let mut accepted = false;
      let mut notified = false;
      let notify_ready = |accepted_flag: bool,
                          ready_opt: &Option<std::sync::mpsc::Sender<()>>,
                          notified_flag: &mut bool| {
        if accepted_flag && !*notified_flag {
          *notified_flag = true;
          if let Some(tx) = ready_opt {
            let _ = tx.send(());
          }
        }
      };
      let mut stream = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(("127.0.0.1", port)),
      )
      .await
      .expect("player connect must not hang")
      .expect("player must connect");
      stream.set_nodelay(true).expect("player must set nodelay");
      let mut handshake = Handshake::new(PeerType::Client);
      let outbound = handshake
        .generate_outbound_p0_and_p1()
        .expect("player must start handshake");
      stream
        .write_all(&outbound)
        .await
        .expect("player must write handshake");
      stream.flush().await.expect("player must flush");
      let mut buf = vec![0u8; 16 * 1024];
      let carry = loop {
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
          .await
          .expect("handshake must not hang")
          .expect("server must answer handshake");
        assert!(n > 0, "server closed during handshake");
        match handshake
          .process_bytes(&buf[..n])
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
      config.tc_url = Some(format!("rtmp://127.0.0.1:{port}/{app}"));
      let (mut session, _initial) = ClientSession::new(config).expect("player must create session");
      if !carry.is_empty() {
        let results = session
          .handle_input(&carry)
          .expect("early server bytes must parse");
        for result in results {
          let _ = writeln!(transcript, "{:?}", result);
          player_write(&mut stream, result).await;
        }
      }
      let connect = session
        .request_connection_with_properties(app.to_owned(), probe_capabilities())
        .expect("player must request connection");
      player_write(&mut stream, connect).await;
      stream.flush().await.expect("player must flush");
      let mut connected = false;
      let deadline = Instant::now() + Duration::from_secs(5);
      while !connected {
        assert!(
          Instant::now() < deadline,
          "server never accepted connection"
        );
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
          .await
          .expect("connect reply must not hang")
          .expect("server must answer connect");
        assert!(n > 0, "server closed before connect accept");
        let results = session
          .handle_input(&buf[..n])
          .expect("connect reply must parse");
        for result in results {
          let _ = writeln!(transcript, "{:?}", result);
          if matches!(
            result,
            ClientSessionResult::RaisedEvent(ClientSessionEvent::ConnectionRequestAccepted { .. })
          ) {
            connected = true;
          }
          if matches!(
            result,
            ClientSessionResult::RaisedEvent(ClientSessionEvent::ConnectionRequestRejected { .. })
          ) {
            return Collected {
              accepted: false,
              videos,
              audios,
              scripts,
              transcript,
            };
          }
          player_write(&mut stream, result).await;
        }
      }
      let play = session
        .request_playback(key.to_owned())
        .expect("player must request playback");
      player_write(&mut stream, play).await;
      stream.flush().await.expect("player must flush play");
      let deadline = Instant::now() + Duration::from_secs(10);
      loop {
        if accepted && videos.len() >= want_video && audios.len() >= want_audio {
          break;
        }
        if Instant::now() >= deadline {
          break;
        }
        let n = match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
          Ok(Ok(n)) => n,
          Ok(Err(e)) => {
            let _ = writeln!(transcript, "read error {e:?}");
            break;
          }
          Err(_) => {
            continue;
          }
        };
        if n == 0 {
          let _ = writeln!(transcript, "server closed connection");
          break;
        }
        let results = match session.handle_input(&buf[..n]) {
          Ok(results) => results,
          Err(e) => {
            let _ = writeln!(transcript, "session input error {e:?}");
            break;
          }
        };
        for result in results {
          let _ = writeln!(transcript, "{:?}", result);
          match result {
            ClientSessionResult::RaisedEvent(ClientSessionEvent::PlaybackRequestAccepted {
              ..
            }) => {
              accepted = true;
              notify_ready(true, &ready, &mut notified);
            }
            ClientSessionResult::RaisedEvent(ClientSessionEvent::VideoDataReceived {
              timestamp,
              data,
              ..
            }) => {
              videos.push((timestamp.value, data.to_vec()));
            }
            ClientSessionResult::RaisedEvent(ClientSessionEvent::AudioDataReceived {
              timestamp,
              data,
              ..
            }) => {
              audios.push((timestamp.value, data.to_vec()));
            }
            ClientSessionResult::RaisedEvent(ClientSessionEvent::StreamDataReceived {
              message,
              ..
            }) => {
              scripts.push((message.timestamp().value, message.into_payload().to_vec()));
            }
            ClientSessionResult::OutboundResponse(packet) => {
              stream
                .write_all(&packet.bytes)
                .await
                .expect("player must write response");
            }
            _ => {}
          }
        }
        // Some servers send media before Play.Start, so signal ready once media arrives too.
        if !videos.is_empty() || !audios.is_empty() {
          notify_ready(true, &ready, &mut notified);
          if !accepted {
            // Treat first media as accepted for liveness when Play.Start is delayed.
            accepted = true;
          }
        }
      }
      Collected {
        accepted,
        videos,
        audios,
        scripts,
        transcript,
      }
    })
}
