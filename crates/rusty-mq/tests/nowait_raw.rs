//! FR-P08: nowait semantics at the frame level. Every nowait method must
//! suppress its `-ok` reply (with one documented exception: a nowait
//! queue.declare with a server-generated name still replies, because the
//! client cannot learn the name otherwise). Suppression is proven with an
//! ordering probe: after each nowait method the client sends `basic.qos`
//! and the very next frame must be `qos-ok` — the single writer guarantees
//! a leaked reply would have arrived first.

use std::time::Duration;

use amq_protocol::frame::{AMQPContentHeader, AMQPFrame};
use amq_protocol::protocol::{
    basic as basic7, channel as ch7, connection as conn7, exchange as ex7, queue as q7, AMQPClass,
};
use amq_protocol::types::LongString;
use rusty_mq_protocol::{encode_frame, FrameReader, ProtocolLimits, PROTOCOL_HEADER_0_9_1};
use tokio::io::AsyncReadExt;

async fn start_broker() -> std::net::SocketAddr {
    let dir = std::env::temp_dir().join(format!(
        "rmq-nowait-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    let broker = std::sync::Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(listener, broker));
    addr
}

struct RawSession {
    sock: tokio::net::TcpStream,
    reader: FrameReader,
    buf: Vec<u8>,
}

impl RawSession {
    async fn send(&mut self, frame: &AMQPFrame) {
        use tokio::io::AsyncWriteExt;
        self.sock.write_all(&encode_frame(frame)).await.unwrap();
    }

    /// Next METHOD frame (heartbeats skipped), failing on timeout.
    /// Buffered frames are drained before any socket read.
    async fn next_method(&mut self) -> AMQPFrame {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                while let Ok(Some(frame)) = self.reader.next_frame() {
                    if matches!(frame, AMQPFrame::Heartbeat(_)) {
                        continue;
                    }
                    if matches!(frame, AMQPFrame::Method(..)) {
                        return frame;
                    }
                }
                let mut chunk = [0u8; 4096];
                let n = self.sock.read(&mut chunk).await.expect("socket read");
                assert!(n > 0, "connection closed while waiting for a method");
                self.reader.feed(&chunk[..n]).unwrap();
            }
        })
        .await
        .expect("timeout waiting for a method frame")
    }

    /// Consume the content (header + body frames) that follows a delivery.
    async fn drain_content(&mut self, size: u64) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut body = Vec::new();
            loop {
                // Buffered frames first: the delivery, header, and body
                // routinely share one TCP segment.
                while let Ok(Some(frame)) = self.reader.next_frame() {
                    match frame {
                        AMQPFrame::Body(_, bytes) => body.extend_from_slice(&bytes),
                        AMQPFrame::Header(..) | AMQPFrame::Heartbeat(_) => {}
                        other => panic!("unexpected frame mid-content: {other:?}"),
                    }
                }
                if body.len() as u64 >= size {
                    return body;
                }
                let mut chunk = [0u8; 4096];
                let n = self.sock.read(&mut chunk).await.expect("socket read");
                assert!(n > 0, "connection closed mid-content");
                self.reader.feed(&chunk[..n]).unwrap();
            }
        })
        .await
        .expect("timeout waiting for content")
    }

    /// Send `basic.qos` on channel 1 and return the next method frame:
    /// if a suppressed nowait reply leaked, it is returned instead of
    /// `qos-ok`.
    async fn order_probe(&mut self) -> AMQPFrame {
        self.send(&AMQPFrame::Method(
            1,
            AMQPClass::Basic(basic7::AMQPMethod::Qos(basic7::Qos {
                prefetch_count: 10,
                global: false,
            })),
        ))
        .await;
        self.next_method().await
    }
}

async fn handshake(addr: std::net::SocketAddr) -> RawSession {
    use tokio::io::AsyncWriteExt;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(&PROTOCOL_HEADER_0_9_1).await.unwrap();

    let limits = ProtocolLimits::default();
    let negotiated = limits
        .negotiate(limits.max_channel_max, limits.max_frame_max, 0)
        .unwrap();
    let mut session = RawSession {
        sock,
        reader: FrameReader::new_post_header(&negotiated),
        buf: vec![],
    };
    let _ = &mut session.buf;

    session
        .send(&AMQPFrame::Method(
            0,
            AMQPClass::Connection(conn7::AMQPMethod::StartOk(conn7::StartOk {
                client_properties: Default::default(),
                mechanism: "PLAIN".into(),
                response: LongString::from(vec![
                    0, b'g', b'u', b'e', b's', b't', 0, b'g', b'u', b'e', b's', b't',
                ]),
                locale: "en_US".into(),
            })),
        ))
        .await;

    loop {
        match session.next_method().await {
            // The server's connection.start races with our start-ok send.
            AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::Start(_))) => {}
            AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::Tune(t))) => {
                session
                    .send(&AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(conn7::AMQPMethod::TuneOk(conn7::TuneOk {
                            channel_max: t.channel_max,
                            frame_max: t.frame_max,
                            heartbeat: t.heartbeat,
                        })),
                    ))
                    .await;
                session
                    .send(&AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(conn7::AMQPMethod::Open(conn7::Open {
                            virtual_host: "/".into(),
                        })),
                    ))
                    .await;
            }
            AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::OpenOk(_))) => {
                session
                    .send(&AMQPFrame::Method(
                        1,
                        AMQPClass::Channel(ch7::AMQPMethod::Open(ch7::Open {})),
                    ))
                    .await;
            }
            AMQPFrame::Method(1, AMQPClass::Channel(ch7::AMQPMethod::OpenOk(_))) => {
                return session;
            }
            other => panic!("unexpected frame during handshake: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nowait_suppresses_replies_except_generated_names() {
    let addr = start_broker().await;
    let mut s = handshake(addr).await;

    // queue.declare(nowait): no declare-ok may leak.
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Queue(q7::AMQPMethod::Declare(q7::Declare {
            queue: "nw.q".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: true,
            arguments: Default::default(),
        })),
    ))
    .await;
    match s.order_probe().await {
        AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::QosOk(_))) => {}
        other => panic!("nowait queue.declare leaked a reply: {other:?}"),
    }

    // exchange.declare(nowait): no declare-ok may leak.
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Exchange(ex7::AMQPMethod::Declare(ex7::Declare {
            exchange: "nw.ex".into(),
            kind: "topic".into(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: true,
            arguments: Default::default(),
        })),
    ))
    .await;
    match s.order_probe().await {
        AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::QosOk(_))) => {}
        other => panic!("nowait exchange.declare leaked a reply: {other:?}"),
    }

    // queue.bind(nowait): no bind-ok may leak.
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Queue(q7::AMQPMethod::Bind(q7::Bind {
            queue: "nw.q".into(),
            exchange: "nw.ex".into(),
            routing_key: "a.*.c".into(),
            nowait: true,
            arguments: Default::default(),
        })),
    ))
    .await;
    match s.order_probe().await {
        AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::QosOk(_))) => {}
        other => panic!("nowait queue.bind leaked a reply: {other:?}"),
    }

    // basic.consume(nowait): no consume-ok may leak, and the consumer
    // must still be ACTIVE — verified by publishing and receiving the
    // delivery under the client-supplied tag.
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Basic(basic7::AMQPMethod::Consume(basic7::Consume {
            queue: "nw.q".into(),
            consumer_tag: "nw.tag".into(),
            no_local: false,
            no_ack: true,
            exclusive: false,
            nowait: true,
            arguments: Default::default(),
        })),
    ))
    .await;
    match s.order_probe().await {
        AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::QosOk(_))) => {}
        other => panic!("nowait basic.consume leaked a reply: {other:?}"),
    }

    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Basic(basic7::AMQPMethod::Publish(basic7::Publish {
            exchange: "nw.ex".into(),
            routing_key: "a.b.c".into(),
            mandatory: false,
            immediate: false,
        })),
    ))
    .await;
    s.send(&AMQPFrame::Header(
        1,
        60,
        Box::new(AMQPContentHeader {
            class_id: 60,
            body_size: 5,
            properties: Default::default(),
        }),
    ))
    .await;
    s.send(&AMQPFrame::Body(1, b"hello".to_vec())).await;

    match s.next_method().await {
        AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::Deliver(d))) => {
            assert_eq!(d.consumer_tag.as_str(), "nw.tag");
            assert_eq!(d.exchange.as_str(), "nw.ex");
            assert_eq!(d.routing_key.as_str(), "a.b.c");
            let body = s.drain_content(5).await;
            assert_eq!(body, b"hello");
        }
        other => panic!("expected delivery for nowait consumer, got {other:?}"),
    }

    // queue.purge(nowait) and queue.delete(nowait): no -ok may leak.
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Queue(q7::AMQPMethod::Purge(q7::Purge {
            queue: "nw.q".into(),
            nowait: true,
        })),
    ))
    .await;
    match s.order_probe().await {
        AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::QosOk(_))) => {}
        other => panic!("nowait queue.purge leaked a reply: {other:?}"),
    }

    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Queue(q7::AMQPMethod::Delete(q7::Delete {
            queue: "nw.q".into(),
            if_unused: false,
            if_empty: false,
            nowait: true,
        })),
    ))
    .await;
    match s.order_probe().await {
        AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::QosOk(_))) => {}
        other => panic!("nowait queue.delete leaked a reply: {other:?}"),
    }

    // The documented exception: a nowait declare with a SERVER-GENERATED
    // name still replies declare-ok — the client cannot learn the name
    // any other way.
    s.send(&AMQPFrame::Method(
        2,
        AMQPClass::Channel(ch7::AMQPMethod::Open(ch7::Open {})),
    ))
    .await;
    match s.next_method().await {
        AMQPFrame::Method(2, AMQPClass::Channel(ch7::AMQPMethod::OpenOk(_))) => {}
        other => panic!("unexpected frame opening channel 2: {other:?}"),
    }
    s.send(&AMQPFrame::Method(
        2,
        AMQPClass::Queue(q7::AMQPMethod::Declare(q7::Declare {
            queue: "".into(),
            passive: false,
            durable: false,
            exclusive: true,
            auto_delete: false,
            nowait: true,
            arguments: Default::default(),
        })),
    ))
    .await;
    match s.next_method().await {
        AMQPFrame::Method(2, AMQPClass::Queue(q7::AMQPMethod::DeclareOk(ok))) => {
            assert!(
                !ok.queue.as_str().is_empty(),
                "generated-name declare must reply with the name"
            );
        }
        other => panic!("generated-name nowait declare must still reply: {other:?}"),
    }
}

/// T03: multiple interleaved channels on ONE connection — concurrent
/// confirms, deliveries, and nowait operations must never cross channels
/// (no orphan reply, no per-channel content interleaving). Frame-level:
/// every received frame is checked against the channel it arrived on.
#[tokio::test(flavor = "multi_thread")]
async fn interleaved_channels_keep_content_isolated() {
    let addr = start_broker().await;
    let mut s = handshake(addr).await;

    let open =
        |id: u16| AMQPFrame::Method(id, AMQPClass::Channel(ch7::AMQPMethod::Open(ch7::Open {})));

    // Channels 2 and 3 open on the same connection as the confirming
    // publisher on channel 1.
    for id in [2u16, 3] {
        s.send(&open(id)).await;
        match s.next_method().await {
            AMQPFrame::Method(ch, AMQPClass::Channel(ch7::AMQPMethod::OpenOk(_))) => {
                assert_eq!(ch, id, "open-ok must arrive on the opening channel");
            }
            other => panic!("unexpected while opening channel {id}: {other:?}"),
        }
    }

    // Declare iso.a on channel 1 (exclusive: the frozen profile rejects
    // shared transient queues).
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Queue(q7::AMQPMethod::Declare(q7::Declare {
            queue: "iso.a".into(),
            passive: false,
            durable: false,
            exclusive: true,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        })),
    ))
    .await;
    match s.next_method().await {
        AMQPFrame::Method(1, AMQPClass::Queue(q7::AMQPMethod::DeclareOk(ok))) => {
            assert_eq!(ok.queue.as_str(), "iso.a");
        }
        other => panic!("unexpected instead of declare-ok: {other:?}"),
    }

    // NOWAIT declare on channel 3: no reply may ever arrive for it. It is
    // proven below — the only frames that may follow are the ones channel
    // 1 and 2 are owed, and any channel-3 frame fails the match arms.
    s.send(&AMQPFrame::Method(
        3,
        AMQPClass::Queue(q7::AMQPMethod::Declare(q7::Declare {
            queue: "iso.b".into(),
            passive: false,
            durable: false,
            exclusive: true,
            auto_delete: false,
            nowait: true,
            arguments: Default::default(),
        })),
    ))
    .await;

    // Confirm-select on channel 1 only.
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Confirm(amq_protocol::protocol::confirm::AMQPMethod::Select(
            amq_protocol::protocol::confirm::Select { nowait: false },
        )),
    ))
    .await;
    match s.next_method().await {
        AMQPFrame::Method(
            1,
            AMQPClass::Confirm(amq_protocol::protocol::confirm::AMQPMethod::SelectOk(_)),
        ) => {}
        other => panic!("unexpected instead of select-ok: {other:?}"),
    }

    // Consume iso.a on channel 2 (no-ack push consumer).
    s.send(&AMQPFrame::Method(
        2,
        AMQPClass::Basic(basic7::AMQPMethod::Consume(basic7::Consume {
            queue: "iso.a".into(),
            consumer_tag: "iso.tag".into(),
            no_local: false,
            no_ack: true,
            exclusive: false,
            nowait: false,
            arguments: Default::default(),
        })),
    ))
    .await;
    match s.next_method().await {
        AMQPFrame::Method(2, AMQPClass::Basic(basic7::AMQPMethod::ConsumeOk(ok))) => {
            assert_eq!(ok.consumer_tag.as_str(), "iso.tag");
        }
        other => panic!("unexpected instead of consume-ok: {other:?}"),
    }

    // The interleave under test: a confirmed publish on channel 1 races
    // the delivery on channel 2. Whichever order they arrive in, the
    // confirm is on channel 1 and the delivery (with its content) is on
    // channel 2 — never swapped, never duplicated, never on channel 3.
    s.send(&AMQPFrame::Method(
        1,
        AMQPClass::Basic(basic7::AMQPMethod::Publish(basic7::Publish {
            exchange: "".into(),
            routing_key: "iso.a".into(),
            mandatory: false,
            immediate: false,
        })),
    ))
    .await;
    s.send(&AMQPFrame::Header(
        1,
        60,
        Box::new(AMQPContentHeader {
            class_id: 60,
            body_size: 4,
            properties: Default::default(),
        }),
    ))
    .await;
    s.send(&AMQPFrame::Body(1, b"iso0".to_vec())).await;

    let mut got_deliver = false;
    let mut got_confirm = false;
    for _ in 0..2 {
        match s.next_method().await {
            AMQPFrame::Method(2, AMQPClass::Basic(basic7::AMQPMethod::Deliver(d))) => {
                assert!(!got_deliver, "duplicate delivery");
                assert_eq!(d.consumer_tag.as_str(), "iso.tag");
                assert_eq!(d.routing_key.as_str(), "iso.a");
                let body = s.drain_content(4).await;
                assert_eq!(body, b"iso0");
                got_deliver = true;
            }
            AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::Ack(a))) => {
                assert!(!got_confirm, "duplicate confirm");
                assert!(a.delivery_tag >= 1, "confirm sequence starts at 1");
                assert!(a.multiple || a.delivery_tag == 1);
                got_confirm = true;
            }
            AMQPFrame::Method(3, _) => panic!("nowait declare on channel 3 leaked a reply"),
            other => panic!("frame crossed channels or unexpected: {other:?}"),
        }
    }
    assert!(got_deliver, "delivery must arrive on channel 2");
    assert!(got_confirm, "confirm must arrive on channel 1");
    let _ = conn_close(&mut s).await;
}

async fn conn_close(s: &mut RawSession) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    s.sock
        .write_all(&encode_frame(&AMQPFrame::Method(
            0,
            AMQPClass::Connection(conn7::AMQPMethod::Close(conn7::Close {
                reply_code: 200,
                reply_text: "bye".into(),
                class_id: 0,
                method_id: 0,
            })),
        )))
        .await
}
