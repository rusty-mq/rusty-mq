//! T06/T04 slices: publish → routing → basic.get round trips through a real
//! client (lapin): direct/topic/fanout/default-exchange routing, INV-05
//! single enqueue per destination, typed property preservation, message
//! counts, manual-ack settlement, and requeue-on-channel-close (FR-C06).
//!
//! The mandatory-return behavior is verified at the frame level in
//! `mandatory_return_frame_level` because lapin only surfaces basic.return
//! through confirm mode (not yet implemented).

use std::time::Duration;

use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};

async fn start_broker() -> std::net::SocketAddr {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
    let broker = rusty_mq::Broker::new("guest".into(), "guest".into());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener(listener, broker));
    addr
}

async fn connect(addr: std::net::SocketAddr) -> Connection {
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect within timeout")
    .expect("handshake")
}

fn durable_queue() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..Default::default()
    }
}

async fn declare_queue(ch: &Channel, name: &str) {
    ch.queue_declare(name.into(), durable_queue(), FieldTable::default())
        .await
        .expect("queue declare");
}

fn sample_properties() -> BasicProperties {
    let mut headers = FieldTable::default();
    headers.insert("x-num".into(), AMQPValue::LongInt(42));
    headers.insert("x-str".into(), AMQPValue::LongString("héllo wörld".into()));
    headers.insert("x-bool".into(), AMQPValue::Boolean(true));
    BasicProperties::default()
        .with_content_type("application/json".into())
        .with_headers(headers)
        .with_delivery_mode(2)
        .with_priority(5)
        .with_correlation_id("corr-1".into())
        .with_message_id("msg-1".into())
        .with_app_id("test-suite".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_route_get_roundtrip_with_properties() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    declare_queue(&ch, "work").await;
    ch.exchange_declare(
        "ex.direct".into(),
        ExchangeKind::Direct,
        lapin::options::ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "work".into(),
        "ex.direct".into(),
        "tasks".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    // FR-M01: binary bodies including zeros and the frame-end octet.
    let bodies: Vec<Vec<u8>> = vec![
        b"first".to_vec(),
        vec![0u8, 1, 0xCE, 0x00, 255, 0xCE],
        "unicode payload ✓".as_bytes().to_vec(),
    ];
    for body in &bodies {
        ch.basic_publish(
            "ex.direct".into(),
            "tasks".into(),
            BasicPublishOptions::default(),
            body,
            sample_properties(),
        )
        .await
        .unwrap();
    }
    // Unmatched routing key must not reach the queue.
    ch.basic_publish(
        "ex.direct".into(),
        "other".into(),
        BasicPublishOptions::default(),
        b"nope".as_ref(),
        BasicProperties::default(),
    )
    .await
    .unwrap();

    // FIFO out, with descending message counts (FR ordering §7.3).
    for (i, body) in bodies.iter().enumerate() {
        let msg = ch
            .basic_get("work".into(), BasicGetOptions { no_ack: true })
            .await
            .unwrap()
            .expect("message present");
        assert_eq!(msg.delivery.data, *body, "bit-identical body");
        assert_eq!(msg.delivery.exchange.as_str(), "ex.direct");
        assert_eq!(msg.delivery.routing_key.as_str(), "tasks");
        assert_eq!(msg.message_count, (bodies.len() - i - 1) as u32);
        // FR-M02: typed properties survive routing and storage.
        let p = &msg.delivery.properties;
        assert_eq!(
            p.content_type().as_ref().map(|s| s.as_str()),
            Some("application/json")
        );
        assert_eq!(*p.delivery_mode(), Some(2));
        assert_eq!(*p.priority(), Some(5));
        assert_eq!(
            p.correlation_id().as_ref().map(|s| s.as_str()),
            Some("corr-1")
        );
        let headers = p.headers().as_ref().expect("headers preserved");
        assert_eq!(headers.inner().get("x-num"), Some(&AMQPValue::LongInt(42)));
        assert_eq!(
            headers.inner().get("x-bool"),
            Some(&AMQPValue::Boolean(true))
        );
        match headers.inner().get("x-str") {
            Some(AMQPValue::LongString(s)) => {
                assert_eq!(s.as_bytes(), "héllo wörld".as_bytes());
            }
            other => panic!("x-str not a long string: {other:?}"),
        }
    }

    // Queue drained; the unmatched publish never arrived.
    let empty = ch
        .basic_get("work".into(), BasicGetOptions { no_ack: true })
        .await
        .unwrap();
    assert!(empty.is_none(), "queue must be empty");

    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_fanout_routing_and_inv05() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();

    ch.exchange_declare(
        "ex.topic".into(),
        ExchangeKind::Topic,
        lapin::options::ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "ex.fanout".into(),
        ExchangeKind::Fanout,
        lapin::options::ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    declare_queue(&ch, "t1").await;
    declare_queue(&ch, "t2").await;
    declare_queue(&ch, "f1").await;
    declare_queue(&ch, "f2").await;

    ch.queue_bind(
        "t1".into(),
        "ex.topic".into(),
        "a.*.c".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "t2".into(),
        "ex.topic".into(),
        "a.#.c".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    // INV-05: two bindings matching the same key on t1 must still produce a
    // single enqueue.
    ch.queue_bind(
        "t1".into(),
        "ex.topic".into(),
        "a.b.#".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "f1".into(),
        "ex.fanout".into(),
        "".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "f2".into(),
        "ex.fanout".into(),
        "".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    // "a.b.c" matches t1 (twice via bindings), t2, and both fanout queues.
    ch.basic_publish(
        "ex.topic".into(),
        "a.b.c".into(),
        BasicPublishOptions::default(),
        b"topic-msg".as_ref(),
        BasicProperties::default(),
    )
    .await
    .unwrap();
    ch.basic_publish(
        "ex.fanout".into(),
        "ignored".into(),
        BasicPublishOptions::default(),
        b"fanout-msg".as_ref(),
        BasicProperties::default(),
    )
    .await
    .unwrap();

    let get = |q: &str| {
        let ch = ch.clone();
        let q = q.to_string();
        async move {
            ch.basic_get(q.into(), BasicGetOptions { no_ack: true })
                .await
                .unwrap()
        }
    };

    let m = get("t1").await.expect("t1 first copy");
    assert_eq!(m.delivery.data, b"topic-msg".as_ref());
    assert!(
        get("t1").await.is_none(),
        "INV-05: exactly one enqueue despite two matching bindings"
    );
    let m = get("t2").await.expect("t2 got its copy");
    assert_eq!(m.delivery.data, b"topic-msg".as_ref());
    assert!(get("t2").await.is_none());

    for q in ["f1", "f2"] {
        let m = get(q).await.expect("fanout queue got a copy");
        assert_eq!(m.delivery.data, b"fanout-msg".as_ref());
        assert!(get(q).await.is_none());
    }

    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn default_exchange_routes_by_queue_name() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    declare_queue(&ch, "by-name").await;
    // FR-E07: routing key resolves to the queue name inside the vhost.
    ch.basic_publish(
        "".into(),
        "by-name".into(),
        BasicPublishOptions::default(),
        b"default-ex".as_ref(),
        BasicProperties::default(),
    )
    .await
    .unwrap();
    let m = ch
        .basic_get("by-name".into(), BasicGetOptions { no_ack: true })
        .await
        .unwrap()
        .expect("routed via default exchange");
    assert_eq!(m.delivery.data, b"default-ex".as_ref());
    assert_eq!(m.delivery.exchange.as_str(), "");
    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_ack_settles_and_requeues_on_channel_close() {
    let addr = start_broker().await;
    let conn = connect(addr).await;

    // Two messages consumed with manual ack on channel 1.
    let ch1 = conn.create_channel().await.unwrap();
    declare_queue(&ch1, "manual").await;
    for body in [b"one".as_ref(), b"two".as_ref()] {
        ch1.basic_publish(
            "".into(),
            "manual".into(),
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        )
        .await
        .unwrap();
    }
    let m1 = ch1
        .basic_get("manual".into(), BasicGetOptions { no_ack: false })
        .await
        .unwrap()
        .expect("first");
    assert_eq!(m1.delivery.data, b"one".as_ref());
    let m2 = ch1
        .basic_get("manual".into(), BasicGetOptions { no_ack: false })
        .await
        .unwrap()
        .expect("second");
    assert_eq!(m2.delivery.data, b"two".as_ref());

    // Ack all outstanding via multiple on the highest tag; tags are
    // channel-scoped and monotonic (FR-C04, §6.1 multiple settlement).
    assert!(m1.delivery.delivery_tag < m2.delivery.delivery_tag);
    m2.delivery
        .acker
        .ack(BasicAckOptions { multiple: true })
        .await
        .expect("ack multiple");

    // Closing the channel requeues any outstanding unacked delivery
    // (FR-C06); here both are settled, so nothing comes back.
    ch1.close(200, "done".into()).await.unwrap();
    let ch2 = conn.create_channel().await.unwrap();
    assert!(
        ch2.basic_get("manual".into(), BasicGetOptions { no_ack: false })
            .await
            .unwrap()
            .is_none(),
        "acked entries must not reappear"
    );

    // Now an UNACKED delivery requeues when its channel closes.
    ch2.basic_publish(
        "".into(),
        "manual".into(),
        BasicPublishOptions::default(),
        b"three".as_ref(),
        BasicProperties::default(),
    )
    .await
    .unwrap();
    let m3 = ch2
        .basic_get("manual".into(), BasicGetOptions { no_ack: false })
        .await
        .unwrap()
        .expect("third");
    assert_eq!(m3.delivery.data, b"three".as_ref());
    ch2.close(200, "leaving unacked".into()).await.unwrap();

    let ch3 = conn.create_channel().await.unwrap();
    let m3 = ch3
        .basic_get("manual".into(), BasicGetOptions { no_ack: false })
        .await
        .unwrap()
        .expect("requeued after channel close");
    assert_eq!(m3.delivery.data, b"three".as_ref());
    assert!(
        m3.delivery.redelivered,
        "requeued deliveries carry the redelivered hint"
    );
    m3.delivery
        .acker
        .ack(BasicAckOptions::default())
        .await
        .expect("settle");

    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_delivery_mode_is_rejected() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    declare_queue(&ch, "mode-q").await;
    // lapin resolves the publish write immediately; the server rejects the
    // content at header time and closes the channel — observed here on the
    // next channel operation.
    ch.basic_publish(
        "".into(),
        "mode-q".into(),
        BasicPublishOptions::default(),
        b"x".as_ref(),
        BasicProperties::default().with_delivery_mode(9),
    )
    .await
    .expect("write accepted");
    let err = ch
        .queue_declare("mode-q".into(), durable_queue(), FieldTable::default())
        .await
        .expect_err("channel must be closed by the rejected publish");
    assert!(
        err.to_string().to_lowercase().contains("invalid")
            || err.to_string().to_lowercase().contains("command"),
        "expected COMMAND_INVALID from the close, got: {err}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn expiration_property_is_rejected() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    declare_queue(&ch, "ttl-q").await;
    ch.basic_publish(
        "".into(),
        "ttl-q".into(),
        BasicPublishOptions::default(),
        b"x".as_ref(),
        BasicProperties::default().with_expiration("60000".into()),
    )
    .await
    .expect("write accepted");
    let err = ch
        .queue_declare("ttl-q".into(), durable_queue(), FieldTable::default())
        .await
        .expect_err("channel must be closed by the rejected publish");
    assert!(
        err.to_string().to_lowercase().contains("not_implemented")
            || err.to_string().to_lowercase().contains("not open"),
        "channel must be closed (540 NOT_IMPLEMENTED or local closed state), got: {err}"
    );
    // The substantive outcome: the expired-TTL message was never enqueued.
    let ch2 = conn.create_channel().await.unwrap();
    let got = ch2
        .basic_get("ttl-q".into(), BasicGetOptions { no_ack: true })
        .await
        .unwrap();
    assert!(got.is_none(), "rejected publish must not be enqueued");
    let _ = conn.close(200, "bye".into()).await;
}

/// FR-PUB02 at the frame level: mandatory publish with zero destinations
/// returns basic.return(312 NO_ROUTE) followed by the message content,
/// before any positive outcome could be inferred.
#[tokio::test(flavor = "multi_thread")]
async fn mandatory_return_frame_level() {
    use amq_protocol::frame::AMQPFrame;
    use amq_protocol::protocol::{basic as basic7, connection as conn7, AMQPClass};
    use amq_protocol::types::LongString as LongString7;
    use rusty_mq_protocol::{encode_frame, FrameReader, ProtocolLimits, PROTOCOL_HEADER_0_9_1};
    use tokio::io::AsyncWriteExt;

    let addr = start_broker().await;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(&PROTOCOL_HEADER_0_9_1).await.unwrap();

    let start_ok = AMQPFrame::Method(
        0,
        AMQPClass::Connection(conn7::AMQPMethod::StartOk(conn7::StartOk {
            client_properties: Default::default(),
            mechanism: "PLAIN".into(),
            response: LongString7::from(vec![
                0, b'g', b'u', b'e', b's', b't', 0, b'g', b'u', b'e', b's', b't',
            ]),
            locale: "en_US".into(),
        })),
    );
    sock.write_all(&encode_frame(&start_ok)).await.unwrap();

    let limits = ProtocolLimits::default();
    let negotiated = limits
        .negotiate(limits.max_channel_max, limits.max_frame_max, 0)
        .unwrap();
    let mut reader = FrameReader::new_post_header(&negotiated);
    let mut buf = vec![0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got_return = false;
    let mut channel_opened = false;

    while tokio::time::Instant::now() < deadline {
        let n = match sock.try_read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            Err(e) => panic!("socket error: {e}"),
        };
        reader.feed(&buf[..n]).unwrap();
        while let Ok(Some(frame)) = reader.next_frame() {
            match frame {
                AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::Tune(t))) => {
                    let tune_ok = AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(conn7::AMQPMethod::TuneOk(conn7::TuneOk {
                            channel_max: t.channel_max,
                            frame_max: t.frame_max,
                            heartbeat: t.heartbeat,
                        })),
                    );
                    sock.write_all(&encode_frame(&tune_ok)).await.unwrap();
                    let open = AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(conn7::AMQPMethod::Open(conn7::Open {
                            virtual_host: "/".into(),
                        })),
                    );
                    sock.write_all(&encode_frame(&open)).await.unwrap();
                }
                AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::OpenOk(_))) => {
                    let ch_open = AMQPFrame::Method(
                        1,
                        AMQPClass::Channel(amq_protocol::protocol::channel::AMQPMethod::Open(
                            Default::default(),
                        )),
                    );
                    sock.write_all(&encode_frame(&ch_open)).await.unwrap();
                }
                AMQPFrame::Method(1, AMQPClass::Channel(_)) => {
                    channel_opened = true;
                    // Declare an exchange with no bindings: valid publish,
                    // zero destinations.
                    let declare = AMQPFrame::Method(
                        1,
                        AMQPClass::Exchange(amq_protocol::protocol::exchange::AMQPMethod::Declare(
                            amq_protocol::protocol::exchange::Declare {
                                exchange: "ex.nobind".into(),
                                kind: "fanout".into(),
                                passive: false,
                                durable: false,
                                auto_delete: false,
                                internal: false,
                                nowait: false,
                                arguments: Default::default(),
                            },
                        )),
                    );
                    sock.write_all(&encode_frame(&declare)).await.unwrap();
                }
                AMQPFrame::Method(1, AMQPClass::Exchange(_)) => {
                    // DeclareOk: now a mandatory publish that cannot route.
                    let publish = AMQPFrame::Method(
                        1,
                        AMQPClass::Basic(basic7::AMQPMethod::Publish(basic7::Publish {
                            exchange: "ex.nobind".into(),
                            routing_key: "k".into(),
                            mandatory: true,
                            immediate: false,
                        })),
                    );
                    sock.write_all(&encode_frame(&publish)).await.unwrap();
                    let header = AMQPFrame::Header(
                        1,
                        60,
                        Box::new(amq_protocol::frame::AMQPContentHeader {
                            class_id: 60,
                            body_size: 3,
                            properties: Default::default(),
                        }),
                    );
                    sock.write_all(&encode_frame(&header)).await.unwrap();
                    let body = AMQPFrame::Body(1, b"abc".to_vec());
                    sock.write_all(&encode_frame(&body)).await.unwrap();
                }
                AMQPFrame::Method(1, AMQPClass::Basic(basic7::AMQPMethod::Return(r))) => {
                    assert_eq!(r.reply_code, 312, "must be NO_ROUTE");
                    assert_eq!(r.reply_text.as_str(), "NO_ROUTE");
                    assert_eq!(r.exchange.as_str(), "ex.nobind");
                    got_return = true;
                }
                _ => {}
            }
        }
        if got_return && channel_opened {
            break;
        }
    }
    assert!(channel_opened, "channel must open");
    assert!(got_return, "server must return the mandatory message");
}
