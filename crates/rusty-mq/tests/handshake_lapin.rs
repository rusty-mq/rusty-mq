//! T01 (partial): a real AMQP client (Rust `lapin`, one of the five target
//! clients) completes the handshake, opens and closes channels, and closes
//! the connection cleanly — the M1 exit-gate slice.
//!
//! Also covers T10/T20 slices: unknown method on an open channel must
//! produce a channel-scoped 540 close without killing other channels, and
//! bad credentials / unknown vhost must be refused at connection scope.

use std::time::Duration;

use lapin::{options::BasicQosOptions, Connection, ConnectionProperties};

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

#[tokio::test(flavor = "multi_thread")]
async fn lapin_handshake_channel_lifecycle_and_close() {
    let addr = start_broker().await;
    let uri = format!("amqp://guest:guest@{addr}/%2F");

    let conn = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect within timeout")
    .expect("lapin completes AMQP handshake with rusty-mq");

    // FR-P03: channels are independent.
    let ch1 = conn.create_channel().await.expect("channel 1 opens");
    let ch2 = conn.create_channel().await.expect("channel 2 opens");

    // basic.qos is implemented (M3): it must succeed now.
    ch2.basic_qos(1, BasicQosOptions::default())
        .await
        .expect("basic.qos succeeds");

    // Strict profile: deferred methods still close the channel with 540
    // NOT_IMPLEMENTED (tx.* per the feature matrix) — lapin surfaces this
    // as an error.
    ch2.tx_select().await.expect_err("tx.select must be 540");

    // ch1 must be unaffected by ch2's channel-scoped error.
    ch1.close(200, "bye".into())
        .await
        .expect("channel 1 closes cleanly after ch2 errored");

    conn.close(200, "bye".into())
        .await
        .expect("connection closes cleanly");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_password_is_refused() {
    let addr = start_broker().await;
    let uri = format!("amqp://guest:wrongpassword@{addr}/%2F");
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect attempt completes within timeout");
    assert!(result.is_err(), "auth must be refused for bad credentials");
}

/// Unknown vhost must be refused with a connection-scope 403 close.
///
/// Verified at the frame level rather than through lapin's high-level
/// `connect`: lapin 4.12 has a known client-side gap where a server
/// `connection.close` arriving while it awaits `connection.open-ok` never
/// rejects its connect future (see lapin issues incl. #237; recorded in
/// docs/implementation-status.md). The server behavior below is the
/// protocol-correct part and is what this test pins.
///
/// Reads use bounded `try_read` polling: an in-process test client socket
/// paired with an in-process server in one runtime can miss the async
/// readiness wake; polling keeps the test deterministic.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_vhost_is_refused_with_403_close() {
    use amq_protocol::frame::AMQPFrame;
    use amq_protocol::protocol::{connection, AMQPClass};
    use amq_protocol::types::LongString;
    use rusty_mq_protocol::{encode_frame, FrameReader, ProtocolLimits, PROTOCOL_HEADER_0_9_1};
    use tokio::io::AsyncWriteExt;

    let addr = start_broker().await;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(&PROTOCOL_HEADER_0_9_1).await.unwrap();

    let start_ok = AMQPFrame::Method(
        0,
        AMQPClass::Connection(connection::AMQPMethod::StartOk(connection::StartOk {
            client_properties: Default::default(),
            mechanism: "PLAIN".into(),
            response: LongString::from(vec![
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
    // Server-to-client direction: no protocol header expected.
    let mut reader = FrameReader::new_post_header(&negotiated);
    let mut buf = vec![0u8; 4096];
    let mut close_seen = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    // Drive: expect tune, send tune-ok, send open("nope"), expect close(403).
    while !close_seen && tokio::time::Instant::now() < deadline {
        let n = match sock.try_read(&mut buf) {
            Ok(0) => break, // server closed the socket
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
                AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Tune(t))) => {
                    let tune_ok = AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(connection::AMQPMethod::TuneOk(connection::TuneOk {
                            channel_max: t.channel_max,
                            frame_max: t.frame_max,
                            heartbeat: t.heartbeat,
                        })),
                    );
                    sock.write_all(&encode_frame(&tune_ok)).await.unwrap();
                    let open = AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(connection::AMQPMethod::Open(connection::Open {
                            virtual_host: "nope".into(),
                        })),
                    );
                    sock.write_all(&encode_frame(&open)).await.unwrap();
                }
                AMQPFrame::Method(
                    0,
                    AMQPClass::Connection(connection::AMQPMethod::Close(close)),
                ) => {
                    assert_eq!(close.reply_code, 403, "must be ACCESS_REFUSED");
                    assert!(
                        close.reply_text.as_str().contains("vhost"),
                        "close text mentions vhost: {}",
                        close.reply_text
                    );
                    close_seen = true;
                }
                _ => {}
            }
        }
    }
    assert!(close_seen, "server must send connection.close(403)");
}
