//! FR-S02/T20 slice: the TLS listener — a full AMQP handshake completes
//! over TLS with the self-signed cert properly verified as a root (no
//! danger-config), and plaintext AMQP to the TLS port is refused.

use std::sync::Arc;
use std::time::Duration;

use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::{connection as conn7, AMQPClass};
use amq_protocol::types::LongString;

use rusty_mq_protocol::{encode_frame, FrameReader, ProtocolLimits, PROTOCOL_HEADER_0_9_1};

/// Generate a self-signed cert/key pair; returns (cert PEM, key PEM, DER).
fn self_signed() -> (String, String, Vec<u8>) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    (
        certified.cert.pem(),
        certified.key_pair.serialize_pem(),
        certified.cert.der().to_vec(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn amqp_handshake_completes_over_verified_tls() {
    let dir = std::env::temp_dir().join(format!("rmq-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (cert_pem, key_pem, cert_der) = self_signed();
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("server.pem");
    let key_path = dir.join("server-key.pem");
    std::fs::write(&cert_path, cert_pem).unwrap();
    std::fs::write(&key_path, key_pem).unwrap();

    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = rusty_mq::tls::load(&cert_path, &key_path).unwrap().acceptor;
    let b = broker.clone();
    tokio::spawn(rusty_mq::server::serve_tls_shared(listener, b, acceptor));

    // Client: rustls with our self-signed cert installed as the ONLY root —
    // real verification, no disabled checks.
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots
        .add(tokio_rustls::rustls::pki_types::CertificateDer::from(
            cert_der,
        ))
        .unwrap();
    let config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost")
        .unwrap()
        .to_owned();
    let tls = connector.connect(server_name, tcp).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Drive the AMQP handshake over TLS at the frame level.
    let mut tls = tls;
    tls.write_all(&PROTOCOL_HEADER_0_9_1).await.unwrap();
    let start_ok = AMQPFrame::Method(
        0,
        AMQPClass::Connection(conn7::AMQPMethod::StartOk(conn7::StartOk {
            client_properties: amq_protocol::types::FieldTable::default(),
            mechanism: "PLAIN".into(),
            response: LongString::from(vec![
                0, b'g', b'u', b'e', b's', b't', 0, b'g', b'u', b'e', b's', b't',
            ]),
            locale: "en_US".into(),
        })),
    );
    tls.write_all(&encode_frame(&start_ok)).await.unwrap();

    let limits = ProtocolLimits::default();
    let negotiated = limits
        .negotiate(limits.max_channel_max, limits.max_frame_max, 0)
        .unwrap();
    let mut reader = FrameReader::new_post_header(&negotiated);
    let mut buf = vec![0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut opened = false;
    while tokio::time::Instant::now() < deadline && !opened {
        let n = match tokio::time::timeout(Duration::from_secs(2), tls.read(&mut buf)).await {
            Err(_) => continue, // step timeout; loop deadline governs
            Ok(Ok(0)) => panic!("TLS stream closed mid-handshake"),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => panic!("tls read: {e}"),
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
                    tls.write_all(&encode_frame(&tune_ok)).await.unwrap();
                    let open = AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(conn7::AMQPMethod::Open(conn7::Open {
                            virtual_host: "/".into(),
                        })),
                    );
                    tls.write_all(&encode_frame(&open)).await.unwrap();
                }
                AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::OpenOk(_))) => {
                    opened = true;
                }
                _ => {}
            }
        }
    }
    assert!(opened, "AMQP handshake completed over verified TLS");

    // A channel opens too (the full connection state machine ran).
    let ch_open = AMQPFrame::Method(
        1,
        AMQPClass::Channel(amq_protocol::protocol::channel::AMQPMethod::Open(
            Default::default(),
        )),
    );
    tls.write_all(&encode_frame(&ch_open)).await.unwrap();
    let mut saw_open_ok = false;
    while tokio::time::Instant::now() < deadline && !saw_open_ok {
        let n = match tokio::time::timeout(Duration::from_secs(2), tls.read(&mut buf)).await {
            Err(_) => continue,
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(_)) => break,
        };
        reader.feed(&buf[..n]).unwrap();
        while let Ok(Some(frame)) = reader.next_frame() {
            if let AMQPFrame::Method(1, AMQPClass::Channel(_)) = frame {
                saw_open_ok = true;
            }
        }
    }
    assert!(saw_open_ok, "channel.open-ok over TLS");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_amqp_to_tls_port_is_refused() {
    let dir = std::env::temp_dir().join(format!("rmq-tls-plain-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (cert_pem, key_pem, _) = self_signed();
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("server.pem");
    let key_path = dir.join("server-key.pem");
    std::fs::write(&cert_path, cert_pem).unwrap();
    std::fs::write(&key_path, key_pem).unwrap();

    let broker = Arc::new(rusty_mq::Broker::new("guest".into(), "guest".into()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = rusty_mq::tls::load(&cert_path, &key_path).unwrap().acceptor;
    let b = broker.clone();
    tokio::spawn(rusty_mq::server::serve_tls_shared(listener, b, acceptor));

    // Send a plaintext AMQP protocol header: the TLS handshake cannot
    // proceed. The server answers with a TLS fatal alert record (content
    // type 21) — never AMQP bytes — and closes; either an immediate close
    // or alert-then-close is a correct refusal.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(&PROTOCOL_HEADER_0_9_1).await.unwrap();
    let mut buf = vec![0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(3), tcp.read(&mut buf)).await;
    match read {
        Err(_) => panic!("server left a plaintext connection hanging"),
        Ok(Ok(0)) | Ok(Err(_)) => {} // closed: correct
        Ok(Ok(n)) => {
            let bytes = &buf[..n];
            assert_eq!(
                bytes[0], 21,
                "response must be a TLS alert, not AMQP: {bytes:?}"
            );
            // Alert-then-close: the follow-up read must end the stream.
            let after = tokio::time::timeout(Duration::from_secs(3), tcp.read(&mut buf)).await;
            match after {
                Err(_) => panic!("connection left open after alert"),
                Ok(Ok(0)) | Ok(Err(_)) => {}
                Ok(Ok(m)) => panic!("unexpected additional {m} bytes: {:?}", &buf[..m]),
            }
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
