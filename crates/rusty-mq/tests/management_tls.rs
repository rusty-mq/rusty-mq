//! §13: management exposed over TLS — the PRD requires authenticated TLS
//! when the management plane is reachable beyond loopback. Round-trip
//! test: rcgen self-signed material, `serve_tls` (axum over a TLS
//! listener), and a rustls client with the cert as its only root doing
//! a real verified HTTPS request.

use std::sync::Arc;

fn self_signed() -> (String, String, Vec<u8>) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    (
        certified.cert.pem(),
        certified.key_pair.serialize_pem(),
        certified.cert.der().to_vec(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn management_https_round_trip() {
    let dir = std::env::temp_dir().join(format!("rmq-mgmt-tls-{}", std::process::id()));
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
    let app = rusty_mq_management::router(broker);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = rusty_mq::tls::load(&cert_path, &key_path).unwrap().acceptor;
    tokio::spawn(rusty_mq_management::serve_tls(listener, app, acceptor));

    // Client: rustls with the self-signed cert as the ONLY root — real
    // verification, no disabled checks.
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
    let mut tls = connector.connect(server_name, tcp).await.unwrap();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tls.write_all(b"GET /health/live HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    tls.read_to_end(&mut response).await.unwrap();
    let head = String::from_utf8_lossy(&response[..response.len().min(64)]).into_owned();
    assert!(
        head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.0 200"),
        "expected 200 over verified TLS, got: {head:?}"
    );

    // And the config layer enforces the remote rule: non-loopback
    // management without TLS material is refused at validation.
    let cfg = rusty_mq::config::Config::default();
    assert!(cfg.management.remote_requires_tls);
    let _ = std::fs::remove_dir_all(&dir);
}
