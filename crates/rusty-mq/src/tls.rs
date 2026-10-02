//! TLS listener configuration (FR-S02): load a PEM cert/key pair into a
//! `TlsAcceptor` once at startup; failures refuse startup loudly.

use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

pub struct TlsSetup {
    pub acceptor: TlsAcceptor,
}

/// Load a PEM certificate chain + key into a ServerConfig (no client-cert
/// mutual TLS in V1; the management/AMQP planes authenticate at their own
/// layers).
pub fn load(cert_path: &Path, key_path: &Path) -> io::Result<TlsSetup> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut io::BufReader::new(std::fs::File::open(cert_path)?))
            .collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no certificates found in {}", cert_path.display()),
        ));
    }
    let key: Option<PrivateKeyDer<'static>> =
        rustls_pemfile::private_key(&mut io::BufReader::new(std::fs::File::open(key_path)?))?;
    let key = key.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no private key found in {}", key_path.display()),
        )
    })?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("bad TLS material: {e}"))
        })?;
    Ok(TlsSetup {
        acceptor: TlsAcceptor::from(Arc::new(config)),
    })
}
