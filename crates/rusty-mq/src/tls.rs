//! TLS listener configuration (FR-S02): load a PEM cert/key pair into a
//! `TlsAcceptor` once at startup; failures refuse startup loudly.
//!
//! PEM parsing uses the rustls-pki-types `PemObject` API (rustls' own
//! maintained path) — the separate `rustls-pemfile` crate is archived
//! (RUSTSEC-2025-0134) and was removed from the tree.

use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
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
        CertificateDer::pem_reader_iter(&mut io::BufReader::new(std::fs::File::open(cert_path)?))
            .collect::<Result<_, _>>()
            .map_err(|e| io::Error::other(format!("{}: {e}", cert_path.display())))?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no certificates found in {}", cert_path.display()),
        ));
    }
    let key: Option<PrivateKeyDer<'static>> =
        PrivateKeyDer::from_pem_reader(&mut io::BufReader::new(std::fs::File::open(key_path)?))
            .ok();
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
