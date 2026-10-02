//! TCP accept loop and connection admission (ADR-0004 budgets).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::broker::Broker;
use crate::connection::Connection;

/// Hard connection cap for the development alpha (config surface later).
const MAX_CONNECTIONS: usize = 1024;

pub async fn serve(listen: SocketAddr, broker: Broker) -> std::io::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    tracing::info!(%listen, "rusty-mq listening (AMQP 0-9-1, memory-backed, M1 scope)");
    serve_listener(listener, broker).await;
    Ok(())
}

/// TLS accept loop: wraps accepted sockets in the loaded acceptor and
/// runs the SAME connection state machine over the TLS stream (FR-S02).
pub async fn serve_tls_shared(
    listener: TcpListener,
    broker: Arc<Broker>,
    acceptor: tokio_rustls::TlsAcceptor,
) {
    let live = Arc::new(AtomicUsize::new(0));
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("tls accept error: {e}");
                continue;
            }
        };
        if live.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
            tracing::warn!("connection cap reached; refusing");
            drop(socket);
            continue;
        }
        live.fetch_add(1, Ordering::Relaxed);
        let broker = broker.clone();
        let live = live.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            match acceptor.accept(socket).await {
                Ok(stream) => {
                    Connection::run(stream, peer.to_string(), broker).await;
                }
                Err(e) => {
                    tracing::debug!(peer = %peer, error = %e, "tls handshake failed");
                }
            }
            live.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

/// Serve with a shared broker handle (the management API holds one too).
pub async fn serve_shared(listen: SocketAddr, broker: Arc<Broker>) -> std::io::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    tracing::info!(%listen, "rusty-mq listening (AMQP 0-9-1)");
    serve_listener_shared(listener, broker).await;
    Ok(())
}

/// Accept loop over an already-bound, shared broker (tests that need a
/// live handle for failpoint control).
pub async fn serve_listener_shared(listener: TcpListener, broker: Arc<Broker>) {
    let live = Arc::new(AtomicUsize::new(0));
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };
        if live.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
            tracing::warn!("connection cap reached; refusing");
            drop(socket);
            continue;
        }
        live.fetch_add(1, Ordering::Relaxed);
        let broker = broker.clone();
        let live = live.clone();
        tokio::spawn(async move {
            Connection::run(socket, peer.to_string(), broker).await;
            live.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

/// Accept loop over an already-bound listener (tests bind port 0 first).
pub async fn serve_listener(listener: TcpListener, broker: Broker) {
    let broker = Arc::new(broker);
    let live = Arc::new(AtomicUsize::new(0));
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };
        if live.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
            tracing::warn!("connection cap reached; refusing");
            drop(socket);
            continue;
        }
        live.fetch_add(1, Ordering::Relaxed);
        let broker = broker.clone();
        let live = live.clone();
        tokio::spawn(async move {
            Connection::run(socket, peer.to_string(), broker).await;
            live.fetch_sub(1, Ordering::Relaxed);
        });
    }
}
