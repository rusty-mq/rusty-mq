//! Per-connection state machine: handshake, channels, heartbeats, errors.
//!
//! Layout (ADR-0003): one reader loop drives the protocol; one writer task
//! serializes all socket writes behind a bounded queue. A full writer closes
//! the connection (bounded by construction in M1; the connection.blocked
//! extension lands with FR-R06/R07).
//!
//! After the server sends `connection.close` it keeps reading for the
//! client's `close-ok` (bounded linger) so clients can complete the close
//! handshake before the socket goes away.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::{channel, connection, exchange, queue, AMQPClass};
use amq_protocol::types::{FieldTable, LongString};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

use rusty_mq_core::ChannelGeneration;
use rusty_mq_protocol::{
    encode_frame, FrameReader, NegotiatedLimits, ProtocolError, ProtocolLimits,
    PROTOCOL_HEADER_0_9_1,
};

use crate::broker::Broker;

/// Bounded outbound queue per connection (frames waiting for the writer).
const OUTBOUND_CAP: usize = 256;
/// Read chunk size; frames larger than this simply span multiple chunks.
const READ_CHUNK: usize = 16 * 1024;
/// How long to wait for the client's `connection.close-ok` after the server
/// sent `connection.close`.
const CLOSE_LINGER: Duration = Duration::from_secs(5);

/// Handshake phase of the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Awaiting `connection.start-ok`.
    AwaitStartOk,
    /// Awaiting `connection.tune-ok`.
    AwaitTuneOk,
    /// Awaiting `connection.open`.
    AwaitOpen,
    /// Fully open; channels and (later) traffic.
    Running,
    /// Sent `connection.close`; awaiting close-ok or linger expiry.
    Closing,
}

/// Per-channel state (ADR-0003 generations).
struct ChannelState {
    #[allow(dead_code)] // consumed by delivery ownership from M3
    generation: ChannelGeneration,
}

/// Owns one accepted connection until it ends.
pub struct Connection {
    broker: Arc<Broker>,
    limits: ProtocolLimits,
    negotiated: Option<NegotiatedLimits>,
    phase: Phase,
    channels: HashMap<u16, ChannelState>,
    outbound: mpsc::Sender<Vec<u8>>,
}

impl Connection {
    /// Run a connection to completion. Never panics on socket errors.
    pub async fn run(socket: tokio::net::TcpStream, broker: Arc<Broker>) {
        let peer = socket
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".into());
        let conn_id = broker.next_connection_id();
        let (outbound_tx, outbound_rx) = mpsc::channel::<Vec<u8>>(OUTBOUND_CAP);
        let (read_half, write_half) = socket.into_split();

        let writer = tokio::spawn(writer_task(write_half, outbound_rx));

        let mut conn = Self {
            broker,
            limits: ProtocolLimits::default(),
            negotiated: None,
            phase: Phase::AwaitStartOk,
            channels: HashMap::new(),
            outbound: outbound_tx,
        };

        tracing::info!(connection = %conn_id, peer = %peer, "connection opened");
        conn.drive(read_half).await;

        // Reclaim connection-scoped resources (FR-P09): channels now;
        // exclusive/auto-delete queues from M2.
        conn.channels.clear();
        drop(conn.outbound);
        let _ = writer.await;
        tracing::info!(connection = %conn_id, peer = %peer, "connection closed");
    }

    /// Main read loop: handshake with deadline, then heartbeat idle detection.
    async fn drive(&mut self, mut read: OwnedReadHalf) {
        let handshake_deadline = tokio::time::Instant::now()
            + Duration::from_secs(self.limits.handshake_timeout_seconds as u64);
        let mut reader = FrameReader::new(&self.server_view_limits());
        let mut buf = vec![0u8; READ_CHUNK];

        // Protocol header: read until one complete frame header is buffered.
        loop {
            match tokio::time::timeout_at(handshake_deadline, read.read(&mut buf)).await {
                Ok(Ok(0)) => return, // EOF before header
                Ok(Ok(n)) => {
                    if reader.feed(&buf[..n]).is_err() {
                        return;
                    }
                }
                Ok(Err(_)) | Err(_) => {
                    tracing::debug!("handshake read failed or timed out");
                    return;
                }
            }
            match reader.next_frame() {
                Ok(Some(AMQPFrame::ProtocolHeader(_))) => break,
                Ok(Some(_)) => {
                    self.protocol_error(
                        0,
                        &ProtocolError::command_invalid("first frame must be the protocol header"),
                    )
                    .await;
                    return;
                }
                Ok(None) => continue, // partial header; keep reading
                Err(e) => {
                    // Unsupported AMQP version or garbage: reply with our
                    // header so well-behaved clients can diagnose, then stop.
                    let _ = self.outbound.try_send(PROTOCOL_HEADER_0_9_1.to_vec());
                    self.protocol_error(0, &e).await;
                    return;
                }
            }
        }

        if self.send_start().await.is_err() {
            return;
        }

        loop {
            // Drain any frames already buffered (e.g. a start-ok that
            // arrived in the same TCP segment as the protocol header).
            loop {
                match reader.next_frame() {
                    Ok(Some(frame)) => {
                        if !self.handle_frame(frame).await {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        self.protocol_error(0, &e).await;
                        // Framing errors are fatal: stop without lingering.
                        return;
                    }
                }
            }
            // Then block for more bytes.
            let idle = self.idle_timeout(handshake_deadline);
            let n = match tokio::time::timeout(idle, read.read(&mut buf)).await {
                Ok(Ok(n)) => n,
                Ok(Err(_)) => break,
                Err(_) => {
                    tracing::debug!("idle timeout");
                    break;
                }
            };
            if n == 0 {
                break; // EOF
            }
            if reader.feed(&buf[..n]).is_err() {
                tracing::warn!("reader budget exceeded");
                break;
            }
        }
    }

    /// Idle deadline: handshake timeout until Running; 2× the negotiated
    /// heartbeat when Running (0 disables); a short linger while Closing.
    fn idle_timeout(&self, handshake_deadline: tokio::time::Instant) -> Duration {
        match self.phase {
            Phase::Running => match self.negotiated.as_ref().map(|n| n.heartbeat_seconds) {
                Some(0) | None => Duration::from_secs(3600),
                Some(hb) => Duration::from_secs(2 * hb as u64),
            },
            Phase::Closing => CLOSE_LINGER,
            _ => handshake_deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .max(Duration::from_millis(1)),
        }
    }

    /// Inbound budget before negotiation: server defaults (safe superset).
    fn server_view_limits(&self) -> NegotiatedLimits {
        self.limits
            .negotiate(self.limits.max_channel_max, self.limits.max_frame_max, 0)
            .expect("server defaults self-negotiate")
    }

    async fn send_start(&mut self) -> Result<(), ()> {
        let mut props = FieldTable::default();
        props.insert(
            "product".into(),
            amq_protocol::types::AMQPValue::LongString("rusty-mq".into()),
        );
        props.insert(
            "version".into(),
            amq_protocol::types::AMQPValue::LongString(env!("CARGO_PKG_VERSION").into()),
        );
        // M1: no extension capabilities advertised (ADR-0005/§4.3).
        props.insert(
            "capabilities".into(),
            amq_protocol::types::AMQPValue::FieldTable(FieldTable::default()),
        );
        props.insert(
            "platform".into(),
            amq_protocol::types::AMQPValue::LongString("Rust".into()),
        );
        let start = connection::AMQPMethod::Start(connection::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: props,
            mechanisms: LongString::from("PLAIN".to_string()),
            locales: LongString::from("en_US".to_string()),
        });
        self.send(AMQPFrame::Method(0, AMQPClass::Connection(start)))
            .await
    }

    /// Handle one complete inbound frame. Returns false when the connection
    /// is finished.
    async fn handle_frame(&mut self, frame: AMQPFrame) -> bool {
        match frame {
            AMQPFrame::Heartbeat(_) => self.send(AMQPFrame::Heartbeat(0)).await.is_ok(),
            AMQPFrame::Header(channel_id, _class_id, _header) => {
                // No publish path until M2: content frames are unexpected.
                let e = ProtocolError::unexpected_frame(format!(
                    "content header on channel {channel_id} (publishing lands in M2)"
                ))
                .with_fatal();
                self.protocol_error(channel_id, &e).await
            }
            AMQPFrame::Body(channel_id, _) => {
                let e = ProtocolError::unexpected_frame(format!(
                    "content body on channel {channel_id} (publishing lands in M2)"
                ))
                .with_fatal();
                self.protocol_error(channel_id, &e).await
            }
            AMQPFrame::Method(channel_id, class) => self.handle_method(channel_id, class).await,
            AMQPFrame::ProtocolHeader(_) => {
                self.protocol_error(
                    0,
                    &ProtocolError::command_invalid("duplicate protocol header"),
                )
                .await
            }
        }
    }

    async fn handle_method(&mut self, channel_id: u16, class: AMQPClass) -> bool {
        match self.phase {
            Phase::AwaitStartOk => self.phase_start_ok(channel_id, class).await,
            Phase::AwaitTuneOk => self.phase_tune_ok(channel_id, class).await,
            Phase::AwaitOpen => self.phase_open(channel_id, class).await,
            Phase::Running | Phase::Closing => self.phase_running(channel_id, class).await,
        }
    }

    async fn phase_start_ok(&mut self, channel_id: u16, class: AMQPClass) -> bool {
        let AMQPClass::Connection(connection::AMQPMethod::StartOk(start_ok)) = class else {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::command_invalid(
                        "expected connection.start-ok during handshake",
                    ),
                )
                .await;
        };
        if channel_id != 0 {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::command_invalid("connection.start-ok on non-zero channel"),
                )
                .await;
        }
        // SASL PLAIN: authzid NUL authcid NUL passwd.
        if start_ok.mechanism.as_str() != "PLAIN" {
            return self
                .protocol_error(
                    0,
                    &ProtocolError::connection(
                        rusty_mq_protocol::error::reply_code::COMMAND_INVALID,
                        format!(
                            "COMMAND_INVALID - mechanism '{}' not offered (PLAIN only in V1)",
                            start_ok.mechanism
                        ),
                    ),
                )
                .await;
        }
        let parts: Vec<&[u8]> = start_ok.response.as_bytes().split(|b| *b == 0).collect();
        if parts.len() != 3 {
            return self
                .protocol_error(
                    0,
                    &ProtocolError::connection(
                        rusty_mq_protocol::error::reply_code::COMMAND_INVALID,
                        "COMMAND_INVALID - malformed PLAIN response",
                    ),
                )
                .await;
        }
        let user = String::from_utf8_lossy(parts[1]).to_string();
        let pass = String::from_utf8_lossy(parts[2]).to_string();
        let expected = &self.broker.test_user;
        if user != expected.username || pass != expected.password {
            tracing::warn!(user = %user, "authentication refused");
            return self
                .protocol_error(
                    0,
                    &ProtocolError::connection(
                        rusty_mq_protocol::error::reply_code::ACCESS_REFUSED,
                        "ACCESS_REFUSED - login refused for user",
                    ),
                )
                .await;
        }

        // Offer server tune values; client tune-ok completes negotiation.
        let tune = connection::AMQPMethod::Tune(connection::Tune {
            channel_max: self.limits.max_channel_max,
            frame_max: self.limits.max_frame_max,
            heartbeat: self.limits.heartbeat_seconds as u16,
        });
        self.phase = Phase::AwaitTuneOk;
        self.send(AMQPFrame::Method(0, AMQPClass::Connection(tune)))
            .await
            .is_ok()
    }

    async fn phase_tune_ok(&mut self, channel_id: u16, class: AMQPClass) -> bool {
        let AMQPClass::Connection(connection::AMQPMethod::TuneOk(tune_ok)) = class else {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::command_invalid("expected connection.tune-ok during handshake"),
                )
                .await;
        };
        if channel_id != 0 {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::command_invalid("connection.tune-ok on non-zero channel"),
                )
                .await;
        }
        match self
            .limits
            .negotiate(tune_ok.channel_max, tune_ok.frame_max, tune_ok.heartbeat)
        {
            Ok(n) => {
                self.negotiated = Some(n);
                self.phase = Phase::AwaitOpen;
                true
            }
            Err(e) => {
                self.protocol_error(0, &ProtocolError::frame_error(e.to_string()))
                    .await
            }
        }
    }

    async fn phase_open(&mut self, channel_id: u16, class: AMQPClass) -> bool {
        let AMQPClass::Connection(connection::AMQPMethod::Open(open)) = class else {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::command_invalid("expected connection.open during handshake"),
                )
                .await;
        };
        if channel_id != 0 {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::command_invalid("connection.open on non-zero channel"),
                )
                .await;
        }
        let vhost_ok = {
            let topo = self.broker.topology.lock().unwrap();
            topo.find_vhost(open.virtual_host.as_str()).is_some()
        };
        if !vhost_ok {
            return self
                .protocol_error(
                    0,
                    &ProtocolError::connection(
                        rusty_mq_protocol::error::reply_code::ACCESS_REFUSED,
                        format!(
                            "ACCESS_REFUSED - vhost '{}' not found or access refused",
                            open.virtual_host
                        ),
                    ),
                )
                .await;
        }
        self.phase = Phase::Running;
        self.broker.connections_opened();
        let ok = connection::AMQPMethod::OpenOk(connection::OpenOk {});
        self.send(AMQPFrame::Method(0, AMQPClass::Connection(ok)))
            .await
            .is_ok()
    }

    async fn phase_running(&mut self, channel_id: u16, class: AMQPClass) -> bool {
        if self.phase == Phase::Closing {
            // Only the close-ok we are waiting for is meaningful now.
            return match class {
                AMQPClass::Connection(connection::AMQPMethod::CloseOk(_)) => false,
                _ => true, // ignore stray frames during the linger
            };
        }
        if channel_id == 0 {
            return self.handle_connection_method(class).await;
        }
        if !self
            .negotiated
            .as_ref()
            .is_some_and(|n| n.validate_channel(channel_id))
        {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::frame_error(format!(
                        "channel {channel_id} outside negotiated channel_max"
                    )),
                )
                .await;
        }
        match class {
            AMQPClass::Channel(channel::AMQPMethod::Open(_)) => {
                if self.channels.contains_key(&channel_id) {
                    return self
                        .protocol_error(
                            channel_id,
                            &ProtocolError::frame_error(format!(
                                "channel {channel_id} already open"
                            )),
                        )
                        .await;
                }
                self.channels.insert(
                    channel_id,
                    ChannelState {
                        generation: ChannelGeneration::new(),
                    },
                );
                let ok = channel::AMQPMethod::OpenOk(channel::OpenOk {});
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Channel(ok)))
                    .await
                    .is_ok()
            }
            AMQPClass::Channel(channel::AMQPMethod::Close(close)) => {
                tracing::debug!(
                    channel = channel_id,
                    code = close.reply_code,
                    "channel closed by client: {}",
                    close.reply_text
                );
                self.channels.remove(&channel_id);
                let ok = channel::AMQPMethod::CloseOk(channel::CloseOk {});
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Channel(ok)))
                    .await
                    .is_ok()
            }
            AMQPClass::Channel(channel::AMQPMethod::CloseOk(_)) => {
                // Response to a channel.close we sent.
                self.channels.remove(&channel_id);
                true
            }
            other => {
                if !self.channels.contains_key(&channel_id) {
                    return self
                        .protocol_error(
                            channel_id,
                            &ProtocolError::connection(
                                rusty_mq_protocol::error::reply_code::CHANNEL_ERROR,
                                format!("channel {channel_id} is not open"),
                            ),
                        )
                        .await;
                }
                // M2+ methods: strict profile -> 540 at channel scope.
                let (class_id, method_id) = class_method_ids(&other);
                let e = ProtocolError::not_implemented(
                    format!("method (class {class_id}, method {method_id})"),
                    class_id,
                    method_id,
                );
                self.protocol_error(channel_id, &e).await
            }
        }
    }

    async fn handle_connection_method(&mut self, class: AMQPClass) -> bool {
        match class {
            AMQPClass::Connection(connection::AMQPMethod::Close(_)) => {
                let ok = connection::AMQPMethod::CloseOk(connection::CloseOk {});
                let _ = self
                    .send(AMQPFrame::Method(0, AMQPClass::Connection(ok)))
                    .await;
                false
            }
            AMQPClass::Connection(connection::AMQPMethod::CloseOk(_)) => false,
            other => {
                let (class_id, method_id) = class_method_ids(&other);
                self.protocol_error(
                    0,
                    &ProtocolError::command_invalid(format!(
                        "method (class {class_id}, method {method_id}) invalid on channel 0"
                    )),
                )
                .await
            }
        }
    }

    /// Emit the proper close frame for a protocol error (frozen error
    /// profile) and report whether the connection should keep draining.
    ///
    /// Channel-scope errors close only `channel_id`; connection-scope errors
    /// switch the connection to a bounded `Closing` linger.
    async fn protocol_error(&mut self, channel_id: u16, e: &ProtocolError) -> bool {
        tracing::debug!(channel = channel_id, error = %e, "protocol error");
        match e.scope {
            rusty_mq_protocol::error::ErrorScope::Channel => {
                let close = channel::AMQPMethod::Close(channel::Close {
                    reply_code: e.reply_code,
                    reply_text: e.reply_text(),
                    class_id: e.class_id,
                    method_id: e.method_id,
                });
                let _ = self
                    .send(AMQPFrame::Method(channel_id, AMQPClass::Channel(close)))
                    .await;
                // The channel is logically closed now; drop state immediately
                // so a stale settlement cannot act on it (INV-04).
                self.channels.remove(&channel_id);
                true
            }
            rusty_mq_protocol::error::ErrorScope::Connection => {
                let close = connection::AMQPMethod::Close(connection::Close {
                    reply_code: e.reply_code,
                    reply_text: e.reply_text(),
                    class_id: e.class_id,
                    method_id: e.method_id,
                });
                let _ = self
                    .send(AMQPFrame::Method(0, AMQPClass::Connection(close)))
                    .await;
                self.phase = Phase::Closing;
                // Drain briefly for the client's close-ok. Framing-garbage
                // callers stop immediately regardless (drive() returns).
                true
            }
        }
    }

    /// Send a frame; on a full or closed writer the connection is dropped
    /// (bounded by construction; FR-R06 arrives with alarms).
    async fn send(&self, frame: AMQPFrame) -> Result<(), ()> {
        let bytes = encode_frame(&frame);
        match self.outbound.try_send(bytes) {
            Ok(()) => Ok(()),
            Err(_) => {
                tracing::warn!("outbound queue full or closed; dropping connection");
                Err(())
            }
        }
    }
}

/// Class/method ids of an AMQPClass for error reporting.
fn class_method_ids(class: &AMQPClass) -> (u16, u16) {
    use amq_protocol::protocol::{access, confirm, tx};
    let method_id = match class {
        AMQPClass::Basic(m) => basic_id(m),
        AMQPClass::Connection(m) => conn_id(m),
        AMQPClass::Channel(m) => channel_id_of(m),
        AMQPClass::Access(m) => match m {
            access::AMQPMethod::Request(x) => x.get_amqp_method_id(),
            access::AMQPMethod::RequestOk(x) => x.get_amqp_method_id(),
        },
        AMQPClass::Exchange(m) => match m {
            exchange::AMQPMethod::Declare(x) => x.get_amqp_method_id(),
            exchange::AMQPMethod::DeclareOk(x) => x.get_amqp_method_id(),
            exchange::AMQPMethod::Delete(x) => x.get_amqp_method_id(),
            exchange::AMQPMethod::DeleteOk(x) => x.get_amqp_method_id(),
            exchange::AMQPMethod::Bind(x) => x.get_amqp_method_id(),
            exchange::AMQPMethod::BindOk(x) => x.get_amqp_method_id(),
            exchange::AMQPMethod::Unbind(x) => x.get_amqp_method_id(),
            exchange::AMQPMethod::UnbindOk(x) => x.get_amqp_method_id(),
        },
        AMQPClass::Queue(m) => match m {
            queue::AMQPMethod::Declare(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::DeclareOk(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::Bind(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::BindOk(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::Unbind(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::UnbindOk(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::Purge(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::PurgeOk(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::Delete(x) => x.get_amqp_method_id(),
            queue::AMQPMethod::DeleteOk(x) => x.get_amqp_method_id(),
        },
        AMQPClass::Tx(m) => match m {
            tx::AMQPMethod::Select(x) => x.get_amqp_method_id(),
            tx::AMQPMethod::SelectOk(x) => x.get_amqp_method_id(),
            tx::AMQPMethod::Commit(x) => x.get_amqp_method_id(),
            tx::AMQPMethod::CommitOk(x) => x.get_amqp_method_id(),
            tx::AMQPMethod::Rollback(x) => x.get_amqp_method_id(),
            tx::AMQPMethod::RollbackOk(x) => x.get_amqp_method_id(),
        },
        AMQPClass::Confirm(m) => match m {
            confirm::AMQPMethod::Select(x) => x.get_amqp_method_id(),
            confirm::AMQPMethod::SelectOk(x) => x.get_amqp_method_id(),
        },
    };
    (class.get_amqp_class_id(), method_id)
}

fn basic_id(m: &amq_protocol::protocol::basic::AMQPMethod) -> u16 {
    use amq_protocol::protocol::basic::AMQPMethod as M;
    match m {
        M::Qos(x) => x.get_amqp_method_id(),
        M::QosOk(x) => x.get_amqp_method_id(),
        M::Consume(x) => x.get_amqp_method_id(),
        M::ConsumeOk(x) => x.get_amqp_method_id(),
        M::Cancel(x) => x.get_amqp_method_id(),
        M::CancelOk(x) => x.get_amqp_method_id(),
        M::Publish(x) => x.get_amqp_method_id(),
        M::Return(x) => x.get_amqp_method_id(),
        M::Deliver(x) => x.get_amqp_method_id(),
        M::Get(x) => x.get_amqp_method_id(),
        M::GetOk(x) => x.get_amqp_method_id(),
        M::GetEmpty(x) => x.get_amqp_method_id(),
        M::Ack(x) => x.get_amqp_method_id(),
        M::Reject(x) => x.get_amqp_method_id(),
        M::RecoverAsync(x) => x.get_amqp_method_id(),
        M::Recover(x) => x.get_amqp_method_id(),
        M::RecoverOk(x) => x.get_amqp_method_id(),
        M::Nack(x) => x.get_amqp_method_id(),
    }
}

fn conn_id(m: &connection::AMQPMethod) -> u16 {
    use connection::AMQPMethod as M;
    match m {
        M::Start(x) => x.get_amqp_method_id(),
        M::StartOk(x) => x.get_amqp_method_id(),
        M::Secure(x) => x.get_amqp_method_id(),
        M::SecureOk(x) => x.get_amqp_method_id(),
        M::Tune(x) => x.get_amqp_method_id(),
        M::TuneOk(x) => x.get_amqp_method_id(),
        M::Open(x) => x.get_amqp_method_id(),
        M::OpenOk(x) => x.get_amqp_method_id(),
        M::Close(x) => x.get_amqp_method_id(),
        M::CloseOk(x) => x.get_amqp_method_id(),
        M::Blocked(x) => x.get_amqp_method_id(),
        M::Unblocked(x) => x.get_amqp_method_id(),
        M::UpdateSecret(x) => x.get_amqp_method_id(),
        M::UpdateSecretOk(x) => x.get_amqp_method_id(),
    }
}

fn channel_id_of(m: &channel::AMQPMethod) -> u16 {
    use channel::AMQPMethod as M;
    match m {
        M::Open(x) => x.get_amqp_method_id(),
        M::OpenOk(x) => x.get_amqp_method_id(),
        M::Flow(x) => x.get_amqp_method_id(),
        M::FlowOk(x) => x.get_amqp_method_id(),
        M::Close(x) => x.get_amqp_method_id(),
        M::CloseOk(x) => x.get_amqp_method_id(),
    }
}

/// Writer task: sole owner of the socket write half (FR-P07, ADR-0003).
async fn writer_task(mut write: OwnedWriteHalf, mut rx: mpsc::Receiver<Vec<u8>>) {
    while let Some(bytes) = rx.recv().await {
        if write.write_all(&bytes).await.is_err() {
            break;
        }
        if write.flush().await.is_err() {
            break;
        }
    }
    let _ = write.shutdown().await;
}
