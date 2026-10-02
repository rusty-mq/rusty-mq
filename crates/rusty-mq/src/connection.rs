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

use amq_protocol::frame::{AMQPContentHeader, AMQPFrame};
use amq_protocol::protocol::basic::{self, parse_properties, AMQPProperties};
use amq_protocol::protocol::{channel, connection, exchange, queue, AMQPClass};
use amq_protocol::types::{FieldTable, LongString};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

use rusty_mq_core::store::{AdmitError, QueueEntry, StoredMessage};
use rusty_mq_core::topology::TopologyError;
use rusty_mq_core::{ChannelGeneration, ConnectionId, QueueId, VhostId};
use rusty_mq_protocol::error::reply_code;
use rusty_mq_protocol::{
    encode_frame, FrameReader, MessageAssembler, NegotiatedLimits, ProtocolError, ProtocolLimits,
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

/// Per-channel state (ADR-0003): generations, last-declared-queue
/// shorthand, in-flight publish content assembly, and outstanding
/// manual-ack deliveries with channel-scoped tags (FR-C04).
struct ChannelState {
    #[allow(dead_code)] // consumed by delivery ownership from M3
    generation: ChannelGeneration,
    /// Most recently declared queue on this channel (empty-name shorthand).
    last_queue: Option<String>,
    /// Content assembly for the basic.publish in flight on this channel.
    content: Option<InFlightPublish>,
    /// Next channel-scoped delivery tag (monotonic, FR-C04).
    next_delivery_tag: u64,
    /// Outstanding manual-ack deliveries: tag -> held entry.
    unacked: HashMap<u64, UnackedDelivery>,
}

/// A basic.publish whose content frames are still arriving.
struct InFlightPublish {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    property_bytes: Vec<u8>,
    persistent: bool,
    assembler: MessageAssembler,
}

/// A delivery held out of the ready set pending settlement (§6.1).
struct UnackedDelivery {
    queue: QueueId,
    entry: QueueEntry,
}

/// Owns one accepted connection until it ends.
pub struct Connection {
    broker: Arc<Broker>,
    conn_id: ConnectionId,
    /// Vhost bound at `connection.open` (None until then).
    vhost: Option<VhostId>,
    /// Channels we closed and whose close-ok has not arrived: frames for
    /// these channels are ignored (close-handshake interlude; content may
    /// still be in flight from the client).
    awaiting_close_ok: HashMap<u16, ()>,
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
            conn_id,
            vhost: None,
            awaiting_close_ok: HashMap::new(),
            limits: ProtocolLimits::default(),
            negotiated: None,
            phase: Phase::AwaitStartOk,
            channels: HashMap::new(),
            outbound: outbound_tx,
        };

        tracing::info!(connection = %conn_id, peer = %peer, "connection opened");
        conn.drive(read_half).await;

        // Reclaim connection-scoped resources (FR-P09): exclusive queues
        // disappear with their owning connection (FR-Q04).
        conn.drop_all_channel_state();
        if let Some(vhost) = conn.vhost {
            let removed = conn
                .broker
                .topology
                .lock()
                .unwrap()
                .remove_owned_queues(vhost, conn.conn_id);
            let mut store = conn.broker.store.lock().unwrap();
            for q in &removed {
                store.drain(*q);
            }
            if !removed.is_empty() {
                tracing::debug!(connection = %conn_id, count = removed.len(), "exclusive queues reclaimed");
            }
        }
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
            AMQPFrame::Header(channel_id, _class_id, header) => {
                self.handle_content_header(channel_id, *header).await
            }
            AMQPFrame::Body(channel_id, data) => self.handle_content_body(channel_id, &data).await,
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
        let vhost = {
            let topo = self.broker.topology.lock().unwrap();
            topo.find_vhost(open.virtual_host.as_str())
        };
        if vhost.is_none() {
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
        self.vhost = vhost;
        self.phase = Phase::Running;
        self.broker.connections_opened();
        let ok = connection::AMQPMethod::OpenOk(connection::OpenOk {});
        self.send(AMQPFrame::Method(0, AMQPClass::Connection(ok)))
            .await
            .is_ok()
    }

    async fn phase_running(&mut self, channel_id: u16, class: AMQPClass) -> bool {
        if channel_id != 0
            && self.awaiting_close_ok.contains_key(&channel_id)
            && !matches!(class, AMQPClass::Channel(channel::AMQPMethod::CloseOk(_)))
        {
            // Close-handshake interlude: content or methods still in flight
            // for a server-closed channel are dropped (RabbitMQ behavior);
            // close-ok falls through to its real handler below.
            return true;
        }
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
                        last_queue: None,
                        content: None,
                        next_delivery_tag: 1,
                        unacked: HashMap::new(),
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
                // FR-C06: unacked manual-ack deliveries requeue.
                self.drop_channel_state(channel_id);
                self.awaiting_close_ok.remove(&channel_id);
                let ok = channel::AMQPMethod::CloseOk(channel::CloseOk {});
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Channel(ok)))
                    .await
                    .is_ok()
            }
            AMQPClass::Channel(channel::AMQPMethod::CloseOk(_)) => {
                // Response to a channel.close we sent: handshake done.
                self.channels.remove(&channel_id);
                self.awaiting_close_ok.remove(&channel_id);
                true
            }
            AMQPClass::Exchange(exchange::AMQPMethod::Declare(d)) => {
                self.handle_exchange_declare(channel_id, d).await
            }
            AMQPClass::Exchange(exchange::AMQPMethod::Delete(d)) => {
                self.handle_exchange_delete(channel_id, d).await
            }
            AMQPClass::Queue(queue::AMQPMethod::Declare(d)) => {
                self.handle_queue_declare(channel_id, d).await
            }
            AMQPClass::Queue(queue::AMQPMethod::Delete(d)) => {
                self.handle_queue_delete(channel_id, d).await
            }
            AMQPClass::Queue(queue::AMQPMethod::Bind(d)) => {
                self.handle_queue_bind(channel_id, d).await
            }
            AMQPClass::Queue(queue::AMQPMethod::Unbind(d)) => {
                self.handle_queue_unbind(channel_id, d).await
            }
            AMQPClass::Queue(queue::AMQPMethod::Purge(d)) => {
                self.handle_queue_purge(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::Publish(d)) => {
                self.handle_basic_publish(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::Get(d)) => {
                self.handle_basic_get(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::Ack(d)) => {
                self.handle_basic_ack(channel_id, d).await
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

    // ------------------------------------------------------------------
    // Topology methods (M2): declare/delete/bind/unbind for exchanges and
    // queues, wired to rusty-mq-core with the frozen error profile.
    //
    // Lock discipline: every std-mutex section is block-scoped so no guard
    // is alive across an await (the future must stay Send).
    // ------------------------------------------------------------------

    /// Vhost bound at open; Running-phase handlers may assume it.
    fn vhost(&self) -> VhostId {
        self.vhost
            .expect("phase Running implies connection.open-ok")
    }

    /// Map a core topology error onto the frozen error profile.
    fn topology_error(e: TopologyError, class_id: u16, method_id: u16) -> ProtocolError {
        match e {
            TopologyError::VhostNotFound => ProtocolError::channel(
                reply_code::NOT_FOUND,
                "NOT_FOUND - vhost",
                class_id,
                method_id,
            ),
            TopologyError::ExchangeNotFound(n) => ProtocolError::channel(
                reply_code::NOT_FOUND,
                format!("NOT_FOUND - no exchange '{n}' in vhost"),
                class_id,
                method_id,
            ),
            TopologyError::QueueNotFound(n) => ProtocolError::channel(
                reply_code::NOT_FOUND,
                format!("NOT_FOUND - no queue '{n}' in vhost"),
                class_id,
                method_id,
            ),
            TopologyError::ExchangePreconditionFailed(n) => ProtocolError::channel(
                reply_code::PRECONDITION_FAILED,
                format!("PRECONDITION_FAILED - inequivalent arg for exchange '{n}'"),
                class_id,
                method_id,
            ),
            TopologyError::QueuePreconditionFailed(n) => ProtocolError::channel(
                reply_code::PRECONDITION_FAILED,
                format!("PRECONDITION_FAILED - inequivalent arg for queue '{n}'"),
                class_id,
                method_id,
            ),
            TopologyError::QueueLocked(n) => ProtocolError::channel(
                reply_code::RESOURCE_LOCKED,
                format!("RESOURCE_LOCKED - queue '{n}' is exclusive to another connection"),
                class_id,
                method_id,
            ),
            TopologyError::BindingExists => {
                ProtocolError::channel(reply_code::NO_ROUTE, "BINDING_EXISTS", class_id, method_id)
            }
            TopologyError::BindingNotFound => ProtocolError::channel(
                reply_code::NOT_FOUND,
                "NOT_FOUND - no such binding",
                class_id,
                method_id,
            ),
            TopologyError::ReservedName(n) => ProtocolError::channel(
                reply_code::ACCESS_REFUSED,
                format!("ACCESS_REFUSED - operation not permitted on '{n}'"),
                class_id,
                method_id,
            ),
            TopologyError::QueueInUse => ProtocolError::channel(
                reply_code::PRECONDITION_FAILED,
                "PRECONDITION_FAILED - queue in use",
                class_id,
                method_id,
            ),
            TopologyError::QueueNotEmpty => ProtocolError::channel(
                reply_code::PRECONDITION_FAILED,
                "PRECONDITION_FAILED - queue not empty",
                class_id,
                method_id,
            ),
        }
    }

    /// Queue-declare rejections: V1 profile gates + wrapped topology errors.
    fn queue_declare_error(
        e: rusty_mq_core::topology::DeclareQueueError,
        class_id: u16,
        method_id: u16,
    ) -> ProtocolError {
        use rusty_mq_core::topology::DeclareQueueError;
        match e {
            DeclareQueueError::DurableExclusive => ProtocolError::precondition_failed(
                "durable+exclusive queues are not supported by rusty-mq in V1",
                class_id,
                method_id,
            ),
            DeclareQueueError::DurableAutoDelete => ProtocolError::precondition_failed(
                "durable+auto-delete queues are not supported by rusty-mq in V1",
                class_id,
                method_id,
            ),
            DeclareQueueError::TransientNonExclusive => ProtocolError::precondition_failed(
                "transient non-exclusive queues require the compatibility switch",
                class_id,
                method_id,
            ),
            DeclareQueueError::Topology(t) => Self::topology_error(t, class_id, method_id),
        }
    }

    /// Exchange-declare rejections.
    fn exchange_declare_error(
        e: rusty_mq_core::topology::DeclareExchangeError,
        class_id: u16,
        method_id: u16,
    ) -> ProtocolError {
        use rusty_mq_core::topology::DeclareExchangeError;
        match e {
            DeclareExchangeError::UnsupportedType(t) => ProtocolError::not_implemented(
                format!("exchange type '{t}' (direct, fanout, topic only in V1)"),
                class_id,
                method_id,
            ),
            DeclareExchangeError::InternalConflict => ProtocolError::precondition_failed(
                "internal-exchange conflict",
                class_id,
                method_id,
            ),
            DeclareExchangeError::Topology(t) => Self::topology_error(t, class_id, method_id),
        }
    }

    /// Strict argument policy (ADR-0005): queue arguments accept only
    /// `x-queue-type=classic`; anything behavior-bearing is 540.
    fn validate_queue_arguments(args: &FieldTable) -> Result<(), ProtocolError> {
        for (key, value) in args.inner().iter() {
            match key.as_str() {
                "x-queue-type" => {
                    let ok = matches!(
                        value,
                        amq_protocol::types::AMQPValue::LongString(s) if s.as_bytes() == b"classic"
                    );
                    if !ok {
                        return Err(ProtocolError::not_implemented(
                            "queue type (only x-queue-type=classic is supported)",
                            rusty_mq_protocol::error::class_id::QUEUE,
                            10, // queue.declare
                        ));
                    }
                }
                other => {
                    return Err(ProtocolError::not_implemented(
                        format!("queue argument '{other}'"),
                        rusty_mq_protocol::error::class_id::QUEUE,
                        10, // queue.declare
                    ));
                }
            }
        }
        Ok(())
    }

    /// Exchange/bind/unbind arguments accept nothing in V1.
    fn require_no_arguments(
        args: &FieldTable,
        what: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ProtocolError> {
        if let Some((key, _)) = args.inner().first_key_value() {
            return Err(ProtocolError::not_implemented(
                format!("{what} argument '{key}'"),
                class_id,
                method_id,
            ));
        }
        Ok(())
    }

    /// Resolve an empty queue name via the channel's last declared queue
    /// (FR-Q02); `None` yields the 404 the frozen profile requires.
    fn resolve_queue_name(&self, channel_id: u16, name: &str) -> Result<String, ProtocolError> {
        if !name.is_empty() {
            return Ok(name.to_string());
        }
        match self
            .channels
            .get(&channel_id)
            .and_then(|c| c.last_queue.clone())
        {
            Some(last) => Ok(last),
            None => Err(ProtocolError::not_found(
                "no previously declared queue to use as default",
                rusty_mq_protocol::error::class_id::QUEUE,
                10,
            )),
        }
    }

    async fn handle_exchange_declare(&mut self, channel_id: u16, d: exchange::Declare) -> bool {
        const CLASS: u16 = 40;
        let method = d.get_amqp_method_id();
        if let Err(e) = Self::require_no_arguments(&d.arguments, "exchange", CLASS, method) {
            return self.protocol_error(channel_id, &e).await;
        }
        let vhost = self.vhost();

        if d.passive {
            // Passive: existence check only; 404 when missing (FR-E02).
            let found = {
                let topo = self.broker.topology.lock().unwrap();
                topo.find_exchange(vhost, d.exchange.as_str())
            };
            return match found {
                Some(_) if !d.nowait => {
                    let ok = exchange::AMQPMethod::DeclareOk(exchange::DeclareOk {});
                    self.send(AMQPFrame::Method(channel_id, AMQPClass::Exchange(ok)))
                        .await
                        .is_ok()
                }
                Some(_) => true,
                None => {
                    let e = ProtocolError::not_found(
                        format!("no exchange '{}' in vhost", d.exchange),
                        CLASS,
                        method,
                    );
                    self.protocol_error(channel_id, &e).await
                }
            };
        }

        // Active declare.
        let kind = match rusty_mq_core::routing::ExchangeType::from_wire_name(d.kind.as_str()) {
            Some(k) => k,
            None => {
                let e = ProtocolError::not_implemented(
                    format!(
                        "exchange type '{}' (direct, fanout, topic only in V1)",
                        d.kind
                    ),
                    CLASS,
                    method,
                );
                return self.protocol_error(channel_id, &e).await;
            }
        };
        let declared: Result<(), rusty_mq_core::topology::DeclareExchangeError> = {
            let mut topo = self.broker.topology.lock().unwrap();
            topo.declare_exchange(
                vhost,
                d.exchange.as_str(),
                kind,
                d.durable,
                d.auto_delete,
                d.internal,
            )
            .map(|_| ())
        };
        match declared {
            Ok(()) if !d.nowait => {
                let ok = exchange::AMQPMethod::DeclareOk(exchange::DeclareOk {});
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Exchange(ok)))
                    .await
                    .is_ok()
            }
            Ok(()) => true,
            Err(e) => {
                let e = Self::exchange_declare_error(e, CLASS, method);
                self.protocol_error(channel_id, &e).await
            }
        }
    }

    async fn handle_exchange_delete(&mut self, channel_id: u16, d: exchange::Delete) -> bool {
        const CLASS: u16 = 40;
        let method = d.get_amqp_method_id();
        let vhost = self.vhost();
        let outcome: Result<(), ProtocolError> = {
            let mut topo = self.broker.topology.lock().unwrap();
            let found = topo.find_exchange(vhost, d.exchange.as_str());
            match found {
                None => Err(ProtocolError::not_found(
                    format!("no exchange '{}' in vhost", d.exchange),
                    CLASS,
                    method,
                )),
                Some(id) if d.if_unused && topo.binding_count(vhost, id) > 0 => {
                    Err(ProtocolError::precondition_failed(
                        format!("exchange '{}' in use", d.exchange),
                        CLASS,
                        method,
                    ))
                }
                Some(_) => topo
                    .delete_exchange(vhost, d.exchange.as_str())
                    .map(|_| ())
                    .map_err(|e| Self::topology_error(e, CLASS, method)),
            }
        };
        match outcome {
            Ok(()) if !d.nowait => {
                let ok = exchange::AMQPMethod::DeleteOk(exchange::DeleteOk {});
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Exchange(ok)))
                    .await
                    .is_ok()
            }
            Ok(()) => true,
            Err(e) => self.protocol_error(channel_id, &e).await,
        }
    }

    async fn handle_queue_declare(&mut self, channel_id: u16, d: queue::Declare) -> bool {
        const CLASS: u16 = 50;
        let method = d.get_amqp_method_id();
        if let Err(e) = Self::validate_queue_arguments(&d.arguments) {
            return self.protocol_error(channel_id, &e).await;
        }
        let vhost = self.vhost();
        let generated = d.queue.as_str().is_empty();
        let profile = rusty_mq_core::topology::QueueProfile {
            durable: d.durable,
            exclusive: d.exclusive,
            auto_delete: d.auto_delete,
        };
        let owner = if d.exclusive {
            Some(self.conn_id)
        } else {
            None
        };

        if d.passive {
            // Passive: existence + exclusivity check; 404/405 otherwise.
            // Ok(None) = missing; Err = locked.
            let inspected: Result<Option<(String, u32)>, ProtocolError> = {
                let topo = self.broker.topology.lock().unwrap();
                let found = topo
                    .find_queue(vhost, d.queue.as_str())
                    .filter(|_| !generated);
                match found {
                    None => Ok(None),
                    // FR-Q04: exclusivity gates inspection by other connections.
                    Some(id)
                        if !topo
                            .check_exclusive_access(id, self.conn_id)
                            .unwrap_or(false) =>
                    {
                        Err(ProtocolError::channel(
                            reply_code::RESOURCE_LOCKED,
                            format!(
                                "RESOURCE_LOCKED - queue '{}' is exclusive to another connection",
                                d.queue
                            ),
                            CLASS,
                            method,
                        ))
                    }
                    Some(id) => {
                        let name = topo
                            .queue_record(id)
                            .map(|r| r.name.clone())
                            .unwrap_or_default();
                        drop(topo);
                        let ready = self.broker.store.lock().unwrap().len(id);
                        Ok(Some((name, ready as u32)))
                    }
                }
            };
            return match inspected {
                Ok(Some((name, message_count))) if !d.nowait => {
                    let ok = queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                        queue: name.into(),
                        // Ready count from the store; consumer count arrives
                        // with the M3 consumer registry.
                        message_count,
                        consumer_count: 0,
                    });
                    self.send(AMQPFrame::Method(channel_id, AMQPClass::Queue(ok)))
                        .await
                        .is_ok()
                }
                Ok(Some((_, _))) => true,
                Ok(None) => {
                    let e = ProtocolError::not_found(
                        format!("no queue '{}' in vhost", d.queue),
                        CLASS,
                        method,
                    );
                    self.protocol_error(channel_id, &e).await
                }
                Err(e) => self.protocol_error(channel_id, &e).await,
            };
        }

        // Active declare.
        let declared: Result<String, rusty_mq_core::topology::DeclareQueueError> = {
            let mut topo = self.broker.topology.lock().unwrap();
            topo.declare_queue(vhost, d.queue.as_str(), profile, owner)
                .map(|id| {
                    topo.queue_record(id)
                        .map(|r| r.name.clone())
                        .unwrap_or_default()
                })
        };
        match declared {
            Ok(name) => {
                if let Some(ch) = self.channels.get_mut(&channel_id) {
                    ch.last_queue = Some(name.clone());
                }
                if d.nowait && !generated {
                    // nowait suppresses replies — except server-generated
                    // names, where the client could not learn the name
                    // otherwise (FR-P08, spec guidance).
                    return true;
                }
                let ready = {
                    let topo = self.broker.topology.lock().unwrap();
                    topo.find_queue(vhost, &name)
                        .map(|id| self.broker.store.lock().unwrap().len(id))
                        .unwrap_or(0)
                };
                let ok = queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                    queue: name.into(),
                    message_count: ready as u32,
                    consumer_count: 0,
                });
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Queue(ok)))
                    .await
                    .is_ok()
            }
            Err(e) => {
                let e = Self::queue_declare_error(e, CLASS, method);
                self.protocol_error(channel_id, &e).await
            }
        }
    }

    async fn handle_queue_delete(&mut self, channel_id: u16, d: queue::Delete) -> bool {
        const CLASS: u16 = 50;
        let method = d.get_amqp_method_id();
        let Ok(name) = self.resolve_queue_name(channel_id, d.queue.as_str()) else {
            let e = ProtocolError::not_found(
                "no previously declared queue to use as default",
                CLASS,
                method,
            );
            return self.protocol_error(channel_id, &e).await;
        };
        let vhost = self.vhost();
        let outcome: Result<(), ProtocolError> = {
            let mut topo = self.broker.topology.lock().unwrap();
            let found = topo.find_queue(vhost, &name);
            match found {
                None => Err(ProtocolError::not_found(
                    format!("no queue '{name}' in vhost"),
                    CLASS,
                    method,
                )),
                Some(id)
                    if !topo
                        .check_exclusive_access(id, self.conn_id)
                        .unwrap_or(false) =>
                {
                    Err(ProtocolError::channel(
                        reply_code::RESOURCE_LOCKED,
                        format!(
                            "RESOURCE_LOCKED - queue '{name}' is exclusive to another connection"
                        ),
                        CLASS,
                        method,
                    ))
                }
                Some(_) => {
                    // Consumer/ready counts come from the scheduler (M3):
                    // zero for now, so if_unused/if_empty pass trivially.
                    topo.delete_queue(vhost, &name, d.if_unused, d.if_empty, 0, 0)
                        .map(|_| ())
                        .map_err(|e| Self::topology_error(e, CLASS, method))
                }
            }
        };
        match outcome {
            Ok(()) => {
                if let Some(ch) = self.channels.get_mut(&channel_id) {
                    if ch.last_queue.as_deref() == Some(name.as_str()) {
                        ch.last_queue = None;
                    }
                }
                if d.nowait {
                    true
                } else {
                    let ok = queue::AMQPMethod::DeleteOk(queue::DeleteOk { message_count: 0 });
                    self.send(AMQPFrame::Method(channel_id, AMQPClass::Queue(ok)))
                        .await
                        .is_ok()
                }
            }
            Err(e) => self.protocol_error(channel_id, &e).await,
        }
    }

    async fn handle_queue_bind(&mut self, channel_id: u16, d: queue::Bind) -> bool {
        const CLASS: u16 = 50;
        let method = d.get_amqp_method_id();
        if let Err(e) = Self::require_no_arguments(&d.arguments, "bind", CLASS, method) {
            return self.protocol_error(channel_id, &e).await;
        }
        let Ok(queue_name) = self.resolve_queue_name(channel_id, d.queue.as_str()) else {
            let e = ProtocolError::not_found(
                "no previously declared queue to use as default",
                CLASS,
                method,
            );
            return self.protocol_error(channel_id, &e).await;
        };
        let vhost = self.vhost();
        let outcome: Result<(), ProtocolError> = {
            let mut topo = self.broker.topology.lock().unwrap();
            let queue_id = topo.find_queue(vhost, &queue_name);
            let exchange_id = topo.find_exchange(vhost, d.exchange.as_str());
            match (queue_id, exchange_id) {
                (None, _) => Err(ProtocolError::not_found(
                    format!("no queue '{queue_name}' in vhost"),
                    CLASS,
                    method,
                )),
                (_, None) => Err(ProtocolError::not_found(
                    format!("no exchange '{}' in vhost", d.exchange),
                    CLASS,
                    method,
                )),
                (Some(q), Some(e)) => topo
                    .bind(vhost, e, q, d.routing_key.as_str())
                    .map_err(|err| Self::topology_error(err, CLASS, method)),
            }
        };
        match outcome {
            Ok(()) if !d.nowait => {
                let ok = queue::AMQPMethod::BindOk(queue::BindOk {});
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Queue(ok)))
                    .await
                    .is_ok()
            }
            Ok(()) => true,
            Err(e) => self.protocol_error(channel_id, &e).await,
        }
    }

    async fn handle_queue_unbind(&mut self, channel_id: u16, d: queue::Unbind) -> bool {
        const CLASS: u16 = 50;
        let method = d.get_amqp_method_id();
        if let Err(e) = Self::require_no_arguments(&d.arguments, "unbind", CLASS, method) {
            return self.protocol_error(channel_id, &e).await;
        }
        let Ok(queue_name) = self.resolve_queue_name(channel_id, d.queue.as_str()) else {
            let e = ProtocolError::not_found(
                "no previously declared queue to use as default",
                CLASS,
                method,
            );
            return self.protocol_error(channel_id, &e).await;
        };
        let vhost = self.vhost();
        let outcome: Result<(), ProtocolError> = {
            let mut topo = self.broker.topology.lock().unwrap();
            let queue_id = topo.find_queue(vhost, &queue_name);
            let exchange_id = topo.find_exchange(vhost, d.exchange.as_str());
            match (queue_id, exchange_id) {
                (None, _) => Err(ProtocolError::not_found(
                    format!("no queue '{queue_name}' in vhost"),
                    CLASS,
                    method,
                )),
                (_, None) => Err(ProtocolError::not_found(
                    format!("no exchange '{}' in vhost", d.exchange),
                    CLASS,
                    method,
                )),
                (Some(q), Some(e)) => topo
                    .unbind(vhost, e, q, d.routing_key.as_str())
                    .map_err(|err| Self::topology_error(err, CLASS, method)),
            }
        };
        match outcome {
            // queue.unbind carries no nowait bit in 0-9-1.
            Ok(()) => {
                let ok = queue::AMQPMethod::UnbindOk(queue::UnbindOk {});
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Queue(ok)))
                    .await
                    .is_ok()
            }
            Err(e) => self.protocol_error(channel_id, &e).await,
        }
    }

    async fn handle_queue_purge(&mut self, channel_id: u16, d: queue::Purge) -> bool {
        const CLASS: u16 = 50;
        let method = d.get_amqp_method_id();
        let Ok(name) = self.resolve_queue_name(channel_id, d.queue.as_str()) else {
            let e = ProtocolError::not_found(
                "no previously declared queue to use as default",
                CLASS,
                method,
            );
            return self.protocol_error(channel_id, &e).await;
        };
        let vhost = self.vhost();
        // Existence + exclusivity gates, then purge the ready set only
        // (FR-Q06: unacked deliveries are held out of ready and survive).
        let purged: Result<u64, ProtocolError> = {
            let topo = self.broker.topology.lock().unwrap();
            let found = topo.find_queue(vhost, &name);
            match found {
                None => Err(ProtocolError::not_found(
                    format!("no queue '{name}' in vhost"),
                    CLASS,
                    method,
                )),
                Some(id)
                    if !topo
                        .check_exclusive_access(id, self.conn_id)
                        .unwrap_or(false) =>
                {
                    Err(ProtocolError::channel(
                        reply_code::RESOURCE_LOCKED,
                        format!(
                            "RESOURCE_LOCKED - queue '{name}' is exclusive to another connection"
                        ),
                        CLASS,
                        method,
                    ))
                }
                Some(id) => {
                    drop(topo);
                    Ok(self.broker.store.lock().unwrap().purge(id))
                }
            }
        };
        let purged = match purged {
            Ok(n) => n,
            Err(e) => return self.protocol_error(channel_id, &e).await,
        };
        if d.nowait {
            true
        } else {
            let ok = queue::AMQPMethod::PurgeOk(queue::PurgeOk {
                message_count: purged as u32,
            });
            self.send(AMQPFrame::Method(channel_id, AMQPClass::Queue(ok)))
                .await
                .is_ok()
        }
    }

    // ------------------------------------------------------------------
    // Publishing, content assembly, and polling delivery (M2).
    // ------------------------------------------------------------------

    /// Reclaim a channel's state: requeue unacked manual-ack deliveries
    /// (FR-C06) and drop any half-assembled publish.
    fn drop_channel_state(&mut self, channel_id: u16) {
        if let Some(mut ch) = self.channels.remove(&channel_id) {
            ch.content = None;
            if !ch.unacked.is_empty() {
                let mut store = self.broker.store.lock().unwrap();
                for (_, u) in ch.unacked.drain() {
                    store.requeue(u.queue, u.entry);
                }
            }
        }
    }

    /// Requeue every channel's unacked deliveries (connection teardown).
    fn drop_all_channel_state(&mut self) {
        let channel_ids: Vec<u16> = self.channels.keys().copied().collect();
        for id in channel_ids {
            self.drop_channel_state(id);
        }
    }

    /// Max body-frame payload under the negotiated frame_max.
    fn frame_payload_budget(&self) -> usize {
        const MIN_BODY_CHUNK: usize = 512;
        self.negotiated
            .as_ref()
            .map(|n| n.max_frame_payload() as usize)
            .unwrap_or(MIN_BODY_CHUNK)
            .max(MIN_BODY_CHUNK)
    }

    /// Encode+validate properties at publish admission. Returns the opaque
    /// property blob and the persistence flag.
    fn admit_properties(
        props: &AMQPProperties,
        username: &str,
        class_id: u16,
    ) -> Result<(Vec<u8>, bool), ProtocolError> {
        // FR-M05: per-message expiration is a deferred feature; reject it
        // rather than silently ignoring a requested TTL.
        if let Some(exp) = props.expiration() {
            let _ = exp;
            return Err(ProtocolError::not_implemented(
                format!("basic.publish expiration property ('{exp}')"),
                class_id,
                40, // basic.publish
            ));
        }
        // FR-M03: absent delivery mode = transient; 1 and 2 valid; anything
        // else is invalid input (frozen as 503 in the protocol profile).
        let persistent = match props.delivery_mode() {
            None => false,
            Some(1) => false,
            Some(2) => true,
            Some(other) => {
                return Err(ProtocolError::connection_with_method(
                    reply_code::COMMAND_INVALID,
                    format!("COMMAND_INVALID - invalid delivery-mode {other} (1 or 2)"),
                    class_id,
                    40,
                ));
            }
        };
        // FR-M04: a supplied user_id must match the authenticated principal.
        if let Some(uid) = props.user_id() {
            if uid.as_str() != username {
                return Err(ProtocolError::channel(
                    reply_code::ACCESS_REFUSED,
                    "ACCESS_REFUSED - user_id property does not match authenticated user",
                    class_id,
                    40,
                ));
            }
        }
        let blob = rusty_mq_protocol::encode_properties(props);
        Ok((blob, persistent))
    }

    async fn handle_basic_publish(&mut self, channel_id: u16, d: basic::Publish) -> bool {
        const CLASS: u16 = 60;
        // FR-PUB04: immediate is a deferred feature; never simulate it.
        if d.immediate {
            let e = ProtocolError::not_implemented(
                "basic.publish immediate=true",
                CLASS,
                d.get_amqp_method_id(),
            );
            return self.protocol_error(channel_id, &e).await;
        }
        let vhost = self.vhost();
        // FR-PUB03: publishing to a nonexistent exchange is a channel error.
        let exchange_check = {
            let topo = self.broker.topology.lock().unwrap();
            match topo.find_exchange(vhost, d.exchange.as_str()) {
                None => Err(ProtocolError::not_found(
                    format!("no exchange '{}' in vhost", d.exchange),
                    CLASS,
                    d.get_amqp_method_id(),
                )),
                // FR-E03: internal exchanges cannot receive client publishes.
                Some(id) if topo.exchange_record(id).is_some_and(|r| r.internal) => {
                    Err(ProtocolError::channel(
                        reply_code::ACCESS_REFUSED,
                        format!(
                            "ACCESS_REFUSED - cannot publish to internal exchange '{}'",
                            d.exchange
                        ),
                        CLASS,
                        d.get_amqp_method_id(),
                    ))
                }
                Some(_) => Ok(()),
            }
        };
        if let Err(e) = exchange_check {
            return self.protocol_error(channel_id, &e).await;
        }
        let Some(ch) = self.channels.get_mut(&channel_id) else {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::connection(
                        reply_code::CHANNEL_ERROR,
                        format!("channel {channel_id} is not open"),
                    ),
                )
                .await;
        };
        if ch.content.is_some() {
            // Overlapping publishes without waiting for content completion.
            let e = ProtocolError::unexpected_frame(format!(
                "basic.publish on channel {channel_id} while previous message incomplete"
            ))
            .with_fatal();
            return self.protocol_error(channel_id, &e).await;
        }
        ch.content = Some(InFlightPublish {
            exchange: d.exchange.as_str().to_string(),
            routing_key: d.routing_key.as_str().to_string(),
            mandatory: d.mandatory,
            property_bytes: Vec::new(),
            persistent: false,
            assembler: MessageAssembler::new(self.limits.max_message_bytes),
        });
        true
    }

    async fn handle_content_header(&mut self, channel_id: u16, header: AMQPContentHeader) -> bool {
        if self.awaiting_close_ok.contains_key(&channel_id) {
            return true; // stale content for a closed channel
        }
        let Some(ch) = self.channels.get_mut(&channel_id) else {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::connection(
                        reply_code::CHANNEL_ERROR,
                        format!("channel {channel_id} is not open"),
                    ),
                )
                .await;
        };
        let Some(inflight) = ch.content.as_mut() else {
            let e = ProtocolError::unexpected_frame(format!(
                "content header on channel {channel_id} without a pending publish"
            ))
            .with_fatal();
            return self.protocol_error(channel_id, &e).await;
        };
        if let Err(e) = inflight.assembler.start(header.class_id, header.body_size) {
            return self.protocol_error(channel_id, &e).await;
        }
        let (blob, persistent) =
            match Self::admit_properties(&header.properties, &self.broker.test_user.username, 60) {
                Ok(x) => x,
                Err(e) => return self.protocol_error(channel_id, &e).await,
            };
        inflight.property_bytes = blob;
        inflight.persistent = persistent;
        if inflight.assembler.is_complete() {
            let inflight = ch.content.take().expect("checked above");
            return self.finish_publish(channel_id, inflight).await;
        }
        true
    }

    async fn handle_content_body(&mut self, channel_id: u16, chunk: &[u8]) -> bool {
        if self.awaiting_close_ok.contains_key(&channel_id) {
            return true; // stale content for a closed channel
        }
        let Some(ch) = self.channels.get_mut(&channel_id) else {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::connection(
                        reply_code::CHANNEL_ERROR,
                        format!("channel {channel_id} is not open"),
                    ),
                )
                .await;
        };
        let Some(inflight) = ch.content.as_mut() else {
            let e = ProtocolError::unexpected_frame(format!(
                "content body on channel {channel_id} without a pending publish"
            ))
            .with_fatal();
            return self.protocol_error(channel_id, &e).await;
        };
        if let Err(e) = inflight.assembler.push_body(chunk) {
            return self.protocol_error(channel_id, &e).await;
        }
        if inflight.assembler.is_complete() {
            let inflight = ch.content.take().expect("checked above");
            return self.finish_publish(channel_id, inflight).await;
        }
        true
    }

    /// Content assembly complete: route to the destination set, one enqueue
    /// per queue (INV-05), and honor mandatory returns (FR-PUB02).
    async fn finish_publish(&mut self, channel_id: u16, inflight: InFlightPublish) -> bool {
        let vhost = self.vhost();
        let mut inflight = inflight;
        let body = inflight
            .assembler
            .take()
            .expect("finish_publish called only with complete content");
        let message = StoredMessage {
            property_bytes: inflight.property_bytes,
            body,
            exchange: inflight.exchange.clone(),
            routing_key: inflight.routing_key.clone(),
            persistent: inflight.persistent,
            redelivered: false,
        };

        // Resolve the destination set at a consistent ordering point.
        let destinations = {
            let topo = self.broker.topology.lock().unwrap();
            if inflight.exchange.is_empty() {
                // FR-E07: default exchange routes by queue name.
                topo.find_queue(vhost, &inflight.routing_key)
                    .into_iter()
                    .collect()
            } else {
                let ex_id = topo.find_exchange(vhost, &inflight.exchange);
                match ex_id
                    .and_then(|id| topo.exchange_record(id))
                    .map(|r| r.kind)
                {
                    Some(kind) => {
                        let bindings = ex_id.map(|id| topo.bindings_of(vhost, id)).unwrap_or(&[]);
                        rusty_mq_core::routing::route_message(kind, bindings, &inflight.routing_key)
                    }
                    None => Default::default(),
                }
            }
        };

        // Admit to every destination under the store budget.
        for queue in &destinations {
            let admitted = self
                .broker
                .store
                .lock()
                .unwrap()
                .enqueue(*queue, message.clone());
            match admitted {
                Ok(_) => {}
                // Never fabricate success on admission failure (§6.4).
                Err(AdmitError::BudgetExceeded) => {
                    let e = ProtocolError::channel(
                        reply_code::RESOURCE_ERROR,
                        "RESOURCE_ERROR - in-memory message budget exceeded",
                        rusty_mq_protocol::error::class_id::BASIC,
                        40, // basic.publish
                    );
                    return self.protocol_error(channel_id, &e).await;
                }
            }
        }

        // FR-PUB02: mandatory + zero destinations returns the message.
        if destinations.is_empty() && inflight.mandatory {
            return self
                .send_return(channel_id, reply_code::NO_ROUTE, "NO_ROUTE", &message)
                .await;
        }
        true
    }

    /// basic.return with the full message content (FR-PUB02).
    async fn send_return(
        &mut self,
        channel_id: u16,
        code: u16,
        text: &str,
        message: &StoredMessage,
    ) -> bool {
        let ret = basic::AMQPMethod::Return(basic::Return {
            reply_code: code,
            reply_text: text.into(),
            exchange: message.exchange.as_str().into(),
            routing_key: message.routing_key.as_str().into(),
        });
        if self
            .send(AMQPFrame::Method(channel_id, AMQPClass::Basic(ret)))
            .await
            .is_err()
        {
            return false;
        }
        let props = parse_properties(&message.property_bytes[..]).map(|(_, p)| p);
        let header = AMQPFrame::Header(
            channel_id,
            60,
            Box::new(AMQPContentHeader {
                class_id: 60,
                body_size: message.body.len() as u64,
                properties: props.unwrap_or_default(),
            }),
        );
        if self.send(header).await.is_err() {
            return false;
        }
        let budget = self.frame_payload_budget();
        for chunk in message.body.chunks(budget) {
            if self
                .send(AMQPFrame::Body(channel_id, chunk.to_vec()))
                .await
                .is_err()
            {
                return false;
            }
        }
        true
    }

    /// Deliver get-ok/deliver + content frames for a stored entry.
    async fn send_content(
        &self,
        channel_id: u16,
        method_frame: AMQPFrame,
        message: &StoredMessage,
    ) -> Result<(), ()> {
        self.send(method_frame).await?;
        let props = parse_properties(&message.property_bytes[..])
            .map(|(_, p)| p)
            .unwrap_or_default();
        let header = AMQPFrame::Header(
            channel_id,
            60,
            Box::new(AMQPContentHeader {
                class_id: 60,
                body_size: message.body.len() as u64,
                properties: props,
            }),
        );
        self.send(header).await?;
        let budget = self.frame_payload_budget();
        for chunk in message.body.chunks(budget) {
            self.send(AMQPFrame::Body(channel_id, chunk.to_vec()))
                .await?;
        }
        Ok(())
    }

    async fn handle_basic_get(&mut self, channel_id: u16, d: basic::Get) -> bool {
        const CLASS: u16 = 60;
        let method = d.get_amqp_method_id();
        let Ok(name) = self.resolve_queue_name(channel_id, d.queue.as_str()) else {
            let e = ProtocolError::not_found(
                "no previously declared queue to use as default",
                CLASS,
                method,
            );
            return self.protocol_error(channel_id, &e).await;
        };
        let vhost = self.vhost();
        // Resolve queue + exclusivity, then pop one entry.
        let outcome: Result<Option<(QueueEntry, u64)>, ProtocolError> = {
            let topo = self.broker.topology.lock().unwrap();
            let found = topo.find_queue(vhost, &name);
            match found {
                None => Err(ProtocolError::not_found(
                    format!("no queue '{name}' in vhost"),
                    CLASS,
                    method,
                )),
                Some(id)
                    if !topo
                        .check_exclusive_access(id, self.conn_id)
                        .unwrap_or(false) =>
                {
                    Err(ProtocolError::channel(
                        reply_code::RESOURCE_LOCKED,
                        format!(
                            "RESOURCE_LOCKED - queue '{name}' is exclusive to another connection"
                        ),
                        CLASS,
                        method,
                    ))
                }
                Some(id) => {
                    drop(topo);
                    let mut store = self.broker.store.lock().unwrap();
                    let remaining = store.len(id);
                    Ok(store
                        .pop_ready(id)
                        .map(|e| (e, remaining.saturating_sub(1))))
                }
            }
        };
        let entry = match outcome {
            Err(e) => return self.protocol_error(channel_id, &e).await,
            Ok(None) => {
                // Empty queue (FR-C02).
                let empty = basic::AMQPMethod::GetEmpty(basic::GetEmpty {});
                return self
                    .send(AMQPFrame::Method(channel_id, AMQPClass::Basic(empty)))
                    .await
                    .is_ok();
            }
            Ok(Some((entry, remaining))) => (entry, remaining),
        };
        let (entry, remaining) = entry;

        let delivery_tag = if d.no_ack {
            // Settled at delivery (§9.6 no-ack boundary, memory-backed form).
            0
        } else {
            let Some(ch) = self.channels.get_mut(&channel_id) else {
                return false;
            };
            let tag = ch.next_delivery_tag;
            ch.next_delivery_tag += 1;
            let queue = {
                let topo = self.broker.topology.lock().unwrap();
                topo.find_queue(vhost, &name)
            };
            let Some(queue_id) = queue else {
                return false;
            };
            ch.unacked.insert(
                tag,
                UnackedDelivery {
                    queue: queue_id,
                    entry: entry.clone(),
                },
            );
            tag
        };

        let get_ok = basic::AMQPMethod::GetOk(basic::GetOk {
            delivery_tag,
            redelivered: entry.message.redelivered,
            exchange: entry.message.exchange.as_str().into(),
            routing_key: entry.message.routing_key.as_str().into(),
            message_count: remaining as u32,
        });
        self.send_content(
            channel_id,
            AMQPFrame::Method(channel_id, AMQPClass::Basic(get_ok)),
            &entry.message,
        )
        .await
        .is_ok()
    }

    async fn handle_basic_ack(&mut self, channel_id: u16, d: basic::Ack) -> bool {
        const CLASS: u16 = 60;
        let method = d.get_amqp_method_id();
        let Some(ch) = self.channels.get_mut(&channel_id) else {
            return self
                .protocol_error(
                    channel_id,
                    &ProtocolError::connection(
                        reply_code::CHANNEL_ERROR,
                        format!("channel {channel_id} is not open"),
                    ),
                )
                .await;
        };
        let settled: Vec<u64> = if d.multiple {
            if d.delivery_tag == 0 {
                ch.unacked.keys().copied().collect()
            } else {
                ch.unacked
                    .keys()
                    .copied()
                    .filter(|t| *t <= d.delivery_tag)
                    .collect()
            }
        } else {
            if !ch.unacked.contains_key(&d.delivery_tag) {
                // Invalid/duplicate settlement is a protocol error (§6.1).
                let e = ProtocolError::precondition_failed(
                    format!("unknown delivery tag {}", d.delivery_tag),
                    CLASS,
                    method,
                );
                return self.protocol_error(channel_id, &e).await;
            }
            vec![d.delivery_tag]
        };
        // Positive acknowledgement is terminal: the entry was already held
        // out of the ready set, so settling just drops it.
        for tag in settled {
            ch.unacked.remove(&tag);
        }
        true
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
                // so a stale settlement cannot act on it (INV-04). Unacked
                // manual-ack deliveries requeue (FR-C06). Frames still in
                // flight for this channel are ignored until close-ok.
                self.drop_channel_state(channel_id);
                self.awaiting_close_ok.insert(channel_id, ());
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
