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
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use rusty_mq_core::store::{AdmitError, QueueEntry, StoredMessage};
use rusty_mq_core::topology::TopologyError;
use rusty_mq_core::{ChannelGeneration, ConnectionId, QueueId, VhostId};
use rusty_mq_protocol::error::reply_code;
use rusty_mq_protocol::{
    encode_frame, FrameReader, MessageAssembler, NegotiatedLimits, ProtocolError, ProtocolLimits,
    PROTOCOL_HEADER_0_9_1,
};

use std::sync::atomic::Ordering;

use crate::broker::Broker;
use crate::consumers::{Consumer, Job};
use crate::metrics::Metrics;

/// Bounded outbound queue per connection (frames waiting for the writer).
const OUTBOUND_CAP: usize = 256;
/// Read chunk size; frames larger than this simply span multiple chunks.
const READ_CHUNK: usize = 16 * 1024;
/// How long to wait for the client's `connection.close-ok` after the server
/// sent `connection.close`.
const CLOSE_LINGER: Duration = Duration::from_secs(5);
/// Bounded consumer-delivery mailbox per connection (ADR-0004; a full
/// mailbox requeues entries and stops scheduling to that consumer).
const CONSUMER_MAILBOX_CAP: usize = 256;
/// Outstanding unconfirmed publishes per channel (FR-PUB07 protective
/// ceiling; exceeding it closes the channel with 506 rather than letting a
/// publisher monopolize server bookkeeping).
const MAX_PENDING_CONFIRMS: u64 = 10_000;

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
    /// Consumer tags active on this channel (registry mirror).
    consumers: std::collections::HashSet<String>,
    /// Confirm mode (confirm.select): publishes get sequenced confirms.
    confirm_mode: bool,
    /// Next publish sequence (confirm namespace, FR-C04: independent of
    /// delivery tags).
    publish_seq: u64,
    /// Outstanding unconfirmed publishes (cap: FR-PUB07).
    pending_confirms: u64,
    /// Prefetch applied to consumers created after a basic.qos(global=false)
    /// (§6.2 rule 2). None = unlimited.
    prefetch_new_consumers: Option<u16>,
}

/// A basic.publish whose content frames are still arriving.
struct InFlightPublish {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    property_bytes: Vec<u8>,
    persistent: bool,
    assembler: MessageAssembler,
    /// Confirm sequence when the channel is in confirm mode (0 otherwise).
    confirm_seq: u64,
}

/// Which terminal settlement a journal record represents.
enum SettlementKind {
    Ack,
    Discard,
}

/// A delivery held out of the ready set pending settlement (§6.1).
struct UnackedDelivery {
    queue: QueueId,
    entry: QueueEntry,
    /// Set when the delivery came from a consumer (credit release on
    /// settlement).
    consumer_tag: Option<String>,
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
    /// Sender side of this connection's consumer mailbox (registry holds
    /// clones for dispatch).
    mailbox_tx: mpsc::Sender<Job>,
    /// Client declared the consumer_cancel_notify capability (§4.3).
    client_cancel_notify: bool,
    /// Client declared connection.blocked (§4.3 gates our notifications).
    client_blocking: bool,
    /// Authenticated principal (set at start-ok; empty never — auth is
    /// required before tune).
    username: String,
    /// Peer address (throttling + logging).
    peer: String,
    limits: ProtocolLimits,
    negotiated: Option<NegotiatedLimits>,
    phase: Phase,
    channels: HashMap<u16, ChannelState>,
    outbound: mpsc::Sender<Vec<u8>>,
}

impl Connection {
    /// Run a connection to completion. Never panics on socket errors.
    pub async fn run<S>(socket: S, peer: String, broker: Arc<Broker>)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let conn_id = broker.next_connection_id();
        let (outbound_tx, outbound_rx) = mpsc::channel::<Vec<u8>>(OUTBOUND_CAP);
        // Consumer mailbox: bounded push-delivery queue (ADR-0004).
        let (mailbox_tx, mailbox_rx) = mpsc::channel::<Job>(CONSUMER_MAILBOX_CAP);
        // Control channel: alarm notifications (FR-R07), bounded.
        let (control_tx, control_rx) = mpsc::channel::<crate::broker::Control>(8);
        let (read_half, write_half) = tokio::io::split(socket);

        let writer = tokio::spawn(writer_task(write_half, outbound_rx));

        // FR-S05: throttled peers are refused before the handshake burns
        // Argon2 time. Key on the HOST part — source ports differ per
        // connection, so the full peer string would never trip the
        // per-peer window for a reconnecting attacker.
        let peer_host = peer_host_of(&peer);
        if broker.auth_throttle.check(&peer_host) == crate::throttle::Decision::Throttled {
            tracing::warn!(peer = %peer, "connection refused: auth throttle");
            return;
        }
        Metrics::inc(&broker.metrics.connections_opened);
        broker.register_connection(crate::broker::LiveConnection {
            id: conn_id,
            username: String::new(), // enriched post-auth with the registry pass
            control: control_tx,
        });
        let mut conn = Self {
            broker,
            conn_id,
            vhost: None,
            awaiting_close_ok: HashMap::new(),
            mailbox_tx,
            client_cancel_notify: false,
            client_blocking: false,
            username: String::new(),
            peer: peer.clone(),
            limits: ProtocolLimits::default(),
            negotiated: None,
            phase: Phase::AwaitStartOk,
            channels: HashMap::new(),
            outbound: outbound_tx,
        };

        tracing::info!(connection = %conn_id, peer = %peer, "connection opened");
        conn.drive(read_half, mailbox_rx, control_rx).await;
        self_unregister(&conn.broker, conn_id);

        // Reclaim connection-scoped resources (FR-P09): exclusive queues
        // disappear with their owning connection (FR-Q04).
        // Deregister every consumer of this connection before general
        // teardown so no further jobs are scheduled to this mailbox
        // (consumers die with their connection, FR-C01).
        {
            let affected = conn
                .broker
                .consumers
                .lock()
                .unwrap()
                .deregister_connection(conn.conn_id);
            conn.broker.maybe_auto_delete_queues(&affected);
        }
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

    /// Main loop: handshake with deadline, then socket reads + consumer
    /// mailbox jobs concurrently (heartbeats idle-detect the socket side).
    async fn drive(
        &mut self,
        mut read: ReadHalf<impl AsyncRead + AsyncWrite + Unpin>,
        mut mailbox: mpsc::Receiver<Job>,
        mut control: mpsc::Receiver<crate::broker::Control>,
    ) {
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
            // Socket reads and consumer jobs run concurrently; the idle
            // deadline applies to the socket side (heartbeats).
            let idle = self.idle_timeout(handshake_deadline);
            tokio::select! {
                read = tokio::time::timeout(idle, read.read(&mut buf)) => {
                    let n = match read {
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
                job = mailbox.recv() => {
                    if let Some(job) = job {
                        if !self.handle_job(job).await {
                            return;
                        }
                    }
                    // None: all senders dropped (broker teardown); keep
                    // the socket side alive.
                }
                msg = control.recv() => {
                    if let Some(msg) = msg {
                        if !self.handle_control(msg).await {
                            return;
                        }
                    }
                }
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
        // §4.3: only implemented and tested capabilities are advertised.
        let mut caps = FieldTable::default();
        caps.insert(
            "publisher_confirms".into(),
            amq_protocol::types::AMQPValue::Boolean(true),
        );
        caps.insert(
            "basic.nack".into(),
            amq_protocol::types::AMQPValue::Boolean(true),
        );
        caps.insert(
            "consumer_cancel_notify".into(),
            amq_protocol::types::AMQPValue::Boolean(true),
        );
        caps.insert(
            "connection.blocked".into(),
            amq_protocol::types::AMQPValue::Boolean(true),
        );
        props.insert(
            "capabilities".into(),
            amq_protocol::types::AMQPValue::FieldTable(caps),
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
        if !self.broker.authenticate(&user, &pass) {
            tracing::warn!(user = %user, "authentication refused");
            self.broker
                .auth_throttle
                .record_failure(&peer_host_of(&self.peer));
            return self
                .protocol_error(
                    0,
                    &ProtocolError::connection(
                        reply_code::ACCESS_REFUSED,
                        "ACCESS_REFUSED - login refused for user",
                    ),
                )
                .await;
        }
        self.username = user.clone();
        self.broker.set_connection_user(self.conn_id, &user);
        // Extension capabilities the client declared (§4.3 gating).
        self.client_blocking = start_ok
            .client_properties
            .inner()
            .get("capabilities")
            .and_then(|v| match v {
                amq_protocol::types::AMQPValue::FieldTable(t) => Some(t),
                _ => None,
            })
            .is_some_and(|t| {
                matches!(
                    t.inner().get("connection.blocked"),
                    Some(amq_protocol::types::AMQPValue::Boolean(true))
                )
            });
        self.client_cancel_notify = start_ok
            .client_properties
            .inner()
            .get("capabilities")
            .and_then(|v| match v {
                amq_protocol::types::AMQPValue::FieldTable(t) => Some(t),
                _ => None,
            })
            .is_some_and(|t| {
                matches!(
                    t.inner().get("consumer_cancel_notify"),
                    Some(amq_protocol::types::AMQPValue::Boolean(true))
                )
            });
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
        if vhost.is_some()
            && !self
                .broker
                .auth
                .lock()
                .unwrap()
                .may_access_vhost(&self.username, open.virtual_host.as_str())
        {
            return self
                .protocol_error(
                    0,
                    &ProtocolError::connection(
                        reply_code::ACCESS_REFUSED,
                        format!(
                            "ACCESS_REFUSED - user '{}' has no access to vhost '{}'",
                            self.username, open.virtual_host
                        ),
                    ),
                )
                .await;
        }
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
                        consumers: std::collections::HashSet::new(),
                        prefetch_new_consumers: None,
                        confirm_mode: false,
                        publish_seq: 1,
                        pending_confirms: 0,
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
            AMQPClass::Basic(basic::AMQPMethod::Consume(d)) => {
                self.handle_basic_consume(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::Cancel(d)) => {
                self.handle_basic_cancel(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::Qos(d)) => {
                self.handle_basic_qos(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::CancelOk(_)) => {
                // Client ack of a server-initiated basic.cancel; the tag was
                // already removed when the cancel was sent.
                true
            }
            AMQPClass::Basic(basic::AMQPMethod::Reject(d)) => {
                self.handle_basic_reject(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::Nack(d)) => {
                self.handle_basic_nack(channel_id, d).await
            }
            AMQPClass::Basic(basic::AMQPMethod::Recover(d)) => {
                self.handle_basic_recover(channel_id, d).await
            }
            AMQPClass::Confirm(amq_protocol::protocol::confirm::AMQPMethod::Select(_d)) => {
                // Confirm mode is per-channel (FR-PUB05).
                if let Some(ch) = self.channels.get_mut(&channel_id) {
                    ch.confirm_mode = true;
                }
                let ok = amq_protocol::protocol::confirm::AMQPMethod::SelectOk(
                    amq_protocol::protocol::confirm::SelectOk {},
                );
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Confirm(ok)))
                    .await
                    .is_ok()
            }
            AMQPClass::Channel(channel::AMQPMethod::Flow(d)) => {
                self.handle_channel_flow(channel_id, d).await
            }
            AMQPClass::Channel(channel::AMQPMethod::FlowOk(_)) => {
                // Response to a server flow method; rusty-mq never sends
                // one (flow is a documented no-op), so ignore.
                true
            }
            AMQPClass::Basic(basic::AMQPMethod::RecoverAsync(_) | basic::AMQPMethod::Return(_)) => {
                // Obsolete / server-only methods from clients are 540.
                let e = ProtocolError::not_implemented(
                    "method (client-initiated basic.recover-async/return)",
                    60,
                    0,
                );
                self.protocol_error(channel_id, &e).await
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

    /// §11.2 permission check for this connection's authenticated user.
    /// Err carries the 403 close; Ok(()) proceeds.
    fn require_access(
        &self,
        vhost: VhostId,
        vhost_name: &str,
        access: rusty_mq_core::auth::Access,
        resource: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ProtocolError> {
        let allowed =
            self.broker
                .auth
                .lock()
                .unwrap()
                .check(&self.username, vhost_name, access, resource);
        if allowed {
            Ok(())
        } else {
            Err(ProtocolError::channel(
                reply_code::ACCESS_REFUSED,
                format!(
                    "ACCESS_REFUSED - {} access to '{resource}' refused for user '{}'",
                    match access {
                        rusty_mq_core::auth::Access::Configure => "configure",
                        rusty_mq_core::auth::Access::Write => "write",
                        rusty_mq_core::auth::Access::Read => "read",
                    },
                    self.username
                ),
                class_id,
                method_id,
            ))
        }
        .map(|_| {
            let _ = vhost;
        })
    }

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
            TopologyError::ResourceErrorJournal => ProtocolError::channel(
                reply_code::RESOURCE_ERROR,
                "RESOURCE_ERROR - durable journal commit failed",
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

        // Active declare: §11.2 configure on the named exchange.
        if let Err(e) = self.require_access(
            vhost,
            "/",
            rusty_mq_core::auth::Access::Configure,
            d.exchange.as_str(),
            CLASS,
            method,
        ) {
            return self.protocol_error(channel_id, &e).await;
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
            let result = topo.declare_exchange(
                vhost,
                d.exchange.as_str(),
                kind,
                d.durable,
                d.auto_delete,
                d.internal,
            );
            // Durable declarations wait for the journal commit before their
            // success reply (§9.1); memory mode commits trivially.
            let mut journal_failure = false;
            if let (Ok(id), true) = (&result, d.durable) {
                let record =
                    rusty_mq_storage::Record::ExchangeDeclare(rusty_mq_storage::ExchangeRecord {
                        name: d.exchange.as_str().to_string(),
                        id: id.to_raw(),
                        kind: match kind {
                            rusty_mq_core::routing::ExchangeType::Direct => 0,
                            rusty_mq_core::routing::ExchangeType::Fanout => 1,
                            rusty_mq_core::routing::ExchangeType::Topic => 2,
                        },
                        durable: d.durable,
                        auto_delete: d.auto_delete,
                        internal: d.internal,
                    });
                if self.broker.journal_commit(&[record]).is_err() {
                    topo.remove_exchange_by_id(vhost, *id);
                    journal_failure = true;
                }
            }
            if journal_failure {
                Err(rusty_mq_core::topology::DeclareExchangeError::Topology(
                    rusty_mq_core::topology::TopologyError::ResourceErrorJournal,
                ))
            } else {
                result.map(|_| ())
            }
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
        // §11.2: delete requires configure on the exchange.
        if let Err(e) = self.require_access(
            vhost,
            "/",
            rusty_mq_core::auth::Access::Configure,
            d.exchange.as_str(),
            CLASS,
            method,
        ) {
            return self.protocol_error(channel_id, &e).await;
        }
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

        // Active declare: §11.2 configure on the queue name.
        if let Err(e) = self.require_access(
            vhost,
            "/",
            rusty_mq_core::auth::Access::Configure,
            d.queue.as_str(),
            CLASS,
            method,
        ) {
            return self.protocol_error(channel_id, &e).await;
        }
        // Active declare.
        let declared: Result<String, rusty_mq_core::topology::DeclareQueueError> = {
            let mut topo = self.broker.topology.lock().unwrap();
            let result = topo.declare_queue(vhost, d.queue.as_str(), profile, owner);
            // Durable declarations wait for the journal commit (§9.1).
            let mut journal_failure = false;
            if let (Ok(id), true) = (&result, d.durable) {
                let name = topo
                    .queue_record(*id)
                    .map(|r| r.name.clone())
                    .unwrap_or_default();
                let record =
                    rusty_mq_storage::Record::QueueDeclare(rusty_mq_storage::QueueRecord {
                        name,
                        id: id.to_raw(),
                        durable: profile.durable,
                        exclusive: false, // durable+exclusive rejected upstream
                        auto_delete: false,
                        owner: 0,
                    });
                if self.broker.journal_commit(&[record]).is_err() {
                    topo.remove_queue_by_id(vhost, *id);
                    journal_failure = true;
                }
            }
            if journal_failure {
                Err(rusty_mq_core::topology::DeclareQueueError::Topology(
                    rusty_mq_core::topology::TopologyError::ResourceErrorJournal,
                ))
            } else {
                result.map(|id| {
                    topo.queue_record(id)
                        .map(|r| r.name.clone())
                        .unwrap_or_default()
                })
            }
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
        // Resolve, condition-check, delete, tear down consumers, drain.
        let outcome: Result<u64, ProtocolError> = {
            let (found, lock_ok) = {
                let topo = self.broker.topology.lock().unwrap();
                let found = topo.find_queue(vhost, &name);
                let lock_ok = found.is_some_and(|id| {
                    topo.check_exclusive_access(id, self.conn_id)
                        .unwrap_or(false)
                });
                (found, lock_ok)
            };
            let owned = found.filter(|_| lock_ok);
            if owned.is_none() {
                let e = if found.is_some() {
                    ProtocolError::channel(
                        reply_code::RESOURCE_LOCKED,
                        format!(
                            "RESOURCE_LOCKED - queue '{name}' is exclusive to another connection"
                        ),
                        CLASS,
                        method,
                    )
                } else {
                    ProtocolError::not_found(format!("no queue '{name}' in vhost"), CLASS, method)
                };
                return self.protocol_error(channel_id, &e).await;
            }
            // §11.2: delete requires configure on the queue (computed in
            // the outcome so the block's value carries the refusal).
            let id = owned.expect("checked above");
            let access_denied = self
                .require_access(
                    vhost,
                    "/",
                    rusty_mq_core::auth::Access::Configure,
                    &name,
                    CLASS,
                    method,
                )
                .err();
            // Real counts for the if_unused/if_empty conditions (FR-Q06)
            // and the delete-ok reply.
            let ready = self.broker.store.lock().unwrap().len(id);
            let consumer_count = self.broker.consumers.lock().unwrap().consumer_count(id);
            let durable = self
                .broker
                .topology
                .lock()
                .unwrap()
                .queue_record(id)
                .is_some_and(|r| r.profile.durable);
            // Durable deletes commit to the journal BEFORE the live removal:
            // a failed commit leaves everything intact (§6.4).
            let denied_error = access_denied;
            let journal_ok = !durable
                || self
                    .broker
                    .journal_commit(&[rusty_mq_storage::Record::QueueDelete { id: id.to_raw() }])
                    .is_ok();
            if let Some(e) = denied_error {
                Err(e)
            } else if !journal_ok {
                Err(ProtocolError::channel(
                    reply_code::RESOURCE_ERROR,
                    "RESOURCE_ERROR - durable journal commit failed",
                    CLASS,
                    method,
                ))
            } else {
                let deleted = self.broker.topology.lock().unwrap().delete_queue(
                    vhost,
                    &name,
                    d.if_unused,
                    d.if_empty,
                    consumer_count,
                    ready,
                );
                match deleted {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Self::topology_error(e, CLASS, method)),
                }
                .map(|_| {
                    // Tear down consumers: notify capable ones, deregister all
                    // (FR-Q09), then drop stored entries.
                    let jobs = self.broker.cancel_and_deregister_queue(id);
                    for (mailbox, job) in jobs {
                        let _ = mailbox.try_send(job);
                    }
                    self.broker.store.lock().unwrap().drain(id)
                })
            }
        };
        match outcome {
            Ok(discarded) => {
                if let Some(ch) = self.channels.get_mut(&channel_id) {
                    if ch.last_queue.as_deref() == Some(name.as_str()) {
                        ch.last_queue = None;
                    }
                }
                if d.nowait {
                    true
                } else {
                    let ok = queue::AMQPMethod::DeleteOk(queue::DeleteOk {
                        message_count: discarded as u32,
                    });
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
        // §11.2: bind/unbind = write on the destination queue + read on the
        // source exchange (names resolved in a guard-free prelude).
        {
            // Scoped: the guard ends on every path before any await.
            let denied = {
                let topo = self.broker.topology.lock().unwrap();
                topo.find_queue(vhost, &queue_name)
                    .zip(topo.find_exchange(vhost, d.exchange.as_str()))
                    .and_then(|(q, e)| {
                        let q_name = topo
                            .queue_record(q)
                            .map(|r| r.name.clone())
                            .unwrap_or_default();
                        let ex_name = topo
                            .exchange_record(e)
                            .map(|r| r.name.clone())
                            .unwrap_or_default();
                        self.require_access(
                            vhost,
                            "/",
                            rusty_mq_core::auth::Access::Write,
                            &q_name,
                            CLASS,
                            method,
                        )
                        .and_then(|()| {
                            self.require_access(
                                vhost,
                                "/",
                                rusty_mq_core::auth::Access::Read,
                                &ex_name,
                                CLASS,
                                method,
                            )
                        })
                        .err()
                    })
            };
            if let Some(err) = denied {
                return self.protocol_error(channel_id, &err).await;
            }
        }
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
                (Some(q), Some(e)) => {
                    let both_durable = topo.exchange_record(e).is_some_and(|r| r.durable)
                        && topo.queue_record(q).is_some_and(|r| r.profile.durable);
                    let result = topo
                        .bind(vhost, e, q, d.routing_key.as_str())
                        .map_err(|err| Self::topology_error(err, CLASS, method));
                    if result.is_ok() && both_durable {
                        let record = rusty_mq_storage::Record::Bind(rusty_mq_storage::Binding {
                            exchange: e.to_raw(),
                            queue: q.to_raw(),
                            routing_key: d.routing_key.as_str().to_string(),
                        });
                        drop(topo);
                        if self.broker.journal_commit(&[record]).is_err() {
                            Err(ProtocolError::channel(
                                reply_code::RESOURCE_ERROR,
                                "RESOURCE_ERROR - durable journal commit failed",
                                CLASS,
                                method,
                            ))
                        } else {
                            result
                        }
                    } else {
                        result
                    }
                }
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
        // §11.2: bind/unbind = write on the destination queue + read on the
        // source exchange (names resolved in a guard-free prelude).
        {
            // Scoped: the guard ends on every path before any await.
            let denied = {
                let topo = self.broker.topology.lock().unwrap();
                topo.find_queue(vhost, &queue_name)
                    .zip(topo.find_exchange(vhost, d.exchange.as_str()))
                    .and_then(|(q, e)| {
                        let q_name = topo
                            .queue_record(q)
                            .map(|r| r.name.clone())
                            .unwrap_or_default();
                        let ex_name = topo
                            .exchange_record(e)
                            .map(|r| r.name.clone())
                            .unwrap_or_default();
                        self.require_access(
                            vhost,
                            "/",
                            rusty_mq_core::auth::Access::Write,
                            &q_name,
                            CLASS,
                            method,
                        )
                        .and_then(|()| {
                            self.require_access(
                                vhost,
                                "/",
                                rusty_mq_core::auth::Access::Read,
                                &ex_name,
                                CLASS,
                                method,
                            )
                        })
                        .err()
                    })
            };
            if let Some(err) = denied {
                return self.protocol_error(channel_id, &err).await;
            }
        }
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
                (Some(q), Some(e)) => {
                    let both_durable = topo.exchange_record(e).is_some_and(|r| r.durable)
                        && topo.queue_record(q).is_some_and(|r| r.profile.durable);
                    let result = topo
                        .unbind(vhost, e, q, d.routing_key.as_str())
                        .map_err(|err| Self::topology_error(err, CLASS, method));
                    if result.is_ok() && both_durable {
                        let record = rusty_mq_storage::Record::Unbind(rusty_mq_storage::Binding {
                            exchange: e.to_raw(),
                            queue: q.to_raw(),
                            routing_key: d.routing_key.as_str().to_string(),
                        });
                        drop(topo);
                        if self.broker.journal_commit(&[record]).is_err() {
                            Err(ProtocolError::channel(
                                reply_code::RESOURCE_ERROR,
                                "RESOURCE_ERROR - durable journal commit failed",
                                CLASS,
                                method,
                            ))
                        } else {
                            result
                        }
                    } else {
                        result
                    }
                }
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
                    let durable = topo.queue_record(id).is_some_and(|r| r.profile.durable);
                    drop(topo);
                    // Capture the ready set at this ordering point (§9.4),
                    // journal it for durable queues, then purge live. A
                    // failed journal commit aborts with everything intact.
                    let mut store = self.broker.store.lock().unwrap();
                    let mut journal_failure = false;
                    if durable {
                        let seqs = store.ready_seqs(id);
                        if !seqs.is_empty() {
                            let record = rusty_mq_storage::Record::Purge {
                                queue: id.to_raw(),
                                seqs,
                            };
                            if self.broker.journal_commit(&[record]).is_err() {
                                journal_failure = true;
                            }
                        }
                    }
                    if journal_failure {
                        Err(ProtocolError::channel(
                            reply_code::RESOURCE_ERROR,
                            "RESOURCE_ERROR - durable journal commit failed",
                            CLASS,
                            method,
                        ))
                    } else {
                        Ok(store.purge(id))
                    }
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

    /// Reclaim a channel's state: deregister its consumers (auto-delete
    /// check follows), requeue unacked manual-ack deliveries (FR-C06), and
    /// drop any half-assembled publish.
    fn drop_channel_state(&mut self, channel_id: u16) {
        if let Some(mut ch) = self.channels.remove(&channel_id) {
            ch.content = None;
            let affected = {
                let mut consumers = self.broker.consumers.lock().unwrap();
                let mut store = self.broker.store.lock().unwrap();
                let queues = consumers.deregister_channel(self.conn_id, channel_id);
                for (_, u) in ch.unacked.drain() {
                    store.requeue(u.queue, u.entry);
                }
                queues
            };
            self.broker.maybe_auto_delete_queues(&affected);
            // Requeued entries may be deliverable to other consumers.
            for q in affected {
                self.broker.dispatch_queue(q);
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
        // §11.2: publish requires write on the exchange; the default
        // exchange normalizes to amq.default.
        {
            let perm_name = if d.exchange.as_str().is_empty() {
                rusty_mq_core::auth::AuthState::DEFAULT_EXCHANGE_PERMISSION_NAME
            } else {
                d.exchange.as_str()
            };
            if let Err(e) = self.require_access(
                vhost,
                "/",
                rusty_mq_core::auth::Access::Write,
                perm_name,
                CLASS,
                d.get_amqp_method_id(),
            ) {
                return self.protocol_error(channel_id, &e).await;
            }
        }
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
        if ch.pending_confirms >= MAX_PENDING_CONFIRMS {
            let e = ProtocolError::channel(
                reply_code::RESOURCE_ERROR,
                format!(
                    "RESOURCE_ERROR - outstanding publisher confirms exceed the ceiling ({MAX_PENDING_CONFIRMS})"
                ),
                rusty_mq_protocol::error::class_id::BASIC,
                40, // basic.publish
            );
            return self.protocol_error(channel_id, &e).await;
        }
        // Confirm-mode sequencing (FR-PUB05); the sequence is reserved at
        // method time and resolved (ack/nack) when the publish completes or
        // is rejected.
        let confirm_seq = if ch.confirm_mode {
            ch.pending_confirms += 1;
            ch.publish_seq
        } else {
            0
        };
        if ch.confirm_mode {
            ch.publish_seq += 1;
        }
        ch.content = Some(InFlightPublish {
            exchange: d.exchange.as_str().to_string(),
            routing_key: d.routing_key.as_str().to_string(),
            mandatory: d.mandatory,
            property_bytes: Vec::new(),
            persistent: false,
            assembler: MessageAssembler::new(self.limits.max_message_bytes),
            confirm_seq,
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
            let seq = inflight.confirm_seq;
            return self.reject_publish(channel_id, seq, &e).await;
        }
        let (blob, persistent) =
            match Self::admit_properties(&header.properties, &self.broker.test_user.username, 60) {
                Ok(x) => x,
                Err(e) => {
                    let seq = inflight.confirm_seq;
                    return self.reject_publish(channel_id, seq, &e).await;
                }
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

        // §10/FR-R02: a memory alarm stops new message admissions (bounded
        // memory; confirmed persistent messages are never evicted).
        if self.broker.memory_alarm() {
            let e = ProtocolError::channel(
                reply_code::RESOURCE_ERROR,
                "RESOURCE_ERROR - memory alarm: message admissions paused",
                rusty_mq_protocol::error::class_id::BASIC,
                40, // basic.publish
            );
            self.broker.evaluate_alarms_and_notify();
            return self.protocol_error(channel_id, &e).await;
        }
        // Durable-destination routing for persistent messages: pre-assign
        // sequences and commit the Enqueue record BEFORE live admission
        // (§9.5). The store lock is held across the commit so the assigned
        // sequences cannot race another publisher (correctness before
        // throughput; group commit batches this in M5).
        let durable_destinations: Vec<QueueId> = if message.persistent {
            let topo = self.broker.topology.lock().unwrap();
            destinations
                .iter()
                .copied()
                .filter(|q| topo.queue_record(*q).is_some_and(|r| r.profile.durable))
                .collect()
        } else {
            Vec::new()
        };
        // The commit happens inside a scoped block so the store guard ends
        // before any await on this path.
        let commit_failed = if !durable_destinations.is_empty() {
            let store = self.broker.store.lock().unwrap();
            let assigned: Vec<(u64, u64)> = durable_destinations
                .iter()
                .map(|q| (q.to_raw(), store.next_seq_of(*q)))
                .collect();
            let record = rusty_mq_storage::Record::Enqueue(rusty_mq_storage::Enqueue {
                // Message identity is the (queue, seq) pair in V1; the
                // broker-wide id arrives with the redb projection (M5).
                message_id: 0,
                property_bytes: message.property_bytes.clone(),
                body: message.body.clone(),
                exchange: message.exchange.clone(),
                routing_key: message.routing_key.clone(),
                persistent: true,
                destinations: assigned,
            });
            let failed = self.broker.journal_commit(&[record]).is_err();
            drop(store); // unconditional within this block
            failed
        } else {
            false
        };
        if commit_failed {
            let e = ProtocolError::channel(
                reply_code::RESOURCE_ERROR,
                "RESOURCE_ERROR - durable journal commit failed",
                rusty_mq_protocol::error::class_id::BASIC,
                40, // basic.publish
            );
            return self.protocol_error(channel_id, &e).await;
        }

        Metrics::inc(&self.broker.metrics.messages_published);
        // Admit to every destination under the store budget, then dispatch
        // to waiting consumers. Admission crossing the budget raises the
        // memory alarm (evaluate + notify below).
        let mut admission_failure: Option<AdmitError> = None;
        for queue in &destinations {
            let admitted = self
                .broker
                .store
                .lock()
                .unwrap()
                .enqueue(*queue, message.clone());
            match admitted {
                Ok(_) => self.broker.dispatch_queue(*queue),
                // Never fabricate success on admission failure (§6.4);
                // the budget being hit IS the memory alarm transition.
                Err(e @ AdmitError::BudgetExceeded) => {
                    admission_failure = Some(e);
                    break;
                }
            }
        }

        self.broker.evaluate_alarms_and_notify();
        if admission_failure.is_some() {
            let e = ProtocolError::channel(
                reply_code::RESOURCE_ERROR,
                "RESOURCE_ERROR - memory alarm: message budget exceeded",
                rusty_mq_protocol::error::class_id::BASIC,
                40, // basic.publish
            );
            return self.protocol_error(channel_id, &e).await;
        }
        // FR-PUB02: mandatory + zero destinations returns the message.
        if destinations.is_empty() && inflight.mandatory {
            let sent = self
                .send_return(channel_id, reply_code::NO_ROUTE, "NO_ROUTE", &message)
                .await;
            // The return serializes BEFORE the positive confirm (§6.3).
            return sent
                && self
                    .emit_confirm(channel_id, inflight.confirm_seq, true)
                    .await;
        }
        // INV-01: the positive confirm is emitted only after every selected
        // destination's admission completed — durable destinations already
        // crossed the journal commit boundary above.
        self.emit_confirm(channel_id, inflight.confirm_seq, true)
            .await
    }

    /// A publish was rejected before admission: in confirm mode the
    /// reserved sequence is nacked first (§6.4 "nack where safe"), then
    /// the channel error closes as usual (outcomes for later publishes
    /// are explicitly uncertain).
    async fn reject_publish(&mut self, channel_id: u16, seq: u64, e: &ProtocolError) -> bool {
        if seq != 0 {
            let _ = self.emit_confirm(channel_id, seq, false).await;
        }
        self.protocol_error(channel_id, e).await
    }

    /// Emit a publisher confirm (basic.ack in the confirm namespace,
    /// FR-C04) or nack; no-op outside confirm mode (seq 0).
    async fn emit_confirm(&mut self, channel_id: u16, seq: u64, positive: bool) -> bool {
        if seq == 0 {
            return true;
        }
        if let Some(ch) = self.channels.get_mut(&channel_id) {
            ch.pending_confirms = ch.pending_confirms.saturating_sub(1);
        }
        let method = if positive {
            basic::AMQPMethod::Ack(basic::Ack {
                delivery_tag: seq,
                multiple: false,
            })
        } else {
            basic::AMQPMethod::Nack(basic::Nack {
                delivery_tag: seq,
                multiple: false,
                requeue: false,
            })
        };
        self.send(AMQPFrame::Method(channel_id, AMQPClass::Basic(method)))
            .await
            .is_ok()
    }

    /// basic.return with the full message content (FR-PUB02).
    #[allow(clippy::too_many_arguments)]
    async fn send_return(
        &mut self,
        channel_id: u16,
        code: u16,
        text: &str,
        message: &StoredMessage,
    ) -> bool {
        Metrics::inc(&self.broker.metrics.messages_returned);
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
        let outcome: Result<Option<(QueueEntry, QueueId, u64)>, ProtocolError> = {
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
                        .map(|e| (e, id, remaining.saturating_sub(1))))
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
            Ok(Some(x)) => x,
        };
        let (entry, queue_id, remaining) = entry;

        // §9.6: journal-before-exposure gate for the polling path.
        if !self
            .journal_before_delivery(queue_id, &entry, d.no_ack)
            .await
        {
            return true;
        }

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
                    consumer_tag: None, // basic.get, not a consumer
                },
            );
            tag
        };

        Metrics::inc(&self.broker.metrics.messages_delivered);
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

    /// channel.flow: deprecated in practice; reply flow-ok(active=true)
    /// without pausing the channel (RabbitMQ-compatible no-op — documented
    /// in the protocol profile). A client asking to stop content cannot be
    /// honored mid-stream in V1; it should stop consuming instead.
    async fn handle_channel_flow(&mut self, channel_id: u16, d: channel::Flow) -> bool {
        let ok = channel::AMQPMethod::FlowOk(channel::FlowOk { active: true });
        let _ = d.active;
        self.send(AMQPFrame::Method(channel_id, AMQPClass::Channel(ok)))
            .await
            .is_ok()
    }

    /// Collect the delivery tags a settlement applies to: a single tag, or
    /// all outstanding (multiple with tag 0) / all ≤ tag (multiple with a
    /// tag). Unknown single tags are 406 (§6.1).
    fn settlement_tags(
        &self,
        channel_id: u16,
        delivery_tag: u64,
        multiple: bool,
        class_id: u16,
        method_id: u16,
    ) -> Result<Vec<u64>, ProtocolError> {
        let Some(ch) = self.channels.get(&channel_id) else {
            return Err(ProtocolError::connection(
                reply_code::CHANNEL_ERROR,
                format!("channel {channel_id} is not open"),
            ));
        };
        if multiple {
            if delivery_tag == 0 {
                Ok(ch.unacked.keys().copied().collect())
            } else {
                Ok(ch
                    .unacked
                    .keys()
                    .copied()
                    .filter(|t| *t <= delivery_tag)
                    .collect())
            }
        } else if ch.unacked.contains_key(&delivery_tag) {
            Ok(vec![delivery_tag])
        } else {
            Err(ProtocolError::precondition_failed(
                format!("unknown delivery tag {delivery_tag}"),
                class_id,
                method_id,
            ))
        }
    }

    /// Apply a settlement to `tags`: drop (ack/nack without requeue —
    /// terminal discard in V1, DLX is deferred) or requeue (original
    /// relative position, redelivered hint). Releases consumer credit and
    /// re-dispatched freed queues. Terminal settlements of persistent
    /// entries from durable queues are journaled (INV-02: a positively
    /// settled entry must never resurrect).
    fn apply_settlement(
        &mut self,
        channel_id: u16,
        tags: Vec<u64>,
        requeue: bool,
        ack_kind: SettlementKind,
    ) {
        let settled_count = tags.len();
        let mut requeued_queues: Vec<QueueId> = Vec::new();
        let mut released: Vec<Option<String>> = Vec::new();
        let mut journal_records: Vec<rusty_mq_storage::Record> = Vec::new();
        if let Some(ch) = self.channels.get_mut(&channel_id) {
            for tag in tags {
                if let Some(u) = ch.unacked.remove(&tag) {
                    released.push(u.consumer_tag);
                    if requeue {
                        self.broker.store.lock().unwrap().requeue(u.queue, u.entry);
                        if !requeued_queues.contains(&u.queue) {
                            requeued_queues.push(u.queue);
                        }
                    } else {
                        // Terminal: journal when persistent + durable queue.
                        if u.entry.message.persistent {
                            let durable = self
                                .broker
                                .topology
                                .lock()
                                .unwrap()
                                .queue_record(u.queue)
                                .is_some_and(|r| r.profile.durable);
                            if durable {
                                journal_records.push(match ack_kind {
                                    SettlementKind::Ack => rusty_mq_storage::Record::SettleAck {
                                        queue: u.queue.to_raw(),
                                        seq: u.entry.seq,
                                    },
                                    SettlementKind::Discard => {
                                        rusty_mq_storage::Record::SettleDiscard {
                                            queue: u.queue.to_raw(),
                                            seq: u.entry.seq,
                                        }
                                    }
                                });
                            }
                        }
                        // The entry was already held out of the ready set;
                        // dropping it from unacked completes the settlement.
                    }
                }
            }
        }
        match ack_kind {
            SettlementKind::Ack => {
                self.broker
                    .metrics
                    .messages_acked
                    .fetch_add(settled_count as u64, Ordering::Relaxed);
            }
            SettlementKind::Discard => {
                self.broker
                    .metrics
                    .messages_nacked
                    .fetch_add(settled_count as u64, Ordering::Relaxed);
            }
        }
        if !journal_records.is_empty() && self.broker.journal_commit(&journal_records).is_err() {
            tracing::error!(
                count = journal_records.len(),
                "settlement journal commit failed; entry may redeliver after restart"
            );
            // §6.4: never fabricate success — but the settlement was
            // already applied in-memory; the failure is logged and the
            // entries remain recoverable (at-least-once, never loss of
            // an unsettled message).
        }
        for consumer_tag in released.into_iter().flatten() {
            self.broker
                .consumers
                .lock()
                .unwrap()
                .settle(self.conn_id, channel_id, &consumer_tag);
        }
        // Freed credit and requeued entries may enable deliveries.
        let mut queues = self
            .broker
            .consumers
            .lock()
            .unwrap()
            .queues_with_consumers_on(self.conn_id, channel_id);
        for q in requeued_queues {
            if !queues.contains(&q) {
                queues.push(q);
            }
        }
        for q in queues {
            self.broker.dispatch_queue(q);
        }
        // Freed bytes may clear the memory alarm (notify).
        self.broker.evaluate_alarms_and_notify();
    }

    async fn handle_basic_ack(&mut self, channel_id: u16, d: basic::Ack) -> bool {
        const CLASS: u16 = 60;
        let tags = match self.settlement_tags(
            channel_id,
            d.delivery_tag,
            d.multiple,
            CLASS,
            d.get_amqp_method_id(),
        ) {
            Ok(tags) => tags,
            Err(e) => return self.protocol_error(channel_id, &e).await,
        };
        self.apply_settlement(channel_id, tags, false, SettlementKind::Ack);
        true
    }

    async fn handle_basic_reject(&mut self, channel_id: u16, d: basic::Reject) -> bool {
        const CLASS: u16 = 60;
        let tags = match self.settlement_tags(
            channel_id,
            d.delivery_tag,
            false,
            CLASS,
            d.get_amqp_method_id(),
        ) {
            Ok(tags) => tags,
            Err(e) => return self.protocol_error(channel_id, &e).await,
        };
        self.apply_settlement(channel_id, tags, d.requeue, SettlementKind::Discard);
        true
    }

    async fn handle_basic_nack(&mut self, channel_id: u16, d: basic::Nack) -> bool {
        const CLASS: u16 = 60;
        let tags = match self.settlement_tags(
            channel_id,
            d.delivery_tag,
            d.multiple,
            CLASS,
            d.get_amqp_method_id(),
        ) {
            Ok(tags) => tags,
            Err(e) => return self.protocol_error(channel_id, &e).await,
        };
        self.apply_settlement(channel_id, tags, d.requeue, SettlementKind::Discard);
        true
    }

    async fn handle_basic_recover(&mut self, channel_id: u16, d: basic::Recover) -> bool {
        const CLASS: u16 = 60;
        let method = d.get_amqp_method_id();
        if !d.requeue {
            // FR-C08: only requeue=true is in the V1 subset.
            let e = ProtocolError::not_implemented("basic.recover requeue=false", CLASS, method);
            return self.protocol_error(channel_id, &e).await;
        }
        // Requeue every outstanding delivery on this channel; they become
        // redeliverable to any consumer (§6.1 recover).
        let tags: Vec<u64> = self
            .channels
            .get(&channel_id)
            .map(|ch| ch.unacked.keys().copied().collect())
            .unwrap_or_default();
        // recover requeues everything; nothing is terminal, so no journal
        // record (Discard kind is only recorded when requeue=false).
        self.apply_settlement(channel_id, tags, true, SettlementKind::Discard);
        let ok = basic::AMQPMethod::RecoverOk(basic::RecoverOk {});
        self.send(AMQPFrame::Method(channel_id, AMQPClass::Basic(ok)))
            .await
            .is_ok()
    }

    // ------------------------------------------------------------------
    // Consumers (M3): basic.consume/cancel/qos and mailbox jobs.
    // ------------------------------------------------------------------

    async fn handle_basic_consume(&mut self, channel_id: u16, d: basic::Consume) -> bool {
        const CLASS: u16 = 60;
        let method = d.get_amqp_method_id();
        // Deferred features are rejected up front (T22 posture).
        if d.no_local {
            let e = ProtocolError::not_implemented("basic.consume no_local=true", CLASS, method);
            return self.protocol_error(channel_id, &e).await;
        }
        if let Err(e) = Self::require_no_arguments(&d.arguments, "consume", CLASS, method) {
            return self.protocol_error(channel_id, &e).await;
        }
        let Ok(name) = self.resolve_queue_name(channel_id, d.queue.as_str()) else {
            let e = ProtocolError::not_found(
                "no previously declared queue to use as default",
                CLASS,
                method,
            );
            return self.protocol_error(channel_id, &e).await;
        };
        let vhost = self.vhost();
        // §11.2: consume requires read on the queue.
        if let Err(e) = self.require_access(
            vhost,
            "/",
            rusty_mq_core::auth::Access::Read,
            &name,
            CLASS,
            method,
        ) {
            return self.protocol_error(channel_id, &e).await;
        }
        let prefetch = self
            .channels
            .get(&channel_id)
            .map(|c| c.prefetch_new_consumers)
            .unwrap_or(None);

        // Fully synchronous resolution: queue, exclusivity, tag generation,
        // duplicate-tag and exclusive-consumer checks (no guard crosses an
        // await).
        let outcome: Result<(QueueId, String), ProtocolError> = {
            let topo = self.broker.topology.lock().unwrap();
            let found = topo.find_queue(vhost, &name);
            let lock_ok = found.is_some_and(|id| {
                topo.check_exclusive_access(id, self.conn_id)
                    .unwrap_or(false)
            });
            match found {
                None => Err(ProtocolError::not_found(
                    format!("no queue '{name}' in vhost"),
                    CLASS,
                    method,
                )),
                Some(_) if !lock_ok => Err(ProtocolError::channel(
                    reply_code::RESOURCE_LOCKED,
                    format!("RESOURCE_LOCKED - queue '{name}' is exclusive to another connection"),
                    CLASS,
                    method,
                )),
                Some(id) => {
                    let consumers = self.broker.consumers.lock().unwrap();
                    // Exclusive consumer: one consumer per queue (FR-C01).
                    let conflict = d.exclusive && consumers.has_consumers(id);
                    let tag = if d.consumer_tag.as_str().is_empty() {
                        self.broker.next_consumer_tag()
                    } else {
                        d.consumer_tag.as_str().to_string()
                    };
                    let duplicate = consumers.find_by_tag(self.conn_id, &tag).is_some();
                    drop(consumers);
                    drop(topo);
                    if conflict {
                        Err(ProtocolError::channel(
                            reply_code::ACCESS_REFUSED,
                            format!(
                                "ACCESS_REFUSED - exclusive consumer already active on queue '{name}'"
                            ),
                            CLASS,
                            method,
                        ))
                    } else if duplicate {
                        Err(ProtocolError::precondition_failed(
                            format!("consumer tag '{tag}' already in use"),
                            CLASS,
                            method,
                        ))
                    } else {
                        Ok((id, tag))
                    }
                }
            }
        };
        let (queue_id, tag) = match outcome {
            Ok(x) => x,
            Err(e) => return self.protocol_error(channel_id, &e).await,
        };

        // Register the consumer (mailbox back to this connection).
        {
            let mut topo = self.broker.topology.lock().unwrap();
            topo.queue_got_consumer(queue_id); // FR-Q05 auto-delete fact
        }
        {
            let mut consumers = self.broker.consumers.lock().unwrap();
            consumers.register(
                queue_id,
                Consumer::new(
                    self.conn_id,
                    channel_id,
                    tag.clone(),
                    d.no_ack,
                    prefetch,
                    self.client_cancel_notify,
                    self.mailbox_tx.clone(),
                ),
            );
        }
        if let Some(ch) = self.channels.get_mut(&channel_id) {
            ch.consumers.insert(tag.clone());
        }
        // A ready backlog is delivered immediately (FR-C09).
        self.broker.dispatch_queue(queue_id);

        if d.nowait && !d.consumer_tag.as_str().is_empty() {
            return true;
        }
        // consume-ok carries the effective tag (also with nowait when the
        // server generated it, mirroring queue.declare guidance).
        let ok = basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
            consumer_tag: tag.into(),
        });
        self.send(AMQPFrame::Method(channel_id, AMQPClass::Basic(ok)))
            .await
            .is_ok()
    }

    async fn handle_basic_cancel(&mut self, channel_id: u16, d: basic::Cancel) -> bool {
        const CLASS: u16 = 60;
        let method = d.get_amqp_method_id();
        // Cancel stops new deliveries; outstanding unacked deliveries stay
        // with the channel (§6.1, [R3]).
        let affected = {
            let mut consumers = self.broker.consumers.lock().unwrap();
            let id = consumers.find_by_tag(self.conn_id, d.consumer_tag.as_str());
            match id {
                Some((id, _queue)) => consumers.deregister(id).into_iter().collect::<Vec<_>>(),
                None => Vec::new(),
            }
        };
        if affected.is_empty() {
            let e = ProtocolError::not_found(
                format!("no consumer tag '{}'", d.consumer_tag),
                CLASS,
                method,
            );
            return self.protocol_error(channel_id, &e).await;
        }
        if let Some(ch) = self.channels.get_mut(&channel_id) {
            ch.consumers.remove(d.consumer_tag.as_str());
        }
        self.broker.maybe_auto_delete_queues(&affected);
        if d.nowait {
            return true;
        }
        let ok = basic::AMQPMethod::CancelOk(basic::CancelOk {
            consumer_tag: d.consumer_tag.clone(),
        });
        self.send(AMQPFrame::Method(channel_id, AMQPClass::Basic(ok)))
            .await
            .is_ok()
    }

    async fn handle_basic_qos(&mut self, channel_id: u16, d: basic::Qos) -> bool {
        // NOTE: amq-protocol 7.x does not model basic.qos's reserved
        // prefetch_size field, so a nonzero value cannot be distinguished
        // here; recorded in the ledger (codec upgrade task).
        let limit = if d.prefetch_count == 0 {
            None // unlimited (§6.2 rule 1)
        } else {
            Some(d.prefetch_count)
        };
        if d.global {
            // Shared channel limit (§6.2 rule 3).
            self.broker.consumers.lock().unwrap().set_shared_prefetch(
                self.conn_id,
                channel_id,
                limit,
            );
        } else {
            // Default for consumers created afterwards (rule 2); existing
            // consumers keep their assigned limit.
            if let Some(ch) = self.channels.get_mut(&channel_id) {
                ch.prefetch_new_consumers = limit;
            }
        }
        // A relaxed limit may release queued work immediately.
        let queues = self
            .broker
            .consumers
            .lock()
            .unwrap()
            .queues_with_consumers_on(self.conn_id, channel_id);
        for q in queues {
            self.broker.dispatch_queue(q);
        }
        let ok = basic::AMQPMethod::QosOk(basic::QosOk {});
        self.send(AMQPFrame::Method(channel_id, AMQPClass::Basic(ok)))
            .await
            .is_ok()
    }

    /// §9.6 journal-before-exposure gate for consumer deliveries of
    /// persistent entries in durable queues.
    ///
    /// Manual-ack: journal a `Delivered` marker (conservative redelivery
    /// hint after a crash; a marker without a delivery is permitted).
    ///
    /// No-ack: journal the terminal dequeue BEFORE the delivery is handed
    /// to the connection writer (the entry is then settled even if the
    /// socket write fails — deliberately outside at-least-once).
    /// Returns false when the journal failed: the entry is requeued and NOT
    /// delivered (never a fabricated outcome).
    async fn journal_before_delivery(
        &mut self,
        queue: QueueId,
        entry: &rusty_mq_core::store::QueueEntry,
        no_ack: bool,
    ) -> bool {
        if !entry.message.persistent {
            return true;
        }
        let durable = self
            .broker
            .topology
            .lock()
            .unwrap()
            .queue_record(queue)
            .is_some_and(|r| r.profile.durable);
        if !durable {
            return true;
        }
        let record = if no_ack {
            rusty_mq_storage::Record::SettleDiscard {
                queue: queue.to_raw(),
                seq: entry.seq,
            }
        } else {
            rusty_mq_storage::Record::Delivered {
                queue: queue.to_raw(),
                seq: entry.seq,
            }
        };
        if self.broker.journal_commit(&[record]).is_err() {
            tracing::error!(
                queue = ?queue,
                seq = entry.seq,
                "delivery journal gate failed; entry requeued, not delivered"
            );
            self.broker
                .store
                .lock()
                .unwrap()
                .requeue(queue, entry.clone());
            return false;
        }
        // Reflect the conservative hint in live state too.
        if !no_ack {
            self.broker
                .store
                .lock()
                .unwrap()
                .mark_redelivered(queue, entry.seq);
        }
        true
    }

    /// FR-R07: emit connection.blocked/unblocked when the client declared
    /// the capability (§4.3 gating; we advertise it since this run).
    async fn handle_control(&mut self, msg: crate::broker::Control) -> bool {
        use crate::broker::Control;
        match &msg {
            Control::Blocked(reason) => {
                // §4.3: blocked/unblocked only for capability-declaring
                // clients; close is always delivered.
                if !self.client_blocking {
                    return true;
                }
                let blocked = connection::AMQPMethod::Blocked(connection::Blocked {
                    reason: reason.as_str().into(),
                });
                self.send(AMQPFrame::Method(0, AMQPClass::Connection(blocked)))
                    .await
                    .is_ok()
            }
            Control::Unblocked => {
                if !self.client_blocking {
                    return true;
                }
                let unblocked = connection::AMQPMethod::Unblocked(connection::Unblocked {});
                self.send(AMQPFrame::Method(0, AMQPClass::Connection(unblocked)))
                    .await
                    .is_ok()
            }
            Control::Close { reply_code, reason } => {
                // Server-initiated close: emit connection.close and linger
                // briefly for the client's close-ok, then end.
                self.protocol_error(0, &ProtocolError::connection(*reply_code, reason.clone()))
                    .await;
                self.phase = Phase::Closing;
                true
            }
        }
    }

    /// Handle a job from this connection's consumer mailbox.
    async fn handle_job(&mut self, job: Job) -> bool {
        match job {
            Job::Deliver {
                consumer_tag,
                channel: job_channel,
                queue,
                no_ack,
                entry,
            } => {
                // Undeliverable (channel closed or consumer cancelled since
                // dispatch): requeue; the credit was already unwound with
                // the consumer's deregistration.
                let deliverable = !self.awaiting_close_ok.contains_key(&job_channel)
                    && self
                        .channels
                        .get(&job_channel)
                        .is_some_and(|ch| ch.consumers.contains(&consumer_tag));
                if !deliverable {
                    self.broker.store.lock().unwrap().requeue(queue, entry);
                    self.broker.dispatch_queue(queue);
                    return true;
                }
                // §9.6: the durable boundary precedes exposure.
                if !self.journal_before_delivery(queue, &entry, no_ack).await {
                    return true;
                }
                let mut delivery_tag = 0;
                if let Some(ch) = self.channels.get_mut(&job_channel) {
                    delivery_tag = ch.next_delivery_tag;
                    ch.next_delivery_tag += 1;
                    if !no_ack {
                        // no-ack deliveries settle at pop; only manual-ack
                        // deliveries join the unacked set.
                        ch.unacked.insert(
                            delivery_tag,
                            UnackedDelivery {
                                queue,
                                entry: entry.clone(),
                                consumer_tag: Some(consumer_tag.clone()),
                            },
                        );
                    }
                }
                Metrics::inc(&self.broker.metrics.messages_delivered);
                let deliver = basic::AMQPMethod::Deliver(basic::Deliver {
                    consumer_tag: consumer_tag.into(),
                    delivery_tag,
                    redelivered: entry.message.redelivered,
                    exchange: entry.message.exchange.as_str().into(),
                    routing_key: entry.message.routing_key.as_str().into(),
                });
                self.send_content(
                    job_channel,
                    AMQPFrame::Method(job_channel, AMQPClass::Basic(deliver)),
                    &entry.message,
                )
                .await
                .is_ok()
            }
            Job::CancelNotify { consumer_tag } => {
                // The queue this consumer was on was deleted (FR-Q09); sent
                // only because the client declared the capability. Find the
                // channel carrying the tag and emit basic.cancel.
                let Some(channel_id) = self
                    .channels
                    .iter()
                    .find(|(_, ch)| ch.consumers.contains(&consumer_tag))
                    .map(|(id, _)| *id)
                else {
                    return true; // channel already closed: nothing to notify
                };
                if let Some(ch) = self.channels.get_mut(&channel_id) {
                    ch.consumers.remove(&consumer_tag);
                }
                let cancel = basic::AMQPMethod::Cancel(basic::Cancel {
                    consumer_tag: consumer_tag.into(),
                    nowait: false,
                });
                self.send(AMQPFrame::Method(channel_id, AMQPClass::Basic(cancel)))
                    .await
                    .is_ok()
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
        if e.reply_code == reply_code::ACCESS_REFUSED {
            Metrics::inc(&self.broker.metrics.auth_refusals);
        }
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

    /// Send a frame with bounded backpressure: the writer queue is
    /// capacity-bounded (memory stays capped); a full queue AWAITs here —
    /// TCP backpressure propagates to the client — instead of dropping the
    /// connection. Under sustained publish load the try-send-and-drop M1
    /// posture killed healthy connections (found by the §14 benchmark:
    /// connection reset by peer on all publishers).
    async fn send(&self, frame: AMQPFrame) -> Result<(), ()> {
        let bytes = encode_frame(&frame);
        self.outbound.send(bytes).await.map_err(|_| ())
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

/// Host portion of a peer address (strip the port; also handles IPv6
/// "[::1]:p" forms).
fn peer_host_of(peer: &str) -> String {
    if let Some(rest) = peer.strip_prefix('[') {
        // IPv6 literal: [host]:port
        rest.split(']').next().unwrap_or(rest).to_string()
    } else {
        match peer.rsplit_once(':') {
            Some((host, _)) => host.to_string(),
            None => peer.to_string(),
        }
    }
}

fn self_unregister(broker: &Arc<Broker>, id: ConnectionId) {
    broker.unregister_connection(id);
}

/// Writer task: sole owner of the socket write half (FR-P07, ADR-0003).
async fn writer_task(
    mut write: WriteHalf<impl AsyncWrite + Unpin>,
    mut rx: mpsc::Receiver<Vec<u8>>,
) {
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
