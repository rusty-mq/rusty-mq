//! Consumer registry and dispatch engine (M3): which consumers are attached
//! to which queues, per-consumer and channel-shared prefetch credit, and
//! round-robin delivery scheduling (FR-C01, FR-C07, FR-C09).
//!
//! Lock order discipline: consumers → store (never topology while holding
//! consumers). Mailbox sends use `try_send` so dispatch never blocks.

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use rusty_mq_core::store::{MessageStore, QueueEntry};
use rusty_mq_core::{ConnectionId, QueueId};

/// Store surface the dispatcher needs (implemented by the in-memory store
/// and by test stand-ins).
pub trait EntrySource {
    fn pop_ready(&mut self, queue: QueueId) -> Option<QueueEntry>;
    fn requeue(&mut self, queue: QueueId, entry: QueueEntry);
}

impl EntrySource for MessageStore {
    fn pop_ready(&mut self, queue: QueueId) -> Option<QueueEntry> {
        MessageStore::pop_ready(self, queue)
    }
    fn requeue(&mut self, queue: QueueId, entry: QueueEntry) {
        MessageStore::requeue(self, queue, entry)
    }
}

/// Work destined for a connection's consumer loop.
pub enum Job {
    /// Push a delivery to a consumer on one of this connection's channels.
    /// The queue id lets the receiving connection requeue undeliverable
    /// entries.
    Deliver {
        consumer_tag: String,
        channel: u16,
        queue: QueueId,
        no_ack: bool,
        entry: QueueEntry,
    },
    /// The queue a consumer was on was deleted (sent only to clients that
    /// declared the consumer_cancel_notify capability, §4.3).
    CancelNotify { consumer_tag: String },
}

/// One registered consumer.
pub struct Consumer {
    pub connection: ConnectionId,
    pub channel: u16,
    pub tag: String,
    /// no_ack consumers settle at delivery; their outstanding stays 0.
    pub no_ack: bool,
    /// Per-consumer prefetch (the channel's global=false setting at
    /// registration time, §6.2 rule 2). None = unlimited.
    pub prefetch: Option<u16>,
    /// Manual-ack deliveries currently outstanding (credit accounting).
    pub outstanding: usize,
    pub supports_cancel_notify: bool,
    mailbox: mpsc::Sender<Job>,
}

impl Consumer {
    /// Build a consumer with a fresh mailbox-clone and zeroed counters.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection: ConnectionId,
        channel: u16,
        tag: String,
        no_ack: bool,
        prefetch: Option<u16>,
        supports_cancel_notify: bool,
        mailbox: mpsc::Sender<Job>,
    ) -> Self {
        Self {
            connection,
            channel,
            tag,
            no_ack,
            prefetch,
            outstanding: 0,
            supports_cancel_notify,
            mailbox,
        }
    }

    fn has_credit(&self, channel_outstanding: usize, shared_limit: Option<u16>) -> bool {
        if self.no_ack {
            return true;
        }
        if self
            .prefetch
            .is_some_and(|l| self.outstanding >= l as usize)
        {
            return false;
        }
        if shared_limit.is_some_and(|s| channel_outstanding >= s as usize) {
            return false;
        }
        true
    }
}

/// Broker-wide consumer registry.
#[derive(Default)]
pub struct Consumers {
    /// Queue -> consumer ids; the first id is the next to be scheduled
    /// (rotated after each delivery for fairness).
    by_queue: HashMap<QueueId, Vec<usize>>,
    all: HashMap<usize, Consumer>,
    /// Shared prefetch limit per (connection, channel) from
    /// basic.qos(global=true) (§6.2 rule 3). None = unlimited.
    shared_prefetch: HashMap<(ConnectionId, u16), Option<u16>>,
    /// Unacked deliveries per (connection, channel) — the shared limit
    /// applies against this.
    channel_outstanding: HashMap<(ConnectionId, u16), usize>,
    next_id: usize,
}

impl Consumers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a consumer on a queue. Returns its registry id.
    pub fn register(&mut self, queue: QueueId, consumer: Consumer) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.by_queue.entry(queue).or_default().push(id);
        self.all.insert(id, consumer);
        id
    }

    /// Consumers currently attached to the queue.
    pub fn consumer_count(&self, queue: QueueId) -> usize {
        self.by_queue.get(&queue).map_or(0, |v| v.len())
    }

    /// Whether the queue already has any consumer (exclusive-consumer
    /// conflict, FR-C01).
    pub fn has_consumers(&self, queue: QueueId) -> bool {
        self.consumer_count(queue) > 0
    }

    /// Find a consumer by tag on a connection (for basic.cancel).
    pub fn find_by_tag(&self, connection: ConnectionId, tag: &str) -> Option<(usize, QueueId)> {
        for (queue, ids) in &self.by_queue {
            for id in ids {
                if self
                    .all
                    .get(id)
                    .is_some_and(|c| c.connection == connection && c.tag == tag)
                {
                    return Some((*id, *queue));
                }
            }
        }
        None
    }

    /// Deregister one consumer; returns the queue it was on.
    pub fn deregister(&mut self, id: usize) -> Option<QueueId> {
        let consumer = self.all.remove(&id)?;
        let key = (consumer.connection, consumer.channel);
        if let Some(n) = self.channel_outstanding.get_mut(&key) {
            *n = n.saturating_sub(consumer.outstanding);
        }
        for (queue, ids) in self.by_queue.iter_mut() {
            let before = ids.len();
            ids.retain(|i| *i != id);
            if ids.len() != before {
                return Some(*queue);
            }
        }
        None
    }

    /// Deregister every consumer of one queue (queue deleted).
    pub fn deregister_queue(&mut self, queue: QueueId) {
        if let Some(ids) = self.by_queue.remove(&queue) {
            for id in ids {
                if let Some(consumer) = self.all.remove(&id) {
                    let key = (consumer.connection, consumer.channel);
                    if let Some(n) = self.channel_outstanding.get_mut(&key) {
                        *n = n.saturating_sub(consumer.outstanding);
                    }
                }
            }
        }
    }

    /// Deregister every consumer on a channel; returns affected queues
    /// (for auto-delete checks).
    pub fn deregister_channel(&mut self, connection: ConnectionId, channel: u16) -> Vec<QueueId> {
        self.deregister_where(|c| c.connection == connection && c.channel == channel)
    }

    /// Deregister every consumer of a connection; returns affected queues.
    pub fn deregister_connection(&mut self, connection: ConnectionId) -> Vec<QueueId> {
        self.deregister_where(|c| c.connection == connection)
    }

    fn deregister_where(&mut self, pred: impl Fn(&Consumer) -> bool) -> Vec<QueueId> {
        let removed: Vec<usize> = self
            .all
            .iter()
            .filter(|(_, c)| pred(c))
            .map(|(id, _)| *id)
            .collect();
        let mut queues = Vec::new();
        for id in removed {
            if let Some(q) = self.deregister(id) {
                if !queues.contains(&q) {
                    queues.push(q);
                }
            }
        }
        queues
    }

    /// Set the channel-shared prefetch limit (basic.qos global=true).
    pub fn set_shared_prefetch(
        &mut self,
        connection: ConnectionId,
        channel: u16,
        limit: Option<u16>,
    ) {
        self.shared_prefetch.insert((connection, channel), limit);
    }

    /// Credit release on settlement of a manual-ack consumer delivery.
    pub fn settle(&mut self, connection: ConnectionId, channel: u16, tag: &str) {
        for c in self.all.values_mut() {
            if c.connection == connection && c.channel == channel && c.tag == tag {
                c.outstanding = c.outstanding.saturating_sub(1);
            }
        }
        let key = (connection, channel);
        if let Some(n) = self.channel_outstanding.get_mut(&key) {
            *n = n.saturating_sub(1);
        }
    }

    /// Deliver ready entries to eligible consumers, round-robin, while
    /// credit allows (FR-C09). `pop`/`requeue` come from the store so this
    /// stays store-agnostic. Returns the number of deliveries scheduled.
    #[allow(clippy::while_let_loop)] // multiple break reasons; loop+let-else reads clearest
    pub fn dispatch(&mut self, queue: QueueId, source: &mut dyn EntrySource) -> usize {
        let mut delivered = 0;
        loop {
            let Some(ids) = self.by_queue.get(&queue).cloned() else {
                break;
            };
            // First eligible consumer in rotated order = fair scheduling.
            let chosen = ids.iter().copied().find(|id| {
                self.all.get(id).is_some_and(|c| {
                    let key = (c.connection, c.channel);
                    let shared = self.shared_prefetch.get(&key).copied().flatten();
                    let outstanding = self.channel_outstanding.get(&key).copied().unwrap_or(0);
                    c.has_credit(outstanding, shared)
                })
            });
            let Some(chosen) = chosen else {
                break; // no consumer has credit
            };
            let Some(entry) = source.pop_ready(queue) else {
                break; // queue drained
            };
            // Reserve credit atomically before scheduling (§6.2 rule 7).
            let manual = self.all.get(&chosen).is_some_and(|c| !c.no_ack);
            let credit_key = manual.then(|| {
                let Some(c) = self.all.get_mut(&chosen) else {
                    unreachable!("checked above");
                };
                c.outstanding += 1;
                let key = (c.connection, c.channel);
                *self.channel_outstanding.entry(key).or_insert(0) += 1;
                key
            });
            let mailbox = self
                .all
                .get(&chosen)
                .map(|c| c.mailbox.clone())
                .expect("consumer exists");
            let tag = self
                .all
                .get(&chosen)
                .map(|c| c.tag.clone())
                .expect("consumer exists");
            let job = Job::Deliver {
                consumer_tag: tag,
                channel: self
                    .all
                    .get(&chosen)
                    .map(|c| c.channel)
                    .expect("consumer exists"),
                queue,
                no_ack: self
                    .all
                    .get(&chosen)
                    .map(|c| c.no_ack)
                    .expect("consumer exists"),
                entry,
            };
            if let Err(back) = mailbox.try_send(job) {
                // Mailbox full or closed: unwind the credit reservation and
                // put the entry back (bounded, ADR-0004).
                if let Some(k) = credit_key {
                    if let Some(c) = self.all.get_mut(&chosen) {
                        c.outstanding = c.outstanding.saturating_sub(1);
                    }
                    if let Some(n) = self.channel_outstanding.get_mut(&k) {
                        *n = n.saturating_sub(1);
                    }
                }
                match back {
                    TrySendError::Full(Job::Deliver { entry, queue, .. })
                    | TrySendError::Closed(Job::Deliver { entry, queue, .. }) => {
                        source.requeue(queue, entry);
                    }
                    _ => {}
                }
                break;
            }
            delivered += 1;
            // Rotate the chosen consumer to the back for fairness.
            if let Some(ids) = self.by_queue.get_mut(&queue) {
                if let Some(pos) = ids.iter().position(|i| *i == chosen) {
                    let id = ids.remove(pos);
                    ids.push(id);
                }
            }
        }
        delivered
    }

    /// Queues with consumers on a channel (re-dispatch after QoS changes).
    pub fn queues_with_consumers_on(&self, connection: ConnectionId, channel: u16) -> Vec<QueueId> {
        self.by_queue
            .iter()
            .filter(|(_, ids)| {
                ids.iter().any(|i| {
                    self.all
                        .get(i)
                        .is_some_and(|c| c.connection == connection && c.channel == channel)
                })
            })
            .map(|(q, _)| *q)
            .collect()
    }

    /// Cancel-notify jobs for all consumers of a queue (queue deletion,
    /// FR-Q09), only for capability-declaring clients.
    pub fn cancel_notify_jobs(&self, queue: QueueId) -> Vec<(mpsc::Sender<Job>, Job)> {
        self.by_queue
            .get(&queue)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| {
                        let c = self.all.get(id)?;
                        c.supports_cancel_notify.then(|| {
                            (
                                c.mailbox.clone(),
                                Job::CancelNotify {
                                    consumer_tag: c.tag.clone(),
                                },
                            )
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consumer(conn: ConnectionId, tag: &str, no_ack: bool, prefetch: Option<u16>) -> Consumer {
        let (tx, rx) = mpsc::channel(16);
        // Keep the receiver alive for the test's lifetime (a dropped
        // receiver closes the mailbox and dispatch would requeue).
        std::mem::forget(rx);
        Consumer {
            connection: conn,
            channel: 1,
            tag: tag.to_string(),
            no_ack,
            prefetch,
            outstanding: 0,
            supports_cancel_notify: false,
            mailbox: tx,
        }
    }

    fn entry(seq: u64) -> QueueEntry {
        QueueEntry {
            seq,
            message: rusty_mq_core::StoredMessage {
                property_bytes: vec![],
                body: vec![seq as u8],
                exchange: String::new(),
                routing_key: String::new(),
                persistent: false,
                redelivered: false,
            },
        }
    }

    /// Store stand-in: a simple FIFO of entries.
    struct Backlog {
        entries: Vec<QueueEntry>,
    }

    impl Backlog {
        fn new(n: usize) -> Self {
            Self {
                entries: (0..n as u64).map(entry).collect(),
            }
        }
    }

    impl EntrySource for Backlog {
        fn pop_ready(&mut self, _queue: QueueId) -> Option<QueueEntry> {
            if self.entries.is_empty() {
                return None;
            }
            Some(self.entries.remove(0))
        }

        fn requeue(&mut self, _queue: QueueId, e: QueueEntry) {
            self.entries.push(e);
        }
    }

    #[tokio::test]
    async fn delivers_to_all_consumers_round_robin() {
        let conn = ConnectionId::new();
        let mut reg = Consumers::new();
        let q = QueueId::for_test(1);
        let (r1, mut rx1) = mpsc::channel(16);
        let (r2, mut rx2) = mpsc::channel(16);
        reg.register(
            q,
            Consumer {
                mailbox: r1,
                ..consumer(conn, "a", true, None)
            },
        );
        reg.register(
            q,
            Consumer {
                mailbox: r2,
                ..consumer(conn, "b", true, None)
            },
        );
        let mut backlog = Backlog::new(4);
        let delivered = reg.dispatch(q, &mut backlog);
        assert_eq!(delivered, 4);
        // Fair: two deliveries each.
        let mut count1 = 0;
        let mut count2 = 0;
        while rx1.try_recv().is_ok() {
            count1 += 1;
        }
        while rx2.try_recv().is_ok() {
            count2 += 1;
        }
        assert_eq!(count1, 2);
        assert_eq!(count2, 2);
    }

    #[tokio::test]
    async fn prefetch_gates_and_settle_frees() {
        let conn = ConnectionId::new();
        let mut reg = Consumers::new();
        let q = QueueId::for_test(2);
        reg.register(q, consumer(conn, "a", false, Some(1)));
        let mut backlog = Backlog::new(3);
        let delivered = reg.dispatch(q, &mut backlog);
        assert_eq!(delivered, 1, "prefetch=1 allows one delivery");

        reg.settle(conn, 1, "a");
        let delivered2 = reg.dispatch(q, &mut backlog);
        assert_eq!(delivered2, 1, "settlement frees credit");
    }

    #[tokio::test]
    async fn shared_prefetch_gates_across_consumers() {
        let conn = ConnectionId::new();
        let mut reg = Consumers::new();
        let q = QueueId::for_test(3);
        reg.register(q, consumer(conn, "a", false, None));
        reg.register(q, consumer(conn, "b", false, None));
        reg.set_shared_prefetch(conn, 1, Some(2));
        let mut backlog = Backlog::new(5);
        let delivered = reg.dispatch(q, &mut backlog);
        assert_eq!(delivered, 2, "shared channel limit gates both consumers");
    }

    #[tokio::test]
    async fn no_consumers_no_dispatch() {
        let mut reg = Consumers::new();
        let q = QueueId::for_test(4);
        let mut empty = Backlog::new(0);
        assert_eq!(reg.dispatch(q, &mut empty), 0);
    }

    #[tokio::test]
    async fn mailbox_full_requeues_and_reserves_credit() {
        let conn = ConnectionId::new();
        let mut reg = Consumers::new();
        let q = QueueId::for_test(5);
        // Capacity-1 mailbox, pre-filled: next try_send fails.
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(Job::CancelNotify {
            consumer_tag: "x".into(),
        })
        .unwrap();
        reg.register(
            q,
            Consumer {
                mailbox: tx,
                ..consumer(conn, "a", true, None)
            },
        );
        let mut backlog = Backlog::new(2);
        let delivered = reg.dispatch(q, &mut backlog);
        assert_eq!(delivered, 0);
        assert_eq!(backlog.entries.len(), 2, "entry returned to the store");
    }

    #[tokio::test]
    async fn deregister_channel_scopes_correctly() {
        let conn = ConnectionId::new();
        let mut reg = Consumers::new();
        let q = QueueId::for_test(6);
        reg.register(
            q,
            Consumer {
                channel: 1,
                ..consumer(conn, "ch1", true, None)
            },
        );
        reg.register(
            q,
            Consumer {
                channel: 2,
                ..consumer(conn, "ch2", true, None)
            },
        );
        assert_eq!(reg.consumer_count(q), 2);
        let affected = reg.deregister_channel(conn, 1);
        assert_eq!(affected, vec![q]);
        assert_eq!(reg.consumer_count(q), 1);
    }

    #[tokio::test]
    async fn cancel_notify_only_for_capable_consumers() {
        let conn = ConnectionId::new();
        let mut reg = Consumers::new();
        let q = QueueId::for_test(7);
        let (tx, _rx) = mpsc::channel(4);
        reg.register(
            q,
            Consumer {
                supports_cancel_notify: true,
                mailbox: tx,
                ..consumer(conn, "capable", true, None)
            },
        );
        reg.register(q, consumer(conn, "not-capable", true, None));
        let jobs = reg.cancel_notify_jobs(q);
        assert_eq!(jobs.len(), 1);
    }
}
