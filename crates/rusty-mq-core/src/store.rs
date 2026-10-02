//! In-memory message store (M2): ready FIFO queues keyed by opaque QueueId.
//!
//! Wire-free by design: message properties are opaque encoded blobs; the
//! protocol layer encodes on admission and decodes on delivery, so core never
//! depends on a codec. The durable journal replaces/augments this in M4 with
//! the same entry identities (queue id + monotonic sequence).
//!
//! Bounds: a configurable aggregate byte budget is enforced at admission
//! (INV-09); exceeding it is an explicit error, never silent acceptance.

use std::collections::{HashMap, VecDeque};

use crate::ids::QueueId;

/// An immutable stored message. Property bytes are the encoded AMQP basic
/// property list (opaque here).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredMessage {
    pub property_bytes: Vec<u8>,
    pub body: Vec<u8>,
    /// Original envelope (for deliveries and returns).
    pub exchange: String,
    pub routing_key: String,
    /// delivery_mode=2.
    pub persistent: bool,
    /// Set when the entry has been delivered at least once (conservative
    /// redelivery hint, §9.6).
    pub redelivered: bool,
}

impl StoredMessage {
    pub fn size_bytes(&self) -> usize {
        self.property_bytes.len() + self.body.len()
    }
}

/// One ready queue entry with its stable sequence number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueEntry {
    pub seq: u64,
    pub message: StoredMessage,
}

/// Why an admission was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AdmitError {
    #[error("aggregate message byte budget exceeded")]
    BudgetExceeded,
}

/// Ready-state for one queue.
#[derive(Default)]
struct QueueReady {
    next_seq: u64,
    entries: VecDeque<QueueEntry>,
}

/// The broker-wide in-memory store.
pub struct MessageStore {
    queues: HashMap<QueueId, QueueReady>,
    total_bytes: usize,
    byte_budget: usize,
}

impl MessageStore {
    pub fn new(byte_budget: usize) -> Self {
        Self {
            queues: HashMap::new(),
            total_bytes: 0,
            byte_budget,
        }
    }

    /// Enqueue a message; returns its queue sequence (identity for requeue
    /// and, later, journaling).
    pub fn enqueue(&mut self, queue: QueueId, message: StoredMessage) -> Result<u64, AdmitError> {
        let size = message.size_bytes();
        if self.total_bytes + size > self.byte_budget {
            return Err(AdmitError::BudgetExceeded);
        }
        let q = self.queues.entry(queue).or_default();
        let seq = q.next_seq;
        q.next_seq += 1;
        q.entries.push_back(QueueEntry { seq, message });
        self.total_bytes += size;
        Ok(seq)
    }

    /// Ready count for a queue.
    pub fn len(&self, queue: QueueId) -> u64 {
        self.queues
            .get(&queue)
            .map_or(0, |q| q.entries.len() as u64)
    }

    /// Sequences of the current ready set (purge records must name the
    /// exact set selected at the ordering point — §9.4).
    pub fn ready_seqs(&self, queue: QueueId) -> Vec<u64> {
        self.queues
            .get(&queue)
            .map(|q| q.entries.iter().map(|e| e.seq).collect())
            .unwrap_or_default()
    }

    /// The sequence the next enqueue on this queue will receive (pure
    /// peek: does NOT consume; the caller holds the store lock across the
    /// journal commit and the following enqueue, so the value cannot
    /// change in between).
    pub fn next_seq_of(&self, queue: QueueId) -> u64 {
        self.queues.get(&queue).map_or(0, |q| q.next_seq)
    }

    /// Replay-only: restore an entry with its journaled identity (queue id
    /// + sequence); the next mint continues past it. Idempotent per seq.
    pub fn restore_with_seq(&mut self, queue: QueueId, seq: u64, message: StoredMessage) {
        let size = message.size_bytes();
        let q = self.queues.entry(queue).or_default();
        q.next_seq = q.next_seq.max(seq + 1);
        if q.entries.iter().any(|e| e.seq == seq) {
            return; // idempotent replay
        }
        q.entries.push_back(QueueEntry { seq, message });
        self.total_bytes += size;
    }

    /// Aggregate ready bytes (budget accounting and metrics).
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Pop the oldest ready entry (FIFO; FR scheduling order §7.3).
    pub fn pop_ready(&mut self, queue: QueueId) -> Option<QueueEntry> {
        let q = self.queues.get_mut(&queue)?;
        let entry = q.entries.pop_front()?;
        self.total_bytes -= entry.message.size_bytes();
        Some(entry)
    }

    /// Requeue an entry at its original relative position (§6.1: original
    /// position "where practical" — sequence order preserves it). The entry
    /// carries its redelivery flag already.
    pub fn requeue(&mut self, queue: QueueId, mut entry: QueueEntry) {
        let size = entry.message.size_bytes();
        let q = self.queues.entry(queue).or_default();
        // Entries are seq-ordered; insert before the first entry with a
        // higher sequence.
        let pos = q
            .entries
            .iter()
            .position(|e| e.seq > entry.seq)
            .unwrap_or(q.entries.len());
        if entry.seq >= q.next_seq {
            q.next_seq = entry.seq + 1;
        }
        entry.message.redelivered = true;
        q.entries.insert(pos, entry);
        self.total_bytes += size;
    }

    /// Purge ready entries only (FR-Q06: never touches in-flight entries).
    /// Returns the number purged.
    pub fn purge(&mut self, queue: QueueId) -> u64 {
        let Some(q) = self.queues.get_mut(&queue) else {
            return 0;
        };
        let purged: usize = q.entries.len();
        for e in &q.entries {
            self.total_bytes -= e.message.size_bytes();
        }
        q.entries.clear();
        purged as u64
    }

    /// Drop all state for a deleted queue. Returns the number of ready
    /// entries discarded.
    pub fn drain(&mut self, queue: QueueId) -> u64 {
        match self.queues.remove(&queue) {
            Some(q) => {
                let n = q.entries.len() as u64;
                for e in &q.entries {
                    self.total_bytes -= e.message.size_bytes();
                }
                n
            }
            None => 0,
        }
    }

    /// Discard one entry permanently (terminal settlement).
    pub fn discard(&mut self, queue: QueueId, seq: u64) {
        if let Some(q) = self.queues.get_mut(&queue) {
            if let Some(pos) = q.entries.iter().position(|e| e.seq == seq) {
                if let Some(e) = q.entries.remove(pos) {
                    self.total_bytes -= e.message.size_bytes();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(body: &[u8]) -> StoredMessage {
        StoredMessage {
            property_bytes: vec![1, 2],
            body: body.to_vec(),
            exchange: "ex".into(),
            routing_key: "k".into(),
            persistent: false,
            redelivered: false,
        }
    }

    fn q() -> QueueId {
        QueueId::for_test(1)
    }

    #[test]
    fn fifo_order_and_counts() {
        let mut s = MessageStore::new(1024);
        let q = q();
        s.enqueue(q, msg(b"a")).unwrap();
        s.enqueue(q, msg(b"b")).unwrap();
        assert_eq!(s.len(q), 2);
        assert_eq!(s.pop_ready(q).unwrap().message.body, b"a");
        assert_eq!(s.pop_ready(q).unwrap().message.body, b"b");
        assert_eq!(s.len(q), 0);
        assert!(s.pop_ready(q).is_none());
    }

    #[test]
    fn requeue_preserves_relative_position_and_marks_redelivered() {
        let mut s = MessageStore::new(1024);
        let q = q();
        let s0 = s.enqueue(q, msg(b"a")).unwrap();
        s.enqueue(q, msg(b"b")).unwrap();
        s.enqueue(q, msg(b"c")).unwrap();
        let e0 = s.pop_ready(q).unwrap();
        assert_eq!(e0.seq, s0);
        s.requeue(q, e0);
        // Order restored: a (redelivered), b, c.
        let first = s.pop_ready(q).unwrap();
        assert_eq!(first.message.body, b"a");
        assert!(first.message.redelivered);
        assert_eq!(s.pop_ready(q).unwrap().message.body, b"b");
        assert_eq!(s.pop_ready(q).unwrap().message.body, b"c");
        // New enqueues continue after the highest used seq.
        let s3 = s.enqueue(q, msg(b"d")).unwrap();
        assert_eq!(s3, 3);
    }

    #[test]
    fn byte_budget_enforced() {
        let mut s = MessageStore::new(10);
        let q = q();
        assert!(s.enqueue(q, msg(b"01234567")).is_ok()); // 8 body + 2 props
        assert_eq!(s.enqueue(q, msg(b"xy")), Err(AdmitError::BudgetExceeded));
        // After popping, the budget frees up again.
        s.pop_ready(q);
        assert!(s.enqueue(q, msg(b"xy")).is_ok());
    }

    #[test]
    fn purge_counts_and_frees_budget() {
        let mut s = MessageStore::new(1024);
        let q = q();
        s.enqueue(q, msg(b"a")).unwrap();
        s.enqueue(q, msg(b"b")).unwrap();
        assert_eq!(s.purge(q), 2);
        assert_eq!(s.len(q), 0);
        assert_eq!(s.total_bytes(), 0);
        assert_eq!(s.purge(q), 0);
    }

    #[test]
    fn drain_removes_queue_state() {
        let mut s = MessageStore::new(1024);
        let q = q();
        s.enqueue(q, msg(b"a")).unwrap();
        assert_eq!(s.drain(q), 1);
        assert_eq!(s.len(q), 0);
        assert_eq!(s.drain(q), 0);
        // Re-enqueue after drain starts fresh (queue identity is new in
        // topology anyway — INV-07).
        assert!(s.enqueue(q, msg(b"z")).is_ok());
    }

    #[test]
    fn discard_removes_specific_entry() {
        let mut s = MessageStore::new(1024);
        let q = q();
        let s0 = s.enqueue(q, msg(b"a")).unwrap();
        let _s1 = s.enqueue(q, msg(b"b")).unwrap();
        s.discard(q, s0);
        assert_eq!(s.len(q), 1);
        assert_eq!(s.pop_ready(q).unwrap().message.body, b"b");
    }

    #[test]
    fn queues_are_independent() {
        let mut s = MessageStore::new(1024);
        let q1 = QueueId::for_test(1);
        let q2 = QueueId::for_test(2);
        s.enqueue(q1, msg(b"a")).unwrap();
        s.enqueue(q2, msg(b"b")).unwrap();
        assert_eq!(s.len(q1), 1);
        assert_eq!(s.len(q2), 1);
        s.purge(q1);
        assert_eq!(s.len(q1), 0);
        assert_eq!(s.len(q2), 1);
    }
}
