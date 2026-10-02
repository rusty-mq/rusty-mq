//! Durable storage for rusty-mq (M4-M6).
//!
//! **Not implemented yet.** The design contract is frozen in
//! `docs/adr/0002-authoritative-journal.md` and PRD §9:
//!
//! - one append-only, segmented journal is the authoritative recovery root;
//! - an atomically published manifest + immutable snapshot + committed
//!   journal suffix defines the recovery state;
//! - `redb` is a derived, rebuildable projection only;
//! - startup fails explicitly on corruption; it never silently reinitializes.
//!
//! Until M4 lands, the broker runs memory-backed and reports no persistence
//! capability (PRD early safety constraint: an in-memory backend must never
//! positively claim persistence).

pub mod backup;
pub mod journal;
pub mod projection;
pub mod rebuild;
pub mod record;
pub mod snapshot;

pub use journal::{
    recover, JournalConfig, JournalWriter, RecoveredRecord, FORMAT_MAJOR, FORMAT_MINOR,
};
pub use record::{Binding, Enqueue, ExchangeRecord, FormatError, QueueRecord, Record};

/// Reasons durable operations are refused before M4.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NotDurableYet {
    #[error("durable storage is not implemented until milestone M4; this broker is memory-backed and makes no persistence claim")]
    MemoryBacked,
}
