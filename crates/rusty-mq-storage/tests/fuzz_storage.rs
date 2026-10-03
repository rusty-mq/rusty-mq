//! T26 (storage layer): adversarial input over the journal and snapshot
//! decoders — bit-flips, truncation, absurd counts, corrupted payloads.
//! Invariants: no panic, bounded work (absurd counts never drive
//! pre-allocation), and corruption is either cleanly rejected or a torn
//! tail is discarded — never silently interpreted as valid data.

use proptest::prelude::*;

use rusty_mq_storage::journal::{recover, JournalConfig, JournalWriter};
use rusty_mq_storage::record::{FormatError, Record};
use rusty_mq_storage::snapshot::{read_snapshot, write_snapshot};

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rmq-fuzz-{}-{tag}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

proptest! {
    /// Record::decode over arbitrary (kind, payload) pairs: never panics;
    /// success implies the payload round-trips.
    #[test]
    fn record_decode_arbitrary(
        kind in any::<u8>(),
        payload in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        if let Ok(rec) = Record::decode(kind, &payload) {
            prop_assert_eq!(rec.kind(), kind);
            prop_assert_eq!(rec.encode(), payload);
        }
    }

    /// A committed record's payload, corrupted at an arbitrary byte,
    /// either fails checksum (journal) or fails decode — never a silent
    /// different record.
    #[test]
    fn journal_bitflip_is_never_silent(
        flip in 0usize..512,
    ) {
        let dir = tmp("bitflip");
        let w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        w.commit(&[Record::QueueDeclare(rusty_mq_storage::QueueRecord {
            name: "fuzz".into(),
            id: 1,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })])
        .unwrap();
        w.release_lock();
        drop(w);
        let seg = dir.join("00000000000000000001.log");
        let mut data = std::fs::read(&seg).unwrap();
        if flip < data.len() {
            data[flip] ^= 0xFF;
        }
        std::fs::write(&seg, &data).unwrap();
        // Outcomes: Err(checksum/corruption) or Ok with fewer/no records —
        // never a record set that silently differs from what was written.
        let result = recover(&dir);
        if let Ok(records) = result {
            for r in &records {
                let ok = matches!(
                    r.record,
                    Record::QueueDeclare(ref q) if q.name == "fuzz" && q.id == 1
                );
                prop_assert!(ok, "corruption produced alien record {:?}", r.record);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Truncation at any offset: the journal recovers a prefix of the
    /// committed set (the declare either survived whole or not at all).
    #[test]
    fn journal_truncation_recovers_prefix(
        cut in 0usize..512,
    ) {
        let dir = tmp("trunc");
        let w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        w.commit(&[Record::QueueDeclare(rusty_mq_storage::QueueRecord {
            name: "fuzz".into(),
            id: 1,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })])
        .unwrap();
        w.release_lock();
        drop(w);
        let seg = dir.join("00000000000000000001.log");
        let data = std::fs::read(&seg).unwrap();
        std::fs::write(&seg, &data[..cut.min(data.len())]).unwrap();
        let result = recover(&dir);
        if let Ok(records) = result {
            for r in &records {
                let ok = matches!(
                    r.record,
                    Record::QueueDeclare(ref q) if q.name == "fuzz" && q.id == 1
                );
                prop_assert!(ok);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Snapshot payloads with corrupted headers/counts: explicit error or
    /// empty — never a fabricated record stream.
    #[test]
    fn snapshot_corruption_never_fabricates(
        flip in 8usize..512,
        value in any::<u8>(),
    ) {
        let dir = tmp("snap");
        let snap_dir = write_snapshot(
            &dir,
            1,
            1,
            &[Record::Purge { queue: 1, seqs: vec![1, 2, 3] }],
        )
        .unwrap();
        let path = snap_dir.join("state.bin");
        let mut data = std::fs::read(&path).unwrap();
        if flip < data.len() {
            data[flip] = value;
        }
        std::fs::write(&path, &data).unwrap();
        match read_snapshot(&snap_dir) {
            Ok(snap) => {
                for rec in &snap.records {
                    let ok = matches!(rec, Record::Purge { queue: 1, .. });
                    prop_assert!(ok, "fabricated {:?}", rec);
                }
            }
            Err(FormatError::Checksum(_))
            | Err(FormatError::Corruption(_))
            | Err(FormatError::UnsupportedMajor(_)) => {}
            Err(other) => return Err(proptest::test_runner::TestCaseError::fail(
                format!("unexpected error class: {other}"),
            )),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Absurd count fields in record payloads must be rejected by bounds
    /// checks, not by attempting giant allocations (decode with a
    /// 4-billion-entry purge list).
    #[test]
    fn absurd_counts_rejected(
        count in 0xF000_0000u32..u32::MAX,
    ) {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u64.to_le_bytes()); // queue
        payload.extend_from_slice(&count.to_le_bytes()); // absurd count
        prop_assert!(Record::decode(rusty_mq_storage::record::kind::PURGE, &payload).is_err());
    }
}
