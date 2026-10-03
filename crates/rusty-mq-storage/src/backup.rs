//! Offline backup, verification, and restore (§9.10, T23).
//!
//! A backup is a complete copy of the recovery chain — MANIFEST (if
//! published), `snapshots/`, and `journal/` — taken while no writer holds
//! the data directory (enforced via the LOCK liveness probe). Restore
//! targets must be empty; a nonempty target is refused rather than merged
//! or overwritten (INV: never silently initialize over existing state).
//!
//! Verification replays the backup through the real recovery fold: a
//! backup that cannot be recovered is not a valid backup.

use std::fs;
use std::path::Path;

use crate::record::FormatError;

fn io_err(e: std::io::Error) -> FormatError {
    FormatError::Io(e.to_string())
}

/// Copy `from` into a fresh `to` (created), recursively, excluding the
/// LOCK file (session state, not part of the recovery chain).
fn copy_dir_excluding_lock(from: &Path, to: &Path) -> Result<(), FormatError> {
    fs::create_dir_all(to).map_err(io_err)?;
    for entry in fs::read_dir(from).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        if entry.file_name() == "LOCK" || entry.file_name() == "MANIFEST.tmp" {
            continue;
        }
        let target = to.join(entry.file_name());
        if entry.file_type().map_err(io_err)?.is_dir() {
            copy_dir_excluding_lock(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target).map_err(io_err)?;
        }
    }
    Ok(())
}

/// Create a backup of `data_dir` under `output` (created fresh; must not
/// exist or be empty). Refuses while a live writer holds the directory.
pub fn create(data_dir: &Path, output: &Path) -> Result<(), FormatError> {
    if crate::journal::writer_lock_alive(data_dir) {
        return Err(FormatError::Io(
            "data directory is held by a live writer; stop the broker first (§9.10 offline backup)"
                .into(),
        ));
    }
    if !data_dir.exists() {
        return Err(FormatError::Io(format!(
            "data directory {} does not exist",
            data_dir.display()
        )));
    }
    if output.exists() {
        return Err(FormatError::Io(format!(
            "backup output {} already exists; refusing to overwrite",
            output.display()
        )));
    }
    copy_dir_excluding_lock(data_dir, output)
}

/// Verify a backup: it must be a complete, consistent recovery root that
/// the real recovery fold can rebuild from. Returns the rebuilt summary on
/// success (records replayed + queue count as a sanity signal).
pub fn verify(backup: &Path) -> Result<VerifySummary, FormatError> {
    // The data-directory layout is flat: numbered `*.log` segments at the
    // root, `snapshots/`, `MANIFEST`. A backup must be that recovery root.
    let has_segments = fs::read_dir(backup)
        .map_err(io_err)?
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".log") && n.len() == 24)
        });
    if !has_segments && !backup.join("MANIFEST").exists() {
        return Err(FormatError::Io(
            "not a rusty-mq backup: no journal segments and no MANIFEST".into(),
        ));
    }
    let rebuilt = crate::rebuild::rebuild(backup, usize::MAX / 2)?;
    let vhost = rebuilt
        .topology
        .find_vhost("/")
        .expect("recovery always constructs the default vhost");
    let queues = rebuilt.topology.iter_queues().count();
    let _ = vhost;
    Ok(VerifySummary {
        replayed: rebuilt.replayed,
        queues,
    })
}

/// Restore a verified backup into `target`: the target must not exist or
/// be empty — restore never merges into or overwrites existing state, and
/// never targets a directory a live writer holds.
pub fn restore(backup: &Path, target: &Path) -> Result<(), FormatError> {
    // Verify FIRST: never restore garbage (T23).
    verify(backup)?;
    if target.exists() {
        let nonempty = fs::read_dir(target).map_err(io_err)?.next().is_some();
        if nonempty {
            return Err(FormatError::Io(format!(
                "restore target {} is not empty; refusing to overwrite (restore into an empty directory)",
                target.display()
            )));
        }
    }
    if crate::journal::writer_lock_alive(target) {
        return Err(FormatError::Io(
            "restore target is held by a live writer".into(),
        ));
    }
    copy_dir_excluding_lock(backup, target)
}

/// Result of a successful verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerifySummary {
    pub replayed: usize,
    pub queues: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{JournalConfig, JournalWriter};
    use crate::record::{Enqueue, QueueRecord, Record};

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-backup-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn seed(dir: &Path) {
        let w = JournalWriter::open(dir, JournalConfig::default()).unwrap();
        w.commit(&[Record::QueueDeclare(QueueRecord {
            name: "jobs".into(),
            id: 5,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })])
        .unwrap();
        w.commit(&[Record::Enqueue(Enqueue {
            message_id: 0,
            property_bytes: vec![],
            body: b"payload-one".to_vec(),
            exchange: "".into(),
            routing_key: "jobs".into(),
            persistent: true,
            destinations: vec![(5, 0)],
        })])
        .unwrap();
        // Release the lock and close (drop would too, but be explicit for
        // the offline contract).
        w.release_lock();
        drop(w);
    }

    #[test]
    fn backup_verify_restore_roundtrip() {
        let dir = tmp("src");
        seed(&dir);
        let backup = tmp("backup");
        create(&dir, &backup).unwrap();
        let summary = verify(&backup).unwrap();
        assert_eq!(summary.queues, 1);
        assert!(summary.replayed >= 2);

        // Restore into a fresh directory and recover the identical state.
        let restored = tmp("restored");
        restore(&backup, &restored).unwrap();
        let a = crate::rebuild::rebuild(&dir, usize::MAX / 2).unwrap();
        let mut b = crate::rebuild::rebuild(&restored, usize::MAX / 2).unwrap();
        assert_eq!(a.replayed, b.replayed);
        let vhost = b.topology.find_vhost("/").unwrap();
        let qid = b
            .topology
            .find_queue(vhost, "jobs")
            .expect("queue restored");
        let entry = b.store.pop_ready(qid).expect("message restored").message;
        assert_eq!(entry.body, b"payload-one".to_vec());

        // The restored directory is immediately usable by a writer.
        let w = JournalWriter::open(&restored, JournalConfig::default()).unwrap();
        w.commit(&[Record::QueueDelete { id: 5 }]).unwrap();
        drop(w);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
        let _ = fs::remove_dir_all(&restored);
    }

    #[test]
    fn live_writer_refuses_backup() {
        let dir = tmp("live");
        let _w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        let backup = tmp("live-backup");
        let err = create(&dir, &backup).unwrap_err();
        assert!(err.to_string().contains("live writer"));
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }

    #[test]
    fn stale_lock_is_tolerated() {
        let dir = tmp("stale");
        seed(&dir);
        // Simulate a crashed writer: stale LOCK pointing at a dead pid.
        fs::write(dir.join("LOCK"), "999999999").unwrap();
        let backup = tmp("stale-backup");
        create(&dir, &backup).unwrap(); // tolerated
        assert!(!backup.join("LOCK").exists(), "LOCK never enters backups");
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }

    #[test]
    fn nonempty_restore_target_refused() {
        let dir = tmp("src2");
        seed(&dir);
        let backup = tmp("backup2");
        create(&dir, &backup).unwrap();
        let target = tmp("target2");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("existing.txt"), b"x").unwrap();
        let err = restore(&backup, &target).unwrap_err();
        assert!(err.to_string().contains("not empty"));
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
        let _ = fs::remove_dir_all(&target);
    }

    #[test]
    fn tampered_backup_fails_verification() {
        let dir = tmp("src3");
        seed(&dir);
        let backup = tmp("backup3");
        create(&dir, &backup).unwrap();
        // Flip a byte inside the first journal segment payload (flat
        // layout: segments live at the recovery-root top level).
        let seg = backup.join("00000000000000000001.log");
        let mut data = fs::read(&seg).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&seg, data).unwrap();
        assert!(verify(&backup).is_err(), "corrupted backups do not verify");
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }
}
